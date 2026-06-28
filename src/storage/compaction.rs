//! The background thread that keeps the on-disk shape of the LSM-tree healthy.
//!
//! It wakes on a signal (a write crossed the memtable threshold) or on a short
//! timer, and performs two jobs:
//!
//! 1. **Flush** — if the active memtable is large enough, roll it into an
//!    SSTable (delegated to [`super::Shared::flush`]).
//! 2. **Compact** — if too many SSTables have accumulated, merge them all into
//!    a single fresh table, discarding overwritten values and tombstones.
//!
//! Being a child module of `storage`, it can reach the private internals of
//! [`Shared`] directly.

use std::collections::{BTreeMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use super::record::Value;
use super::sstable::{self, SSTable};
use super::Shared;

/// Poll interval used as an upper bound on how long the thread sleeps between
/// checks even if no signal arrives.
const TICK: Duration = Duration::from_millis(200);

/// Background thread entry point. Runs until the engine signals shutdown.
pub fn run(shared: Arc<Shared>) {
    loop {
        // Sleep until signalled or the tick elapses.
        {
            let guard = shared.bg_mutex.lock().unwrap();
            let _ = shared.bg_signal.wait_timeout(guard, TICK);
        }

        if shared.is_shutting_down() {
            // Best-effort final flush so a clean shutdown leaves no data only
            // in the (about to be discarded) memtable... it is still in the
            // WAL, but flushing keeps restarts fast.
            let _ = shared.flush();
            break;
        }

        if shared.memtable_over_threshold() {
            if let Err(e) = shared.flush() {
                eprintln!("[evrika] flush failed: {}", e);
            }
        }

        if shared.sstable_count() > shared.config().compaction_trigger {
            if let Err(e) = compact(&shared) {
                eprintln!("[evrika] compaction failed: {}", e);
            }
        }
    }
}

/// Merge every current SSTable into one. Newer tables win on key conflicts and
/// tombstones are dropped (after a full merge there is nothing older for them
/// to shadow).
///
/// Exposed to the rest of `storage` (incl. tests) via `pub(super)` so it can be
/// invoked deterministically rather than only through the timer thread.
pub(super) fn compact(shared: &Shared) -> io::Result<()> {
    let _c = shared.compaction_lock.lock().unwrap();

    // Snapshot the table list (newest first). Tables flushed *during* this
    // compaction are not in the snapshot and will be preserved.
    let snapshot: Vec<Arc<SSTable>> = shared.sstables.read().unwrap().clone();
    if snapshot.len() <= shared.config().compaction_trigger {
        return Ok(()); // someone else already handled it
    }

    // Merge oldest → newest so the latest value for each key wins.
    let mut merged: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
    for sst in snapshot.iter().rev() {
        for (k, v) in sst.scan_all()? {
            merged.insert(k, v);
        }
    }
    merged.retain(|_, v| !matches!(v, Value::Tombstone));

    let compacted: HashSet<u64> = snapshot.iter().map(|s| s.id()).collect();

    // Build the replacement table unless the merge collapsed to nothing.
    let new_table = if merged.is_empty() {
        None
    } else {
        let id = shared.next_sst_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let path = sstable::sstable_path(&shared.config().data_dir, id);
        Some(Arc::new(SSTable::create(path, id, merged.iter())?))
    };

    // Swap atomically: drop the compacted tables, keep any newer ones that
    // appeared meanwhile, and place the merged table at the oldest position.
    let removed_paths: Vec<PathBuf> = {
        let mut ssts = shared.sstables.write().unwrap();
        let mut removed = Vec::new();
        ssts.retain(|s| {
            if compacted.contains(&s.id()) {
                removed.push(s.path().to_path_buf());
                false
            } else {
                true
            }
        });
        if let Some(table) = new_table {
            ssts.push(table); // oldest (back of the newest-first vec)
        }
        shared.persist_manifest(&ssts)?;
        removed
    };

    // Now that the manifest no longer references them, the old files are safe
    // to delete.
    for path in removed_paths {
        let _ = std::fs::remove_file(path);
    }
    Ok(())
}
