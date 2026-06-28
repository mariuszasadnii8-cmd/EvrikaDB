//! The LSM-tree storage engine.
//!
//! ```text
//!            writes                                  reads
//!              │                                       │
//!              ▼                                       ▼
//!   ┌─────────────────────┐   over threshold   check newest → oldest:
//!   │  WAL (append+fsync)  │ ────────┐          memtable ▸ flushing ▸ SSTs
//!   └─────────────────────┘         │
//!              │                     ▼
//!              ▼              ┌──────────────┐ flush  ┌───────────────────┐
//!   ┌─────────────────────┐  │  immutable   │ ─────▶ │  SSTable (sst-N)   │
//!   │  memtable (BTreeMap) │─▶│  "flushing"  │        └───────────────────┘
//!   └─────────────────────┘  └──────────────┘                 │
//!                                                     background compaction
//!                                                       merges many → one
//! ```
//!
//! Concurrency model (the heart of the exercise):
//!
//! * `wal: Mutex<Wal>` doubles as the **writer serializer** — holding it while
//!   appending guarantees the WAL order matches the memtable insertion order.
//! * `memtable: RwLock<MemTable>` lets many readers run concurrently; writers
//!   only hold the write lock for the brief in-memory insert (the slow fsync
//!   happens under the WAL lock, not the memtable lock).
//! * `sstables: RwLock<Vec<Arc<SSTable>>>` is read-mostly; flush/compaction
//!   swap it under the write lock. `Arc` lets a reader keep using a table even
//!   if compaction removes it from the list.
//! * a dedicated background thread performs compaction.

mod compaction;
mod manifest;
mod memtable;
mod record;
mod sstable;
mod wal;

pub use record::Value;

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;

use memtable::MemTable;
use sstable::SSTable;
use wal::Wal;

/// Tunable engine parameters.
#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: PathBuf,
    /// Flush the memtable to an SSTable once it grows past this many bytes.
    pub memtable_threshold: usize,
    /// Compact when the number of live SSTables exceeds this.
    pub compaction_trigger: usize,
    /// `fsync` the WAL after every write (durable but slower).
    pub sync_on_write: bool,
}

impl Config {
    pub fn new<P: Into<PathBuf>>(data_dir: P) -> Config {
        Config {
            data_dir: data_dir.into(),
            memtable_threshold: 1 << 20, // 1 MiB
            compaction_trigger: 4,
            sync_on_write: true,
        }
    }
}

/// Shared, thread-safe engine state, always held behind an `Arc`.
struct Shared {
    config: Config,
    /// Active, mutable write buffer.
    memtable: RwLock<MemTable>,
    /// Memtables that have been rotated out and are being flushed to disk;
    /// still readable. Oldest first.
    flushing: RwLock<Vec<Arc<BTreeMap<Vec<u8>, Value>>>>,
    /// On-disk sorted runs, newest first.
    sstables: RwLock<Vec<Arc<SSTable>>>,
    /// WAL for the active memtable. Also serializes all writers.
    wal: Mutex<Wal>,
    /// Ensures only one flush happens at a time.
    flush_lock: Mutex<()>,
    /// Ensures only one compaction happens at a time.
    compaction_lock: Mutex<()>,
    /// Monotonic id source for new SSTables.
    next_sst_id: AtomicU64,
    /// Set when the engine is shutting down (stops the background thread).
    shutdown: AtomicBool,
    /// Wakes the background thread when there may be work to do.
    bg_signal: Condvar,
    bg_mutex: Mutex<()>,
}

/// Public handle to the storage engine. Cloning shares the same underlying
/// state (it is just an `Arc`).
#[derive(Clone)]
pub struct Engine {
    shared: Arc<Shared>,
    bg: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Engine {
    /// Open (or create) a database in `config.data_dir`, recovering any state
    /// left by a previous run and starting the background compaction thread.
    pub fn open(config: Config) -> io::Result<Engine> {
        std::fs::create_dir_all(&config.data_dir)?;

        // --- recover SSTables from the manifest (authoritative ordering) ---
        let (mut sstables, next_id_from_manifest) = recover_sstables(&config.data_dir)?;

        // --- recover the memtable by replaying all WAL segments ---
        let mut memtable = MemTable::new();
        let wal_segments = wal::discover(&config.data_dir)?;
        let mut max_wal_seq = 0;
        for (seq, path) in &wal_segments {
            max_wal_seq = max_wal_seq.max(*seq);
            Wal::replay(path, |k, v| memtable.insert(k, v))?;
        }

        // Active WAL is the next sequence after whatever we found.
        let active_seq = max_wal_seq + 1;
        let active_wal = Wal::create(&config.data_dir, active_seq, config.sync_on_write)?;
        // The replayed segments are now folded into the active memtable; drop
        // the old segment files so they are not replayed twice next time.
        for (seq, path) in &wal_segments {
            if *seq != active_seq {
                Wal::remove(path)?;
            }
        }

        // Reverse so the vec is newest-first (recover returns oldest-first).
        sstables.reverse();
        let next_sst_id = next_id_from_manifest
            .max(sstables.iter().map(|s| s.id() + 1).max().unwrap_or(0));

        let shared = Arc::new(Shared {
            config,
            memtable: RwLock::new(memtable),
            flushing: RwLock::new(Vec::new()),
            sstables: RwLock::new(sstables),
            wal: Mutex::new(active_wal),
            flush_lock: Mutex::new(()),
            compaction_lock: Mutex::new(()),
            next_sst_id: AtomicU64::new(next_sst_id),
            shutdown: AtomicBool::new(false),
            bg_signal: Condvar::new(),
            bg_mutex: Mutex::new(()),
        });

        // Persist a fresh manifest reflecting the recovered state.
        shared.persist_manifest(&shared.sstables.read().unwrap())?;

        let engine = Engine {
            shared: shared.clone(),
            bg: Arc::new(Mutex::new(None)),
        };
        engine.start_background();
        Ok(engine)
    }

    /// Store `value` under `key`.
    pub fn put(&self, key: &[u8], value: Vec<u8>) -> io::Result<()> {
        self.write(key, Value::Put(value))
    }

    /// Delete `key` (writes a tombstone).
    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        self.write(key, Value::Tombstone)
    }

    fn write(&self, key: &[u8], value: Value) -> io::Result<()> {
        let s = &self.shared;
        // Hold the WAL lock across both the durable append and the memtable
        // insert so the two can never disagree on ordering.
        let size = {
            let mut wal = s.wal.lock().unwrap();
            wal.append(key, &value)?;
            let mut mt = s.memtable.write().unwrap();
            mt.insert(key.to_vec(), value);
            mt.size_bytes()
        };
        if size >= s.config.memtable_threshold {
            // Wake the background thread to flush; if it is busy, the writer
            // path stays responsive (no blocking on disk here).
            s.signal_background();
        }
        Ok(())
    }

    /// Look up `key`, returning its value if present and not deleted.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let s = &self.shared;

        // 1. active memtable
        if let Some(v) = s.memtable.read().unwrap().get(key) {
            return Ok(resolve(v));
        }
        // 2. memtables currently being flushed, newest first
        {
            let flushing = s.flushing.read().unwrap();
            for mt in flushing.iter().rev() {
                if let Some(v) = mt.get(key) {
                    return Ok(resolve(v));
                }
            }
        }
        // 3. on-disk SSTables, newest first
        let snapshot: Vec<Arc<SSTable>> = s.sstables.read().unwrap().clone();
        for sst in &snapshot {
            if let Some(v) = sst.get(key)? {
                return Ok(resolve(&v));
            }
        }
        Ok(None)
    }

    /// Whether `key` currently exists.
    pub fn contains(&self, key: &[u8]) -> io::Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Collect every live (non-deleted) key. Used by `KEYS` / `SCAN`.
    ///
    /// Builds the merged view oldest → newest so that the most recent write
    /// for each key wins, then keeps only keys whose latest state is a put.
    pub fn live_keys(&self) -> io::Result<Vec<Vec<u8>>> {
        let s = &self.shared;
        let mut state: BTreeMap<Vec<u8>, bool> = BTreeMap::new(); // key -> is_tombstone

        // oldest → newest: SSTables (oldest last in the newest-first vec)
        {
            let ssts = s.sstables.read().unwrap();
            for sst in ssts.iter().rev() {
                for (key, is_tomb) in sst.key_flags() {
                    state.insert(key.clone(), is_tomb);
                }
            }
        }
        // then flushing memtables (oldest first)
        {
            let flushing = s.flushing.read().unwrap();
            for mt in flushing.iter() {
                for (key, value) in mt.iter() {
                    state.insert(key.clone(), matches!(value, Value::Tombstone));
                }
            }
        }
        // finally the active memtable (newest)
        {
            let mt = s.memtable.read().unwrap();
            for (key, value) in mt.iter() {
                state.insert(key.clone(), matches!(value, Value::Tombstone));
            }
        }

        Ok(state
            .into_iter()
            .filter(|(_, is_tomb)| !*is_tomb)
            .map(|(k, _)| k)
            .collect())
    }

    /// Force the active memtable to flush to an SSTable (used by tests and by
    /// the background thread). Safe to call when there is nothing to flush.
    pub fn flush(&self) -> io::Result<()> {
        self.shared.flush()
    }

    fn start_background(&self) {
        let shared = self.shared.clone();
        let handle = std::thread::Builder::new()
            .name("evrika-compaction".into())
            .spawn(move || compaction::run(shared))
            .expect("spawn background thread");
        *self.bg.lock().unwrap() = Some(handle);
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // Only the last Engine handle (which uniquely owns `bg`) shuts down.
        let mut guard = match self.bg.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Some(handle) = guard.take() {
            self.shared.shutdown.store(true, Ordering::SeqCst);
            self.shared.signal_background();
            let _ = handle.join();
        }
    }
}

impl Shared {
    /// Wake the background thread.
    fn signal_background(&self) {
        let _g = self.bg_mutex.lock().unwrap();
        self.bg_signal.notify_all();
    }

    /// Rotate the active memtable into an immutable one, write it out as a new
    /// SSTable, register it and drop the now-redundant WAL segment.
    fn flush(&self) -> io::Result<()> {
        let _flush = self.flush_lock.lock().unwrap();

        // --- atomically rotate the memtable and the WAL ---
        let (drained, old_wal_path) = {
            let mut wal = self.wal.lock().unwrap();
            let mut mt = self.memtable.write().unwrap();
            if mt.is_empty() {
                return Ok(());
            }
            let drained = mt.take();

            let old_wal_path = wal.path().to_path_buf();
            let new_wal = Wal::create(&self.config.data_dir, wal.seq() + 1, self.config.sync_on_write)?;
            *wal = new_wal;
            (drained, old_wal_path)
        };

        // Keep the rotated data readable while we write it to disk.
        let drained = Arc::new(drained);
        {
            self.flushing.write().unwrap().push(drained.clone());
        }

        // --- write the SSTable (no memtable/WAL locks held) ---
        let id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let path = sstable::sstable_path(&self.config.data_dir, id);
        let sst = match SSTable::create(path, id, drained.iter()) {
            Ok(sst) => Arc::new(sst),
            Err(e) => {
                // Roll back the flushing entry so the data is not lost from the
                // read path (it is still in the WAL we have not deleted).
                self.flushing
                    .write()
                    .unwrap()
                    .retain(|m| !Arc::ptr_eq(m, &drained));
                return Err(e);
            }
        };

        // --- publish: register table, persist manifest, drop flushing entry ---
        {
            let mut ssts = self.sstables.write().unwrap();
            ssts.insert(0, sst); // newest first
            self.persist_manifest(&ssts)?;
        }
        self.flushing
            .write()
            .unwrap()
            .retain(|m| !Arc::ptr_eq(m, &drained));

        // The data is durably in the SSTable now; the WAL segment is redundant.
        Wal::remove(&old_wal_path)?;
        Ok(())
    }

    /// Write the manifest from the current (newest-first) SSTable list. Caller
    /// must hold the `sstables` write lock to keep file and memory consistent.
    fn persist_manifest(&self, ssts: &[Arc<SSTable>]) -> io::Result<()> {
        let ids: Vec<u64> = ssts.iter().map(|s| s.id()).collect();
        let next_id = self.next_sst_id.load(Ordering::SeqCst);
        manifest::store(&self.config.data_dir, next_id, &ids)
    }
}

/// Map a stored [`Value`] to the user-visible read result.
fn resolve(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::Put(bytes) => Some(bytes.clone()),
        Value::Tombstone => None,
    }
}

/// Recover the SSTable set on startup. Returns the tables **oldest-first** and
/// the `next_id` to use. Prefers the manifest; falls back to scanning the
/// directory if no manifest exists (e.g. a database created by an older build).
fn recover_sstables(dir: &Path) -> io::Result<(Vec<Arc<SSTable>>, u64)> {
    match manifest::load(dir)? {
        Some(m) => {
            // Manifest is newest-first; open in that order then reverse to
            // oldest-first for the caller.
            let mut tables = Vec::new();
            for id in &m.sstable_ids {
                let path = sstable::sstable_path(dir, *id);
                tables.push(Arc::new(SSTable::open(path, *id)?));
            }
            tables.reverse();

            // Delete any orphan .sst files not referenced by the manifest
            // (e.g. left behind by a crash mid-compaction).
            let live: std::collections::HashSet<u64> = m.sstable_ids.iter().copied().collect();
            for (id, path) in sstable::discover(dir)? {
                if !live.contains(&id) {
                    let _ = std::fs::remove_file(path);
                }
            }
            Ok((tables, m.next_id))
        }
        None => {
            // No manifest: trust file ids for ordering (oldest-first).
            let mut tables = Vec::new();
            let mut next_id = 0;
            for (id, path) in sstable::discover(dir)? {
                next_id = next_id.max(id + 1);
                tables.push(Arc::new(SSTable::open(path, id)?));
            }
            Ok((tables, next_id))
        }
    }
}

// Internal hooks used by the compaction module.
impl Shared {
    fn config(&self) -> &Config {
        &self.config
    }
    fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }
    fn memtable_over_threshold(&self) -> bool {
        self.memtable.read().unwrap().size_bytes() >= self.config.memtable_threshold
    }
    fn sstable_count(&self) -> usize {
        self.sstables.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests;
