//! Integration-style tests for the engine, living inside the `storage` module
//! so they can poke at private internals (table counts, manual compaction).

use super::*;

/// A unique temp directory per test, cleaned up at the end.
struct TmpDir(PathBuf);

impl TmpDir {
    fn new(tag: &str) -> TmpDir {
        // No `Math.random`/time available constraints here — we're in normal
        // Rust, so use a per-test counter plus pid for uniqueness.
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "evrika-engine-{}-{}-{}",
            tag,
            std::process::id(),
            n
        ));
        let _ = std::fs::remove_dir_all(&dir);
        TmpDir(dir)
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn config(dir: &Path) -> Config {
    let mut c = Config::new(dir);
    c.memtable_threshold = 256; // tiny, so flushes happen quickly
    c.compaction_trigger = 3;
    c.sync_on_write = false; // faster tests
    c
}

#[test]
fn put_get_delete_roundtrip() {
    let tmp = TmpDir::new("roundtrip");
    let engine = Engine::open(config(&tmp.0)).unwrap();

    engine.put(b"key", b"value".to_vec()).unwrap();
    assert_eq!(engine.get(b"key").unwrap(), Some(b"value".to_vec()));
    assert!(engine.contains(b"key").unwrap());

    engine.delete(b"key").unwrap();
    assert_eq!(engine.get(b"key").unwrap(), None);
    assert!(!engine.contains(b"key").unwrap());
}

#[test]
fn overwrite_returns_latest() {
    let tmp = TmpDir::new("overwrite");
    let engine = Engine::open(config(&tmp.0)).unwrap();
    engine.put(b"k", b"v1".to_vec()).unwrap();
    engine.put(b"k", b"v2".to_vec()).unwrap();
    assert_eq!(engine.get(b"k").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn read_falls_through_to_sstable_after_flush() {
    let tmp = TmpDir::new("flush");
    let engine = Engine::open(config(&tmp.0)).unwrap();
    engine.put(b"persisted", b"on-disk".to_vec()).unwrap();
    engine.flush().unwrap();
    // Memtable is empty now; the value must come from the SSTable.
    assert!(engine.shared.memtable.read().unwrap().is_empty());
    assert_eq!(engine.shared.sstables.read().unwrap().len(), 1);
    assert_eq!(engine.get(b"persisted").unwrap(), Some(b"on-disk".to_vec()));
}

#[test]
fn tombstone_in_memtable_shadows_sstable() {
    let tmp = TmpDir::new("shadow");
    let engine = Engine::open(config(&tmp.0)).unwrap();
    engine.put(b"k", b"v".to_vec()).unwrap();
    engine.flush().unwrap(); // value now in SSTable
    engine.delete(b"k").unwrap(); // tombstone in memtable
    assert_eq!(engine.get(b"k").unwrap(), None);
}

#[test]
fn compaction_merges_tables_and_preserves_data() {
    let tmp = TmpDir::new("compact");
    let engine = Engine::open(config(&tmp.0)).unwrap();

    // Create several SSTables by flushing repeatedly.
    for i in 0..5 {
        engine
            .put(format!("key{}", i).as_bytes(), format!("val{}", i).into_bytes())
            .unwrap();
        engine.flush().unwrap();
    }
    // Overwrite one and delete another, then flush again.
    engine.put(b"key1", b"updated".to_vec()).unwrap();
    engine.delete(b"key2").unwrap();
    engine.flush().unwrap();

    assert!(engine.shared.sstables.read().unwrap().len() > engine.shared.config.compaction_trigger);

    // Run compaction deterministically.
    super::compaction::compact(&engine.shared).unwrap();
    assert_eq!(engine.shared.sstables.read().unwrap().len(), 1);

    // Data is intact after compaction.
    assert_eq!(engine.get(b"key0").unwrap(), Some(b"val0".to_vec()));
    assert_eq!(engine.get(b"key1").unwrap(), Some(b"updated".to_vec()));
    assert_eq!(engine.get(b"key2").unwrap(), None); // deleted
    assert_eq!(engine.get(b"key3").unwrap(), Some(b"val3".to_vec()));
}

#[test]
fn live_keys_reflects_puts_and_deletes() {
    let tmp = TmpDir::new("livekeys");
    let engine = Engine::open(config(&tmp.0)).unwrap();
    engine.put(b"a", b"1".to_vec()).unwrap();
    engine.put(b"b", b"2".to_vec()).unwrap();
    engine.flush().unwrap();
    engine.put(b"c", b"3".to_vec()).unwrap();
    engine.delete(b"a").unwrap();

    let mut keys = engine.live_keys().unwrap();
    keys.sort();
    assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);
}

#[test]
fn data_survives_reopen() {
    let tmp = TmpDir::new("reopen");
    {
        let engine = Engine::open(config(&tmp.0)).unwrap();
        engine.put(b"durable", b"yes".to_vec()).unwrap();
        engine.put(b"in-memtable", b"also-yes".to_vec()).unwrap();
        engine.flush().unwrap(); // durable -> SSTable
        engine.put(b"only-wal", b"recovered".to_vec()).unwrap();
        // Drop without flushing "only-wal": it must be recovered from the WAL.
    }
    {
        let engine = Engine::open(config(&tmp.0)).unwrap();
        assert_eq!(engine.get(b"durable").unwrap(), Some(b"yes".to_vec()));
        assert_eq!(engine.get(b"only-wal").unwrap(), Some(b"recovered".to_vec()));
    }
}

#[test]
fn concurrent_writers_and_readers() {
    use std::sync::Arc;
    let tmp = TmpDir::new("concurrent");
    let engine = Arc::new(Engine::open(config(&tmp.0)).unwrap());

    let mut handles = Vec::new();
    for t in 0..4 {
        let e = engine.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..100 {
                let key = format!("t{}-k{}", t, i);
                e.put(key.as_bytes(), b"x".to_vec()).unwrap();
                // Interleave reads of our own writes.
                assert_eq!(e.get(key.as_bytes()).unwrap(), Some(b"x".to_vec()));
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // All 400 keys must be present.
    for t in 0..4 {
        for i in 0..100 {
            let key = format!("t{}-k{}", t, i);
            assert_eq!(engine.get(key.as_bytes()).unwrap(), Some(b"x".to_vec()));
        }
    }
}
