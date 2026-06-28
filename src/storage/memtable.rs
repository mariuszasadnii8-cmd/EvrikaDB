//! The memtable: the in-memory, write-buffer tier of the LSM-tree.
//!
//! It is an ordered `BTreeMap` so that flushing to an SSTable produces a
//! sorted run for free. Keys map to a [`Value`] (a put or a tombstone); a
//! tombstone is kept in memory so it can shadow the same key living in an
//! older on-disk SSTable.

use std::collections::BTreeMap;

use super::record::Value;

/// An ordered, in-memory collection of the most recent writes.
#[derive(Default)]
pub struct MemTable {
    map: BTreeMap<Vec<u8>, Value>,
    /// Rough heap footprint (keys + values), used to trigger a flush.
    size_bytes: usize,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable::default()
    }

    /// Insert or overwrite `key`, keeping `size_bytes` in sync incrementally
    /// (O(log n), no full recount).
    pub fn insert(&mut self, key: Vec<u8>, value: Value) {
        let key_len = key.len();
        let new_payload = value.heap_size();
        match self.map.insert(key, value) {
            Some(old) => {
                // Key already present, so only the value payload changes.
                self.size_bytes = self.size_bytes + new_payload - old.heap_size();
            }
            None => {
                self.size_bytes += key_len + new_payload;
            }
        }
    }

    /// Look up `key`. Returns the stored [`Value`] (which may be a tombstone).
    pub fn get(&self, key: &[u8]) -> Option<&Value> {
        self.map.get(key)
    }

    /// Total approximate byte footprint.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Number of entries (puts + tombstones).
    #[allow(dead_code)] // part of the natural API; used in tests/inspection
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Iterate entries in sorted key order — used when flushing to an SSTable.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Value)> {
        self.map.iter()
    }

    /// Take ownership of the underlying map, leaving an empty memtable behind.
    /// Used by the flush path to atomically rotate the active memtable.
    pub fn take(&mut self) -> BTreeMap<Vec<u8>, Value> {
        self.size_bytes = 0;
        std::mem::take(&mut self.map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut mt = MemTable::new();
        mt.insert(b"a".to_vec(), Value::Put(b"1".to_vec()));
        assert_eq!(mt.get(b"a"), Some(&Value::Put(b"1".to_vec())));
        assert_eq!(mt.get(b"missing"), None);
    }

    #[test]
    fn overwrite_updates_size() {
        let mut mt = MemTable::new();
        mt.insert(b"k".to_vec(), Value::Put(b"short".to_vec()));
        let s1 = mt.size_bytes();
        mt.insert(b"k".to_vec(), Value::Put(b"a-much-longer-value".to_vec()));
        assert!(mt.size_bytes() > s1);
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn take_resets() {
        let mut mt = MemTable::new();
        mt.insert(b"a".to_vec(), Value::Put(b"1".to_vec()));
        let drained = mt.take();
        assert_eq!(drained.len(), 1);
        assert!(mt.is_empty());
        assert_eq!(mt.size_bytes(), 0);
    }

    #[test]
    fn iter_is_sorted() {
        let mut mt = MemTable::new();
        mt.insert(b"c".to_vec(), Value::Put(b"3".to_vec()));
        mt.insert(b"a".to_vec(), Value::Put(b"1".to_vec()));
        mt.insert(b"b".to_vec(), Value::Tombstone);
        let keys: Vec<_> = mt.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }
}
