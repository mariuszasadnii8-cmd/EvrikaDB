//! SSTable — a Sorted String Table, the immutable on-disk tier.
//!
//! A memtable is flushed into one SSTable file. The file has three regions:
//!
//! ```text
//! +================= data =================+
//! | record | record | record | ...        |   (sorted by key, see `record`)
//! +================ index =================+
//! | entry  | entry  | ...                  |   key -> (flag, data offset)
//! +================ footer ================+
//! | index_offset:u64 | count:u32 | magic:u32 |
//! +=======================================+
//! ```
//!
//! The index is loaded fully into memory when the SSTable is opened, so point
//! lookups are one `seek` + one `read` of the data record. Tombstones are
//! recorded in the index too, which lets `KEYS`/`SCAN` answer purely from
//! memory without touching the data region.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::record::{read_record, write_record, Value};

/// Magic number in the footer: ASCII "EVRS".
const MAGIC: u32 = 0x4556_5253;
/// Footer is `u64 + u32 + u32` = 16 bytes, always the last 16 bytes of file.
const FOOTER_LEN: u64 = 16;

/// One in-memory index slot: where to find a key and whether it's a tombstone.
#[derive(Debug, Clone)]
struct IndexEntry {
    is_tombstone: bool,
    offset: u64,
}

/// A read handle to an on-disk SSTable. Cheap to clone the path; the index is
/// held once and shared via the owning `Arc` in the engine.
pub struct SSTable {
    id: u64,
    path: PathBuf,
    index: BTreeMap<Vec<u8>, IndexEntry>,
}

impl SSTable {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Flush a sorted sequence of `(key, value)` pairs into a new SSTable file
    /// at `path`, then reopen it for reading. The iterator MUST yield keys in
    /// ascending order (a `BTreeMap` does this naturally).
    pub fn create<'a, I>(path: PathBuf, id: u64, entries: I) -> io::Result<SSTable>
    where
        I: Iterator<Item = (&'a Vec<u8>, &'a Value)>,
    {
        let file = File::create(&path)?;
        let mut w = BufWriter::new(file);
        let mut index: Vec<(Vec<u8>, IndexEntry)> = Vec::new();
        let mut offset: u64 = 0;

        // --- data region ---
        for (key, value) in entries {
            let entry = IndexEntry {
                is_tombstone: matches!(value, Value::Tombstone),
                offset,
            };
            let written = write_record(&mut w, key, value)?;
            offset += written as u64;
            index.push((key.clone(), entry));
        }

        // --- index region ---
        let index_offset = offset;
        let count = index.len() as u32;
        for (key, entry) in &index {
            w.write_all(&(key.len() as u32).to_le_bytes())?;
            w.write_all(key)?;
            w.write_all(&[entry.is_tombstone as u8])?;
            w.write_all(&entry.offset.to_le_bytes())?;
        }

        // --- footer ---
        w.write_all(&index_offset.to_le_bytes())?;
        w.write_all(&count.to_le_bytes())?;
        w.write_all(&MAGIC.to_le_bytes())?;
        w.flush()?;
        w.get_ref().sync_data()?;
        drop(w);

        SSTable::open(path, id)
    }

    /// Open an existing SSTable file and load its index into memory.
    pub fn open(path: PathBuf, id: u64) -> io::Result<SSTable> {
        let mut file = File::open(&path)?;
        let file_len = file.metadata()?.len();
        if file_len < FOOTER_LEN {
            return Err(corrupt(&path, "file shorter than footer"));
        }

        // Read footer.
        file.seek(SeekFrom::Start(file_len - FOOTER_LEN))?;
        let index_offset = read_u64(&mut file)?;
        let count = read_u32(&mut file)?;
        let magic = read_u32(&mut file)?;
        if magic != MAGIC {
            return Err(corrupt(&path, "bad magic number"));
        }

        // Read the index region.
        file.seek(SeekFrom::Start(index_offset))?;
        let mut reader = BufReader::new(file);
        let mut index = BTreeMap::new();
        for _ in 0..count {
            let key_len = read_u32(&mut reader)? as usize;
            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key)?;
            let mut flag = [0u8; 1];
            reader.read_exact(&mut flag)?;
            let offset = read_u64(&mut reader)?;
            index.insert(
                key,
                IndexEntry {
                    is_tombstone: flag[0] != 0,
                    offset,
                },
            );
        }

        Ok(SSTable { id, path, index })
    }

    /// Point lookup. Returns:
    /// * `Some(Value::Put(..))` — key lives here with a value,
    /// * `Some(Value::Tombstone)` — key was deleted in this table,
    /// * `None` — key is absent from this table (check older tables).
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Value>> {
        let entry = match self.index.get(key) {
            Some(e) => e,
            None => return Ok(None),
        };
        if entry.is_tombstone {
            return Ok(Some(Value::Tombstone));
        }
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(entry.offset))?;
        let mut reader = BufReader::new(file);
        match read_record(&mut reader)? {
            Some((_, value)) => Ok(Some(value)),
            None => Err(corrupt(&self.path, "index pointed past end of data")),
        }
    }

    /// All keys present in this table, with tombstone flags, from the in-memory
    /// index (no disk reads). Used to build the live key-set for KEYS/SCAN.
    pub fn key_flags(&self) -> impl Iterator<Item = (&Vec<u8>, bool)> {
        self.index.iter().map(|(k, e)| (k, e.is_tombstone))
    }

    /// Stream every record (in sorted order) by scanning the data region.
    /// Used by compaction to merge tables. Returns an owning vector to keep the
    /// borrow story simple.
    pub fn scan_all(&self) -> io::Result<Vec<(Vec<u8>, Value)>> {
        let mut file = File::open(&self.path)?;
        let index_offset = {
            let len = file.metadata()?.len();
            file.seek(SeekFrom::Start(len - FOOTER_LEN))?;
            read_u64(&mut file)?
        };
        file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(file).take(index_offset);
        let mut out = Vec::with_capacity(self.index.len());
        while let Some((k, v)) = read_record(&mut reader)? {
            out.push((k, v));
        }
        Ok(out)
    }
}

fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

fn read_u64<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn corrupt(path: &Path, msg: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("corrupt SSTable {}: {}", path.display(), msg),
    )
}

/// Path of the SSTable file for table `id`.
pub fn sstable_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("sst-{:020}.sst", id))
}

/// Discover existing SSTable files, returning `(id, path)` sorted ascending by
/// id (oldest first).
pub fn discover(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = name
            .strip_prefix("sst-")
            .and_then(|s| s.strip_suffix(".sst"))
            .and_then(|s| s.parse::<u64>().ok())
        {
            found.push((id, entry.path()));
        }
    }
    found.sort_by_key(|(id, _)| *id);
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("evrika-sst-test-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn build(dir: &Path, id: u64, pairs: &[(&[u8], Value)]) -> SSTable {
        let map: BTreeMap<Vec<u8>, Value> =
            pairs.iter().map(|(k, v)| (k.to_vec(), v.clone())).collect();
        SSTable::create(sstable_path(dir, id), id, map.iter()).unwrap()
    }

    #[test]
    fn get_put_and_tombstone() {
        let dir = tmp_dir("get");
        let sst = build(
            &dir,
            1,
            &[
                (b"alpha", Value::Put(b"1".to_vec())),
                (b"beta", Value::Tombstone),
            ],
        );
        assert_eq!(sst.get(b"alpha").unwrap(), Some(Value::Put(b"1".to_vec())));
        assert_eq!(sst.get(b"beta").unwrap(), Some(Value::Tombstone));
        assert_eq!(sst.get(b"missing").unwrap(), None);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn reopen_keeps_data() {
        let dir = tmp_dir("reopen");
        let path = sstable_path(&dir, 7);
        build(&dir, 7, &[(b"k", Value::Put(b"v".to_vec()))]);
        let reopened = SSTable::open(path, 7).unwrap();
        assert_eq!(reopened.get(b"k").unwrap(), Some(Value::Put(b"v".to_vec())));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn scan_all_returns_sorted_records() {
        let dir = tmp_dir("scan");
        let sst = build(
            &dir,
            1,
            &[
                (b"c", Value::Put(b"3".to_vec())),
                (b"a", Value::Put(b"1".to_vec())),
                (b"b", Value::Tombstone),
            ],
        );
        let all = sst.scan_all().unwrap();
        let keys: Vec<_> = all.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
