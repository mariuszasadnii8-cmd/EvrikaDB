//! Write-ahead log (WAL).
//!
//! Every mutation is appended here *before* it touches the memtable, so an
//! unexpected crash can be recovered by replaying the log. Each active
//! memtable has exactly one WAL file (`wal-<seq>.log`); when the memtable is
//! flushed to an SSTable its WAL becomes redundant and is deleted.
//!
//! The file is just a back-to-back stream of [`record`] entries.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use super::record::{read_record, write_record, Value};

/// An append-only log file for one memtable generation.
pub struct Wal {
    path: PathBuf,
    seq: u64,
    writer: BufWriter<File>,
    /// Whether to `fsync` after every append (durable but slower).
    sync_on_write: bool,
}

impl Wal {
    /// Create (or truncate) the WAL for generation `seq` inside `dir`.
    pub fn create(dir: &Path, seq: u64, sync_on_write: bool) -> io::Result<Wal> {
        let path = wal_path(dir, seq);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        Ok(Wal {
            path,
            seq,
            writer: BufWriter::new(file),
            sync_on_write,
        })
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one mutation and (optionally) flush it durably to disk.
    pub fn append(&mut self, key: &[u8], value: &Value) -> io::Result<()> {
        write_record(&mut self.writer, key, value)?;
        self.writer.flush()?;
        if self.sync_on_write {
            self.writer.get_ref().sync_data()?;
        }
        Ok(())
    }

    /// Replay a WAL file from disk, invoking `apply` for each record in order.
    /// Returns the number of records replayed. A truncated trailing record
    /// (e.g. from a crash mid-write) stops replay cleanly rather than failing.
    pub fn replay<F: FnMut(Vec<u8>, Value)>(path: &Path, mut apply: F) -> io::Result<usize> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };
        let mut reader = BufReader::new(file);
        let mut count = 0;
        loop {
            match read_record(&mut reader) {
                Ok(Some((k, v))) => {
                    apply(k, v);
                    count += 1;
                }
                Ok(None) => break,
                // A partial record at the tail means the process died while
                // writing it; everything before it is still valid.
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }
        Ok(count)
    }

    /// Remove this WAL file from disk (after its memtable has been flushed).
    pub fn remove(path: &Path) -> io::Result<()> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// Path of the WAL file for generation `seq`.
pub fn wal_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("wal-{:020}.log", seq))
}

/// Scan `dir` for existing WAL files, returning their `(seq, path)` pairs
/// sorted ascending by sequence (i.e. oldest first — replay order).
pub fn discover(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(seq) = name
            .strip_prefix("wal-")
            .and_then(|s| s.strip_suffix(".log"))
            .and_then(|s| s.parse::<u64>().ok())
        {
            found.push((seq, entry.path()));
        }
    }
    found.sort_by_key(|(seq, _)| *seq);
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("evrika-wal-test-{}-{}", tag, std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn append_and_replay() {
        let dir = tmp_dir("replay");
        {
            let mut wal = Wal::create(&dir, 1, false).unwrap();
            wal.append(b"a", &Value::Put(b"1".to_vec())).unwrap();
            wal.append(b"b", &Value::Tombstone).unwrap();
        }
        let mut got = Vec::new();
        let n = Wal::replay(&wal_path(&dir, 1), |k, v| got.push((k, v))).unwrap();
        assert_eq!(n, 2);
        assert_eq!(got[0], (b"a".to_vec(), Value::Put(b"1".to_vec())));
        assert_eq!(got[1], (b"b".to_vec(), Value::Tombstone));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn discover_orders_by_seq() {
        let dir = tmp_dir("discover");
        Wal::create(&dir, 5, false).unwrap();
        Wal::create(&dir, 2, false).unwrap();
        Wal::create(&dir, 9, false).unwrap();
        let seqs: Vec<u64> = discover(&dir).unwrap().into_iter().map(|(s, _)| s).collect();
        assert_eq!(seqs, vec![2, 5, 9]);
        fs::remove_dir_all(&dir).unwrap();
    }
}
