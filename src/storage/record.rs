//! The fundamental on-disk unit shared by the write-ahead log and the data
//! section of every SSTable: a single key together with either a value (a
//! *put*) or a deletion marker (a *tombstone*).
//!
//! Wire layout of one record:
//!
//! ```text
//! +------+-----------+----------+-----------+------------+
//! | flag | key_len   | key ...  | val_len   | value ...  |
//! | u8   | u32 (LE)  | bytes    | u32 (LE)  | bytes      |
//! +------+-----------+----------+-----------+------------+
//!         flag = 0 -> put (val_len + value present)
//!         flag = 1 -> tombstone (val_len + value omitted)
//! ```
//!
//! This is the "custom serialization" layer — everything is hand-rolled
//! little-endian, no serde involved.

use std::io::{self, Read, Write};

const FLAG_PUT: u8 = 0;
const FLAG_TOMBSTONE: u8 = 1;

/// A value as understood by the storage engine: present bytes, or a tombstone
/// recording that the key was deleted (so it can shadow older SSTables).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Put(Vec<u8>),
    Tombstone,
}

impl Value {
    /// Number of bytes this value contributes to the in-memory memtable size
    /// accounting (used to decide when to flush).
    pub fn heap_size(&self) -> usize {
        match self {
            Value::Put(v) => v.len(),
            Value::Tombstone => 0,
        }
    }
}

/// Serialize `(key, value)` as one record onto `w`. Returns the number of
/// bytes written.
pub fn write_record<W: Write>(w: &mut W, key: &[u8], value: &Value) -> io::Result<usize> {
    let mut written = 0;
    match value {
        Value::Put(val) => {
            w.write_all(&[FLAG_PUT])?;
            w.write_all(&(key.len() as u32).to_le_bytes())?;
            w.write_all(key)?;
            w.write_all(&(val.len() as u32).to_le_bytes())?;
            w.write_all(val)?;
            written += 1 + 4 + key.len() + 4 + val.len();
        }
        Value::Tombstone => {
            w.write_all(&[FLAG_TOMBSTONE])?;
            w.write_all(&(key.len() as u32).to_le_bytes())?;
            w.write_all(key)?;
            written += 1 + 4 + key.len();
        }
    }
    Ok(written)
}

/// Read one record from `r`. Returns `Ok(None)` on a clean EOF at a record
/// boundary (used to detect the end of a WAL / SSTable data section).
pub fn read_record<R: Read>(r: &mut R) -> io::Result<Option<(Vec<u8>, Value)>> {
    let mut flag = [0u8; 1];
    match r.read_exact(&mut flag) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    let key_len = read_u32(r)? as usize;
    let mut key = vec![0u8; key_len];
    r.read_exact(&mut key)?;

    match flag[0] {
        FLAG_PUT => {
            let val_len = read_u32(r)? as usize;
            let mut val = vec![0u8; val_len];
            r.read_exact(&mut val)?;
            Ok(Some((key, Value::Put(val))))
        }
        FLAG_TOMBSTONE => Ok(Some((key, Value::Tombstone))),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid record flag: {}", other),
        )),
    }
}

fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn put_round_trip() {
        let mut buf = Vec::new();
        write_record(&mut buf, b"key", &Value::Put(b"value".to_vec())).unwrap();
        let mut cur = Cursor::new(buf);
        let (k, v) = read_record(&mut cur).unwrap().unwrap();
        assert_eq!(k, b"key");
        assert_eq!(v, Value::Put(b"value".to_vec()));
        assert!(read_record(&mut cur).unwrap().is_none());
    }

    #[test]
    fn tombstone_round_trip() {
        let mut buf = Vec::new();
        write_record(&mut buf, b"gone", &Value::Tombstone).unwrap();
        let mut cur = Cursor::new(buf);
        let (k, v) = read_record(&mut cur).unwrap().unwrap();
        assert_eq!(k, b"gone");
        assert_eq!(v, Value::Tombstone);
    }

    #[test]
    fn two_records_sequential() {
        let mut buf = Vec::new();
        write_record(&mut buf, b"a", &Value::Put(b"1".to_vec())).unwrap();
        write_record(&mut buf, b"b", &Value::Tombstone).unwrap();
        let mut cur = Cursor::new(buf);
        assert_eq!(read_record(&mut cur).unwrap().unwrap().0, b"a");
        assert_eq!(read_record(&mut cur).unwrap().unwrap().0, b"b");
        assert!(read_record(&mut cur).unwrap().is_none());
    }
}
