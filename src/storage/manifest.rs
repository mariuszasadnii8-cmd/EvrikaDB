//! The MANIFEST: the small file that records the *order* of live SSTables and
//! the next table id to hand out.
//!
//! Recency cannot be inferred from the table id alone, because compaction
//! produces a brand-new (high-id) table that nonetheless holds *old* data.
//! So we persist the authoritative newest-first ordering here and rewrite it
//! atomically (temp file + rename) whenever the set of tables changes.
//!
//! Layout: `next_id:u64 | count:u32 | id_0:u64 | id_1:u64 | ...`
//! where `id_0` is the newest table.

use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const MANIFEST_NAME: &str = "MANIFEST";
const TMP_NAME: &str = "MANIFEST.tmp";

/// Decoded manifest contents.
pub struct Manifest {
    pub next_id: u64,
    /// SSTable ids, newest first.
    pub sstable_ids: Vec<u64>,
}

/// Full path of the manifest within `dir`.
pub fn manifest_path(dir: &Path) -> PathBuf {
    dir.join(MANIFEST_NAME)
}

/// Load the manifest, or `None` if it does not exist yet (fresh database).
pub fn load(dir: &Path) -> io::Result<Option<Manifest>> {
    let path = manifest_path(dir);
    let mut file = match fs::File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    if bytes.len() < 12 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest too short",
        ));
    }
    let next_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let count = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let mut ids = Vec::with_capacity(count);
    let mut pos = 12;
    for _ in 0..count {
        if pos + 8 > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "manifest truncated",
            ));
        }
        ids.push(u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()));
        pos += 8;
    }
    Ok(Some(Manifest {
        next_id,
        sstable_ids: ids,
    }))
}

/// Atomically write a new manifest. `sstable_ids` must be newest-first.
pub fn store(dir: &Path, next_id: u64, sstable_ids: &[u64]) -> io::Result<()> {
    let mut buf = Vec::with_capacity(12 + sstable_ids.len() * 8);
    buf.extend_from_slice(&next_id.to_le_bytes());
    buf.extend_from_slice(&(sstable_ids.len() as u32).to_le_bytes());
    for id in sstable_ids {
        buf.extend_from_slice(&id.to_le_bytes());
    }

    let tmp = dir.join(TMP_NAME);
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_data()?;
    }
    // Rename is atomic on the same filesystem, giving us crash safety: either
    // the old or the new manifest is fully present, never a torn one.
    fs::rename(&tmp, manifest_path(dir))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("evrika-man-test-{}-{}", tag, std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn missing_manifest_is_none() {
        let dir = tmp_dir("missing");
        assert!(load(&dir).unwrap().is_none());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn round_trip() {
        let dir = tmp_dir("rt");
        store(&dir, 42, &[9, 5, 2]).unwrap();
        let m = load(&dir).unwrap().unwrap();
        assert_eq!(m.next_id, 42);
        assert_eq!(m.sstable_ids, vec![9, 5, 2]);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn overwrite_is_atomic_replace() {
        let dir = tmp_dir("overwrite");
        store(&dir, 1, &[1]).unwrap();
        store(&dir, 2, &[3, 2, 1]).unwrap();
        let m = load(&dir).unwrap().unwrap();
        assert_eq!(m.next_id, 2);
        assert_eq!(m.sstable_ids, vec![3, 2, 1]);
        fs::remove_dir_all(&dir).unwrap();
    }
}
