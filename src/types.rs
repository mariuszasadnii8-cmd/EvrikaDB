//! Redis-level value types and how they are serialized into the raw byte
//! values the LSM engine stores.
//!
//! The engine itself is a dumb `bytes -> bytes` store. To support Redis data
//! structures (strings, lists, hashes) we serialize a [`StoredValue`] —
//! an optional expiry plus the typed payload — into those bytes by hand.
//!
//! On-disk layout of one stored value:
//!
//! ```text
//! version:u8=1 | has_expiry:u8 | [expire_at_ms:u64] | type:u8 | payload...
//! ```
//!
//! Payload per type:
//! * String: `len:u32 | bytes`
//! * List:   `count:u32 | (len:u32 | bytes) * count`
//! * Hash:   `count:u32 | (klen:u32 | k | vlen:u32 | v) * count`

use std::collections::{BTreeMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

const VERSION: u8 = 1;
const T_STRING: u8 = 1;
const T_LIST: u8 = 2;
const T_HASH: u8 = 3;

/// Current wall-clock time in milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A typed Redis value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisValue {
    Str(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Hash(BTreeMap<Vec<u8>, Vec<u8>>),
}

impl RedisValue {
    /// The Redis type name, as returned by the `TYPE` command.
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisValue::Str(_) => "string",
            RedisValue::List(_) => "list",
            RedisValue::Hash(_) => "hash",
        }
    }
}

/// A value together with its optional expiry, exactly as persisted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredValue {
    /// Absolute expiry time (unix ms). `None` means the key never expires.
    pub expire_at_ms: Option<u64>,
    pub value: RedisValue,
}

impl StoredValue {
    pub fn new(value: RedisValue) -> StoredValue {
        StoredValue {
            expire_at_ms: None,
            value,
        }
    }

    /// Whether this value is expired relative to `now_ms`.
    pub fn is_expired(&self, now: u64) -> bool {
        matches!(self.expire_at_ms, Some(t) if t <= now)
    }

    /// Remaining time-to-live in milliseconds, or `None` if no expiry set.
    pub fn ttl_ms(&self, now: u64) -> Option<i64> {
        self.expire_at_ms.map(|t| t as i64 - now as i64)
    }

    /// Serialize to engine bytes (the custom binary format).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.push(VERSION);
        match self.expire_at_ms {
            Some(t) => {
                buf.push(1);
                buf.extend_from_slice(&t.to_le_bytes());
            }
            None => buf.push(0),
        }
        match &self.value {
            RedisValue::Str(s) => {
                buf.push(T_STRING);
                put_bytes(&mut buf, s);
            }
            RedisValue::List(items) => {
                buf.push(T_LIST);
                buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
                for item in items {
                    put_bytes(&mut buf, item);
                }
            }
            RedisValue::Hash(map) => {
                buf.push(T_HASH);
                buf.extend_from_slice(&(map.len() as u32).to_le_bytes());
                for (k, v) in map {
                    put_bytes(&mut buf, k);
                    put_bytes(&mut buf, v);
                }
            }
        }
        buf
    }

    /// Deserialize from engine bytes. Returns `None` on a malformed buffer.
    pub fn decode(bytes: &[u8]) -> Option<StoredValue> {
        let mut c = Cursor { bytes, pos: 0 };
        if c.u8()? != VERSION {
            return None;
        }
        let expire_at_ms = match c.u8()? {
            0 => None,
            1 => Some(c.u64()?),
            _ => return None,
        };
        let value = match c.u8()? {
            T_STRING => RedisValue::Str(c.bytes()?.to_vec()),
            T_LIST => {
                let count = c.u32()? as usize;
                let mut items = VecDeque::with_capacity(count);
                for _ in 0..count {
                    items.push_back(c.bytes()?.to_vec());
                }
                RedisValue::List(items)
            }
            T_HASH => {
                let count = c.u32()? as usize;
                let mut map = BTreeMap::new();
                for _ in 0..count {
                    let k = c.bytes()?.to_vec();
                    let v = c.bytes()?.to_vec();
                    map.insert(k, v);
                }
                RedisValue::Hash(map)
            }
            _ => return None,
        };
        Some(StoredValue {
            expire_at_ms,
            value,
        })
    }
}

fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Minimal forward-only cursor for decoding. Every accessor returns `None` if
/// there are not enough bytes left, so a truncated buffer fails cleanly.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.pos + n > self.bytes.len() {
            return None;
        }
        let slice = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Some(slice)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Option<&'a [u8]> {
        let len = self.u32()? as usize;
        self.take(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(v: StoredValue) {
        let bytes = v.encode();
        let back = StoredValue::decode(&bytes).expect("decode");
        assert_eq!(v, back);
    }

    #[test]
    fn string_round_trip() {
        round_trip(StoredValue::new(RedisValue::Str(b"hello".to_vec())));
    }

    #[test]
    fn string_with_expiry_round_trip() {
        round_trip(StoredValue {
            expire_at_ms: Some(1_700_000_000_000),
            value: RedisValue::Str(b"x".to_vec()),
        });
    }

    #[test]
    fn list_round_trip() {
        let list: VecDeque<Vec<u8>> = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into();
        round_trip(StoredValue::new(RedisValue::List(list)));
    }

    #[test]
    fn hash_round_trip() {
        let mut map = BTreeMap::new();
        map.insert(b"field".to_vec(), b"value".to_vec());
        map.insert(b"f2".to_vec(), b"v2".to_vec());
        round_trip(StoredValue::new(RedisValue::Hash(map)));
    }

    #[test]
    fn truncated_buffer_fails_cleanly() {
        let bytes = StoredValue::new(RedisValue::Str(b"hello".to_vec())).encode();
        assert!(StoredValue::decode(&bytes[..bytes.len() - 2]).is_none());
        assert!(StoredValue::decode(&[]).is_none());
    }

    #[test]
    fn expiry_helpers() {
        let v = StoredValue {
            expire_at_ms: Some(1000),
            value: RedisValue::Str(b"x".to_vec()),
        };
        assert!(v.is_expired(1000));
        assert!(v.is_expired(2000));
        assert!(!v.is_expired(999));
        assert_eq!(v.ttl_ms(900), Some(100));
    }
}
