//! The command layer: it turns a parsed request into reads/writes against the
//! storage engine and produces a [`RespValue`] reply.
//!
//! [`Db`] wraps the raw byte-oriented [`Engine`] with:
//! * typed (de)serialization via [`StoredValue`],
//! * lazy expiry (an expired key is deleted on access and reported absent),
//! * **striped locks** so a read-modify-write command (e.g. `INCR`, `LPUSH`)
//!   is atomic for its key while different keys still run in parallel.

mod hashes;
mod keys;
mod lists;
mod strings;

use std::io;
use std::sync::Mutex;

use crate::resp::RespValue;
use crate::storage::Engine;
use crate::types::{now_ms, StoredValue};

/// Number of lock stripes. A power of two so we can mask instead of modulo.
const STRIPES: usize = 64;

/// A typed, concurrency-safe view over the storage engine.
pub struct Db {
    engine: Engine,
    locks: Vec<Mutex<()>>,
}

impl Db {
    pub fn new(engine: Engine) -> Db {
        Db {
            engine,
            locks: (0..STRIPES).map(|_| Mutex::new(())).collect(),
        }
    }

    /// Lock the stripe owning `key`. Held for the duration of a mutating
    /// command to make its read-modify-write atomic.
    fn lock_for(&self, key: &[u8]) -> std::sync::MutexGuard<'_, ()> {
        let h = fnv1a(key) as usize & (STRIPES - 1);
        self.locks[h].lock().unwrap()
    }

    /// Load and decode a key, applying lazy expiry. Returns `None` if the key
    /// is absent, expired, or holds an undecodable value.
    fn load(&self, key: &[u8]) -> io::Result<Option<StoredValue>> {
        let bytes = match self.engine.get(key)? {
            Some(b) => b,
            None => return Ok(None),
        };
        match StoredValue::decode(&bytes) {
            Some(sv) if sv.is_expired(now_ms()) => {
                let _ = self.engine.delete(key);
                Ok(None)
            }
            Some(sv) => Ok(Some(sv)),
            None => Ok(None),
        }
    }

    fn store(&self, key: &[u8], value: &StoredValue) -> io::Result<()> {
        self.engine.put(key, value.encode())
    }

    fn remove(&self, key: &[u8]) -> io::Result<bool> {
        let existed = self.load(key)?.is_some();
        if existed {
            self.engine.delete(key)?;
        }
        Ok(existed)
    }

    /// Every live, non-expired key.
    fn all_keys(&self) -> io::Result<Vec<Vec<u8>>> {
        let now = now_ms();
        let mut out = Vec::new();
        for key in self.engine.live_keys()? {
            if let Some(bytes) = self.engine.get(&key)? {
                if let Some(sv) = StoredValue::decode(&bytes) {
                    if !sv.is_expired(now) {
                        out.push(key);
                    }
                }
            }
        }
        Ok(out)
    }
}

/// Dispatch one command to its handler, returning the reply to send back.
pub fn dispatch(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("ERR empty command");
    }
    let name = args[0].to_ascii_uppercase();
    match name.as_slice() {
        // --- connection / server ---
        b"PING" => ping(args),
        b"ECHO" => echo(args),
        b"SELECT" | b"CLIENT" | b"COMMAND" | b"HELLO" => RespValue::Simple("OK".into()),
        b"INFO" => RespValue::bulk("# Server\r\nevrika_db:0.1.0\r\n"),
        b"DBSIZE" => map_io(db.all_keys().map(|k| RespValue::Integer(k.len() as i64))),
        b"FLUSHALL" | b"FLUSHDB" => flushall(db),

        // --- generic key commands ---
        b"DEL" => keys::del(db, args),
        b"EXISTS" => keys::exists(db, args),
        b"EXPIRE" => keys::expire(db, args, false),
        b"PEXPIRE" => keys::expire(db, args, true),
        b"TTL" => keys::ttl(db, args, false),
        b"PTTL" => keys::ttl(db, args, true),
        b"PERSIST" => keys::persist(db, args),
        b"TYPE" => keys::type_cmd(db, args),
        b"KEYS" => keys::keys(db, args),
        b"SCAN" => keys::scan(db, args),

        // --- strings ---
        b"SET" => strings::set(db, args),
        b"GET" => strings::get(db, args),
        b"GETSET" => strings::getset(db, args),
        b"SETEX" => strings::setex(db, args, false),
        b"PSETEX" => strings::setex(db, args, true),
        b"APPEND" => strings::append(db, args),
        b"STRLEN" => strings::strlen(db, args),
        b"INCR" => strings::incr_by(db, &args[..], 1),
        b"DECR" => strings::incr_by(db, &args[..], -1),
        b"INCRBY" => strings::incrby(db, args, 1),
        b"DECRBY" => strings::incrby(db, args, -1),

        // --- lists ---
        b"LPUSH" => lists::push(db, args, true),
        b"RPUSH" => lists::push(db, args, false),
        b"LPOP" => lists::pop(db, args, true),
        b"RPOP" => lists::pop(db, args, false),
        b"LLEN" => lists::llen(db, args),
        b"LRANGE" => lists::lrange(db, args),
        b"LINDEX" => lists::lindex(db, args),

        // --- hashes ---
        b"HSET" => hashes::hset(db, args),
        b"HGET" => hashes::hget(db, args),
        b"HDEL" => hashes::hdel(db, args),
        b"HEXISTS" => hashes::hexists(db, args),
        b"HLEN" => hashes::hlen(db, args),
        b"HKEYS" => hashes::hkeys(db, args),
        b"HVALS" => hashes::hvals(db, args),
        b"HGETALL" => hashes::hgetall(db, args),

        other => RespValue::error(format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(other)
        )),
    }
}

fn ping(args: &[Vec<u8>]) -> RespValue {
    match args.len() {
        1 => RespValue::Simple("PONG".into()),
        2 => RespValue::Bulk(args[1].clone()),
        _ => wrong_args("ping"),
    }
}

fn echo(args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return wrong_args("echo");
    }
    RespValue::Bulk(args[1].clone())
}

fn flushall(db: &Db) -> RespValue {
    match db.all_keys() {
        Ok(keys) => {
            for k in keys {
                let _ = db.engine.delete(&k);
            }
            RespValue::Simple("OK".into())
        }
        Err(e) => io_err(e),
    }
}

// --- small shared helpers used across the command modules ---

/// Standard wrong-arity error.
pub(crate) fn wrong_args(name: &str) -> RespValue {
    RespValue::error(format!(
        "ERR wrong number of arguments for '{}' command",
        name
    ))
}

/// Standard type-mismatch error.
pub(crate) fn wrong_type() -> RespValue {
    RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

/// Turn an engine I/O error into a RESP error reply.
pub(crate) fn io_err(e: io::Error) -> RespValue {
    RespValue::error(format!("ERR {}", e))
}

/// Convert an `io::Result<RespValue>` into a reply, mapping errors.
pub(crate) fn map_io(r: io::Result<RespValue>) -> RespValue {
    r.unwrap_or_else(io_err)
}

/// FNV-1a hash, used only to pick a lock stripe.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests;
