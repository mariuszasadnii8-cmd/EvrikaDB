//! End-to-end command tests driving [`dispatch`] against a real engine.

use super::*;
use crate::storage::{Config, Engine};

struct Fixture {
    db: Db,
    dir: std::path::PathBuf,
}

impl Fixture {
    fn new(tag: &str) -> Fixture {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "evrika-cmd-{}-{}-{}",
            tag,
            std::process::id(),
            n
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut cfg = Config::new(&dir);
        cfg.sync_on_write = false;
        let engine = Engine::open(cfg).unwrap();
        Fixture {
            db: Db::new(engine),
            dir,
        }
    }

    /// Run a command given as string tokens.
    fn run(&self, parts: &[&str]) -> RespValue {
        let args: Vec<Vec<u8>> = parts.iter().map(|p| p.as_bytes().to_vec()).collect();
        dispatch(&self.db, &args)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn ping_and_echo() {
    let f = Fixture::new("ping");
    assert_eq!(f.run(&["PING"]), RespValue::Simple("PONG".into()));
    assert_eq!(f.run(&["PING", "hi"]), RespValue::Bulk(b"hi".to_vec()));
    assert_eq!(f.run(&["ECHO", "yo"]), RespValue::Bulk(b"yo".to_vec()));
}

#[test]
fn set_get_del() {
    let f = Fixture::new("setget");
    assert_eq!(f.run(&["SET", "k", "v"]), RespValue::Simple("OK".into()));
    assert_eq!(f.run(&["GET", "k"]), RespValue::Bulk(b"v".to_vec()));
    assert_eq!(f.run(&["EXISTS", "k"]), RespValue::Integer(1));
    assert_eq!(f.run(&["DEL", "k"]), RespValue::Integer(1));
    assert_eq!(f.run(&["GET", "k"]), RespValue::Null);
}

#[test]
fn set_nx_xx() {
    let f = Fixture::new("nxxx");
    assert_eq!(f.run(&["SET", "k", "v1", "NX"]), RespValue::Simple("OK".into()));
    assert_eq!(f.run(&["SET", "k", "v2", "NX"]), RespValue::Null);
    assert_eq!(f.run(&["GET", "k"]), RespValue::Bulk(b"v1".to_vec()));
    assert_eq!(f.run(&["SET", "k", "v3", "XX"]), RespValue::Simple("OK".into()));
    assert_eq!(f.run(&["GET", "k"]), RespValue::Bulk(b"v3".to_vec()));
    assert_eq!(f.run(&["SET", "missing", "v", "XX"]), RespValue::Null);
}

#[test]
fn incr_decr() {
    let f = Fixture::new("incr");
    assert_eq!(f.run(&["INCR", "n"]), RespValue::Integer(1));
    assert_eq!(f.run(&["INCR", "n"]), RespValue::Integer(2));
    assert_eq!(f.run(&["INCRBY", "n", "10"]), RespValue::Integer(12));
    assert_eq!(f.run(&["DECR", "n"]), RespValue::Integer(11));
    assert_eq!(f.run(&["DECRBY", "n", "5"]), RespValue::Integer(6));
    f.run(&["SET", "s", "notanumber"]);
    assert!(matches!(f.run(&["INCR", "s"]), RespValue::Error(_)));
}

#[test]
fn wrong_type_is_reported() {
    let f = Fixture::new("wrongtype");
    f.run(&["SET", "k", "v"]);
    assert!(matches!(f.run(&["LPUSH", "k", "x"]), RespValue::Error(_)));
    assert!(matches!(f.run(&["HSET", "k", "f", "v"]), RespValue::Error(_)));
}

#[test]
fn list_ops() {
    let f = Fixture::new("list");
    assert_eq!(f.run(&["RPUSH", "l", "a", "b", "c"]), RespValue::Integer(3));
    assert_eq!(f.run(&["LPUSH", "l", "z"]), RespValue::Integer(4));
    assert_eq!(f.run(&["LLEN", "l"]), RespValue::Integer(4));
    assert_eq!(
        f.run(&["LRANGE", "l", "0", "-1"]),
        RespValue::Array(vec![
            RespValue::Bulk(b"z".to_vec()),
            RespValue::Bulk(b"a".to_vec()),
            RespValue::Bulk(b"b".to_vec()),
            RespValue::Bulk(b"c".to_vec()),
        ])
    );
    assert_eq!(f.run(&["LINDEX", "l", "0"]), RespValue::Bulk(b"z".to_vec()));
    assert_eq!(f.run(&["LPOP", "l"]), RespValue::Bulk(b"z".to_vec()));
    assert_eq!(f.run(&["RPOP", "l"]), RespValue::Bulk(b"c".to_vec()));
    assert_eq!(f.run(&["LLEN", "l"]), RespValue::Integer(2));
}

#[test]
fn hash_ops() {
    let f = Fixture::new("hash");
    assert_eq!(f.run(&["HSET", "h", "f1", "v1", "f2", "v2"]), RespValue::Integer(2));
    assert_eq!(f.run(&["HSET", "h", "f1", "updated"]), RespValue::Integer(0));
    assert_eq!(f.run(&["HGET", "h", "f1"]), RespValue::Bulk(b"updated".to_vec()));
    assert_eq!(f.run(&["HLEN", "h"]), RespValue::Integer(2));
    assert_eq!(f.run(&["HEXISTS", "h", "f2"]), RespValue::Integer(1));
    assert_eq!(f.run(&["HDEL", "h", "f2"]), RespValue::Integer(1));
    assert_eq!(f.run(&["HEXISTS", "h", "f2"]), RespValue::Integer(0));
}

#[test]
fn expire_and_ttl() {
    let f = Fixture::new("expire");
    f.run(&["SET", "k", "v"]);
    assert_eq!(f.run(&["TTL", "k"]), RespValue::Integer(-1)); // no expiry
    assert_eq!(f.run(&["EXPIRE", "k", "100"]), RespValue::Integer(1));
    match f.run(&["TTL", "k"]) {
        RespValue::Integer(n) => assert!(n > 0 && n <= 100),
        other => panic!("unexpected TTL reply: {:?}", other),
    }
    assert_eq!(f.run(&["PERSIST", "k"]), RespValue::Integer(1));
    assert_eq!(f.run(&["TTL", "k"]), RespValue::Integer(-1));
    assert_eq!(f.run(&["TTL", "missing"]), RespValue::Integer(-2));
}

#[test]
fn immediate_expiry_deletes() {
    let f = Fixture::new("expire0");
    f.run(&["SET", "k", "v"]);
    assert_eq!(f.run(&["EXPIRE", "k", "-1"]), RespValue::Integer(1));
    assert_eq!(f.run(&["GET", "k"]), RespValue::Null);
}

#[test]
fn keys_and_type() {
    let f = Fixture::new("keys");
    f.run(&["SET", "user:1", "a"]);
    f.run(&["SET", "user:2", "b"]);
    f.run(&["SET", "other", "c"]);
    if let RespValue::Array(items) = f.run(&["KEYS", "user:*"]) {
        assert_eq!(items.len(), 2);
    } else {
        panic!("KEYS should return an array");
    }
    assert_eq!(f.run(&["TYPE", "user:1"]), RespValue::Simple("string".into()));
    assert_eq!(f.run(&["TYPE", "missing"]), RespValue::Simple("none".into()));
}

#[test]
fn unknown_command() {
    let f = Fixture::new("unknown");
    assert!(matches!(f.run(&["NOSUCHCMD"]), RespValue::Error(_)));
}
