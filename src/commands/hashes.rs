//! Hash commands: HSET / HGET / HDEL / HEXISTS / HLEN / HKEYS / HVALS / HGETALL.

use std::collections::BTreeMap;

use super::{io_err, wrong_args, wrong_type, Db};
use crate::resp::RespValue;
use crate::types::{RedisValue, StoredValue};

type Hash = BTreeMap<Vec<u8>, Vec<u8>>;

fn as_hash(sv: StoredValue) -> Result<(Hash, Option<u64>), RespValue> {
    match sv.value {
        RedisValue::Hash(h) => Ok((h, sv.expire_at_ms)),
        _ => Err(wrong_type()),
    }
}

/// `HSET key field value [field value ...]` — returns the count of *new* fields.
pub fn hset(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 4 || args.len() % 2 != 0 {
        return wrong_args("hset");
    }
    let key = &args[1];
    let _guard = db.lock_for(key);

    let (mut map, expire) = match db.load(key) {
        Ok(Some(sv)) => match as_hash(sv) {
            Ok(pair) => pair,
            Err(e) => return e,
        },
        Ok(None) => (BTreeMap::new(), None),
        Err(e) => return io_err(e),
    };

    let mut added = 0;
    let mut i = 2;
    while i + 1 < args.len() {
        if map.insert(args[i].clone(), args[i + 1].clone()).is_none() {
            added += 1;
        }
        i += 2;
    }

    let sv = StoredValue {
        expire_at_ms: expire,
        value: RedisValue::Hash(map),
    };
    match db.store(key, &sv) {
        Ok(()) => RespValue::Integer(added),
        Err(e) => io_err(e),
    }
}

/// `HGET key field`
pub fn hget(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return wrong_args("hget");
    }
    match db.load(&args[1]) {
        Ok(Some(sv)) => match sv.value {
            RedisValue::Hash(h) => match h.get(&args[2]) {
                Some(v) => RespValue::Bulk(v.clone()),
                None => RespValue::Null,
            },
            _ => wrong_type(),
        },
        Ok(None) => RespValue::Null,
        Err(e) => io_err(e),
    }
}

/// `HDEL key field [field ...]` — returns the number of fields removed.
pub fn hdel(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 {
        return wrong_args("hdel");
    }
    let key = &args[1];
    let _guard = db.lock_for(key);

    let (mut map, expire) = match db.load(key) {
        Ok(Some(sv)) => match as_hash(sv) {
            Ok(pair) => pair,
            Err(e) => return e,
        },
        Ok(None) => return RespValue::Integer(0),
        Err(e) => return io_err(e),
    };

    let mut removed = 0;
    for field in &args[2..] {
        if map.remove(field).is_some() {
            removed += 1;
        }
    }

    let store_result = if map.is_empty() {
        db.remove(key).map(|_| ())
    } else {
        db.store(
            key,
            &StoredValue {
                expire_at_ms: expire,
                value: RedisValue::Hash(map),
            },
        )
    };
    match store_result {
        Ok(()) => RespValue::Integer(removed),
        Err(e) => io_err(e),
    }
}

/// `HEXISTS key field`
pub fn hexists(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return wrong_args("hexists");
    }
    match db.load(&args[1]) {
        Ok(Some(sv)) => match sv.value {
            RedisValue::Hash(h) => RespValue::Integer(h.contains_key(&args[2]) as i64),
            _ => wrong_type(),
        },
        Ok(None) => RespValue::Integer(0),
        Err(e) => io_err(e),
    }
}

/// `HLEN key`
pub fn hlen(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return wrong_args("hlen");
    }
    match db.load(&args[1]) {
        Ok(Some(sv)) => match sv.value {
            RedisValue::Hash(h) => RespValue::Integer(h.len() as i64),
            _ => wrong_type(),
        },
        Ok(None) => RespValue::Integer(0),
        Err(e) => io_err(e),
    }
}

/// `HKEYS key`
pub fn hkeys(db: &Db, args: &[Vec<u8>]) -> RespValue {
    collection(db, args, "hkeys", |h| {
        h.keys().map(|k| RespValue::Bulk(k.clone())).collect()
    })
}

/// `HVALS key`
pub fn hvals(db: &Db, args: &[Vec<u8>]) -> RespValue {
    collection(db, args, "hvals", |h| {
        h.values().map(|v| RespValue::Bulk(v.clone())).collect()
    })
}

/// `HGETALL key` — flat `[field, value, field, value, ...]` array.
pub fn hgetall(db: &Db, args: &[Vec<u8>]) -> RespValue {
    collection(db, args, "hgetall", |h| {
        let mut out = Vec::with_capacity(h.len() * 2);
        for (k, v) in h {
            out.push(RespValue::Bulk(k.clone()));
            out.push(RespValue::Bulk(v.clone()));
        }
        out
    })
}

/// Shared scaffold for the read-only hash commands that emit an array.
fn collection(
    db: &Db,
    args: &[Vec<u8>],
    name: &str,
    project: impl Fn(&Hash) -> Vec<RespValue>,
) -> RespValue {
    if args.len() != 2 {
        return wrong_args(name);
    }
    match db.load(&args[1]) {
        Ok(Some(sv)) => match sv.value {
            RedisValue::Hash(h) => RespValue::Array(project(&h)),
            _ => wrong_type(),
        },
        Ok(None) => RespValue::Array(Vec::new()),
        Err(e) => io_err(e),
    }
}
