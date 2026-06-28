//! List commands: LPUSH / RPUSH / LPOP / RPOP / LLEN / LRANGE / LINDEX.

use std::collections::VecDeque;

use super::{io_err, wrong_args, wrong_type, Db};
use crate::resp::RespValue;
use crate::types::{RedisValue, StoredValue};

/// Borrow the list payload from a stored value, or signal a type error.
fn as_list(sv: StoredValue) -> Result<(VecDeque<Vec<u8>>, Option<u64>), RespValue> {
    match sv.value {
        RedisValue::List(l) => Ok((l, sv.expire_at_ms)),
        _ => Err(wrong_type()),
    }
}

/// `LPUSH/RPUSH key value [value ...]` — returns the new length.
pub fn push(db: &Db, args: &[Vec<u8>], left: bool) -> RespValue {
    if args.len() < 3 {
        return wrong_args(if left { "lpush" } else { "rpush" });
    }
    let key = &args[1];
    let _guard = db.lock_for(key);

    let (mut list, expire) = match db.load(key) {
        Ok(Some(sv)) => match as_list(sv) {
            Ok(pair) => pair,
            Err(e) => return e,
        },
        Ok(None) => (VecDeque::new(), None),
        Err(e) => return io_err(e),
    };

    for value in &args[2..] {
        if left {
            list.push_front(value.clone());
        } else {
            list.push_back(value.clone());
        }
    }
    let len = list.len() as i64;
    let sv = StoredValue {
        expire_at_ms: expire,
        value: RedisValue::List(list),
    };
    match db.store(key, &sv) {
        Ok(()) => RespValue::Integer(len),
        Err(e) => io_err(e),
    }
}

/// `LPOP/RPOP key [count]`.
pub fn pop(db: &Db, args: &[Vec<u8>], left: bool) -> RespValue {
    if args.len() != 2 && args.len() != 3 {
        return wrong_args(if left { "lpop" } else { "rpop" });
    }
    let count: Option<usize> = if args.len() == 3 {
        match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(n) if n >= 0 => Some(n as usize),
            _ => return RespValue::error("ERR value is out of range, must be positive"),
        }
    } else {
        None
    };

    let key = &args[1];
    let _guard = db.lock_for(key);

    let (mut list, expire) = match db.load(key) {
        Ok(Some(sv)) => match as_list(sv) {
            Ok(pair) => pair,
            Err(e) => return e,
        },
        Ok(None) => {
            return if count.is_some() {
                RespValue::NullArray
            } else {
                RespValue::Null
            }
        }
        Err(e) => return io_err(e),
    };

    let take = count.unwrap_or(1).min(list.len());
    let mut popped = Vec::with_capacity(take);
    for _ in 0..take {
        let item = if left {
            list.pop_front()
        } else {
            list.pop_back()
        };
        match item {
            Some(v) => popped.push(v),
            None => break,
        }
    }

    // Persist the shrunken list, or delete the key if it is now empty.
    let store_result = if list.is_empty() {
        db.remove(key).map(|_| ())
    } else {
        db.store(
            key,
            &StoredValue {
                expire_at_ms: expire,
                value: RedisValue::List(list),
            },
        )
    };
    if let Err(e) = store_result {
        return io_err(e);
    }

    match count {
        None => match popped.into_iter().next() {
            Some(v) => RespValue::Bulk(v),
            None => RespValue::Null,
        },
        Some(_) => {
            if popped.is_empty() {
                RespValue::NullArray
            } else {
                RespValue::Array(popped.into_iter().map(RespValue::Bulk).collect())
            }
        }
    }
}

/// `LLEN key`
pub fn llen(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return wrong_args("llen");
    }
    match db.load(&args[1]) {
        Ok(Some(sv)) => match sv.value {
            RedisValue::List(l) => RespValue::Integer(l.len() as i64),
            _ => wrong_type(),
        },
        Ok(None) => RespValue::Integer(0),
        Err(e) => io_err(e),
    }
}

/// `LRANGE key start stop` with Redis negative-index semantics.
pub fn lrange(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 4 {
        return wrong_args("lrange");
    }
    let start = match parse_index(&args[2]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let stop = match parse_index(&args[3]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let list = match db.load(&args[1]) {
        Ok(Some(sv)) => match sv.value {
            RedisValue::List(l) => l,
            _ => return wrong_type(),
        },
        Ok(None) => return RespValue::Array(Vec::new()),
        Err(e) => return io_err(e),
    };

    let len = list.len() as i64;
    let (start, stop) = (normalize(start, len), normalize(stop, len));
    if start > stop || start >= len {
        return RespValue::Array(Vec::new());
    }
    let out = (start..=stop.min(len - 1))
        .map(|i| RespValue::Bulk(list[i as usize].clone()))
        .collect();
    RespValue::Array(out)
}

/// `LINDEX key index`
pub fn lindex(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return wrong_args("lindex");
    }
    let idx = match parse_index(&args[2]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    match db.load(&args[1]) {
        Ok(Some(sv)) => match sv.value {
            RedisValue::List(l) => {
                let len = l.len() as i64;
                let real = if idx < 0 { len + idx } else { idx };
                if real < 0 || real >= len {
                    RespValue::Null
                } else {
                    RespValue::Bulk(l[real as usize].clone())
                }
            }
            _ => wrong_type(),
        },
        Ok(None) => RespValue::Null,
        Err(e) => io_err(e),
    }
}

fn parse_index(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse::<i64>().ok()
}

/// Clamp a (possibly negative) index into `[0, len)` range for LRANGE.
fn normalize(idx: i64, len: i64) -> i64 {
    let i = if idx < 0 { len + idx } else { idx };
    i.max(0)
}
