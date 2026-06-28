//! String commands: SET / GET / GETSET / SETEX / APPEND / STRLEN and the
//! integer family (INCR / DECR / INCRBY / DECRBY).

use super::{io_err, map_io, wrong_args, wrong_type, Db};
use crate::resp::RespValue;
use crate::types::{now_ms, RedisValue, StoredValue};

/// Pull the string payload out of a stored value, or signal a type error.
fn as_string(sv: StoredValue) -> Result<(Vec<u8>, Option<u64>), RespValue> {
    match sv.value {
        RedisValue::Str(s) => Ok((s, sv.expire_at_ms)),
        _ => Err(wrong_type()),
    }
}

/// `SET key value [EX s | PX ms] [NX | XX]`
pub fn set(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 {
        return wrong_args("set");
    }
    let key = &args[1];
    let value = args[2].clone();

    let mut expire_at: Option<u64> = None;
    let mut nx = false;
    let mut xx = false;

    let mut i = 3;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"EX" | b"PX" => {
                if i + 1 >= args.len() {
                    return RespValue::error("ERR syntax error");
                }
                let n: i64 = match parse_int(&args[i + 1]) {
                    Some(n) if n > 0 => n,
                    _ => return RespValue::error("ERR invalid expire time in 'set' command"),
                };
                let ms = if opt == b"EX" { n * 1000 } else { n };
                expire_at = Some(now_ms() + ms as u64);
                i += 2;
            }
            b"NX" => {
                nx = true;
                i += 1;
            }
            b"XX" => {
                xx = true;
                i += 1;
            }
            _ => return RespValue::error("ERR syntax error"),
        }
    }

    let _guard = db.lock_for(key);
    let exists = match db.load(key) {
        Ok(v) => v.is_some(),
        Err(e) => return io_err(e),
    };
    if (nx && exists) || (xx && !exists) {
        return RespValue::Null;
    }

    let sv = StoredValue {
        expire_at_ms: expire_at,
        value: RedisValue::Str(value),
    };
    match db.store(key, &sv) {
        Ok(()) => RespValue::Simple("OK".into()),
        Err(e) => io_err(e),
    }
}

/// `GET key`
pub fn get(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return wrong_args("get");
    }
    map_io(db.load(&args[1]).map(|opt| match opt {
        Some(sv) => match sv.value {
            RedisValue::Str(s) => RespValue::Bulk(s),
            _ => wrong_type(),
        },
        None => RespValue::Null,
    }))
}

/// `GETSET key value` — set new value, return the old one.
pub fn getset(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return wrong_args("getset");
    }
    let key = &args[1];
    let _guard = db.lock_for(key);

    let old = match db.load(key) {
        Ok(Some(sv)) => match as_string(sv) {
            Ok((s, _)) => RespValue::Bulk(s),
            Err(e) => return e,
        },
        Ok(None) => RespValue::Null,
        Err(e) => return io_err(e),
    };
    // GETSET clears any previous TTL (matches Redis behaviour).
    let sv = StoredValue::new(RedisValue::Str(args[2].clone()));
    match db.store(key, &sv) {
        Ok(()) => old,
        Err(e) => io_err(e),
    }
}

/// `SETEX key seconds value` / `PSETEX key ms value`
pub fn setex(db: &Db, args: &[Vec<u8>], millis: bool) -> RespValue {
    let name = if millis { "psetex" } else { "setex" };
    if args.len() != 4 {
        return wrong_args(name);
    }
    let n: i64 = match parse_int(&args[2]) {
        Some(n) if n > 0 => n,
        _ => return RespValue::error(format!("ERR invalid expire time in '{}' command", name)),
    };
    let ms = if millis { n } else { n * 1000 };
    let key = &args[1];
    let _guard = db.lock_for(key);
    let sv = StoredValue {
        expire_at_ms: Some(now_ms() + ms as u64),
        value: RedisValue::Str(args[3].clone()),
    };
    match db.store(key, &sv) {
        Ok(()) => RespValue::Simple("OK".into()),
        Err(e) => io_err(e),
    }
}

/// `APPEND key value` — returns the new length.
pub fn append(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return wrong_args("append");
    }
    let key = &args[1];
    let _guard = db.lock_for(key);

    let (mut s, expire) = match db.load(key) {
        Ok(Some(sv)) => match as_string(sv) {
            Ok(pair) => pair,
            Err(e) => return e,
        },
        Ok(None) => (Vec::new(), None),
        Err(e) => return io_err(e),
    };
    s.extend_from_slice(&args[2]);
    let new_len = s.len() as i64;
    let sv = StoredValue {
        expire_at_ms: expire,
        value: RedisValue::Str(s),
    };
    match db.store(key, &sv) {
        Ok(()) => RespValue::Integer(new_len),
        Err(e) => io_err(e),
    }
}

/// `STRLEN key`
pub fn strlen(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return wrong_args("strlen");
    }
    match db.load(&args[1]) {
        Ok(Some(sv)) => match sv.value {
            RedisValue::Str(s) => RespValue::Integer(s.len() as i64),
            _ => wrong_type(),
        },
        Ok(None) => RespValue::Integer(0),
        Err(e) => io_err(e),
    }
}

/// `INCR key` / `DECR key` (delta is +1 / -1).
pub fn incr_by(db: &Db, args: &[Vec<u8>], delta: i64) -> RespValue {
    if args.len() != 2 {
        return wrong_args(if delta >= 0 { "incr" } else { "decr" });
    }
    apply_delta(db, &args[1], delta)
}

/// `INCRBY key n` / `DECRBY key n`. `sign` is +1 for INCRBY, -1 for DECRBY.
pub fn incrby(db: &Db, args: &[Vec<u8>], sign: i64) -> RespValue {
    if args.len() != 3 {
        return wrong_args(if sign >= 0 { "incrby" } else { "decrby" });
    }
    let by = match parse_int(&args[2]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let delta = match by.checked_mul(sign) {
        Some(d) => d,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    apply_delta(db, &args[1], delta)
}

fn apply_delta(db: &Db, key: &[u8], delta: i64) -> RespValue {
    let _guard = db.lock_for(key);

    let (cur, expire) = match db.load(key) {
        Ok(Some(sv)) => match as_string(sv) {
            Ok((s, exp)) => match parse_int(&s) {
                Some(n) => (n, exp),
                None => return RespValue::error("ERR value is not an integer or out of range"),
            },
            Err(e) => return e,
        },
        Ok(None) => (0, None),
        Err(e) => return io_err(e),
    };

    let next = match cur.checked_add(delta) {
        Some(n) => n,
        None => return RespValue::error("ERR increment or decrement would overflow"),
    };
    let sv = StoredValue {
        expire_at_ms: expire,
        value: RedisValue::Str(next.to_string().into_bytes()),
    };
    match db.store(key, &sv) {
        Ok(()) => RespValue::Integer(next),
        Err(e) => io_err(e),
    }
}

/// Parse an ASCII decimal integer (no surrounding whitespace).
fn parse_int(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse::<i64>().ok()
}
