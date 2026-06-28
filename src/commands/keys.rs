//! Generic key commands: DEL, EXISTS, EXPIRE/PEXPIRE, TTL/PTTL, PERSIST,
//! TYPE, KEYS and SCAN.

use super::{io_err, wrong_args, Db};
use crate::resp::RespValue;
use crate::types::now_ms;

/// `DEL key [key ...]` — returns the number of keys actually removed.
pub fn del(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return wrong_args("del");
    }
    let mut removed = 0;
    for key in &args[1..] {
        let _guard = db.lock_for(key);
        match db.remove(key) {
            Ok(true) => removed += 1,
            Ok(false) => {}
            Err(e) => return io_err(e),
        }
    }
    RespValue::Integer(removed)
}

/// `EXISTS key [key ...]` — counts existing keys (with repetition).
pub fn exists(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return wrong_args("exists");
    }
    let mut count = 0;
    for key in &args[1..] {
        match db.load(key) {
            Ok(Some(_)) => count += 1,
            Ok(None) => {}
            Err(e) => return io_err(e),
        }
    }
    RespValue::Integer(count)
}

/// `EXPIRE key seconds` / `PEXPIRE key milliseconds`.
pub fn expire(db: &Db, args: &[Vec<u8>], millis: bool) -> RespValue {
    let name = if millis { "pexpire" } else { "expire" };
    if args.len() != 3 {
        return wrong_args(name);
    }
    let n: i64 = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let key = &args[1];
    let _guard = db.lock_for(key);

    let mut sv = match db.load(key) {
        Ok(Some(sv)) => sv,
        Ok(None) => return RespValue::Integer(0),
        Err(e) => return io_err(e),
    };
    let ms = if millis { n } else { n * 1000 };
    let now = now_ms();
    if ms <= 0 {
        // A non-positive expiry deletes the key immediately, like Redis.
        return match db.remove(key) {
            Ok(_) => RespValue::Integer(1),
            Err(e) => io_err(e),
        };
    }
    sv.expire_at_ms = Some(now + ms as u64);
    match db.store(key, &sv) {
        Ok(()) => RespValue::Integer(1),
        Err(e) => io_err(e),
    }
}

/// `TTL key` (seconds) / `PTTL key` (milliseconds).
///
/// Replies: remaining time, `-1` if the key has no expiry, `-2` if it does not
/// exist.
pub fn ttl(db: &Db, args: &[Vec<u8>], millis: bool) -> RespValue {
    if args.len() != 2 {
        return wrong_args(if millis { "pttl" } else { "ttl" });
    }
    match db.load(&args[1]) {
        Ok(Some(sv)) => match sv.ttl_ms(now_ms()) {
            None => RespValue::Integer(-1),
            Some(ms) => {
                let ms = ms.max(0);
                let out = if millis { ms } else { (ms + 999) / 1000 };
                RespValue::Integer(out)
            }
        },
        Ok(None) => RespValue::Integer(-2),
        Err(e) => io_err(e),
    }
}

/// `PERSIST key` — remove the expiry, returning 1 if one was removed.
pub fn persist(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return wrong_args("persist");
    }
    let key = &args[1];
    let _guard = db.lock_for(key);
    let mut sv = match db.load(key) {
        Ok(Some(sv)) => sv,
        Ok(None) => return RespValue::Integer(0),
        Err(e) => return io_err(e),
    };
    if sv.expire_at_ms.take().is_none() {
        return RespValue::Integer(0);
    }
    match db.store(key, &sv) {
        Ok(()) => RespValue::Integer(1),
        Err(e) => io_err(e),
    }
}

/// `TYPE key` — the value's type name, or `none`.
pub fn type_cmd(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return wrong_args("type");
    }
    match db.load(&args[1]) {
        Ok(Some(sv)) => RespValue::Simple(sv.value.type_name().into()),
        Ok(None) => RespValue::Simple("none".into()),
        Err(e) => io_err(e),
    }
}

/// `KEYS pattern` — all keys matching a glob pattern.
pub fn keys(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return wrong_args("keys");
    }
    let pattern = &args[1];
    match db.all_keys() {
        Ok(all) => {
            let matched = all
                .into_iter()
                .filter(|k| glob_match(pattern, k))
                .map(RespValue::Bulk)
                .collect();
            RespValue::Array(matched)
        }
        Err(e) => io_err(e),
    }
}

/// `SCAN cursor [MATCH pattern] [COUNT n]`.
///
/// Our store has no incremental cursor, so we return the entire (matching) key
/// set in one batch and signal completion with a `0` cursor. This is a valid,
/// if non-incremental, SCAN implementation as far as clients are concerned.
pub fn scan(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return wrong_args("scan");
    }
    let mut pattern: Option<Vec<u8>> = None;
    let mut i = 2;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"MATCH" if i + 1 < args.len() => {
                pattern = Some(args[i + 1].clone());
                i += 2;
            }
            b"COUNT" if i + 1 < args.len() => {
                i += 2; // accepted and ignored
            }
            _ => return RespValue::error("ERR syntax error"),
        }
    }

    match db.all_keys() {
        Ok(all) => {
            let matched: Vec<RespValue> = all
                .into_iter()
                .filter(|k| pattern.as_ref().map_or(true, |p| glob_match(p, k)))
                .map(RespValue::Bulk)
                .collect();
            RespValue::Array(vec![RespValue::Bulk(b"0".to_vec()), RespValue::Array(matched)])
        }
        Err(e) => io_err(e),
    }
}

/// Redis-style glob matching supporting `*`, `?`, `[...]` sets and `\` escapes.
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    glob_rec(pattern, text)
}

fn glob_rec(mut p: &[u8], mut t: &[u8]) -> bool {
    while let Some(&pc) = p.first() {
        match pc {
            b'*' => {
                // Collapse consecutive stars, then try to match the rest at
                // every suffix of `t`.
                while p.first() == Some(&b'*') {
                    p = &p[1..];
                }
                if p.is_empty() {
                    return true;
                }
                loop {
                    if glob_rec(p, t) {
                        return true;
                    }
                    if t.is_empty() {
                        return false;
                    }
                    t = &t[1..];
                }
            }
            b'?' => {
                if t.is_empty() {
                    return false;
                }
                p = &p[1..];
                t = &t[1..];
            }
            b'[' => {
                if t.is_empty() {
                    return false;
                }
                match match_class(p, t[0]) {
                    Some(rest) => {
                        p = rest;
                        t = &t[1..];
                    }
                    None => return false,
                }
            }
            b'\\' if p.len() >= 2 => {
                if t.first() != Some(&p[1]) {
                    return false;
                }
                p = &p[2..];
                t = &t[1..];
            }
            c => {
                if t.first() != Some(&c) {
                    return false;
                }
                p = &p[1..];
                t = &t[1..];
            }
        }
    }
    t.is_empty()
}

/// Match a `[...]` character class against `ch`. On success returns the pattern
/// slice just past the closing `]`.
fn match_class(p: &[u8], ch: u8) -> Option<&[u8]> {
    debug_assert_eq!(p.first(), Some(&b'['));
    let mut i = 1;
    let negate = p.get(i) == Some(&b'^');
    if negate {
        i += 1;
    }
    let mut matched = false;
    while i < p.len() && p[i] != b']' {
        // Range like a-z.
        if i + 2 < p.len() && p[i + 1] == b'-' && p[i + 2] != b']' {
            let (lo, hi) = (p[i], p[i + 2]);
            if lo <= ch && ch <= hi {
                matched = true;
            }
            i += 3;
        } else {
            if p[i] == ch {
                matched = true;
            }
            i += 1;
        }
    }
    if i >= p.len() {
        return None; // unterminated class -> no match
    }
    // skip closing ']'
    let rest = &p[i + 1..];
    if matched != negate {
        Some(rest)
    } else {
        None
    }
}

#[cfg(test)]
mod glob_tests {
    use super::glob_match;

    #[test]
    fn literal() {
        assert!(glob_match(b"foo", b"foo"));
        assert!(!glob_match(b"foo", b"bar"));
    }

    #[test]
    fn star() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"h*o", b"hello"));
        assert!(glob_match(b"h*o", b"ho"));
        assert!(!glob_match(b"h*o", b"hey"));
        assert!(glob_match(b"user:*", b"user:42"));
    }

    #[test]
    fn question() {
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(!glob_match(b"h?llo", b"hllo"));
    }

    #[test]
    fn class() {
        assert!(glob_match(b"[abc]", b"b"));
        assert!(!glob_match(b"[abc]", b"d"));
        assert!(glob_match(b"[a-z]", b"m"));
        assert!(glob_match(b"[^a-z]", b"5"));
    }
}
