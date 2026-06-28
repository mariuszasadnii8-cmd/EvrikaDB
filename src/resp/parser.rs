//! Request parsing: turn the bytes coming off a socket into a command, which
//! we represent simply as a vector of byte-string arguments
//! (`["SET", "key", "value"]`).

use std::io::BufRead;

use super::{read_line, ProtocolError};

/// Read one complete command from `r`.
///
/// Supports both RESP framings a real client may use:
///
/// * **Array of bulk strings** — `*3\r\n$3\r\nSET\r\n...` — the normal case.
/// * **Inline command** — a plain text line such as `PING\r\n`, split on
///   whitespace. Useful for manual testing over `telnet`/`nc`.
pub fn read_command<R: BufRead>(r: &mut R) -> Result<Vec<Vec<u8>>, ProtocolError> {
    let line = read_line(r)?;
    if line.is_empty() {
        // Empty inline line — treat as an empty command, caller will ignore it.
        return Ok(Vec::new());
    }

    match line[0] {
        b'*' => read_array(r, &line),
        _ => Ok(parse_inline(&line)),
    }
}

/// Parse an `*N\r\n` header followed by N bulk strings.
fn read_array<R: BufRead>(r: &mut R, header: &[u8]) -> Result<Vec<Vec<u8>>, ProtocolError> {
    let count = parse_len(&header[1..])?;
    if count < 0 {
        // Null array as a request makes no sense — treat as empty.
        return Ok(Vec::new());
    }
    let count = count as usize;
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let bulk_header = read_line(r)?;
        if bulk_header.first() != Some(&b'$') {
            return Err(ProtocolError::Malformed(
                "expected '$' bulk string in array".into(),
            ));
        }
        let len = parse_len(&bulk_header[1..])?;
        if len < 0 {
            args.push(Vec::new());
            continue;
        }
        let len = len as usize;
        // Read exactly `len` bytes plus the trailing CRLF.
        let mut data = vec![0u8; len + 2];
        read_exact(r, &mut data)?;
        if &data[len..] != b"\r\n" {
            return Err(ProtocolError::Malformed(
                "bulk string not terminated by CRLF".into(),
            ));
        }
        data.truncate(len);
        args.push(data);
    }
    Ok(args)
}

/// Split an inline command on ASCII whitespace.
fn parse_inline(line: &[u8]) -> Vec<Vec<u8>> {
    line.split(|b| b.is_ascii_whitespace())
        .filter(|seg| !seg.is_empty())
        .map(|seg| seg.to_vec())
        .collect()
}

/// Parse a base-10 length, which may be negative (e.g. `$-1`).
fn parse_len(bytes: &[u8]) -> Result<i64, ProtocolError> {
    let s = std::str::from_utf8(bytes)
        .map_err(|_| ProtocolError::Malformed("length is not valid UTF-8".into()))?;
    s.trim()
        .parse::<i64>()
        .map_err(|_| ProtocolError::Malformed(format!("invalid length: {:?}", s)))
}

/// `BufRead` has no `read_exact` that respects EOF semantics we want, so wrap
/// the standard `Read::read_exact` and map a premature EOF to a clean error.
fn read_exact<R: BufRead>(r: &mut R, buf: &mut [u8]) -> Result<(), ProtocolError> {
    match r.read_exact(buf) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Err(ProtocolError::Eof),
        Err(e) => Err(ProtocolError::Io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;

    fn parse(input: &[u8]) -> Vec<Vec<u8>> {
        let mut r = BufReader::new(input);
        read_command(&mut r).expect("parse")
    }

    #[test]
    fn parses_array_request() {
        let cmd = parse(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert_eq!(cmd, vec![b"SET".to_vec(), b"foo".to_vec(), b"bar".to_vec()]);
    }

    #[test]
    fn parses_inline_request() {
        let cmd = parse(b"PING hello\r\n");
        assert_eq!(cmd, vec![b"PING".to_vec(), b"hello".to_vec()]);
    }

    #[test]
    fn parses_binary_safe_bulk() {
        // Value contains an embedded CRLF — must survive intact.
        let cmd = parse(b"*2\r\n$3\r\nSET\r\n$4\r\na\r\nb\r\n");
        assert_eq!(cmd[1], b"a\r\nb".to_vec());
    }

    #[test]
    fn eof_between_commands() {
        let mut r = BufReader::new(&b""[..]);
        assert!(matches!(
            read_command(&mut r),
            Err(ProtocolError::Eof)
        ));
    }
}
