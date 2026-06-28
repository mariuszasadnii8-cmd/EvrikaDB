//! RESP — the wire protocol spoken by Redis clients.
//!
//! We implement just enough of RESP2 to talk to `redis-cli` and the standard
//! client libraries:
//!
//! * Requests arrive either as an **array of bulk strings** (the normal mode
//!   used by every real client) or as a bare **inline command** (handy when
//!   poking the server with `telnet`/`nc`).
//! * Replies are produced from the [`RespValue`] tree which covers simple
//!   strings, errors, integers, bulk strings (incl. null) and arrays.

use std::io::{self, BufRead, Write};

mod parser;

pub use parser::read_command;

/// A single RESP value, used for building replies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    /// `+OK\r\n`
    Simple(String),
    /// `-ERR message\r\n`
    Error(String),
    /// `:42\r\n`
    Integer(i64),
    /// `$5\r\nhello\r\n`
    Bulk(Vec<u8>),
    /// `$-1\r\n`
    Null,
    /// `*N\r\n...`
    Array(Vec<RespValue>),
    /// `*-1\r\n`
    NullArray,
}

impl RespValue {
    /// Convenience constructor for a bulk string from anything string-like.
    pub fn bulk<S: Into<Vec<u8>>>(s: S) -> RespValue {
        RespValue::Bulk(s.into())
    }

    /// Build an `-ERR ...` reply.
    pub fn error<S: Into<String>>(msg: S) -> RespValue {
        RespValue::Error(msg.into())
    }

    /// Serialize this value onto `w` following the RESP grammar.
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        match self {
            RespValue::Simple(s) => {
                w.write_all(b"+")?;
                w.write_all(s.as_bytes())?;
                w.write_all(b"\r\n")
            }
            RespValue::Error(s) => {
                w.write_all(b"-")?;
                w.write_all(s.as_bytes())?;
                w.write_all(b"\r\n")
            }
            RespValue::Integer(n) => {
                write!(w, ":{}\r\n", n)
            }
            RespValue::Bulk(bytes) => {
                write!(w, "${}\r\n", bytes.len())?;
                w.write_all(bytes)?;
                w.write_all(b"\r\n")
            }
            RespValue::Null => w.write_all(b"$-1\r\n"),
            RespValue::NullArray => w.write_all(b"*-1\r\n"),
            RespValue::Array(items) => {
                write!(w, "*{}\r\n", items.len())?;
                for item in items {
                    item.encode(w)?;
                }
                Ok(())
            }
        }
    }

    /// Encode into a fresh byte buffer (used in tests).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode(&mut buf).expect("Vec writes never fail");
        buf
    }
}

/// Errors that can occur while parsing a request off the wire.
#[derive(Debug)]
pub enum ProtocolError {
    /// The peer closed the connection cleanly between commands.
    Eof,
    /// The bytes on the wire do not form a valid RESP request.
    Malformed(String),
    /// Underlying socket I/O failed.
    Io(io::Error),
}

impl From<io::Error> for ProtocolError {
    fn from(e: io::Error) -> Self {
        ProtocolError::Io(e)
    }
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolError::Eof => write!(f, "connection closed"),
            ProtocolError::Malformed(m) => write!(f, "protocol error: {}", m),
            ProtocolError::Io(e) => write!(f, "io error: {}", e),
        }
    }
}

/// Read exactly one line terminated by CRLF (or LF) from `r`, with the line
/// terminator stripped. Returns [`ProtocolError::Eof`] at end of stream.
pub(crate) fn read_line<R: BufRead>(r: &mut R) -> Result<Vec<u8>, ProtocolError> {
    let mut buf = Vec::new();
    let n = r.read_until(b'\n', &mut buf)?;
    if n == 0 {
        return Err(ProtocolError::Eof);
    }
    // Strip trailing \n and optional \r.
    if buf.last() == Some(&b'\n') {
        buf.pop();
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_simple_and_error() {
        assert_eq!(RespValue::Simple("OK".into()).to_bytes(), b"+OK\r\n");
        assert_eq!(RespValue::error("boom").to_bytes(), b"-boom\r\n");
    }

    #[test]
    fn encode_integer_and_bulk() {
        assert_eq!(RespValue::Integer(42).to_bytes(), b":42\r\n");
        assert_eq!(RespValue::bulk("hi").to_bytes(), b"$2\r\nhi\r\n");
        assert_eq!(RespValue::Null.to_bytes(), b"$-1\r\n");
    }

    #[test]
    fn encode_array() {
        let v = RespValue::Array(vec![RespValue::bulk("a"), RespValue::Integer(1)]);
        assert_eq!(v.to_bytes(), b"*2\r\n$1\r\na\r\n:1\r\n");
    }
}
