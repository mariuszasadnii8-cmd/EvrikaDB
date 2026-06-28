//! `evrika-cli` — a tiny RESP client, so the project is usable end-to-end even
//! on a machine without `redis-cli` installed.
//!
//! Usage:
//! ```text
//! evrika-cli [HOST:PORT]                 # interactive REPL
//! evrika-cli [HOST:PORT] SET foo bar     # one-shot command
//! ```
//!
//! Note: a real `redis-cli` works against the server too — this binary just
//! reuses the library's RESP encoder to avoid an external dependency.

use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;

use evrika_db::resp::RespValue;
use evrika_db::DEFAULT_ADDR;

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();

    // Optional first arg is HOST:PORT (heuristic: contains ':' and no spaces).
    let addr = if args.first().map_or(false, |a| a.contains(':')) {
        args.remove(0)
    } else {
        DEFAULT_ADDR.to_string()
    };

    let stream = match TcpStream::connect(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("could not connect to {}: {}", addr, e);
            std::process::exit(1);
        }
    };
    stream.set_nodelay(true).ok();
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
    let mut writer = stream;

    if !args.is_empty() {
        // One-shot mode.
        if let Err(e) = run_command(&mut writer, &mut reader, &args) {
            eprintln!("error: {}", e);
            std::process::exit(1);
        }
        return;
    }

    // Interactive REPL.
    println!("connected to {} — type commands, or 'quit' to exit", addr);
    let stdin = io::stdin();
    loop {
        print!("evrika> ");
        io::stdout().flush().ok();
        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap_or(0) == 0 {
            break; // EOF (Ctrl-D)
        }
        let parts: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
        if parts.is_empty() {
            continue;
        }
        if parts[0].eq_ignore_ascii_case("quit") || parts[0].eq_ignore_ascii_case("exit") {
            break;
        }
        if let Err(e) = run_command(&mut writer, &mut reader, &parts) {
            eprintln!("error: {}", e);
            break;
        }
    }
}

/// Send one command and print the reply.
fn run_command<W: Write, R: BufRead>(
    writer: &mut W,
    reader: &mut R,
    parts: &[String],
) -> io::Result<()> {
    let request = RespValue::Array(
        parts
            .iter()
            .map(|p| RespValue::Bulk(p.as_bytes().to_vec()))
            .collect(),
    );
    request.encode(writer)?;
    writer.flush()?;

    let reply = read_reply(reader)?;
    print_reply(&reply, 0);
    Ok(())
}

/// A decoded server reply.
enum Reply {
    Status(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Reply>>),
}

fn read_reply<R: BufRead>(r: &mut R) -> io::Result<Reply> {
    let line = read_line(r)?;
    if line.is_empty() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "empty reply"));
    }
    let (kind, rest) = (line[0], &line[1..]);
    match kind {
        b'+' => Ok(Reply::Status(String::from_utf8_lossy(rest).into_owned())),
        b'-' => Ok(Reply::Error(String::from_utf8_lossy(rest).into_owned())),
        b':' => Ok(Reply::Int(parse_i64(rest)?)),
        b'$' => {
            let len = parse_i64(rest)?;
            if len < 0 {
                return Ok(Reply::Bulk(None));
            }
            let mut data = vec![0u8; len as usize + 2];
            io::Read::read_exact(r, &mut data)?;
            data.truncate(len as usize);
            Ok(Reply::Bulk(Some(data)))
        }
        b'*' => {
            let len = parse_i64(rest)?;
            if len < 0 {
                return Ok(Reply::Array(None));
            }
            let mut items = Vec::with_capacity(len as usize);
            for _ in 0..len {
                items.push(read_reply(r)?);
            }
            Ok(Reply::Array(Some(items)))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected reply byte: {}", other as char),
        )),
    }
}

fn print_reply(reply: &Reply, indent: usize) {
    let pad = "  ".repeat(indent);
    match reply {
        Reply::Status(s) => println!("{}{}", pad, s),
        Reply::Error(e) => println!("{}(error) {}", pad, e),
        Reply::Int(n) => println!("{}(integer) {}", pad, n),
        Reply::Bulk(None) => println!("{}(nil)", pad),
        Reply::Bulk(Some(b)) => println!("{}\"{}\"", pad, String::from_utf8_lossy(b)),
        Reply::Array(None) => println!("{}(nil)", pad),
        Reply::Array(Some(items)) => {
            if items.is_empty() {
                println!("{}(empty array)", pad);
            }
            for (i, item) in items.iter().enumerate() {
                print!("{}{}) ", pad, i + 1);
                // Print scalars on the same line; nest arrays.
                match item {
                    Reply::Array(_) => {
                        println!();
                        print_reply(item, indent + 1);
                    }
                    _ => print_reply(item, 0),
                }
            }
        }
    }
}

fn read_line<R: BufRead>(r: &mut R) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let n = r.read_until(b'\n', &mut buf)?;
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed"));
    }
    if buf.last() == Some(&b'\n') {
        buf.pop();
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
    }
    Ok(buf)
}

fn parse_i64(bytes: &[u8]) -> io::Result<i64> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad integer in reply"))
}
