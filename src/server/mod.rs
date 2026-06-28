//! The network front-end: a blocking, thread-per-job TCP server speaking RESP.
//!
//! `Engine`/`Db` are shared across all worker threads behind an `Arc`; the
//! engine's own `RwLock`/`Mutex`es provide the synchronization, so the server
//! layer just needs to clone the `Arc` for each connection.

mod thread_pool;

use std::io::{self, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use crate::commands::{dispatch, Db};
use crate::resp::{read_command, ProtocolError, RespValue};

pub use thread_pool::ThreadPool;

/// Run the server until the listener errors fatally. Blocks the calling thread.
pub fn run(db: Arc<Db>, addr: &str, num_threads: usize) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    println!(
        "EvrikaDB listening on {} ({} worker threads)",
        listener.local_addr()?,
        num_threads
    );

    let pool = ThreadPool::new(num_threads);
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let db = Arc::clone(&db);
                pool.execute(move || {
                    if let Err(e) = handle_connection(stream, db) {
                        // A reset/closed connection is normal; only log oddities.
                        if e.kind() != io::ErrorKind::ConnectionReset {
                            eprintln!("[evrika] connection error: {}", e);
                        }
                    }
                });
            }
            Err(e) => eprintln!("[evrika] accept failed: {}", e),
        }
    }
    Ok(())
}

/// Serve one client: read commands, dispatch them, write replies, until the
/// peer disconnects or sends `QUIT`.
fn handle_connection(stream: TcpStream, db: Arc<Db>) -> io::Result<()> {
    stream.set_nodelay(true).ok();
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);

    loop {
        match read_command(&mut reader) {
            Ok(args) => {
                if args.is_empty() {
                    continue; // blank inline line
                }
                if args[0].eq_ignore_ascii_case(b"QUIT") {
                    RespValue::Simple("OK".into()).encode(&mut writer)?;
                    writer.flush()?;
                    return Ok(());
                }
                let reply = dispatch(&db, &args);
                reply.encode(&mut writer)?;
                writer.flush()?;
            }
            Err(ProtocolError::Eof) => return Ok(()),
            Err(ProtocolError::Io(e)) => return Err(e),
            Err(ProtocolError::Malformed(msg)) => {
                // Report and close: we can't reliably resync the stream.
                let _ = RespValue::error(format!("ERR Protocol error: {}", msg)).encode(&mut writer);
                let _ = writer.flush();
                return Ok(());
            }
        }
    }
}
