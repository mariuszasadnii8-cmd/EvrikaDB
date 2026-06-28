//! EvrikaDB server entry point.
//!
//! Usage:
//! ```text
//! evrika-server [--addr HOST:PORT] [--dir PATH] [--threads N]
//!               [--memtable-size BYTES] [--compaction-trigger N] [--no-sync]
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use evrika_db::commands::Db;
use evrika_db::server;
use evrika_db::storage::{Config, Engine};
use evrika_db::{DEFAULT_ADDR, DEFAULT_DATA_DIR};

struct Options {
    addr: String,
    threads: usize,
    config: Config,
}

fn parse_args() -> Result<Options, String> {
    let mut addr = DEFAULT_ADDR.to_string();
    let mut threads = default_threads();
    let mut config = Config::new(DEFAULT_DATA_DIR);

    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut next = || args.next().ok_or_else(|| format!("missing value for {}", flag));
        match flag.as_str() {
            "--addr" | "-a" => addr = next()?,
            "--dir" | "-d" => config.data_dir = next()?.into(),
            "--threads" | "-t" => {
                threads = next()?.parse().map_err(|_| "invalid --threads".to_string())?
            }
            "--memtable-size" => {
                config.memtable_threshold =
                    next()?.parse().map_err(|_| "invalid --memtable-size".to_string())?
            }
            "--compaction-trigger" => {
                config.compaction_trigger =
                    next()?.parse().map_err(|_| "invalid --compaction-trigger".to_string())?
            }
            "--no-sync" => config.sync_on_write = false,
            "--help" | "-h" => return Err("help".to_string()),
            other => return Err(format!("unknown argument: {}", other)),
        }
    }
    Ok(Options {
        addr,
        threads,
        config,
    })
}

fn default_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn usage() {
    eprintln!(
        "EvrikaDB — a Redis-compatible LSM-tree store\n\n\
         USAGE:\n  \
         evrika-server [OPTIONS]\n\n\
         OPTIONS:\n  \
         -a, --addr HOST:PORT         listen address (default {addr})\n  \
         -d, --dir PATH               data directory (default {dir})\n  \
         -t, --threads N              worker threads (default: CPU count)\n      \
         --memtable-size BYTES        flush threshold (default 1048576)\n      \
         --compaction-trigger N       compact when SSTables exceed N (default 4)\n      \
         --no-sync                    do not fsync the WAL on every write\n  \
         -h, --help                   show this help",
        addr = DEFAULT_ADDR,
        dir = DEFAULT_DATA_DIR,
    );
}

fn main() -> ExitCode {
    let opts = match parse_args() {
        Ok(o) => o,
        Err(e) => {
            if e != "help" {
                eprintln!("error: {}\n", e);
            }
            usage();
            return if e == "help" {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            };
        }
    };

    let engine = match Engine::open(opts.config.clone()) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("failed to open database at {:?}: {}", opts.config.data_dir, e);
            return ExitCode::FAILURE;
        }
    };
    println!(
        "opened database at {:?} (memtable {} B, compaction at {} SSTables, fsync={})",
        opts.config.data_dir,
        opts.config.memtable_threshold,
        opts.config.compaction_trigger,
        opts.config.sync_on_write,
    );

    let db = Arc::new(Db::new(engine));
    if let Err(e) = server::run(db, &opts.addr, opts.threads) {
        eprintln!("server error: {}", e);
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
