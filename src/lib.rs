//! EvrikaDB — a Redis-compatible key/value store backed by an LSM-tree.
//!
//! The crate is split into four layers, from the wire up to the disk:
//!
//! * [`resp`]     — the RESP (REdis Serialization Protocol) parser & encoder.
//! * [`types`]    — Redis-level value types (string / list / hash) and their
//!                  custom binary serialization into raw engine bytes.
//! * [`commands`] — the command dispatcher that turns a parsed request into a
//!                  read/write against the storage engine.
//! * [`storage`]  — the LSM-tree engine: memtable, write-ahead log, on-disk
//!                  SSTables and background compaction.
//! * [`server`]   — the multithreaded TCP front-end (thread pool + Arc/RwLock).

pub mod commands;
pub mod resp;
pub mod server;
pub mod storage;
pub mod types;

/// Default address the server binds to (matches the Redis default port).
pub const DEFAULT_ADDR: &str = "127.0.0.1:6380";

/// Default on-disk data directory.
pub const DEFAULT_DATA_DIR: &str = "evrika-data";
