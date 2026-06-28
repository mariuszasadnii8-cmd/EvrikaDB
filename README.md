# EvrikaDB

A **Redis-compatible key/value store** backed by an **LSM-tree** storage engine
(à la RocksDB / LevelDB), written in **pure Rust with zero external
dependencies** — only the standard library.

It speaks the real **RESP** wire protocol, so you can talk to it with
`redis-cli`, any Redis client library, or the bundled `evrika-cli`. Data is
durably persisted to disk and recovered on restart, and the engine is fully
multithreaded (`Arc` / `Mutex` / `RwLock`, a worker thread pool, and a
background compaction thread).

```
        RESP over TCP                 typed commands              LSM-tree
   redis-cli ───────────▶  server  ───────────────▶   Db   ──────────────▶  disk
   any Redis client      (thread pool)            (striped locks)     WAL + SSTables
```

## Why this exists

It is a from-scratch exercise in the things that make a real database tick:

* **I/O & the filesystem** — a write-ahead log, immutable on-disk SSTables with
  an in-memory index, an atomically-rewritten `MANIFEST`, crash recovery.
* **Custom serialization** — every byte on disk is hand-rolled little-endian;
  no `serde`.
* **Concurrency** — `Arc<RwLock<…>>` for the read-mostly state, a `Mutex`-based
  thread pool, per-key striped locks for atomic read-modify-write, and a
  dedicated background thread for flush/compaction.
* **Protocols & raw bytes** — a binary-safe RESP parser/encoder.

## Build & run

```sh
cargo build --release

# start the server (defaults: 127.0.0.1:6380, ./evrika-data, CPU-count threads)
./target/release/evrika-server

# …or with options
./target/release/evrika-server --addr 127.0.0.1:6380 --dir ./data --threads 8
```

Connect with the standard Redis CLI:

```sh
redis-cli -p 6380
127.0.0.1:6380> SET hello world
OK
127.0.0.1:6380> GET hello
"world"
```

…or with the bundled dependency-free client (one-shot or interactive):

```sh
./target/release/evrika-cli 127.0.0.1:6380 SET foo bar
./target/release/evrika-cli 127.0.0.1:6380          # REPL
```

### Server options

| Flag | Default | Meaning |
|------|---------|---------|
| `-a, --addr HOST:PORT` | `127.0.0.1:6380` | Listen address |
| `-d, --dir PATH` | `evrika-data` | Data directory |
| `-t, --threads N` | CPU count | Worker threads |
| `--memtable-size BYTES` | `1048576` | Flush the memtable past this size |
| `--compaction-trigger N` | `4` | Compact when live SSTables exceed `N` |
| `--no-sync` | (off) | Don't `fsync` the WAL on every write (faster, less durable) |

## Supported commands

* **Connection/server:** `PING`, `ECHO`, `SELECT`, `INFO`, `DBSIZE`,
  `FLUSHALL` / `FLUSHDB`, `QUIT`
* **Generic keys:** `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`,
  `PERSIST`, `TYPE`, `KEYS`, `SCAN`
* **Strings:** `SET` (`EX`/`PX`/`NX`/`XX`), `GET`, `GETSET`, `SETEX`, `PSETEX`,
  `APPEND`, `STRLEN`, `INCR`, `DECR`, `INCRBY`, `DECRBY`
* **Lists:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`
* **Hashes:** `HSET`, `HGET`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`,
  `HGETALL`

## Architecture

```
src/
├── main.rs              # server binary: arg parsing, wiring
├── bin/cli.rs           # dependency-free RESP client
├── lib.rs               # crate root, module map
├── resp/                # RESP protocol
│   ├── mod.rs           #   RespValue + encoder + ProtocolError
│   └── parser.rs        #   request parser (array & inline framings)
├── types.rs             # RedisValue (string/list/hash), StoredValue,
│                        #   TTL metadata + custom binary (de)serialization
├── commands/            # command layer
│   ├── mod.rs           #   Db (typed view, lazy expiry, striped locks) + dispatch
│   ├── strings.rs       #   SET/GET/INCR/…
│   ├── keys.rs          #   DEL/EXPIRE/TTL/KEYS + glob matching
│   ├── lists.rs         #   LPUSH/LRANGE/…
│   └── hashes.rs        #   HSET/HGETALL/…
├── storage/             # the LSM-tree engine
│   ├── mod.rs           #   Engine facade + read/write paths + recovery
│   ├── record.rs        #   on-disk KV record format (puts & tombstones)
│   ├── memtable.rs      #   in-memory sorted write buffer (BTreeMap)
│   ├── wal.rs           #   write-ahead log: append, replay, rotate
│   ├── sstable.rs       #   immutable sorted run: data + index + footer
│   ├── manifest.rs      #   authoritative SSTable ordering (atomic rewrite)
│   └── compaction.rs    #   background flush + merge-compaction thread
└── server/              # network front-end
    ├── mod.rs           #   TCP accept loop + per-connection RESP loop
    └── thread_pool.rs   #   Arc<Mutex<Receiver>> worker pool
```

### The LSM-tree write/read paths

**Write** `SET k v`:
1. The command serializes the value and appends it to the **WAL** (durable).
2. It inserts into the in-memory **memtable** (a sorted `BTreeMap`).
3. When the memtable crosses the size threshold, the background thread rotates
   it out and **flushes** it to a new immutable **SSTable** on disk, then drops
   the now-redundant WAL segment.
4. When too many SSTables accumulate, the background thread **compacts** them by
   merging into one, dropping overwritten values and tombstones.

**Read** `GET k` checks, newest → oldest: active memtable → memtables currently
being flushed → SSTables (each an in-memory index lookup + one disk seek).
The first hit wins; a **tombstone** counts as "absent".

**Deletes** write a tombstone so that a key in an older SSTable is correctly
shadowed until compaction physically removes it.

### Concurrency model

* `RwLock<MemTable>` — many concurrent readers; writers hold it only for the
  brief in-memory insert.
* `Mutex<Wal>` doubles as the **writer serializer**, guaranteeing WAL order
  matches memtable order. The slow `fsync` happens here, *not* under the
  memtable lock, so it never blocks readers.
* `RwLock<Vec<Arc<SSTable>>>` — read-mostly; flush/compaction swap it under the
  write lock. `Arc` lets a reader keep using a table even as compaction retires
  it from the list.
* A worker **thread pool** (`Arc<Mutex<Receiver>>`) serves connections; a
  dedicated background thread owns flush & compaction.
* The command layer adds **striped per-key locks** so read-modify-write
  commands (`INCR`, `LPUSH`, `HSET`, …) are atomic for a key while different
  keys proceed in parallel.

### On-disk formats

All integers are little-endian, written by hand.

```
Record (WAL + SSTable data):  flag:u8 | key_len:u32 | key | [val_len:u32 | val]
SSTable index entry:          key_len:u32 | key | is_tombstone:u8 | offset:u64
SSTable footer (16 bytes):    index_offset:u64 | count:u32 | magic:u32 ("EVRS")
StoredValue:                  version:u8 | has_expiry:u8 | [expire_ms:u64] | type:u8 | payload
MANIFEST:                     next_id:u64 | count:u32 | sstable_ids:u64×count (newest first)
```

## Tests

```sh
cargo test
```

Covers the RESP codec, the record/SSTable/WAL/manifest formats, engine
behaviour (flush, compaction, tombstone shadowing, crash recovery, concurrent
readers/writers), the value (de)serialization, glob matching, and every command
group end-to-end.

## Limitations

This is a learning project, not a Redis replacement. Notably: a single logical
database (no `SELECT` namespaces), `SCAN` returns the whole matching set in one
batch (cursor always `0`), no pub/sub, transactions, replication, or clustering,
and list/hash mutations rewrite the whole value rather than editing in place.
