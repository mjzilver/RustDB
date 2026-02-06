# rustdb

A learning project: a persistent key-value database server in Rust with write-ahead logging and crash recovery.

## What It Does

- **In-memory key-value store** with TCP interface 
- **Write-ahead log (WAL)** for crash recovery - all writes are durable
- **Automatic snapshots** to compress WAL when it exceeds configured size

## Commands

Connect via `nc localhost 4210`:

```
put <key> <value>
get <key>
delete <key>
amount
shutdown
exit
```
## Todo:
- [X] WAL log
- [X] Snapshotting
- [X] Error handling
- [ ] Proper logging
- [ ] Search command
- [ ] Range queries