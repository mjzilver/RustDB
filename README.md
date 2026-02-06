# rustdb

A learning project: a persistent key-value database server in Rust with write-ahead logging and crash recovery.

## What It Does

- **In-memory key-value store** with TCP interface 
- **Write-ahead log (WAL)** for crash recovery - all writes are durable
- **Automatic snapshots** to compress WAL when it exceeds configured size

## Commands

Connect via `nc localhost 4210`:

```sh
# Mutating commands
put <key> <value>    # Store a key-value pair
delete <key>         # Remove a key

# Viewing commands
get <key>            # Retrieve a value
range <start> <end>  # Query keys in range
keys <needle>        # Search keys
values <needle>      # Search values

# Utility commands
amount               # Count total keys
exit                 # Close connection
shutdown             # Stop server
```

## Todo:
- [X] WAL log
- [X] Snapshotting
- [X] Error handling
- [ ] Proper logging
- [X] Search command
- [X] Range queries