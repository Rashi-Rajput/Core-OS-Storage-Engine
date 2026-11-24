# CoreOS-Storage

A C11 storage engine for Linux. Each TCP connection gets a pthread. The server listens on loopback and keeps core.db and core.wal in its working directory. Keep both files together. The binary format uses the building machine's native integer representation.

## Build and connect

```sh
make
./coreos 5432
# In a second terminal
nc 127.0.0.1 5432
```

Send one semicolon-terminated statement per line. Each reply ends with an OK or ERROR line. Names are case-insensitive. TEXT literals use single quotes. Integers are signed 32-bit values.

```sql
CREATE TABLE student (id INT, name TEXT);
BEGIN;
INSERT INTO student VALUES (1, 'Asha');
COMMIT;
SELECT name, id FROM student WHERE id = 1;
UPDATE student SET name = 'Mira' WHERE id = 1;
DELETE FROM student WHERE name != 'Mira';
STATS;
```

Outside an explicit transaction, each data or schema statement runs in its own transaction and commits before the final OK. BEGIN holds table locks until COMMIT, ABORT, or disconnect. Execution errors, including duplicate keys, roll back the entire transaction. Syntax errors leave an open transaction active. STATS takes a snapshot of cache counters and reads live index and transaction counts.

## Storage and limits

- Four tables, four columns each, first column INT. Identifiers allow 23 ASCII letters, digits or underscores, starting with a letter or underscore. TEXT holds 31 bytes and a terminating zero. Embedded quotes and multiline strings are unsupported.
- Each table has 12 page numbers and at most 36 slots per page, giving 432 rows. A slot reserves space for the widest supported row. Deletion keeps its offset available for reuse. Page zero stores the catalog. The eight-frame cache pins buffers and scans timestamps for the least recently used unpinned frame.
- Each table has 64 hash buckets. Its fixed entry pool uses page and slot positions as entry numbers. Key equality uses one chain. Other predicates scan the table. Supported comparisons are `=`, `!=`, `<`, `>`, `<=` and `>=`.
- Up to 16 active transactions. Shared readers may enter while a writer waits, so a continuous stream of readers can delay a writer. A catalog lock protects table creation. Deadlocks print the full graph and a cycle. The youngest participant rolls back under its locks before the survivors proceed.
- Input lines allow 2047 bytes before the newline and 64 tokens. The server is intended for trusted local clients.
- The WAL retains the complete history, including catalog updates. Every dirty eviction forces the WAL before the data page. COMMIT forces the log before replying. Startup reconstructs the page file from a blank catalog, redoes every UPDATE, reverses incomplete transactions, then rebuilds the hash index. Rollback writes inverse UPDATEs and a terminal ABORT. This lets subsequent recovery replay completed rollbacks in order, including when another transaction later reuses the same slot. Startup time and log size grow with history.

## Tests and debugging

```sh
make test
```

The three Python standard-library tests start real server processes in temporary directories. They exercise SIGKILL after dirty-page eviction, a two-client deadlock, and shared readers followed by a blocked writer. The crash and deadlock tests print the observed server output.

Run Valgrind, connect from another terminal, then close the client before stopping the server with Ctrl-C.

```sh
valgrind --leak-check=full --track-origins=yes ./coreos 5432
```

Build a separate ThreadSanitizer binary and run the same suite on Linux with a supported TSan runtime.

```sh
gcc -std=c11 -g -Wall -pthread -fsanitize=thread -fno-omit-frame-pointer *.c -o coreos-tsan
COREOS_BIN="$PWD/coreos-tsan" TSAN_OPTIONS=halt_on_error=1 python3 -m unittest discover -s tests -v
```

To inspect a deadlock interactively, start GDB, then use two clients to update different tables inside BEGIN and request each other's table.

```sh
gdb --args ./coreos 5432
```

```gdb
break report
run
info threads
thread apply all bt
continue
```
