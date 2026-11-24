# CoreOS-Storage

## common.h

```c
#ifndef COMMON_H
#define COMMON_H
#define _POSIX_C_SOURCE 200809L
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#define PAGE 4096
#define FRAMES 8
#define TABLES 4
#define COLS 4
#define NAME 24
#define TEXT 32
#define PAGES 12
#define SLOTS 36
#define BUCKETS 64
#define TXNS 16
#define TOKENS 64
#define LINE 2048
#define IMAGE 160
#define ROW_BYTES (4 + (COLS - 1) * TEXT)
#define CHECK(x) do { if (!(x)) { perror(#x); exit(1); } } while (0)

enum { LOG_BEGIN, LOG_UPDATE, LOG_COMMIT, LOG_ABORT };
enum { CREATE, INSERT, SELECT, UPDATE, DELETE_ROW, BEGIN, COMMIT, ABORT, STATS };
typedef struct { char name[NAME], col[COLS][NAME]; int n, width[COLS]; } Table;
typedef struct { int len; unsigned char data[IMAGE]; } Image;
typedef struct { int id; int64_t last; } Tx;
typedef struct {
    int op, n, columns, where, types[COLS];
    char table[TEXT], col[COLS][TEXT], val[COLS][TEXT], wc[TEXT], cmp[3], wv[TEXT];
    int quoted[COLS], wquoted;
} Query;
extern Table tables[TABLES];
extern Tx tx[TXNS];
extern atomic_int index_count, active_count;

void disk_open(void);
unsigned char *pin(int page);
void unpin(int page, int dirty);
void cache_stats(FILE *out);
int page_slots(int page);
int free_slot(int page);
Image read_image(int page, int slot);
void put_image(int page, int slot, Image img);
void index_init(void);
int index_find(int t, int key);
void index_change(int page, int slot, Image old, Image next);
void index_rebuild(void);
void locks_init(int last_id);
int reserve_tx(void);
int take_lock(int x, int table, int exclusive);
void release_tx(int x, int commit);
int start_tx(void);
void finish_tx(int *session_tx, int commit);
void wal_open(void);
void wal_mark(int x, int type);
void change(int x, int page, int slot, Image img);
void wal_undo(int x);
void wal_sync(void);
void recover(void);
int parse(char *line, Query *q, char *err);
int execute(int x, Query *q, FILE *out, char *err);
#endif
```

## disk_cache.c

```c
#include "common.h"

static struct {
    unsigned char data[PAGE];
    int used, page, pins, dirty;
    uint64_t stamp;
} frame[FRAMES];
static int fd, total;
static uint64_t clock_tick, hits, misses, reads, writes;
static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t ready = PTHREAD_COND_INITIALIZER;

// Recovery reconstructs the page file from the complete WAL.
void disk_open(void) {
    fd = open("core.db", O_RDWR | O_CREAT, 0600);
    CHECK(fd >= 0);
    CHECK(ftruncate(fd, 0) == 0);
    CHECK(ftruncate(fd, PAGE) == 0);
    total = 1;
}

// Handle short reads and writes on a regular file.
static void transfer(int i, int writing) {
    int done = 0;
    off_t pos = (off_t)frame[i].page * PAGE;
    while (done < PAGE) {
        unsigned char *buf = frame[i].data + done;
        ssize_t n = writing ? pwrite(fd, buf, PAGE - done, pos + done)
                            : pread(fd, buf, PAGE - done, pos + done);
        if (n < 0 && errno == EINTR) continue;
        CHECK(n > 0);
        done += (int)n;
    }
}

// Choose a free frame or the oldest frame with no pins.
static int victim(void) {
    int best = -1;
    for (int i = 0; i < FRAMES; i++) {
        if (!frame[i].used) return i;
        if (!frame[i].pins && (best < 0 || frame[i].stamp < frame[best].stamp))
            best = i;
    }
    return best;
}

// Load a page after making its victim's logged changes durable.
static void load(int i, int page) {
    if (frame[i].dirty) {
        wal_sync();
        transfer(i, 1);
        CHECK(fsync(fd) == 0);
        writes++;
    }
    frame[i].page = page;
    frame[i].used = 1;
    frame[i].dirty = 0;
    if (page >= total) {
        total = page + 1;
        CHECK(ftruncate(fd, (off_t)total * PAGE) == 0);
        memset(frame[i].data, 0, PAGE);
    } else {
        transfer(i, 0);
        reads++;
    }
}

// Keep the returned buffer resident until the caller unpins it.
unsigned char *pin(int page) {
    pthread_mutex_lock(&mu);
    int i;
    for (;;) {
        for (i = 0; i < FRAMES; i++)
            if (frame[i].used && frame[i].page == page) break;
        if (i < FRAMES) { hits++; break; }
        i = victim();
        if (i >= 0) { misses++; load(i, page); break; }
        pthread_cond_wait(&ready, &mu);
    }
    frame[i].stamp = ++clock_tick;
    frame[i].pins++;
    pthread_mutex_unlock(&mu);
    return frame[i].data;
}

// Publish a change before allowing replacement.
void unpin(int page, int dirty) {
    pthread_mutex_lock(&mu);
    for (int i = 0; i < FRAMES; i++) {
        if (frame[i].used && frame[i].page == page) {
            frame[i].dirty |= dirty;
            frame[i].pins--;
            break;
        }
    }
    pthread_cond_signal(&ready);
    pthread_mutex_unlock(&mu);
}

// Snapshot counters before writing to the client.
void cache_stats(FILE *out) {
    pthread_mutex_lock(&mu);
    unsigned long long h = hits, m = misses, r = reads, w = writes;
    int pages = total;
    pthread_mutex_unlock(&mu);
    fprintf(out, "cache hits: %llu\ncache misses: %llu\ndisk reads: %llu\n"
            "disk writes: %llu\ntotal pages: %d\nhash index entries: %d\n"
            "active transactions: %d\n", h, m, r, w, pages, index_count, active_count);
}
```

## page.c

```c
#include "common.h"

typedef struct { uint16_t count, free_end; } Header;
typedef struct { uint16_t offset, length; } Slot;
_Static_assert(sizeof(Table) <= IMAGE, "catalog image too large");

// Read the number of allocated directory entries.
int page_slots(int page) {
    Header h;
    memcpy(&h, pin(page), sizeof h);
    unpin(page, 0);
    return h.count;
}

// Find a reusable slot or room for another directory entry.
int free_slot(int page) {
    unsigned char *buf = pin(page);
    Header h;
    memcpy(&h, buf, sizeof h);
    int slot;
    for (slot = 0; slot < h.count; slot++) {
        Slot s;
        memcpy(&s, buf + sizeof h + slot * sizeof s, sizeof s);
        if (!s.length) break;
    }
    int end = h.count ? h.free_end : PAGE;
    if (slot == h.count && (slot == SLOTS ||
        end - ROW_BYTES < (int)(sizeof h + (slot + 1) * sizeof(Slot)))) slot = -1;
    unpin(page, 0);
    return slot;
}

// Copy a row or catalog entry into a WAL-sized image.
Image read_image(int page, int slot) {
    Image img = {0};
    unsigned char *buf = pin(page);
    if (page == 0) {
        img.len = sizeof(Table);
        memcpy(img.data, buf + slot * sizeof(Table), img.len);
    } else {
        Header h;
        memcpy(&h, buf, sizeof h);
        if (slot < h.count) {
            Slot s;
            memcpy(&s, buf + sizeof h + slot * sizeof s, sizeof s);
            img.len = s.length;
            if (img.len > IMAGE || img.len > ROW_BYTES || s.offset > PAGE - img.len) {
                fprintf(stderr, "invalid row on page %d slot %d\n", page, slot);
                exit(1);
            }
            memcpy(img.data, buf + s.offset, img.len);
        }
    }
    unpin(page, 0);
    return img;
}

// Install an image in its original slot, retaining space on deletion.
void put_image(int page, int slot, Image img) {
    unsigned char *buf = pin(page);
    if (page == 0) {
        memcpy(buf + slot * sizeof(Table), img.data, sizeof(Table));
        memcpy(&tables[slot], img.data, sizeof(Table));
    } else {
        Header h;
        Slot s;
        memcpy(&h, buf, sizeof h);
        unsigned char *entry = buf + sizeof h + slot * sizeof s;
        if (slot == h.count) {
            if (!h.count) h.free_end = PAGE;
            h.count++;
            h.free_end -= ROW_BYTES;
            s.offset = h.free_end;
            memcpy(buf, &h, sizeof h);
        } else memcpy(&s, entry, sizeof s);
        s.length = img.len;
        memcpy(entry, &s, sizeof s);
        memcpy(buf + s.offset, img.data, img.len);
    }
    unpin(page, 1);
}
```

## hash.c

```c
#include "common.h"

static int head[TABLES][BUCKETS];
static struct { int key, next; } entry[TABLES][PAGES * SLOTS];
atomic_int index_count;

// Convert negative keys to unsigned before taking the remainder.
static unsigned bucket(int key) {
    return (unsigned)key % BUCKETS;
}

// An entry's pool index encodes its page within the table and slot.
void index_init(void) {
    for (int t = 0; t < TABLES; t++)
        for (int b = 0; b < BUCKETS; b++) head[t][b] = -1;
}

// Walk one bucket chain under the caller's table lock.
int index_find(int t, int key) {
    for (int i = head[t][bucket(key)]; i >= 0; i = entry[t][i].next)
        if (entry[t][i].key == key) return i;
    return -1;
}

// Unlink the old location and insert the replacement key.
void index_change(int page, int slot, Image old, Image next) {
    if (!page) return;
    int t = (page - 1) / PAGES, i = (page - 1) % PAGES * SLOTS + slot;
    int key;
    if (old.len) {
        memcpy(&key, old.data, 4);
        int *link = &head[t][bucket(key)];
        while (*link >= 0 && *link != i) link = &entry[t][*link].next;
        if (*link == i) { *link = entry[t][i].next; index_count--; }
    }
    if (next.len) {
        memcpy(&key, next.data, 4);
        entry[t][i].key = key;
        entry[t][i].next = head[t][bucket(key)];
        head[t][bucket(key)] = i;
        index_count++;
    }
}

// Scan allocated tables after both recovery passes.
void index_rebuild(void) {
    index_init();
    index_count = 0;
    for (int t = 0; t < TABLES; t++) {
        if (!tables[t].n) continue;
        for (int p = 1 + t * PAGES; p <= (t + 1) * PAGES; p++) {
            int n = page_slots(p);
            for (int s = 0; s < n; s++)
                index_change(p, s, (Image){0}, read_image(p, s));
        }
    }
    printf("hash index rebuilt: %d entries\n", index_count);
}
```

## lock.c

```c
#include "common.h"

Tx tx[TXNS];
atomic_int active_count;
enum tx_state { UNUSED, ACTIVE, DEADLOCK_VICTIM, COMMITTED, ABORTED };
static enum tx_state state[TXNS];
static int held[TXNS][TABLES + 1], want[TXNS], mode[TXNS];
static int graph[TXNS][TXNS], path[TXNS], seen[TXNS], next_id;
static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t changed = PTHREAD_COND_INITIALIZER;

// Recovery supplies the highest previously used id.
void locks_init(int last_id) {
    next_id = last_id;
    for (int i = 0; i < TXNS; i++) want[i] = -1;
}

// Reserve a transaction slot before its BEGIN record.
int reserve_tx(void) {
    pthread_mutex_lock(&mu);
    int x = 0;
    while (x < TXNS && (state[x] == ACTIVE || state[x] == DEADLOCK_VICTIM)) x++;
    if (x < TXNS && next_id < INT_MAX) {
        tx[x] = (Tx){++next_id, -1};
        state[x] = ACTIVE;
        active_count++;
    } else x = -1;
    pthread_mutex_unlock(&mu);
    return x;
}

// Recompute every edge since holders may have changed while we slept.
static int edges(int x) {
    memset(graph, 0, sizeof graph);
    int blocked = 0;
    for (int a = 0; a < TXNS; a++) {
        if (want[a] < 0 || state[a] != ACTIVE) continue;
        for (int b = 0; b < TXNS; b++) {
            int lock = held[b][want[a]];
            if (a != b && lock && (mode[a] == 2 || lock == 2)) {
                graph[a][b] = 1;
                if (a == x) blocked = 1;
            }
        }
    }
    return blocked;
}

// Search for a path back to the newly waiting transaction.
static int cycle(int x, int depth) {
    seen[x] = 1;
    path[depth] = x;
    for (int b = 0; b < TXNS; b++) {
        if (!graph[x][b]) continue;
        if (b == path[0]) return depth + 1;
        if (!seen[b]) {
            int n = cycle(b, depth + 1);
            if (n) return n;
        }
    }
    return 0;
}

// Wake the victim's own thread to undo its work under its locks.
static void report(int n) {
    printf("[DEADLOCK] wait-for graph:\n");
    for (int a = 0; a < TXNS; a++)
        for (int b = 0; b < TXNS; b++)
            if (graph[a][b]) printf("  T%d -> T%d\n", tx[a].id, tx[b].id);
    printf("[DEADLOCK] cycle found: ");
    int victim = path[0];
    for (int i = 0; i < n; i++) {
        printf("T%d -> ", tx[path[i]].id);
        if (tx[path[i]].id > tx[victim].id) victim = path[i];
    }
    printf("T%d\n", tx[path[0]].id);
    printf("[DEADLOCK] victim: T%d (youngest), aborting and releasing locks\n",
           tx[victim].id);
    state[victim] = DEADLOCK_VICTIM;
    pthread_cond_broadcast(&changed);
}

// Retain shared locks during an upgrade to exclusive access.
int take_lock(int x, int table, int exclusive) {
    pthread_mutex_lock(&mu);
    want[x] = table;
    mode[x] = exclusive ? 2 : 1;
    while (state[x] == ACTIVE && held[x][table] < mode[x] && edges(x)) {
        memset(seen, 0, sizeof seen);
        int n = cycle(x, 0);
        if (n) report(n);
        if (state[x] == ACTIVE) pthread_cond_wait(&changed, &mu);
    }
    int ok = state[x] == ACTIVE;
    if (ok && held[x][table] < mode[x]) held[x][table] = mode[x];
    want[x] = -1;
    pthread_mutex_unlock(&mu);
    return ok;
}

// Publish completion only after commit or rollback has finished.
void release_tx(int x, int commit) {
    pthread_mutex_lock(&mu);
    memset(held[x], 0, sizeof held[x]);
    tx[x] = (Tx){0, -1};
    state[x] = commit ? COMMITTED : ABORTED;
    active_count--;
    pthread_cond_broadcast(&changed);
    pthread_mutex_unlock(&mu);
}
```

## wal.c

```c
#include "common.h"
#include <sys/file.h>

typedef struct {
    int64_t lsn, prev;
    int id, type, page, slot;
    Image before, after;
} Record;
static int fd;
static int64_t end;
static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;

// Use fixed record offsets for both sequential replay and backward walks.
static void log_io(Record *r, int64_t pos, int writing) {
    size_t done = 0;
    while (done < sizeof *r) {
        char *buf = (char *)r + done;
        off_t at = pos * sizeof *r + done;
        ssize_t n = writing ? pwrite(fd, buf, sizeof *r - done, at)
                            : pread(fd, buf, sizeof *r - done, at);
        if (n < 0 && errno == EINTR) continue;
        CHECK(n > 0);
        done += (size_t)n;
    }
}

// Exclude a second server and discard an unfinished trailing append.
void wal_open(void) {
    fd = open("core.wal", O_RDWR | O_CREAT, 0600);
    CHECK(fd >= 0);
    CHECK(flock(fd, LOCK_EX | LOCK_NB) == 0);
    struct stat st;
    CHECK(fstat(fd, &st) == 0);
    end = st.st_size / sizeof(Record);
    CHECK(ftruncate(fd, end * sizeof(Record)) == 0);
    int dir = open(".", O_RDONLY);
    CHECK(dir >= 0 && fsync(dir) == 0);
    CHECK(close(dir) == 0);
}

// Serialize appends so LSN order matches log-file order.
static void append(int x, Record *r) {
    pthread_mutex_lock(&mu);
    r->lsn = end;
    r->prev = tx[x].last;
    r->id = tx[x].id;
    log_io(r, end, 1);
    tx[x].last = end++;
    pthread_mutex_unlock(&mu);
}

// Force all preceding appends before commit acknowledgement or page write.
void wal_sync(void) {
    pthread_mutex_lock(&mu);
    CHECK(fsync(fd) == 0);
    pthread_mutex_unlock(&mu);
}

// Append a transaction boundary.
void wal_mark(int x, int type) {
    Record r = {.type = type, .page = -1, .slot = -1};
    append(x, &r);
    if (type == LOG_COMMIT || type == LOG_ABORT) wal_sync();
}

// Log the original and replacement bytes before touching a frame.
void change(int x, int page, int slot, Image img) {
    Record r = {.type = LOG_UPDATE, .page = page, .slot = slot, .after = img};
    r.before = read_image(page, slot);
    append(x, &r);
    put_image(page, slot, img);
    index_change(page, slot, r.before, img);
}

// Follow this transaction's original chain, logging each inverse update.
void wal_undo(int x) {
    for (int64_t p = tx[x].last; p >= 0;) {
        Record r;
        log_io(&r, p, 0);
        if (r.type == LOG_UPDATE) change(x, r.page, r.slot, r.before);
        p = r.prev;
    }
}

// Track only currently open transactions during the forward scan.
static int replay(Record *r) {
    int x = 0;
    while (x < TXNS && tx[x].id != (r->type == LOG_BEGIN ? 0 : r->id)) x++;
    CHECK(x < TXNS && r->id > 0);
    tx[x] = (Tx){r->id, r->lsn};
    if (r->type == LOG_UPDATE) put_image(r->page, r->slot, r->after);
    if (r->type == LOG_COMMIT || r->type == LOG_ABORT) tx[x].id = 0;
    return r->type == LOG_UPDATE;
}

// Redo history, then undo losers in reverse global LSN order.
void recover(void) {
    int64_t stop = end;
    long long redo = 0, undo = 0;
    int highest = 0;
    for (int64_t p = 0; p < stop; p++) {
        Record r;
        log_io(&r, p, 0);
        CHECK(r.lsn == p);
        redo += replay(&r);
        if (r.id > highest) highest = r.id;
    }
    for (int64_t p = stop; p-- > 0;) {
        Record r;
        log_io(&r, p, 0);
        if (r.type != LOG_UPDATE) continue;
        for (int x = 0; x < TXNS; x++) {
            if (tx[x].id != r.id) continue;
            Record back = {.type = LOG_UPDATE, .page = r.page, .slot = r.slot,
                           .before = r.after, .after = r.before};
            append(x, &back);
            put_image(r.page, r.slot, r.before);
            undo++;
        }
    }
    printf("recovery: redo %lld records, undo %lld records (incomplete txns: ", redo, undo);
    int count = 0;
    for (int x = 0; x < TXNS; x++) {
        if (!tx[x].id) continue;
        printf("%s%d", count++ ? ", " : "", tx[x].id);
        wal_mark(x, LOG_ABORT);
        tx[x].id = 0;
    }
    printf("%s)\n", count ? "" : "none");
    locks_init(highest);
}
```

## txn.c

```c
#include "common.h"

// Start an explicit or single-statement transaction.
int start_tx(void) {
    int x = reserve_tx();
    if (x >= 0) wal_mark(x, LOG_BEGIN);
    return x;
}

// Finish the log work before releasing strict two-phase locks.
void finish_tx(int *session_tx, int commit) {
    int x = *session_tx;
    if (!commit) wal_undo(x);
    wal_mark(x, commit ? LOG_COMMIT : LOG_ABORT);
    *session_tx = -1;
    release_tx(x, commit);
}
```

## sql.c

```c
#include "common.h"

typedef struct { char s[TEXT]; int kind; } Token;
typedef struct { Token a[TOKENS + 1]; int i, n; char *err; } Parser;

// Split a single line, retaining the distinction between strings and names.
static int tokenize(char *s, Parser *p) {
    while (*s) {
        if (isspace((unsigned char)*s)) { s++; continue; }
        if (p->n == TOKENS) { strcpy(p->err, "too many tokens"); return 0; }
        Token *t = &p->a[p->n++];
        char *start = s;
        if (*s == '\'') {
            start = ++s;
            while (*s && *s != '\'') s++;
            if (!*s) { strcpy(p->err, "unterminated string"); return 0; }
            t->kind = 1;
        } else if (isalnum((unsigned char)*s) || *s == '_' ||
                   (*s == '-' && isdigit((unsigned char)s[1]))) {
            s++;
            while (isalnum((unsigned char)*s) || *s == '_') s++;
            t->kind = 2;
        } else {
            s++;
            if (strchr("!<>", *start) && *s == '=') s++;
        }
        size_t n = s - start;
        if (n >= TEXT) {
            strcpy(p->err, t->kind == 1 ? "text value too long" : "token too long");
            return 0;
        }
        memcpy(t->s, start, n);
        if (t->kind == 1) s++;
    }
    return 1;
}

// Consume a keyword or punctuation token.
static int eat(Parser *p, const char *s) {
    Token *t = &p->a[p->i];
    if (t->kind == 1 || strcasecmp(t->s, s)) return 0;
    p->i++;
    return 1;
}

// Report the first unexpected feature.
static int bad(Parser *p) {
    const char *s = p->a[p->i].s;
    snprintf(p->err, LINE, "unsupported: %s", *s ? s : "end of statement");
    return 0;
}

// Read a case-insensitive identifier.
static int name(Parser *p, char *dst) {
    Token *t = &p->a[p->i];
    if (t->kind != 2 || !(isalpha((unsigned char)t->s[0]) || t->s[0] == '_') ||
        strlen(t->s) >= NAME || !strcasecmp(t->s, "NULL")) return bad(p);
    for (int i = 0; t->s[i]; i++) dst[i] = tolower((unsigned char)t->s[i]);
    p->i++;
    return 1;
}

// Values are quoted strings or signed decimal integers.
static int value(Parser *p, char *dst, int *quoted) {
    Token *t = &p->a[p->i];
    if (t->kind != 1) {
        char *end;
        errno = 0;
        long v = strtol(t->s, &end, 10);
        if (end == t->s || *end) return bad(p);
        if (errno || v < INT32_MIN || v > INT32_MAX) {
            strcpy(p->err, "integer out of range");
            return 0;
        }
    }
    strcpy(dst, t->s);
    *quoted = t->kind == 1;
    p->i++;
    return 1;
}

// Read CREATE columns or INSERT values.
static int items(Parser *p, Query *q) {
    if (!eat(p, "(")) return bad(p);
    do {
        int i = q->n++;
        if (i == COLS) { strcpy(p->err, "too many columns or values"); return 0; }
        if (q->op == INSERT) {
            if (!value(p, q->val[i], &q->quoted[i])) return 0;
        } else {
            if (!name(p, q->col[i])) return 0;
            if (eat(p, "INT")) q->types[i] = 4;
            else if (eat(p, "TEXT")) q->types[i] = TEXT;
            else return bad(p);
        }
    } while (eat(p, ","));
    return eat(p, ")") || bad(p);
}

// Parse the optional single comparison.
static int condition(Parser *p, Query *q) {
    if (!eat(p, "WHERE")) return 1;
    q->where = 1;
    if (!name(p, q->wc)) return 0;
    const char *ops[] = {"=", "!=", "<", ">", "<=", ">="};
    for (int i = 0; i < 6; i++) {
        if (eat(p, ops[i])) {
            strcpy(q->cmp, ops[i]);
            return value(p, q->wv, &q->wquoted);
        }
    }
    return bad(p);
}

// Parse the table and column positions for a data statement.
static int statement(Parser *p, Query *q) {
    if (q->op == CREATE && !eat(p, "TABLE")) return bad(p);
    if (q->op == INSERT && !eat(p, "INTO")) return bad(p);
    if (q->op == SELECT && !eat(p, "*")) {
        do {
            if (q->columns == COLS) return bad(p);
            if (!name(p, q->col[q->columns++])) return 0;
        } while (eat(p, ","));
    }
    if ((q->op == SELECT || q->op == DELETE_ROW) && !eat(p, "FROM")) return bad(p);
    if (!name(p, q->table)) return 0;
    if (q->op == INSERT && !eat(p, "VALUES")) return bad(p);
    if (q->op == CREATE || q->op == INSERT) return items(p, q);
    if (q->op == UPDATE) {
        if (!eat(p, "SET")) return bad(p);
        if (!name(p, q->col[0])) return 0;
        if (!eat(p, "=")) return bad(p);
        if (!value(p, q->val[0], &q->quoted[0])) return 0;
    }
    return condition(p, q);
}

// Accept exactly one complete statement.
int parse(char *line, Query *q, char *err) {
    Parser p = {.err = err};
    const char *words[] = {"CREATE", "INSERT", "SELECT", "UPDATE", "DELETE",
                           "BEGIN", "COMMIT", "ABORT", "STATS"};
    if (!tokenize(line, &p)) return 0;
    for (q->op = CREATE; q->op <= STATS; q->op++)
        if (eat(&p, words[q->op])) break;
    if (q->op > STATS) return bad(&p);
    if (q->op < BEGIN && !statement(&p, q)) return 0;
    if (!eat(&p, ";")) return bad(&p);
    return p.i == p.n || bad(&p);
}
```

## exec.c

```c
#include "common.h"

Table tables[TABLES];
_Static_assert(sizeof(int) == 4, "requires 32-bit int");
typedef struct {
    int t, wc, set, offset[COLS], proj[COLS], n, width, row_size;
    unsigned char filter[TEXT], replacement[TEXT];
} Plan;

// Save one error message for the server's final response line.
static int fail(char *err, const char *msg) {
    strcpy(err, msg);
    return -1;
}

// Resolve a column once before visiting rows.
static int column(Table *t, char *name, char *err) {
    for (int i = 0; i < t->n; i++)
        if (!strcmp(t->col[i], name)) return i;
    snprintf(err, LINE, "unknown column: %s", name);
    return -1;
}

// Encode a literal in its column's fixed width.
static int encode(unsigned char *dst, int width, char *s, int quoted, char *err) {
    if ((width == TEXT) != quoted) return fail(err, "type mismatch");
    if (quoted) {
        if (strnlen(s, TEXT) == TEXT) return fail(err, "text value too long");
        memset(dst, 0, TEXT);
        memcpy(dst, s, strlen(s));
    } else {
        int v = (int)strtol(s, NULL, 10);
        memcpy(dst, &v, 4);
    }
    return 0;
}

// Install a new schema under the exclusive catalog lock.
static int create_table(int x, Query *q, char *err) {
    Table t = {.n = q->n};
    if (q->types[0] != 4) return fail(err, "first column must be INT");
    strcpy(t.name, q->table);
    for (int i = 0; i < t.n; i++) {
        for (int j = 0; j < i; j++)
            if (!strcmp(t.col[j], q->col[i])) return fail(err, "duplicate column");
        strcpy(t.col[i], q->col[i]);
        t.width[i] = q->types[i];
    }
    int slot = 0;
    while (slot < TABLES && tables[slot].n) slot++;
    if (slot == TABLES) return fail(err, "table limit reached");
    Image img = {.len = sizeof t};
    memcpy(img.data, &t, sizeof t);
    change(x, 0, slot, img);
    return 0;
}

// Encode and insert one uniquely keyed row.
static int insert_row(int x, int t, Query *q, char *err) {
    Table *table = &tables[t];
    Image img = {0};
    if (q->n != table->n) return fail(err, "wrong number of values");
    for (int i = 0; i < q->n; i++) {
        if (encode(img.data + img.len, table->width[i], q->val[i], q->quoted[i], err) < 0)
            return -1;
        img.len += table->width[i];
    }
    int key;
    memcpy(&key, img.data, 4);
    if (index_find(t, key) >= 0) return fail(err, "duplicate key");
    for (int p = 1 + t * PAGES; p <= (t + 1) * PAGES; p++) {
        int slot = free_slot(p);
        if (slot >= 0) { change(x, p, slot, img); return 1; }
    }
    return fail(err, "table full");
}

// Resolve column offsets, the projection and comparison types.
static int prepare(Plan *p, Query *q, char *err) {
    Table *t = &tables[p->t];
    for (int i = 1; i < t->n; i++)
        p->offset[i] = p->offset[i - 1] + t->width[i - 1];
    if (q->where) {
        p->wc = column(t, q->wc, err);
        if (p->wc < 0) return -1;
        p->width = t->width[p->wc];
        if (encode(p->filter, p->width, q->wv, q->wquoted, err) < 0) return -1;
    }
    if (q->op == UPDATE) {
        p->set = column(t, q->col[0], err);
        if (p->set < 0) return -1;
        return encode(p->replacement, t->width[p->set], q->val[0], q->quoted[0], err);
    }
    p->n = q->columns ? q->columns : t->n;
    for (int i = 0; i < p->n; i++) {
        p->proj[i] = q->columns ? column(t, q->col[i], err) : i;
        if (p->proj[i] < 0) return -1;
    }
    return 0;
}

// Compare integers without subtracting values that could overflow.
static int matches(Plan *p, Query *q, Image *img) {
    if (!q->where) return 1;
    unsigned char *v = img->data + p->offset[p->wc];
    int cmp;
    if (p->width == TEXT) cmp = strcmp((char *)v, (char *)p->filter);
    else {
        int a, b;
        memcpy(&a, v, 4);
        memcpy(&b, p->filter, 4);
        cmp = (a > b) - (a < b);
    }
    if (!strcmp(q->cmp, "!=")) return cmp != 0;
    return (q->cmp[0] == '<' && cmp < 0) || (q->cmp[0] == '>' && cmp > 0) ||
           (strchr(q->cmp, '=') && cmp == 0);
}

// Print column names or the projected fields of one record.
static void print_row(Plan *p, Image *img, FILE *out) {
    Table *t = &tables[p->t];
    for (int i = 0; i < p->n; i++) {
        int c = p->proj[i];
        fputs("| ", out);
        if (!img) fputs(t->col[c], out);
        else if (t->width[c] == TEXT) fputs((char *)img->data + p->offset[c], out);
        else {
            int v;
            memcpy(&v, img->data + p->offset[c], 4);
            fprintf(out, "%d", v);
        }
        fputc(' ', out);
    }
    fputs("|\n", out);
}

// Apply the statement to one candidate location.
static int visit(int x, Plan *p, Query *q, int pos, FILE *out, char *err) {
    int page = 1 + p->t * PAGES + pos / SLOTS, slot = pos % SLOTS;
    Image img = read_image(page, slot);
    if (!img.len) return 0;
    if (img.len != p->row_size) return fail(err, "invalid stored row size");
    if (!matches(p, q, &img)) return 0;
    if (q->op == SELECT) print_row(p, &img, out);
    else {
        if (q->op == DELETE_ROW) img = (Image){0};
        else {
            if (p->set == 0) {
                int key;
                memcpy(&key, p->replacement, 4);
                int other = index_find(p->t, key);
                if (other >= 0 && other != pos) return fail(err, "duplicate key");
            }
            memcpy(img.data + p->offset[p->set], p->replacement, tables[p->t].width[p->set]);
        }
        change(x, page, slot, img);
    }
    return 1;
}

// Use the index for key equality and scan page directories otherwise.
static int run(int x, Plan *p, Query *q, FILE *out, char *err) {
    if (prepare(p, q, err) < 0) return -1;
    if (q->op == SELECT) { print_row(p, NULL, out); fputs("--------\n", out); }
    if (q->where && p->wc == 0 && !strcmp(q->cmp, "=")) {
        int key;
        memcpy(&key, p->filter, 4);
        int pos = index_find(p->t, key);
        return pos < 0 ? 0 : visit(x, p, q, pos, out, err);
    }
    int count = 0;
    for (int i = 0; i < PAGES; i++) {
        int n = page_slots(1 + p->t * PAGES + i);
        for (int s = 0; s < n; s++) {
            int rows = visit(x, p, q, i * SLOTS + s, out, err);
            if (rows < 0) return -1;
            count += rows;
        }
    }
    return count;
}

// Lock the catalog before resolving a name, then lock its table.
int execute(int x, Query *q, FILE *out, char *err) {
    if (!take_lock(x, TABLES, q->op == CREATE)) return -2;
    int t = 0;
    while (t < TABLES && strcmp(tables[t].name, q->table)) t++;
    if (q->op == CREATE) {
        if (t < TABLES) return fail(err, "table already exists");
        return create_table(x, q, err);
    }
    if (t == TABLES) {
        snprintf(err, LINE, "unknown table: %s", q->table);
        return -1;
    }
    if (!take_lock(x, t, q->op != SELECT)) return -2;
    int row_size = 0;
    if (tables[t].n < 1 || tables[t].n > COLS) return fail(err, "invalid column count");
    for (int i = 0; i < tables[t].n; i++) {
        int width = tables[t].width[i];
        if (width != 4 && width != TEXT) return fail(err, "invalid column width");
        row_size += width;
    }
    if (row_size > IMAGE || row_size > ROW_BYTES) return fail(err, "row too large");
    if (q->op == INSERT) return insert_row(x, t, q, err);
    Plan p = {.t = t, .row_size = row_size};
    return run(x, &p, q, out, err);
}
```

## server.c

```c
#include "common.h"
#include <arpa/inet.h>
#include <signal.h>

// Handle transaction control without creating an extra transaction.
static void control(int *x, int op, FILE *out) {
    if (op == STATS) { cache_stats(out); fputs("OK\n", out); return; }
    if (op == BEGIN) {
        if (*x >= 0) { fputs("ERROR transaction already active\n", out); return; }
        *x = start_tx();
        if (*x < 0) fputs("ERROR transaction limit reached\n", out);
        else fprintf(out, "OK transaction %d\n", tx[*x].id);
        return;
    }
    if (*x < 0) { fputs("ERROR no active transaction\n", out); return; }
    finish_tx(x, op == COMMIT);
    fputs("OK\n", out);
}

// Complete rollback before acknowledging a failed data statement.
static void request(int *x, char *line, FILE *out) {
    Query q = {0};
    char err[LINE];
    if (!parse(line, &q, err)) { fprintf(out, "ERROR %s\n", err); return; }
    if (q.op >= BEGIN) { control(x, q.op, out); return; }
    int single = *x < 0;
    if (single) *x = start_tx();
    if (*x < 0) { fputs("ERROR transaction limit reached\n", out); return; }
    int id = tx[*x].id;
    int n = execute(*x, &q, out, err);
    if (single || n < 0) finish_tx(x, n >= 0);
    if (n == -2)
        fprintf(out, "ERROR deadlock detected: transaction %d aborted, locks released\n", id);
    else if (n < 0) fprintf(out, "ERROR %s\n", err);
    else if (q.op == SELECT) fprintf(out, "(%d rows)\nOK\n", n);
    else fprintf(out, "OK %d rows affected\n", n);
}

// Read bounded lines and abort an open transaction on disconnect.
static void *client(void *arg) {
    int fd = (int)(intptr_t)arg, x = -1;
    int copy = dup(fd);
    FILE *in = fdopen(fd, "r");
    FILE *out = copy < 0 ? NULL : fdopen(copy, "w");
    if (in && out) {
        char line[LINE];
        int c, n = 0, invalid = 0;
        while ((c = fgetc(in)) != EOF) {
            if (c != '\n') {
                if (!c || n == LINE - 1) invalid = 1;
                else line[n++] = (char)c;
                continue;
            }
            line[n] = 0;
            if (invalid) fputs("ERROR invalid or oversized line\n", out);
            else request(&x, line, out);
            if (fflush(out) || ferror(out)) break;
            n = invalid = 0;
        }
    }
    if (x >= 0) finish_tx(&x, 0);
    if (in) fclose(in); else close(fd);
    if (out) fclose(out); else if (copy >= 0) close(copy);
    return NULL;
}

// Recover in the working directory and serve one thread per connection.
int main(int argc, char **argv) {
    char *end = NULL;
    long port = argc == 2 ? strtol(argv[1], &end, 10) : 5432;
    if (argc > 2 || (end && (*end || end == argv[1])) || port < 1 || port > 65535) {
        fprintf(stderr, "usage: coreos [port 1..65535]\n");
        return 1;
    }
    setvbuf(stdout, NULL, _IOLBF, 0);
    signal(SIGPIPE, SIG_IGN);
    wal_open();
    disk_open();
    recover();
    index_rebuild();
    int fd = socket(AF_INET, SOCK_STREAM, 0), yes = 1;
    CHECK(fd >= 0);
    CHECK(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) == 0);
    struct sockaddr_in addr = {.sin_family = AF_INET, .sin_port = htons(port),
                               .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    CHECK(bind(fd, (struct sockaddr *)&addr, sizeof addr) == 0);
    CHECK(listen(fd, 32) == 0);
    printf("server listening on port %ld\n", port);
    for (;;) {
        int peer = accept(fd, NULL, NULL);
        if (peer < 0) { if (errno == EINTR) continue; perror("accept"); break; }
        pthread_t thread;
        if (pthread_create(&thread, NULL, client, (void *)(intptr_t)peer)) close(peer);
        else pthread_detach(thread);
    }
    return 1;
}
```

## Makefile

```makefile
CC = gcc
CFLAGS = -std=c11 -g -Wall -pthread
SRC = disk_cache.c page.c hash.c lock.c wal.c txn.c sql.c exec.c server.c

coreos: $(SRC) common.h
	$(CC) $(CFLAGS) $(SRC) -o $@

test: coreos
	python3 -m unittest discover -s tests -v

clean:
	rm -f coreos

.PHONY: test clean
```

## tests/support.py

```python
import os
from pathlib import Path
import select
import socket
import subprocess
import tempfile
import time
import unittest

BINARY = Path(os.environ.get("COREOS_BIN", Path(__file__).resolve().parents[1] / "coreos")).resolve()


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=4)
        self.reader = self.sock.makefile("rb")

    def send(self, sql):
        self.sock.sendall((sql + "\n").encode())

    def reply(self):
        lines = []
        while True:
            line = self.reader.readline()
            if not line:
                raise AssertionError("connection closed before reply")
            lines.append(line.decode().rstrip("\n"))
            if lines[-1].startswith(("OK", "ERROR")):
                return "\n".join(lines)

    def ask(self, sql):
        self.send(sql)
        return self.reply()

    def blocked(self):
        return not select.select([self.sock], [], [], 0.25)[0]

    def close(self):
        self.reader.close()
        self.sock.close()


class EngineTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.folder = Path(tmp.name)
        self.log = self.folder / "stdout"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.clients = []
        self.process = None
        self.addCleanup(self.stop)
        self.start()

    def start(self):
        with self.log.open("w") as log:
            self.process = subprocess.Popen([BINARY, str(self.port)], cwd=self.folder,
                                            stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and self.process.poll() is None:
            if "server listening" in self.log.read_text():
                return
            time.sleep(0.02)
        self.fail(self.log.read_text() or "startup timed out")

    def stop(self):
        if self.process:
            self.process.kill()
            self.process.wait(timeout=4)
            self.process = None
        for client in self.clients:
            client.close()
        self.clients.clear()

    def connect(self):
        client = Client(self.port)
        self.clients.append(client)
        return client

    def ok(self, client, sql):
        result = client.ask(sql)
        self.assertTrue(result.splitlines()[-1].startswith("OK"), result)
        return result
```

## tests/test_crash.py

```python
from support import EngineTest


class CrashTest(EngineTest):
    def test_dirty_eviction_and_repeated_recovery(self):
        a = self.connect()
        self.ok(a, "CREATE TABLE items (id INT, tag TEXT);")
        self.ok(a, "INSERT INTO items VALUES (-1, 'saved');")
        loser = self.ok(a, "BEGIN;").split()[-1]
        self.ok(a, "UPDATE items SET tag = 'uncommitted' WHERE id = -1;")
        self.ok(a, "INSERT INTO items VALUES (63, 'collision');")
        self.ok(a, "SELECT * FROM items;")  # Evict the dirty page from eight frames.
        self.assertIn(b"uncommitted", (self.folder / "core.db").read_bytes())
        self.ok(a, "CREATE TABLE transient (id INT);")
        self.stop()  # SIGKILL while the transaction still holds its locks.
        self.start()
        startup = self.log.read_text()
        print(startup, end="")
        self.assertIn(f"undo 3 records (incomplete txns: {loser})", startup)
        self.assertIn("hash index rebuilt: 1 entries", startup)
        a = self.connect()
        self.assertIn("| saved |", self.ok(a, "SELECT tag FROM items WHERE id = -1;"))
        self.assertIn("(0 rows)", self.ok(a, "SELECT * FROM items WHERE id = 63;"))
        self.assertIn("unknown table", a.ask("SELECT * FROM transient;"))
        self.ok(a, "UPDATE items SET id = 63 WHERE id = -1;")
        self.stop()
        self.start()
        a = self.connect()
        self.assertIn("undo 0 records", self.log.read_text())
        self.assertIn("| saved |", self.ok(a, "SELECT tag FROM items WHERE id = 63;"))
        self.assertEqual(a.ask("INSERT INTO items VALUES (63, 'again');"), "ERROR duplicate key")
```

## tests/test_deadlock.py

```python
from support import EngineTest


class DeadlockTest(EngineTest):
    def test_youngest_victim(self):
        a, b = self.connect(), self.connect()
        for table in ("leftside", "rightside"):
            self.ok(a, f"CREATE TABLE {table} (id INT);")
        old = self.ok(a, "BEGIN;").split()[-1]
        young = self.ok(b, "BEGIN;").split()[-1]
        self.ok(a, "INSERT INTO leftside VALUES (1);")
        self.ok(b, "INSERT INTO rightside VALUES (2);")
        b.send("SELECT * FROM leftside;")
        self.assertTrue(b.blocked())
        a.send("INSERT INTO rightside VALUES (2);")
        self.assertEqual(b.reply(), f"ERROR deadlock detected: transaction {young} aborted, locks released")
        self.assertEqual(a.reply(), "OK 1 rows affected")
        self.assertEqual(b.ask("COMMIT;"), "ERROR no active transaction")
        self.assertNotEqual(self.ok(b, "BEGIN;").split()[-1], young)
        self.ok(b, "ABORT;")
        self.ok(a, "COMMIT;")
        self.assertIn("(1 rows)", self.ok(b, "SELECT * FROM rightside WHERE id = 2;"))
        graph = self.log.read_text().split("[DEADLOCK]", 1)[1]
        print("[DEADLOCK]" + graph, end="")
        self.assertIn(f"  T{old} -> T{young}\n", graph)
        self.assertIn(f"  T{young} -> T{old}\n", graph)
        self.assertIn(f"cycle found: T{old} -> T{young} -> T{old}", graph)
        self.assertIn(f"victim: T{young} (youngest), aborting and releasing locks", graph)
```

## tests/test_blocking.py

```python
from support import EngineTest


class BlockingTest(EngineTest):
    def test_readers_share_and_writer_waits_for_commit(self):
        a, b = self.connect(), self.connect()
        self.ok(a, "CREATE TABLE items (id INT);")
        self.ok(a, "BEGIN;")
        self.ok(a, "SELECT * FROM items;")
        self.ok(b, "BEGIN;")
        self.ok(b, "SELECT * FROM items;")
        b.send("INSERT INTO items VALUES (9);")
        self.assertTrue(b.blocked(), "writer passed a shared lock")
        self.ok(a, "COMMIT;")
        self.assertEqual(b.reply(), "OK 1 rows affected")
        a.send("SELECT * FROM items WHERE id = 9;")
        self.assertTrue(a.blocked(), "reader saw an uncommitted write")
        self.ok(b, "ABORT;")
        self.assertIn("(0 rows)", a.reply())
```

## README.md

````markdown
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
````

## Line counts

| File | Lines |
|---|---:|
| common.h | 75 |
| disk_cache.c | 109 |
| page.c | 81 |
| hash.c | 58 |
| lock.c | 110 |
| wal.c | 131 |
| txn.c | 17 |
| sql.c | 150 |
| exec.c | 208 |
| server.c | 97 |
| Makefile | 14 |
| tests/support.py | 85 |
| tests/test_crash.py | 31 |
| tests/test_deadlock.py | 28 |
| tests/test_blocking.py | 19 |
| README.md | 71 |
| **Engine C and header total** | **1036** |
