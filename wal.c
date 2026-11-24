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
