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
