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
