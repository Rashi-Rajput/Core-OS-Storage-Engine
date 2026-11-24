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
