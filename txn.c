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
