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
