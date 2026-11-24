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
