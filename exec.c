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
