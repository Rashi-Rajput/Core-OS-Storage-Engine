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
