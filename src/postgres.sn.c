/* ==============================================================================
 * sindarin-pkg-postgres/src/postgres.sn.c — PostgreSQL client implementation
 * ==============================================================================
 * Implements PgConn, PgStmt, and PgRow via the libpq C API.
 *
 * Row data is copied out of PGresult into heap arrays at query time so that
 * PGresult can be cleared immediately. Rows are stored as SnArray elements
 * with an elem_release callback that frees the per-row heap data.
 *
 * Prepared statements track parameter bindings as string arrays (libpq's
 * PQexecPrepared takes const char * const *). Integers and floats are
 * formatted into heap strings; NULLs are represented as NULL pointers in
 * the param_values array.
 * ============================================================================== */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

#include <libpq-fe.h>

/* ============================================================================
 * Type Definitions
 * ============================================================================ */

typedef __sn__PgConn  RtPgConn;
typedef __sn__PgStmt  RtPgStmt;
typedef __sn__PgRow   RtPgRow;

/* ============================================================================
 * Internal Helpers
 * ============================================================================
 *
 * SnPgConn is the C-side wrapper stored in RtPgConn.conn_ptr. It holds
 * the libpq PGconn alongside the original connection string and a cache
 * of every prepared statement (name → sql). This is what makes the
 * package survive a transient postgres outage:
 *
 *   1. Any libpq op that fails AND finds the connection in a bad state
 *      triggers PQreset on the existing PGconn pointer.
 *   2. After a successful reset every cached prepared statement is
 *      re-issued via PQprepare so server-side state matches client-side
 *      expectations.
 *   3. The original op is retried ONCE. If the retry also fails, the
 *      package falls through to the original exit(1) hard-fail behaviour.
 *
 * The PgStmt struct stores its conn_ptr as a SnPgConn* (not a raw
 * PGconn*) so prepared-statement execs can access the wrapper's
 * statement cache + reconnect machinery the same way.
 *
 * Other consumers do not need to know any of this — the .sn API
 * surface is unchanged.
 */

typedef struct {
    PGconn *conn;
    char   *conn_str;

    /* Prepared statement cache, replayed on reconnect. */
    int     cache_count;
    int     cache_cap;
    char  **cache_names;
    char  **cache_sqls;
} SnPgConn;

#define CONN_W(c)   ((SnPgConn *)(uintptr_t)(c)->conn_ptr)
#define CONN_PTR(c) (CONN_W(c)->conn)

static SnPgConn *snpg_new(PGconn *conn, const char *conn_str)
{
    SnPgConn *w = (SnPgConn *)calloc(1, sizeof(SnPgConn));
    if (!w) {
        fprintf(stderr, "postgres: snpg_new: allocation failed\n");
        exit(1);
    }
    w->conn = conn;
    w->conn_str = conn_str ? strdup(conn_str) : NULL;
    return w;
}

static void snpg_free(SnPgConn *w)
{
    if (!w) return;
    if (w->conn) PQfinish(w->conn);
    free(w->conn_str);
    for (int i = 0; i < w->cache_count; i++) {
        free(w->cache_names[i]);
        free(w->cache_sqls[i]);
    }
    free(w->cache_names);
    free(w->cache_sqls);
    free(w);
}

static void snpg_cache_add(SnPgConn *w, const char *name, const char *sql)
{
    if (!w || !name || !sql) return;
    /* Skip duplicates — re-preparing the same name is harmless on a fresh
     * server but the cache should not grow per-call when the caller uses
     * stable statement names. */
    for (int i = 0; i < w->cache_count; i++) {
        if (strcmp(w->cache_names[i], name) == 0) {
            /* Update SQL in case it changed (rare) */
            if (strcmp(w->cache_sqls[i], sql) != 0) {
                free(w->cache_sqls[i]);
                w->cache_sqls[i] = strdup(sql);
            }
            return;
        }
    }
    if (w->cache_count >= w->cache_cap) {
        int new_cap = w->cache_cap == 0 ? 16 : w->cache_cap * 2;
        char **new_names = (char **)realloc(w->cache_names, (size_t)new_cap * sizeof(char *));
        char **new_sqls  = (char **)realloc(w->cache_sqls,  (size_t)new_cap * sizeof(char *));
        if (!new_names || !new_sqls) {
            fprintf(stderr, "postgres: snpg_cache_add: realloc failed\n");
            exit(1);
        }
        w->cache_names = new_names;
        w->cache_sqls  = new_sqls;
        w->cache_cap   = new_cap;
    }
    w->cache_names[w->cache_count] = strdup(name);
    w->cache_sqls[w->cache_count]  = strdup(sql);
    w->cache_count++;
}

/* Try to bring the connection back up after a server-side crash. Calls
 * PQreset (which keeps the existing PGconn pointer valid — important
 * because PgStmt instances hold pointers into the wrapper) and replays
 * every cached prepared statement. Returns true on success. */
static bool snpg_try_reconnect(SnPgConn *w)
{
    if (!w || !w->conn) return false;
    fprintf(stderr, "postgres: connection lost — attempting PQreset\n");
    PQreset(w->conn);
    if (PQstatus(w->conn) != CONNECTION_OK) {
        fprintf(stderr, "postgres: PQreset failed: %s\n", PQerrorMessage(w->conn));
        return false;
    }
    fprintf(stderr, "postgres: PQreset succeeded; re-preparing %d cached statement(s)\n",
            w->cache_count);
    for (int i = 0; i < w->cache_count; i++) {
        PGresult *r = PQprepare(w->conn, w->cache_names[i], w->cache_sqls[i], 0, NULL);
        ExecStatusType st = PQresultStatus(r);
        if (st != PGRES_COMMAND_OK) {
            fprintf(stderr, "postgres: re-prepare '%s' failed: %s\n",
                    w->cache_names[i], PQerrorMessage(w->conn));
            PQclear(r);
            return false;
        }
        PQclear(r);
    }
    return true;
}

static void pg_check_conn(PGconn *conn, const char *ctx)
{
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "postgres: %s: %s\n", ctx, PQerrorMessage(conn));
        exit(1);
    }
}

/* Called after the FINAL attempt of a query/exec/prepare. If the
 * status is not OK at this point we have already exhausted the
 * retry budget — log and exit. */
static void pg_check_result(PGresult *res, PGconn *conn, const char *ctx)
{
    ExecStatusType status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        fprintf(stderr, "postgres: %s: %s\n", ctx, PQerrorMessage(conn));
        PQclear(res);
        exit(1);
    }
}

/* Inspect the result and decide whether the caller should retry the
 * operation after reconnecting. Returns true ONLY when:
 *   - the result indicates failure
 *   - AND the connection is now in a bad state
 *   - AND PQreset successfully restored it
 * In all other cases returns false (caller proceeds, pg_check_result
 * will exit on a real error). */
static bool pg_should_retry(PGresult *res, SnPgConn *w, const char *ctx)
{
    if (!res || !w) return false;
    ExecStatusType status = PQresultStatus(res);
    if (status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK) return false;
    if (PQstatus(w->conn) == CONNECTION_OK) return false;
    fprintf(stderr, "postgres: %s: server connection dropped (%s)\n",
            ctx, PQerrorMessage(w->conn));
    return snpg_try_reconnect(w);
}

/* ============================================================================
 * Row Building — copy all values out of PGresult before PQclear
 * ============================================================================ */

static void cleanup_pg_row_elem(void *p)
{
    RtPgRow *row = (RtPgRow *)p;
    int count = (int)row->col_count;

    char **names  = (char **)(uintptr_t)row->col_names;
    char **values = (char **)(uintptr_t)row->col_values;
    bool  *nulls  = (bool  *)(uintptr_t)row->col_nulls;

    if (names) {
        for (int i = 0; i < count; i++) free(names[i]);
        free(names);
    }
    if (values) {
        for (int i = 0; i < count; i++) free(values[i]);
        free(values);
    }
    free(nulls);
}

static RtPgRow build_row(PGresult *res, int row_idx)
{
    int count = PQnfields(res);
    RtPgRow row = {0};
    row.col_count = (long long)count;

    char **names  = (char **)calloc((size_t)count, sizeof(char *));
    char **values = (char **)calloc((size_t)count, sizeof(char *));
    bool  *nulls  = (bool  *)calloc((size_t)count, sizeof(bool));

    if (!names || !values || !nulls) {
        fprintf(stderr, "postgres: build_row: allocation failed\n");
        exit(1);
    }

    for (int i = 0; i < count; i++) {
        names[i] = strdup(PQfname(res, i) ? PQfname(res, i) : "");
        nulls[i] = PQgetisnull(res, row_idx, i) != 0;
        if (nulls[i]) {
            values[i] = NULL;
        } else {
            const char *val = PQgetvalue(res, row_idx, i);
            values[i] = strdup(val ? val : "");
        }
    }

    row.col_names  = (long long)(uintptr_t)names;
    row.col_values = (long long)(uintptr_t)values;
    row.col_nulls  = (long long)(uintptr_t)nulls;
    return row;
}

static SnArray *collect_rows(PGresult *res)
{
    SnArray *arr = sn_array_new(sizeof(RtPgRow), 16);
    arr->elem_tag     = SN_TAG_STRUCT;
    arr->elem_release = cleanup_pg_row_elem;

    int nrows = PQntuples(res);
    for (int i = 0; i < nrows; i++) {
        RtPgRow row = build_row(res, i);
        sn_array_push(arr, &row);
    }
    return arr;
}

/* ============================================================================
 * PgRow Accessors
 * ============================================================================ */

static int find_col(RtPgRow *row, const char *col)
{
    char **names = (char **)(uintptr_t)row->col_names;
    int count = (int)row->col_count;
    for (int i = 0; i < count; i++) {
        if (names[i] && strcmp(names[i], col) == 0)
            return i;
    }
    return -1;
}

char *sn_pg_row_get_string(__sn__PgRow *row, char *col)
{
    if (!row || !col) return strdup("");
    int idx = find_col(row, col);
    if (idx < 0) return strdup("");
    bool *nulls = (bool *)(uintptr_t)row->col_nulls;
    if (nulls[idx]) return strdup("");
    char **values = (char **)(uintptr_t)row->col_values;
    return strdup(values[idx] ? values[idx] : "");
}

long long sn_pg_row_get_int(__sn__PgRow *row, char *col)
{
    if (!row || !col) return 0;
    int idx = find_col(row, col);
    if (idx < 0) return 0;
    bool *nulls = (bool *)(uintptr_t)row->col_nulls;
    if (nulls[idx]) return 0;
    char **values = (char **)(uintptr_t)row->col_values;
    if (!values[idx]) return 0;
    return (long long)strtoll(values[idx], NULL, 10);
}

double sn_pg_row_get_float(__sn__PgRow *row, char *col)
{
    if (!row || !col) return 0.0;
    int idx = find_col(row, col);
    if (idx < 0) return 0.0;
    bool *nulls = (bool *)(uintptr_t)row->col_nulls;
    if (nulls[idx]) return 0.0;
    char **values = (char **)(uintptr_t)row->col_values;
    if (!values[idx]) return 0.0;
    return strtod(values[idx], NULL);
}

bool sn_pg_row_is_null(__sn__PgRow *row, char *col)
{
    if (!row || !col) return true;
    int idx = find_col(row, col);
    if (idx < 0) return true;
    bool *nulls = (bool *)(uintptr_t)row->col_nulls;
    return nulls[idx];
}

long long sn_pg_row_column_count(__sn__PgRow *row)
{
    if (!row) return 0;
    return row->col_count;
}

char *sn_pg_row_column_name(__sn__PgRow *row, long long index)
{
    if (!row || index < 0 || index >= row->col_count) return strdup("");
    char **names = (char **)(uintptr_t)row->col_names;
    return strdup(names[index] ? names[index] : "");
}

/* ============================================================================
 * PgConn
 * ============================================================================ */

RtPgConn *sn_pg_conn_connect(char *conn_str)
{
    if (!conn_str) {
        fprintf(stderr, "PgConn.connect: connStr is NULL\n");
        exit(1);
    }

    PGconn *conn = PQconnectdb(conn_str);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "PgConn.connect: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        exit(1);
    }

    SnPgConn *w = snpg_new(conn, conn_str);
    RtPgConn *c = __sn__PgConn__new();
    c->conn_ptr = (long long)(uintptr_t)w;
    return c;
}

void sn_pg_conn_exec(RtPgConn *c, char *sql)
{
    if (!c || !sql) return;
    SnPgConn *w = CONN_W(c);
    PGresult *res = PQexec(w->conn, sql);
    if (pg_should_retry(res, w, "exec")) {
        PQclear(res);
        res = PQexec(w->conn, sql);
    }
    pg_check_result(res, w->conn, "exec");
    PQclear(res);
}

SnArray *sn_pg_conn_query(RtPgConn *c, char *sql)
{
    if (!c || !sql) return sn_array_new(sizeof(RtPgRow), 0);
    SnPgConn *w = CONN_W(c);
    PGresult *res = PQexec(w->conn, sql);
    if (pg_should_retry(res, w, "query")) {
        PQclear(res);
        res = PQexec(w->conn, sql);
    }
    pg_check_result(res, w->conn, "query");
    SnArray *arr = collect_rows(res);
    PQclear(res);
    return arr;
}

RtPgStmt *sn_pg_conn_prepare(RtPgConn *c, char *name, char *sql)
{
    if (!c || !name || !sql) {
        fprintf(stderr, "PgConn.prepare: NULL argument\n");
        exit(1);
    }

    SnPgConn *w = CONN_W(c);
    PGresult *res = PQprepare(w->conn, name, sql, 0, NULL);
    if (pg_should_retry(res, w, "prepare")) {
        PQclear(res);
        res = PQprepare(w->conn, name, sql, 0, NULL);
    }
    pg_check_result(res, w->conn, "prepare");
    PQclear(res);

    /* Cache the (name, sql) so a future reconnect can replay this
     * preparation against the new server-side state. */
    snpg_cache_add(w, name, sql);

    /* Count parameters by scanning for $N placeholders */
    int param_count = 0;
    for (const char *p = sql; *p; p++) {
        if (*p == '$') {
            int n = 0;
            const char *q = p + 1;
            while (*q >= '0' && *q <= '9') { n = n * 10 + (*q - '0'); q++; }
            if (n > param_count) param_count = n;
        }
    }

    RtPgStmt *s = __sn__PgStmt__new();
    s->conn_ptr   = (long long)(uintptr_t)w;
    s->stmt_name  = (uint8_t *)strdup(name);
    s->param_count = (long long)param_count;

    if (param_count > 0) {
        char  **vals  = (char **)calloc((size_t)param_count, sizeof(char *));
        bool   *nulls = (bool  *)calloc((size_t)param_count, sizeof(bool));
        if (!vals || !nulls) {
            fprintf(stderr, "PgConn.prepare: param allocation failed\n");
            exit(1);
        }
        /* All params start as NULL */
        for (int i = 0; i < param_count; i++) nulls[i] = true;
        s->param_values = (long long)(uintptr_t)vals;
        s->param_nulls  = (long long)(uintptr_t)nulls;
    }

    return s;
}

char *sn_pg_conn_last_error(RtPgConn *c)
{
    if (!c) return strdup("");
    const char *msg = PQerrorMessage(CONN_PTR(c));
    return strdup(msg ? msg : "");
}

void sn_pg_conn_dispose(RtPgConn *c)
{
    if (!c) return;
    snpg_free(CONN_W(c));
    c->conn_ptr = 0;
}

/* ============================================================================
 * PgStmt — parameter binding and execution
 *
 * The conn_ptr field on RtPgStmt holds a SnPgConn* (the wrapper), not a
 * raw PGconn*. STMT_CONN extracts the live PGconn from the wrapper so
 * exec/query/dispose all see whatever PQreset has done since the
 * statement was prepared.
 * ============================================================================ */

#define STMT_W(s)     ((SnPgConn *)(uintptr_t)(s)->conn_ptr)
#define STMT_CONN(s)  (STMT_W(s)->conn)
#define STMT_NAME(s)  ((const char *)(s)->stmt_name)
#define STMT_VALS(s)  ((char **)(uintptr_t)(s)->param_values)
#define STMT_NULLS(s) ((bool *)(uintptr_t)(s)->param_nulls)

static void stmt_set_param(RtPgStmt *s, int index, char *value, bool is_null)
{
    if (!s || index < 1 || index > (int)s->param_count) {
        fprintf(stderr, "PgStmt: bind index %d out of range (1..%lld)\n",
                index, s ? s->param_count : 0);
        exit(1);
    }
    int i = index - 1;
    char **vals  = STMT_VALS(s);
    bool  *nulls = STMT_NULLS(s);
    free(vals[i]);
    vals[i]  = value;
    nulls[i] = is_null;
}

void sn_pg_stmt_bind_string(RtPgStmt *s, long long index, char *value)
{
    stmt_set_param(s, (int)index, value ? strdup(value) : NULL, value == NULL);
}

void sn_pg_stmt_bind_int(RtPgStmt *s, long long index, long long value)
{
    char buf[32];
    snprintf(buf, sizeof(buf), "%lld", value);
    stmt_set_param(s, (int)index, strdup(buf), false);
}

void sn_pg_stmt_bind_float(RtPgStmt *s, long long index, double value)
{
    char buf[64];
    snprintf(buf, sizeof(buf), "%.17g", value);
    stmt_set_param(s, (int)index, strdup(buf), false);
}

void sn_pg_stmt_bind_null(RtPgStmt *s, long long index)
{
    stmt_set_param(s, (int)index, NULL, true);
}

static PGresult *stmt_exec_internal(RtPgStmt *s)
{
    int         n     = (int)s->param_count;
    char      **vals  = STMT_VALS(s);
    bool       *nulls = STMT_NULLS(s);

    /* Build the const char * array libpq expects (NULL = SQL NULL) */
    const char **pv = (const char **)calloc((size_t)n, sizeof(char *));
    if (!pv && n > 0) {
        fprintf(stderr, "PgStmt: exec allocation failed\n");
        exit(1);
    }
    for (int i = 0; i < n; i++)
        pv[i] = nulls[i] ? NULL : vals[i];

    PGresult *res = PQexecPrepared(STMT_CONN(s), STMT_NAME(s),
                                   n, pv, NULL, NULL, 0);
    free(pv);
    return res;
}

void sn_pg_stmt_exec(RtPgStmt *s)
{
    if (!s) return;
    SnPgConn *w = STMT_W(s);
    PGresult *res = stmt_exec_internal(s);
    if (pg_should_retry(res, w, "stmt exec")) {
        PQclear(res);
        res = stmt_exec_internal(s);
    }
    pg_check_result(res, w->conn, "stmt exec");
    PQclear(res);
}

SnArray *sn_pg_stmt_query(RtPgStmt *s)
{
    if (!s) return sn_array_new(sizeof(RtPgRow), 0);
    SnPgConn *w = STMT_W(s);
    PGresult *res = stmt_exec_internal(s);
    if (pg_should_retry(res, w, "stmt query")) {
        PQclear(res);
        res = stmt_exec_internal(s);
    }
    pg_check_result(res, w->conn, "stmt query");
    SnArray *arr = collect_rows(res);
    PQclear(res);
    return arr;
}

void sn_pg_stmt_reset(RtPgStmt *s)
{
    if (!s) return;
    int    n    = (int)s->param_count;
    char **vals = STMT_VALS(s);
    bool  *nulls = STMT_NULLS(s);
    for (int i = 0; i < n; i++) {
        free(vals[i]);
        vals[i]  = NULL;
        nulls[i] = true;
    }
}

void sn_pg_stmt_dispose(RtPgStmt *s)
{
    if (!s) return;
    int    n    = (int)s->param_count;
    char **vals = STMT_VALS(s);
    if (vals) {
        for (int i = 0; i < n; i++) free(vals[i]);
        free(vals);
    }
    free((void *)(uintptr_t)s->param_nulls);
    free((void *)s->stmt_name);
    s->conn_ptr    = 0;
    s->stmt_name   = NULL;
    s->param_values = 0;
    s->param_nulls  = 0;
}
