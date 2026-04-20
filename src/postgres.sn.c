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
#include <math.h>

#include <libpq-fe.h>

/* ============================================================================
 * Type OID constants (from server/catalog/pg_type_d.h)
 * ============================================================================ */

#define PG_OID_BOOL         16
#define PG_OID_BYTEA        17
#define PG_OID_CHAR          18
#define PG_OID_NAME         19
#define PG_OID_INT8         20
#define PG_OID_INT2         21
#define PG_OID_INT4         23
#define PG_OID_TEXT         25
#define PG_OID_OID          26
#define PG_OID_JSON        114
#define PG_OID_FLOAT4      700
#define PG_OID_FLOAT8      701
#define PG_OID_BPCHAR     1042
#define PG_OID_VARCHAR    1043
#define PG_OID_DATE       1082
#define PG_OID_TIME       1083
#define PG_OID_TIMESTAMP  1114
#define PG_OID_TIMESTAMPTZ 1184
#define PG_OID_NUMERIC   1700
#define PG_OID_UUID       2950
#define PG_OID_JSONB      3802

#define PG_OID_BOOL_ARR     1000
#define PG_OID_INT2_ARR     1005
#define PG_OID_INT4_ARR     1007
#define PG_OID_TEXT_ARR     1009
#define PG_OID_VARCHAR_ARR  1015
#define PG_OID_INT8_ARR     1016
#define PG_OID_FLOAT4_ARR   1021
#define PG_OID_FLOAT8_ARR   1022
#define PG_OID_NUMERIC_ARR  1231

/* ============================================================================
 * Byte order helpers — postgres binary protocol is big-endian
 * ============================================================================ */

#if defined(_MSC_VER)
#  include <stdlib.h>
#  define pgbswap16 _byteswap_ushort
#  define pgbswap32 _byteswap_ulong
#  define pgbswap64 _byteswap_uint64
#else
#  define pgbswap16 __builtin_bswap16
#  define pgbswap32 __builtin_bswap32
#  define pgbswap64 __builtin_bswap64
#endif

static inline int16_t rd_i16(const uint8_t *p) { uint16_t v; memcpy(&v, p, 2); return (int16_t)pgbswap16(v); }
static inline int32_t rd_i32(const uint8_t *p) { uint32_t v; memcpy(&v, p, 4); return (int32_t)pgbswap32(v); }
static inline int64_t rd_i64(const uint8_t *p) { uint64_t v; memcpy(&v, p, 8); return (int64_t)pgbswap64(v); }
static inline float   rd_f32(const uint8_t *p) { uint32_t v; memcpy(&v, p, 4); v = pgbswap32(v); float  f; memcpy(&f, &v, 4); return f; }
static inline double  rd_f64(const uint8_t *p) { uint64_t v; memcpy(&v, p, 8); v = pgbswap64(v); double d; memcpy(&d, &v, 8); return d; }

#ifdef _WIN32
/* vcpkg's static build of libpgcommon.a bundles _shlib object files that
 * reference PostgreSQL's server-side memory allocator (palloc/pfree).
 * These symbols are not available in a client-only build, so we provide
 * trivial libc wrappers to satisfy the linker. */
void  pfree(void *ptr)                 { free(ptr); }
void *palloc(size_t size)              { void *p = malloc(size); if (!p) { fprintf(stderr, "out of memory\n"); exit(1); } return p; }
char *pstrdup(const char *s)           { return strdup(s); }
void *repalloc(void *ptr, size_t size) { void *p = realloc(ptr, size); if (!p) { fprintf(stderr, "out of memory\n"); exit(1); } return p; }
#endif

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

static void snpg_cache_remove(SnPgConn *w, const char *name)
{
    if (!w || !name) return;
    for (int i = 0; i < w->cache_count; i++) {
        if (strcmp(w->cache_names[i], name) == 0) {
            free(w->cache_names[i]);
            free(w->cache_sqls[i]);
            for (int j = i; j < w->cache_count - 1; j++) {
                w->cache_names[j] = w->cache_names[j + 1];
                w->cache_sqls[j]  = w->cache_sqls[j + 1];
            }
            w->cache_count--;
            return;
        }
    }
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
 *
 * Each column is stored as raw binary bytes + length + type OID. The string
 * getter decodes to postgres-style text on demand; int/float/array getters
 * decode directly from the bytes.
 * ============================================================================ */

static void cleanup_pg_row_elem(void *p)
{
    RtPgRow *row = (RtPgRow *)p;
    int count = (int)row->col_count;

    char    **names  = (char    **)(uintptr_t)row->col_names;
    uint8_t **values = (uint8_t **)(uintptr_t)row->col_values;
    int      *lens   = (int      *)(uintptr_t)row->col_lens;
    Oid      *types  = (Oid      *)(uintptr_t)row->col_types;
    bool     *nulls  = (bool     *)(uintptr_t)row->col_nulls;

    if (names) {
        for (int i = 0; i < count; i++) free(names[i]);
        free(names);
    }
    if (values) {
        for (int i = 0; i < count; i++) free(values[i]);
        free(values);
    }
    free(lens);
    free(types);
    free(nulls);
}

static RtPgRow build_row(PGresult *res, int row_idx)
{
    int count = PQnfields(res);
    RtPgRow row = {0};
    row.col_count = (long long)count;

    char    **names  = (char    **)calloc((size_t)count, sizeof(char *));
    uint8_t **values = (uint8_t **)calloc((size_t)count, sizeof(uint8_t *));
    int      *lens   = (int      *)calloc((size_t)count, sizeof(int));
    Oid      *types  = (Oid      *)calloc((size_t)count, sizeof(Oid));
    bool     *nulls  = (bool     *)calloc((size_t)count, sizeof(bool));

    if (!names || !values || !lens || !types || !nulls) {
        fprintf(stderr, "postgres: build_row: allocation failed\n");
        exit(1);
    }

    for (int i = 0; i < count; i++) {
        names[i] = strdup(PQfname(res, i) ? PQfname(res, i) : "");
        types[i] = PQftype(res, i);
        nulls[i] = PQgetisnull(res, row_idx, i) != 0;
        if (nulls[i]) {
            values[i] = NULL;
            lens[i]   = 0;
        } else {
            int n = PQgetlength(res, row_idx, i);
            const char *src = PQgetvalue(res, row_idx, i);
            uint8_t *buf = (uint8_t *)malloc((size_t)(n > 0 ? n : 1));
            if (!buf) {
                fprintf(stderr, "postgres: build_row: value allocation failed\n");
                exit(1);
            }
            if (n > 0 && src) memcpy(buf, src, (size_t)n);
            values[i] = buf;
            lens[i]   = n;
        }
    }

    row.col_names  = (long long)(uintptr_t)names;
    row.col_values = (long long)(uintptr_t)values;
    row.col_lens   = (long long)(uintptr_t)lens;
    row.col_types  = (long long)(uintptr_t)types;
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
 * Binary decoders — produce postgres-compatible text output for each type
 * ============================================================================ */

/* Float stringify matching postgres output: shortest round-trip representation.
 * postgres uses extra_float_digits=1 (PG12+) for short-repr; %.15g works for
 * most values, bump to %.17g if it fails to round-trip. */
static char *fmt_double(double d)
{
    if (isnan(d)) return strdup("NaN");
    if (isinf(d)) return strdup(d < 0.0 ? "-Infinity" : "Infinity");
    char buf[64];
    for (int prec = 15; prec <= 17; prec++) {
        snprintf(buf, sizeof buf, "%.*g", prec, d);
        double rt = strtod(buf, NULL);
        if (rt == d) break;
    }
    return strdup(buf);
}

/* Julian day → Gregorian (postgres canonical, src/backend/utils/adt/datetime.c) */
static void j2date_pg(int jd, int *y, int *mo, int *d)
{
    unsigned int julian = (unsigned int)jd + 32044;
    unsigned int quad = julian / 146097;
    unsigned int extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    int yy = (int)(julian * 4 / 1461);
    julian = ((yy != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366)) + 123;
    yy += (int)(quad * 4);
    *y = yy - 4800;
    quad = julian * 2141 / 65536;
    *d = (int)(julian - 7834 * quad / 256);
    *mo = (int)((quad + 10) % 12 + 1);
}

#define POSTGRES_EPOCH_JDATE 2451545  /* Julian day for 2000-01-01 */

static char *format_date(int32_t days_since_2000)
{
    int y, mo, d;
    j2date_pg((int)days_since_2000 + POSTGRES_EPOCH_JDATE, &y, &mo, &d);
    char buf[32];
    snprintf(buf, sizeof buf, "%04d-%02d-%02d", y, mo, d);
    return strdup(buf);
}

/* Helper: append "HH:MM:SS[.ffffff]" with trailing-zero trimming on fractional */
static int format_time_of_day(char *out, size_t cap, int64_t time_us)
{
    int64_t sec  = time_us / 1000000;
    int64_t usec = time_us % 1000000;
    int hour = (int)(sec / 3600);
    int min  = (int)((sec / 60) % 60);
    int s    = (int)(sec % 60);
    if (usec) {
        char us_buf[8];
        snprintf(us_buf, sizeof us_buf, "%06lld", (long long)usec);
        int len = 6;
        while (len > 0 && us_buf[len - 1] == '0') us_buf[--len] = 0;
        return snprintf(out, cap, "%02d:%02d:%02d.%s", hour, min, s, us_buf);
    }
    return snprintf(out, cap, "%02d:%02d:%02d", hour, min, s);
}

static char *format_time(int64_t us)
{
    char buf[32];
    format_time_of_day(buf, sizeof buf, us);
    return strdup(buf);
}

static char *format_timestamp(int64_t us, bool has_tz)
{
    const int64_t usec_per_day = 86400000000LL;
    int64_t days, time_us;
    if (us >= 0) {
        days = us / usec_per_day;
        time_us = us % usec_per_day;
    } else {
        days = -(((-us) - 1) / usec_per_day + 1);
        time_us = us - days * usec_per_day;
    }
    int y, mo, d;
    j2date_pg((int)days + POSTGRES_EPOCH_JDATE, &y, &mo, &d);
    char time_buf[32];
    format_time_of_day(time_buf, sizeof time_buf, time_us);
    char buf[64];
    if (has_tz) {
        snprintf(buf, sizeof buf, "%04d-%02d-%02d %s+00", y, mo, d, time_buf);
    } else {
        snprintf(buf, sizeof buf, "%04d-%02d-%02d %s", y, mo, d, time_buf);
    }
    return strdup(buf);
}

static char *format_uuid(const uint8_t *p, int n)
{
    if (n != 16) return strdup("");
    char buf[40];
    snprintf(buf, sizeof buf,
        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7],
        p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
    return strdup(buf);
}

static char *format_bytea(const uint8_t *p, int n)
{
    static const char hex[] = "0123456789abcdef";
    char *out = (char *)malloc((size_t)n * 2 + 3);
    if (!out) return strdup("");
    out[0] = '\\';
    out[1] = 'x';
    for (int i = 0; i < n; i++) {
        out[2 + i * 2]     = hex[p[i] >> 4];
        out[2 + i * 2 + 1] = hex[p[i] & 0x0f];
    }
    out[2 + n * 2] = 0;
    return out;
}

/* NUMERIC binary format:
 *   int16 ndigits, int16 weight, int16 sign, int16 dscale, int16 digits[] */
static char *format_numeric(const uint8_t *p, int n)
{
    if (n < 8) return strdup("");
    int16_t  ndigits = rd_i16(p);
    int16_t  weight  = rd_i16(p + 2);
    uint16_t sign    = (uint16_t)rd_i16(p + 4);
    int16_t  dscale  = rd_i16(p + 6);

    if (sign == 0xC000) return strdup("NaN");
    if (sign == 0xD000) return strdup("Infinity");
    if (sign == 0xF000) return strdup("-Infinity");

    /* Budget: sign + integer part (max 4 chars per group, at most weight+1 groups)
     * + '.' + dscale fractional chars + NUL. Use a generous upper bound. */
    int cap = 16 + (int)(weight + 1 > 0 ? (weight + 1) : 0) * 4 + dscale + 8;
    char *out = (char *)malloc((size_t)cap);
    if (!out) return strdup("");
    int len = 0;
    int d = 0;

    if (sign == 0x4000) out[len++] = '-';

    if (weight < 0) {
        out[len++] = '0';
    } else {
        int group = (d < ndigits) ? rd_i16(p + 8 + d * 2) : 0;
        d++;
        len += snprintf(out + len, (size_t)(cap - len), "%d", group);
        for (int i = 1; i <= weight; i++) {
            group = (d < ndigits) ? rd_i16(p + 8 + d * 2) : 0;
            d++;
            len += snprintf(out + len, (size_t)(cap - len), "%04d", group);
        }
    }

    if (dscale > 0) {
        out[len++] = '.';
        int emitted = 0;
        if (weight < -1) {
            int zeros = (-weight - 1) * 4;
            while (zeros > 0 && emitted < dscale) {
                out[len++] = '0';
                emitted++;
                zeros--;
            }
        }
        while (d < ndigits && emitted < dscale) {
            int group = rd_i16(p + 8 + d * 2);
            d++;
            char g[8];
            snprintf(g, sizeof g, "%04d", group);
            for (int i = 0; i < 4 && emitted < dscale; i++) {
                out[len++] = g[i];
                emitted++;
            }
        }
        while (emitted < dscale) {
            out[len++] = '0';
            emitted++;
        }
    }

    out[len] = 0;
    return out;
}

/* Generic decoder: produce the postgres text form of any binary value. */
static char *decode_to_string(Oid oid, const uint8_t *p, int n)
{
    if (n <= 0 || !p) return strdup("");
    char buf[64];
    switch (oid) {
    case PG_OID_BOOL:
        return strdup(p[0] ? "t" : "f");
    case PG_OID_INT2:
        snprintf(buf, sizeof buf, "%d", (int)rd_i16(p));
        return strdup(buf);
    case PG_OID_INT4:
    case PG_OID_OID:
        snprintf(buf, sizeof buf, "%d", (int)rd_i32(p));
        return strdup(buf);
    case PG_OID_INT8:
        snprintf(buf, sizeof buf, "%lld", (long long)rd_i64(p));
        return strdup(buf);
    case PG_OID_FLOAT4:
        return fmt_double((double)rd_f32(p));
    case PG_OID_FLOAT8:
        return fmt_double(rd_f64(p));
    case PG_OID_TEXT:
    case PG_OID_VARCHAR:
    case PG_OID_BPCHAR:
    case PG_OID_NAME:
    case PG_OID_CHAR:
    case PG_OID_JSON: {
        char *s = (char *)malloc((size_t)n + 1);
        if (!s) return strdup("");
        memcpy(s, p, (size_t)n);
        s[n] = 0;
        return s;
    }
    case PG_OID_JSONB: {
        /* JSONB binary: 1-byte version (currently 1) followed by UTF-8 text */
        if (n < 1) return strdup("");
        int body = n - 1;
        char *s = (char *)malloc((size_t)body + 1);
        if (!s) return strdup("");
        memcpy(s, p + 1, (size_t)body);
        s[body] = 0;
        return s;
    }
    case PG_OID_BYTEA:
        return format_bytea(p, n);
    case PG_OID_UUID:
        return format_uuid(p, n);
    case PG_OID_DATE:
        return format_date(rd_i32(p));
    case PG_OID_TIME:
        return format_time(rd_i64(p));
    case PG_OID_TIMESTAMP:
        return format_timestamp(rd_i64(p), false);
    case PG_OID_TIMESTAMPTZ:
        return format_timestamp(rd_i64(p), true);
    case PG_OID_NUMERIC:
        return format_numeric(p, n);
    default:
        /* Unknown OID — dump as hex bytea form */
        return format_bytea(p, n);
    }
}

static long long decode_to_int(Oid oid, const uint8_t *p, int n)
{
    if (n <= 0 || !p) return 0;
    switch (oid) {
    case PG_OID_BOOL:   return p[0] ? 1 : 0;
    case PG_OID_INT2:   return (long long)rd_i16(p);
    case PG_OID_INT4:
    case PG_OID_OID:    return (long long)rd_i32(p);
    case PG_OID_INT8:   return (long long)rd_i64(p);
    case PG_OID_FLOAT4: return (long long)rd_f32(p);
    case PG_OID_FLOAT8: return (long long)rd_f64(p);
    case PG_OID_NUMERIC: {
        char *s = format_numeric(p, n);
        long long v = strtoll(s, NULL, 10);
        free(s);
        return v;
    }
    default: {
        /* Best-effort: text-typed columns parsed as integer */
        char *s = decode_to_string(oid, p, n);
        long long v = strtoll(s, NULL, 10);
        free(s);
        return v;
    }
    }
}

static double decode_to_float(Oid oid, const uint8_t *p, int n)
{
    if (n <= 0 || !p) return 0.0;
    switch (oid) {
    case PG_OID_BOOL:   return p[0] ? 1.0 : 0.0;
    case PG_OID_INT2:   return (double)rd_i16(p);
    case PG_OID_INT4:
    case PG_OID_OID:    return (double)rd_i32(p);
    case PG_OID_INT8:   return (double)rd_i64(p);
    case PG_OID_FLOAT4: return (double)rd_f32(p);
    case PG_OID_FLOAT8: return rd_f64(p);
    case PG_OID_NUMERIC: {
        char *s = format_numeric(p, n);
        double v = strtod(s, NULL);
        free(s);
        return v;
    }
    default: {
        char *s = decode_to_string(oid, p, n);
        double v = strtod(s, NULL);
        free(s);
        return v;
    }
    }
}

/* Parse array header; returns data pointer after header and total element count.
 *   int32 ndim, int32 flags, int32 elem_oid, { int32 dim, int32 lb } × ndim */
static bool parse_array_header(const uint8_t *p, int n,
                               int *ndim, Oid *elem_oid,
                               int *total_out, const uint8_t **data_out)
{
    if (n < 12) return false;
    *ndim = rd_i32(p);
    /* flags at p+4 ignored — NULL elements carry len=-1 inline */
    *elem_oid = (Oid)rd_i32(p + 8);
    int off = 12;
    int total = (*ndim == 0) ? 0 : 1;
    for (int i = 0; i < *ndim; i++) {
        if (off + 8 > n) return false;
        int dim = rd_i32(p + off);
        total *= dim;
        off += 8;
    }
    *total_out = total;
    *data_out = p + off;
    return true;
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

static bool row_col_at(RtPgRow *row, const char *col,
                       const uint8_t **out_data, int *out_len, Oid *out_oid)
{
    if (!row || !col) return false;
    int idx = find_col(row, col);
    if (idx < 0) return false;
    bool *nulls = (bool *)(uintptr_t)row->col_nulls;
    if (nulls[idx]) return false;
    uint8_t **values = (uint8_t **)(uintptr_t)row->col_values;
    int      *lens   = (int      *)(uintptr_t)row->col_lens;
    Oid      *types  = (Oid      *)(uintptr_t)row->col_types;
    *out_data = values[idx];
    *out_len  = lens[idx];
    *out_oid  = types[idx];
    return true;
}

char *sn_pg_row_get_string(__sn__PgRow *row, char *col)
{
    const uint8_t *p; int n; Oid oid;
    if (!row_col_at(row, col, &p, &n, &oid)) return strdup("");
    return decode_to_string(oid, p, n);
}

long long sn_pg_row_get_int(__sn__PgRow *row, char *col)
{
    const uint8_t *p; int n; Oid oid;
    if (!row_col_at(row, col, &p, &n, &oid)) return 0;
    return decode_to_int(oid, p, n);
}

double sn_pg_row_get_float(__sn__PgRow *row, char *col)
{
    const uint8_t *p; int n; Oid oid;
    if (!row_col_at(row, col, &p, &n, &oid)) return 0.0;
    return decode_to_float(oid, p, n);
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
 * Array getters — decode postgres binary arrays to SnArray of doubles/ints/strs
 * ============================================================================ */

SnArray *sn_pg_row_get_float_array(__sn__PgRow *row, char *col)
{
    const uint8_t *p; int n; Oid col_oid;
    if (!row_col_at(row, col, &p, &n, &col_oid)) {
        SnArray *empty = sn_array_new(sizeof(double), 0);
        empty->elem_tag = SN_TAG_DOUBLE;
        return empty;
    }
    int ndim, total;
    Oid elem_oid;
    const uint8_t *data;
    if (!parse_array_header(p, n, &ndim, &elem_oid, &total, &data)) {
        SnArray *empty = sn_array_new(sizeof(double), 0);
        empty->elem_tag = SN_TAG_DOUBLE;
        return empty;
    }

    SnArray *arr = sn_array_new(sizeof(double), total > 0 ? total : 1);
    arr->elem_tag = SN_TAG_DOUBLE;
    const uint8_t *end = p + n;
    const uint8_t *q = data;
    for (int i = 0; i < total; i++) {
        if (q + 4 > end) break;
        int32_t item_len = rd_i32(q);
        q += 4;
        double d = 0.0;
        if (item_len >= 0) {
            if (q + item_len > end) break;
            d = decode_to_float(elem_oid, q, item_len);
            q += item_len;
        }
        sn_array_push(arr, &d);
    }
    return arr;
}

SnArray *sn_pg_row_get_int_array(__sn__PgRow *row, char *col)
{
    const uint8_t *p; int n; Oid col_oid;
    if (!row_col_at(row, col, &p, &n, &col_oid)) {
        SnArray *empty = sn_array_new(sizeof(long long), 0);
        empty->elem_tag = SN_TAG_INT;
        return empty;
    }
    int ndim, total;
    Oid elem_oid;
    const uint8_t *data;
    if (!parse_array_header(p, n, &ndim, &elem_oid, &total, &data)) {
        SnArray *empty = sn_array_new(sizeof(long long), 0);
        empty->elem_tag = SN_TAG_INT;
        return empty;
    }

    SnArray *arr = sn_array_new(sizeof(long long), total > 0 ? total : 1);
    arr->elem_tag = SN_TAG_INT;
    const uint8_t *end = p + n;
    const uint8_t *q = data;
    for (int i = 0; i < total; i++) {
        if (q + 4 > end) break;
        int32_t item_len = rd_i32(q);
        q += 4;
        long long v = 0;
        if (item_len >= 0) {
            if (q + item_len > end) break;
            v = decode_to_int(elem_oid, q, item_len);
            q += item_len;
        }
        sn_array_push(arr, &v);
    }
    return arr;
}

SnArray *sn_pg_row_get_string_array(__sn__PgRow *row, char *col)
{
    const uint8_t *p; int n; Oid col_oid;
    SnArray *arr;
    if (!row_col_at(row, col, &p, &n, &col_oid)) {
        arr = sn_array_new(sizeof(char *), 0);
        arr->elem_tag     = SN_TAG_STRING;
        arr->elem_release = (void (*)(void *))sn_cleanup_str;
        arr->elem_copy    = sn_copy_str;
        return arr;
    }
    int ndim, total;
    Oid elem_oid;
    const uint8_t *data;
    if (!parse_array_header(p, n, &ndim, &elem_oid, &total, &data)) {
        arr = sn_array_new(sizeof(char *), 0);
        arr->elem_tag     = SN_TAG_STRING;
        arr->elem_release = (void (*)(void *))sn_cleanup_str;
        arr->elem_copy    = sn_copy_str;
        return arr;
    }

    arr = sn_array_new(sizeof(char *), total > 0 ? total : 1);
    arr->elem_tag     = SN_TAG_STRING;
    arr->elem_release = (void (*)(void *))sn_cleanup_str;
    arr->elem_copy    = sn_copy_str;
    const uint8_t *end = p + n;
    const uint8_t *q = data;
    for (int i = 0; i < total; i++) {
        if (q + 4 > end) break;
        int32_t item_len = rd_i32(q);
        q += 4;
        char *s;
        if (item_len < 0) {
            s = strdup("");
        } else {
            if (q + item_len > end) break;
            s = decode_to_string(elem_oid, q, item_len);
            q += item_len;
        }
        sn_array_push(arr, &s);
    }
    return arr;
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
    /* resultFormat=1 requests the postgres binary wire format. */
    PGresult *res = PQexecParams(w->conn, sql, 0, NULL, NULL, NULL, NULL, 1);
    if (pg_should_retry(res, w, "query")) {
        PQclear(res);
        res = PQexecParams(w->conn, sql, 0, NULL, NULL, NULL, NULL, 1);
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
                                   n, pv, NULL, NULL, 1);
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

    /* Deallocate the server-side prepared statement and remove from the
     * reconnect cache so it does not accumulate indefinitely. */
    SnPgConn *w = STMT_W(s);
    if (w && w->conn && s->stmt_name) {
        const char *name = STMT_NAME(s);
        char dealloc[512];
        snprintf(dealloc, sizeof(dealloc), "DEALLOCATE \"%s\"", name);
        PGresult *r = PQexec(w->conn, dealloc);
        PQclear(r);
        snpg_cache_remove(w, name);
    }

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
