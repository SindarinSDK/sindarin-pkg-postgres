# sindarin-pkg-postgres

A PostgreSQL client for the [Sindarin](https://github.com/SindarinSDK/sindarin-compiler) programming language, backed by [libpq](https://www.postgresql.org/docs/current/libpq.html). Supports direct SQL execution, row queries with typed accessors, and named prepared statements with parameter binding and reuse.

## Installation

Add the package as a dependency in your `sn.yaml`:

```yaml
dependencies:
- name: sindarin-pkg-postgres
  git: git@github.com:SindarinSDK/sindarin-pkg-postgres.git
  branch: main
```

Then run `sn --install` to fetch the package.

## Quick Start

```sindarin
import "sindarin-pkg-postgres/src/postgres"

fn main(): void =>
    var conn: PgConn = PgConn.connect("host=localhost dbname=mydb user=postgres")

    conn.exec("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, age INT)")
    conn.exec("INSERT INTO users (name, age) VALUES ('Alice', 30)")

    var rows: PgRow[] = conn.query("SELECT * FROM users ORDER BY id")
    print(rows[0].getString("name"))
    print(rows[0].getInt("age"))

    conn.dispose()
```

---

## PgConn

```sindarin
import "sindarin-pkg-postgres/src/postgres"
```

A database connection. The connection string follows the [libpq keyword/value format](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) or a `postgresql://` URI.

| Method | Signature | Description |
|--------|-----------|-------------|
| `connect` | `static fn connect(connStr: str): PgConn` | Connect to a PostgreSQL server |
| `exec` | `fn exec(sql: str): void` | Execute SQL with no results (CREATE, INSERT, UPDATE, DELETE) |
| `query` | `fn query(sql: str): PgRow[]` | Execute a SELECT and return all rows |
| `prepare` | `fn prepare(name: str, sql: str): PgStmt` | Create a named prepared statement |
| `lastError` | `fn lastError(): str` | Last error message from the server |
| `dispose` | `fn dispose(): void` | Close the connection |

```sindarin
# keyword/value connection string
var conn: PgConn = PgConn.connect("host=localhost port=5432 dbname=mydb user=postgres password=secret")

# or a connection URI
var conn: PgConn = PgConn.connect("postgresql://postgres:secret@localhost/mydb")

conn.exec("CREATE TABLE IF NOT EXISTS items (id SERIAL PRIMARY KEY, name TEXT, price FLOAT8)")
conn.exec("INSERT INTO items (name, price) VALUES ('widget', 9.99)")

conn.dispose()
```

---

## PgRow

A single result row. Column values are accessed by name using typed getters. All values are copied at query time so the row is safe to use after the query returns.

| Method | Signature | Description |
|--------|-----------|-------------|
| `getString` | `fn getString(col: str): str` | Column value as string (`""` for NULL) |
| `getInt` | `fn getInt(col: str): int` | Column value as integer (`0` for NULL) |
| `getFloat` | `fn getFloat(col: str): double` | Column value as float (`0.0` for NULL) |
| `isNull` | `fn isNull(col: str): bool` | True if the column is SQL NULL |
| `columnCount` | `fn columnCount(): int` | Number of columns in this row |
| `columnName` | `fn columnName(index: int): str` | Column name at the given zero-based index |

```sindarin
var rows: PgRow[] = conn.query("SELECT name, price, notes FROM items")

for i: int = 0; i < rows.length; i += 1 =>
    print(rows[i].getString("name"))
    print(rows[i].getFloat("price"))
    if rows[i].isNull("notes") =>
        print("no notes\n")
```

---

## PgStmt

A named prepared statement with parameter binding. Parameters use `$1`, `$2`, ... placeholders. Bind methods return `self` for chaining. Statements can be reset and re-executed with new bindings.

| Method | Signature | Description |
|--------|-----------|-------------|
| `bindString` | `fn bindString(index: int, value: str): PgStmt` | Bind a string to the given parameter (1-based) |
| `bindInt` | `fn bindInt(index: int, value: int): PgStmt` | Bind an integer to the given parameter (1-based) |
| `bindFloat` | `fn bindFloat(index: int, value: double): PgStmt` | Bind a float to the given parameter (1-based) |
| `bindNull` | `fn bindNull(index: int): PgStmt` | Bind SQL NULL to the given parameter (1-based) |
| `exec` | `fn exec(): void` | Execute with no results |
| `query` | `fn query(): PgRow[]` | Execute and return all result rows |
| `reset` | `fn reset(): void` | Clear all bindings for re-use |
| `dispose` | `fn dispose(): void` | Free statement resources |

```sindarin
var stmt: PgStmt = conn.prepare("insert_item", "INSERT INTO items (name, price) VALUES ($1, $2)")

stmt.bindString(1, "gadget").bindFloat(2, 24.99).exec()

stmt.reset()
stmt.bindString(1, "doohickey").bindNull(2).exec()

stmt.dispose()
```

Prepared statements can also return rows:

```sindarin
var sel: PgStmt = conn.prepare("find_cheap", "SELECT * FROM items WHERE price < $1")
var rows: PgRow[] = sel.bindFloat(1, 20.0).query()
sel.dispose()
```

---

## Examples

### Basic CRUD

```sindarin
import "sindarin-pkg-postgres/src/postgres"

fn main(): void =>
    var conn: PgConn = PgConn.connect("host=localhost dbname=mydb user=postgres")

    conn.exec("CREATE TABLE IF NOT EXISTS products (id SERIAL PRIMARY KEY, name TEXT, stock INT)")
    conn.exec("INSERT INTO products (name, stock) VALUES ('alpha', 10)")
    conn.exec("INSERT INTO products (name, stock) VALUES ('beta', 5)")

    var rows: PgRow[] = conn.query("SELECT * FROM products ORDER BY id")
    for i: int = 0; i < rows.length; i += 1 =>
        print($"{rows[i].getString(\"name\")}: {rows[i].getInt(\"stock\")}\n")

    conn.exec("UPDATE products SET stock = 0 WHERE name = 'beta'")
    conn.exec("DELETE FROM products WHERE stock = 0")

    conn.dispose()
```

### Bulk insert with prepared statement

```sindarin
import "sindarin-pkg-postgres/src/postgres"

fn main(): void =>
    var conn: PgConn = PgConn.connect("host=localhost dbname=mydb user=postgres")
    conn.exec("CREATE TABLE IF NOT EXISTS log (msg TEXT, level INT)")

    var stmt: PgStmt = conn.prepare("insert_log", "INSERT INTO log (msg, level) VALUES ($1, $2)")

    stmt.bindString(1, "started").bindInt(2, 1).exec().reset()
    stmt.bindString(1, "processing").bindInt(2, 1).exec().reset()
    stmt.bindString(1, "done").bindInt(2, 2).exec()

    stmt.dispose()
    conn.dispose()
```

### Parameterized query returning rows

```sindarin
import "sindarin-pkg-postgres/src/postgres"

fn main(): void =>
    var conn: PgConn = PgConn.connect("host=localhost dbname=mydb user=postgres")

    var sel: PgStmt = conn.prepare("active_users", "SELECT name, age FROM users WHERE age >= $1 ORDER BY age")
    var rows: PgRow[] = sel.bindInt(1, 18).query()

    for i: int = 0; i < rows.length; i += 1 =>
        print($"{rows[i].getString(\"name\")} ({rows[i].getInt(\"age\")})\n")

    sel.dispose()
    conn.dispose()
```

---

## Development

```bash
# Install dependencies (required before make test)
sn --install

make test    # Build and run all tests
make clean   # Remove build artifacts
```

Tests require a running PostgreSQL server. Set the following environment variables before running:

| Variable | Default | Description |
|----------|---------|-------------|
| `PGHOST` | `localhost` | Server hostname |
| `PGPORT` | `5432` | Server port |
| `PGDATABASE` | `postgres` | Database name |
| `PGUSER` | `postgres` | Username |
| `PGPASSWORD` | _(none)_ | Password |

## Dependencies

- [sindarin-pkg-libs](https://github.com/SindarinSDK/sindarin-pkg-libs) — provides pre-built `libpq`, `libpgcommon`, and `libpgport` static libraries for Linux, macOS, and Windows.
- [sindarin-pkg-sdk](https://github.com/SindarinSDK/sindarin-pkg-sdk) — Sindarin standard library.

## License

MIT License
