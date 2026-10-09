# Compiling the server for WASI

Use the Go version specified in `go.mod` to compile the server for WASI Preview 1:

```sh
make temporal-server-wasi
```

The target writes `temporal-server.wasm` in the repository root. It sets
`GOOS=wasip1`, `GOARCH=wasm`, and `CGO_ENABLED=0`, and builds with readonly modules.
It preserves the native target's build tags and adds `sqlite3_dotlk` to select
the SQLite driver's portable file locking implementation. MySQL, PostgreSQL,
SQLite, and the existing persistence and archival provider registrations remain
included. SQLite uses the ncruces driver on every platform, including FTS5 for
visibility queries.

This target verifies compilation and linking only. Running the server in a WASI
host, including networking, filesystem access, and persistence behavior, requires
separate runtime qualification.
