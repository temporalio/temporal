# libsql persistence plugin

Persistence plugin using [go-libsql](https://github.com/tursodatabase/go-libsql) instead of `modernc.org/sqlite`. Reuses `schema/sqlite/` and behaves identically to the sqlite plugin with the following differences:

- Requires `CGO_ENABLED=1` (go-libsql uses CGO). Supported on linux/darwin, amd64/arm64.
- `IsDupEntryError` matches on SQLite extended error codes (`2067`, `1555`) with a string fallback,
  since go-libsql doesn't expose typed errors like `modernc.org/sqlite` does.
- Pragma-style connect attributes are not forwarded (go-libsql manages its own pragmas).
  Only `mode` and `setup` are recognized.
- DDL statements have double-quoted strings rewritten to single quotes. libsql is compiled
  with `DQS=0` (double-quoted strings are identifiers, not literals), but our schema uses
  double-quoted JSON paths in generated columns. `normalizeDQS` in `admin.go` rewrites
  those to single-quoted so the visibility schema applies correctly.

## Configuration

```yaml
persistence:
  defaultStore: libsql-default
  visibilityStore: libsql-visibility
  datastores:
    libsql-default:
      sql:
        pluginName: "libsql"
        databaseName: "file:/var/temporal/default.db"
        connectAttributes:
          setup: "true"
    libsql-visibility:
      sql:
        pluginName: "libsql"
        databaseName: "file:/var/temporal/visibility.db"
        connectAttributes:
          setup: "true"
```

`databaseName` accepts `file:/path/to/db` or `:memory:`.
