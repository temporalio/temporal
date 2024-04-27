## Run Temporal dependencies on the host

### macOS
While developing Temporal server you may want to run its dependencies locally. One of the reason might be
a bad docker file system performance on macOS. Please follow the doc for the database you use:
[Cassandra](macos/cassandra.md), [MySQL](macos/mysql.md), [PostgreSQL](macos/postgresql.md), or [TiDB](macos/tidb.md).

### Linux
Linux users should use `docker compose` as described in the [contribution guide](../../CONTRIBUTING.md). 