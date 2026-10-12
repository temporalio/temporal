# Build tags for optional integrations

By default the server is built with every persistence, visibility and archival integration it
supports. Embedders that only need a subset, such as a single-node server backed by SQLite, can
compile the others out with the `temporal_minimal` build tag and opt back in to individual
integrations with `temporal_with_<integration>` tags:

```sh
# SQLite only, no optional integrations.
go build -tags disable_grpc_modules,temporal_minimal ./cmd/server

# SQLite plus Elasticsearch visibility and Cassandra persistence.
go build -tags disable_grpc_modules,temporal_minimal,temporal_with_elasticsearch,temporal_with_cassandra ./cmd/server
```

Without `temporal_minimal`, the `temporal_with_*` tags have no effect and every integration is
included. The scheme follows Prometheus' service discovery plugins (`remove_all_sd` plus
`enable_<name>_sd`).

| Tag                           | Integration                           | Dependencies it brings in |
| ----------------------------- | ------------------------------------- | ------------------------- |
| `temporal_with_cassandra`     | Cassandra persistence                 | gocql                     |
| `temporal_with_elasticsearch` | Elasticsearch visibility              | olivere/elastic, AWS SDK  |
| `temporal_with_mysql`         | MySQL persistence and visibility      | go-sql-driver/mysql       |
| `temporal_with_postgresql`    | PostgreSQL persistence and visibility | pgx, lib/pq               |
| `temporal_with_s3store`       | S3 archival                           | AWS SDK                   |
| `temporal_with_gstorage`      | Google Cloud Storage archival         | Google Cloud Storage SDK  |

SQLite and the filestore archiver are always included.

A server built without an integration rejects configuration that uses it at startup, for example
`cassandra persistence is excluded from this build, see build tag temporal_with_cassandra`.

## Adding a build-tagged integration

Each integration is split into an implementation file and a stub with exactly complementary
constraints:

```go
//go:build !temporal_minimal || temporal_with_cassandra   // fx_cassandra.go
//go:build temporal_minimal && !temporal_with_cassandra   // fx_cassandra_excluded.go
```

Keep types and constants that the rest of the server refers to in untagged files, so callers
compile unchanged in both modes. The stub returns an error that names the tag.
