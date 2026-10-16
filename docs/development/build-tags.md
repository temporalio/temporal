# Build tags

These Go build tags compile out parts of the server. For tags that only affect tests, see
[Testing](testing.md#build-tags).

## `disable_grpc_modules`

Excludes the gRPC dependencies of `cloud.google.com/go/storage`, reducing binary size and compile
time. The server only uses the storage REST client (`storage.NewClient`), not the gRPC client
(`storage.NewGRPCClient`).

The Makefile and release builds (`.goreleaser.yml`) set it by default.

## Optional integrations

By default the server is built with every integration it supports. Programs that don't need an
integration can compile it out with the `temporal_minimal` build tag, and opt back in to individual
integrations with `temporal_with_<integration>` tags:

```sh
# Without optional integrations.
make temporal-server BUILD_TAG=temporal_minimal

# Without optional integrations, except Google Cloud Storage archival.
make temporal-server BUILD_TAG=temporal_minimal,temporal_with_gstorage

# Tests with the same tags.
make unit-test TEST_TAG=temporal_minimal
```

Programs that embed the server pass the same tags to `go build -tags`.

Without `temporal_minimal`, the `temporal_with_*` tags have no effect and every integration is
included.

| Tag                      | Integration                   | Dependencies it brings in |
| ------------------------ | ----------------------------- | ------------------------- |
| `temporal_with_gstorage` | Google Cloud Storage archival | Google Cloud Storage SDK  |

A server built without an integration rejects configuration that uses it, for example
`unknown archiver scheme: "gs" (Google Cloud Storage archival is excluded from this build, see build
tag temporal_with_gstorage)`.
