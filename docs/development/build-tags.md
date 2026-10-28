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

# Unit tests of the packages that temporal_minimal changes.
make unit-test-build-tags
```

Without `temporal_minimal`, the `temporal_with_*` tags have no effect and every integration is
included.

| Tag                      | Integration                   | Dependencies it brings in |
| ------------------------ | ----------------------------- | ------------------------- |
| `temporal_with_gstorage` | Google Cloud Storage archival | Google Cloud Storage SDK  |

A server built without an integration refuses to start if its configuration sets the integration up.

`make build-tags-check`, which runs in CI, checks the packages each `temporal_with_*` tag compiles out
of the `temporal_minimal` server, as listed in the `EXCLUDED_PACKAGES_<tag>` variables in the
`Makefile`. When adding an integration, add a variable for its tag.
