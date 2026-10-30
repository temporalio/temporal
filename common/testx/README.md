# go.temporal.io/testx

Collection of general-purpose Go test helpers.

- `parallelsuite`: isolated parallel test suites with fail-fast assertions.
- `freeport`: reserve available ports for tests.

Extend a suite by embedding exported value structs implementing
`parallelsuite.Extension[T]` alongside `parallelsuite.Suite[T]`. Each extension's
`Init(T)` binds it to the fresh suite instance for each test, subtest, or
`WithAssertions` copy. Initializers run in field order after the base is ready.

The base suite has no context or polling dependencies. Context, wait, protobuf,
and history helpers remain in `go.temporal.io/server/common/testing/parallelsuite`.
