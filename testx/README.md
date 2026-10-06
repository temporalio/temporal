# go.temporal.io/testx

General-purpose Go test helpers that don't depend on the Temporal server, published as a separate Go
module so that other projects can use them without depending on `go.temporal.io/server`.

```sh
go get go.temporal.io/testx@<commit>
```

## Rules

- This module must not depend on `go.temporal.io/server`. CI enforces this.
- Keep dependencies minimal. Every dependency here becomes a dependency of every consumer.

## Development

The server's `go.mod` replaces this module with the local `./testx` directory, so server code always
builds against the testx code in the same commit. A single PR can change testx and use the change in
the server.

Consumers of the server ignore that `replace` and get the testx version required in the server's
`go.mod`. After every change to `testx/` on `main`, the `testx-bump` workflow updates that version
to the latest commit that changed `testx/`.

Run tests and lint with `make testx-test` and `make lint-testx`; `./...` from the repo root doesn't
include this module.
