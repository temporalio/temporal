# github.com/temporalio/temporal/testx

Test utilities and CI tools that don't depend on the Temporal server, published as a separate Go
module so that other repositories can use them without depending on `go.temporal.io/server`.

```sh
go get github.com/temporalio/temporal/testx@<version>
go run github.com/temporalio/temporal/testx/cmd/test-runner@<version>
```

## Rules

- This module must not depend on `go.temporal.io/server`. CI enforces this.
- Keep dependencies minimal. Every dependency here becomes a dependency of every consumer.

## Development

The server's `go.mod` requires this module and replaces it with the local `./testx` directory, so
server code always builds against the testx code in the same commit. A single PR can change testx
and use the change in the server.

Downstream consumers of the server ignore the `replace` and resolve the required version instead.
So every PR that changes `testx/` must also bump the version in the root `go.mod`:

```sh
go mod edit -require=github.com/temporalio/temporal/testx@v0.2.0
```

CI rejects testx changes if that version is already tagged. After merging, the `testx-tag` workflow
tags the merge commit as `testx/<version>`, which makes the version resolvable.

Run tests and lint with `make testx-test` and `make lint-testx`; `./...` from the repo root doesn't
include this module.
