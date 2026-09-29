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

The version required in the root `go.mod` is an older pseudo-version and isn't bumped with testx
changes. There are no testx tags.

Run tests and lint with `make testx-test` and `make lint-testx`; `./...` from the repo root doesn't
include this module.

## Depending on the server

Consumers of `go.temporal.io/server` ignore its `replace`. If they build server packages that import
testx (e.g. `temporaltest`, `tests/testcore`), they must require testx at the same commit as the
server:

```sh
sha=$(go list -m -json go.temporal.io/server@<version> | jq -r .Origin.Hash)
go get go.temporal.io/server@<version> github.com/temporalio/temporal/testx@$sha
```

Server release tags like `v1.30.0` don't apply to testx, so always use the commit.
