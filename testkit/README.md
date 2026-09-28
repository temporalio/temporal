# github.com/temporalio/temporal/testkit

Test utilities that don't depend on the Temporal server, published as a separate Go module so that
other repositories can use them without depending on `go.temporal.io/server`.

```sh
go get github.com/temporalio/temporal/testkit@<sha>
```

## Rules

- This module must not import `go.temporal.io/server`. Its separate `go.mod` enforces that.
- Keep dependencies minimal. Every dependency here becomes a dependency of every consumer.
- Don't require newer dependency versions than the server's `go.mod`. With `go.work`, the higher
  version would silently apply to local server builds, but not to downstream consumers.

## Development

The repository root has a `go.work` file that includes this module. Changes here are picked up by the
server immediately, so a single PR can change both.

The server's `go.mod` requires a published version of this module. That version is what downstream
consumers of the server resolve, so after changing the API here:

1. Merge the change to this module.
2. Bump the `github.com/temporalio/temporal/testkit` requirement in the root `go.mod` to the new
   pseudo-version.
3. Only then use the new API from server code.
