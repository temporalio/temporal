# testshard

`go.temporal.io/testx/testshard` distributes tests across CI jobs. Call it at the
start of a test, before expensive setup:

```go
testshard.MaybeSkip(t)
```

Run each shard in a separate job, using the same total and a different index:

```sh
TEST_TOTAL_SHARDS=4 TEST_SHARD_INDEX=0 go test ./...
```

Run indices `0` through `3` for this example. If either variable is empty, all
tests run; invalid values fail the test. Selected tests and their nested
subtests stay on the same shard.

The salt defaults to empty. Pass `testshard.WithSalt("my-suite")` to `MaybeSkip`
to supply a default; a non-empty `TEST_SHARD_SALT` overrides it. All jobs must
use the same salt.

To read different environment variables:

```go
testshard.MaybeSkip(t,
    testshard.WithTotalShardsEnv("CI_TOTAL_SHARDS"),
    testshard.WithShardIndexEnv("CI_SHARD_INDEX"),
    testshard.WithSaltEnv("CI_SHARD_SALT"),
)
```

For repeated checks, use `shard, err := testshard.FromEnv(options...)`, handle
the error, then call `shard.MaybeSkip(t)`.
