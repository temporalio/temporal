# testshard

`go.temporal.io/testx/testshard` distributes tests across CI jobs. Call it at the
start of a test, before expensive setup:

```go
testshard.SkipUnlessInShard(t)
```

Run each shard in a separate job, using the same total and a different index:

```sh
TEST_TOTAL_SHARDS=4 TEST_SHARD_INDEX=0 TEST_SHARD_SALT=my-suite go test ./...
```

Run indices `0` through `3` for this example. If either variable is empty, all
tests run; invalid values fail the test. Selected tests and their nested
subtests stay on the same shard.

Sharded runs require a non-empty salt. Set `TEST_SHARD_SALT` or pass
`testshard.WithSalt("my-suite")` to `SkipUnlessInShard`; a non-empty environment
value takes precedence. All jobs must use the same salt.

To read different environment variables:

```go
testshard.SkipUnlessInShard(t,
    testshard.WithTotalShardsEnv("CI_TOTAL_SHARDS"),
    testshard.WithShardIndexEnv("CI_SHARD_INDEX"),
    testshard.WithSaltEnv("CI_SHARD_SALT"),
)
```

For repeated checks, use `shard, err := testshard.FromEnv(options...)`, handle
the error, then call `shard.SkipUnlessInShard(t)`.

`parallelsuite.Run` and `RunLegacySequential` use the environment automatically.
To pass custom configuration:

```go
shard, err := testshard.FromEnv(testshard.WithSalt("my-suite"))
require.NoError(t, err)
parallelsuite.Run(t, &MySuite{}, parallelsuite.WithShard(shard))
```

Place `WithShard` before any method arguments. Pass `testshard.Shard{}` to
`WithShard` to disable sharding for a suite.
