# testshard

`go.temporal.io/testx/testshard` distributes tests across CI jobs. Call it at the
start of a test, before expensive setup:

```go
testshard.SkipUnlessInShard(t, "my-suite")
```

Run each shard in a separate job, using the same salt and total and a different
index:

```sh
TEST_TOTAL_SHARDS=4 TEST_SHARD_INDEX=0 go test ./...
```

Run indices `0` through `3` for this example. If either variable is empty, all
tests run. Sharded runs require a non-empty salt. Selected tests and their
nested subtests stay on the same shard.

Use `WithTotalShardsEnv("CI_TOTAL_SHARDS")` and
`WithShardIndexEnv("CI_SHARD_INDEX")` to read different variables.

To shard a parallel suite, pass a configured shard to `RunInShard`:

```go
shard, err := testshard.FromEnv("my-suite")
require.NoError(t, err)
parallelsuite.RunInShard(t, shard, &MySuite{})
```

By default, `parallelsuite.Run` ignores the shard environment. To opt a whole
test binary into sharding, call `parallelsuite.ConfigureSharding` during package
initialization.
