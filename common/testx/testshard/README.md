# testshard

`go.temporal.io/testx/testshard` distributes tests across CI jobs. Call it at the
start of a test, before expensive setup:

```go
testshard.SkipUnlessInShard(t)
```

Run each shard in a separate job, using the same total and a different index:

```sh
TESTSHARD_COUNT=4 TESTSHARD_INDEX=0 TESTSHARD_SALT=my-suite go test ./...
```

Run indices `0` through `3` for this example. If either variable is empty, all
tests run; invalid values fail the test. Selected tests and their nested
subtests stay on the same shard.

Sharded runs require a non-empty salt. Set `TESTSHARD_SALT`, or configure
process-wide defaults once in `TestMain` before `m.Run()`:

```go
testshard.SetDefaults(testshard.WithSalt("my-suite"))
```

To rename variables, add `WithTotalShardsEnv("CI_TOTAL_SHARDS")`,
`WithShardIndexEnv("CI_SHARD_INDEX")`, or `WithSaltEnv("CI_SHARD_SALT")` to
`SetDefaults`. Precedence is: non-empty salt environment value, per-call
options, then process defaults. All jobs must use the same salt. Environment
values are read on each call.

For repeated checks, use `shard, err := testshard.FromEnv(options...)`, handle
the error, then call `shard.SkipUnlessInShard(t)`.
