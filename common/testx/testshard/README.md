# testshard

`go.temporal.io/testx/testshard` distributes tests across CI jobs. Call it at the
start of a test, before expensive setup:

```go
shard, err := testshard.FromEnv()
require.NoError(t, err)
shard.MaybeSkip(t)
```

Run each shard in a separate job, using the same total and a different index:

```sh
TEST_TOTAL_SHARDS=4 TEST_SHARD_INDEX=0 go test ./...
```

This example needs four jobs with indices `0` through `3`. If either variable
is empty, all tests run; invalid values return an error.

The salt defaults to empty. Supply a default with
`testshard.FromEnv(testshard.WithSalt("my-suite"))`; a non-empty `TEST_SHARD_SALT`
overrides it. All jobs must use the same salt.

Selected tests and their nested subtests stay on the same shard.
