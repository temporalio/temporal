# go.temporal.io/testx

Collection of general-purpose Go test helpers.

- `await`: context-bounded polling assertions and channel operations.
- `freeport`: reserve available ports for tests.
- `parallelsuite`: isolated parallel test suites with fail-fast assertions.
- `testcontext`: shared test contexts with deadlines and decorators.
- `testshard`: environment-based test selection shared by test harnesses.

`parallelsuite.Run` and `RunLegacySequential` automatically shard test methods when
both `TEST_TOTAL_SHARDS` and `TEST_SHARD_INDEX` are set. The index is zero based.
Each method and all its nested subtests run on one shard, selected by
`FarmHash Fingerprint32(fullTestName + salt) % totalShards`.

The salt defaults to an empty string. Set a non-empty `TEST_SHARD_SALT` to override it, or
pass `parallelsuite.WithShardSalt(salt)` before any test method arguments to
supply a suite default. Existing harnesses can use `testshard.FromEnv(salt)` and
`Shard.MaybeSkip(t)` to share the same selection and retain a parent method's
shard for nested subtests.
