# go.temporal.io/testx

Collection of general-purpose Go test helpers.

- `await`: context-bounded polling assertions and channel operations.
- `freeport`: reserve available ports for tests.
- `parallelsuite`: isolated parallel test suites with fail-fast assertions.
- `testcontext`: shared test contexts with deadlines and decorators.
- `testshard`: environment-based test selection shared by test harnesses.

`testshard.FromEnv()` reads `TEST_TOTAL_SHARDS` and the zero-based
`TEST_SHARD_INDEX`. Sharding is disabled when either variable is empty.
Tests are selected by `FarmHash Fingerprint32(fullTestName + salt) % totalShards`.
The salt defaults to an empty string; `testshard.WithSalt(salt)` supplies a
default, and a non-empty `TEST_SHARD_SALT` overrides it. Use `Shard.MaybeSkip(t)`
to apply the selection; selected tests and their descendants retain the same
shard across subsequent checks.

`parallelsuite.Run` and `RunLegacySequential` automatically shard test methods
when both shard variables are set. Each method and all its nested subtests run
on one shard. Pass `parallelsuite.WithShard(shard)` before any test method
arguments to supply a configured shard. An explicit zero-value shard disables
automatic sharding for the suite.
