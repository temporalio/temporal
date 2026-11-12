# testshard

Environment-based test selection shared by test harnesses.

`testshard.FromEnv()` reads `TEST_TOTAL_SHARDS` and the zero-based
`TEST_SHARD_INDEX`. Sharding is disabled when either variable is empty.
Tests are selected by `FarmHash Fingerprint32(fullTestName + salt) % totalShards`.
The salt defaults to an empty string; `testshard.WithSalt(salt)` supplies a
default, and a non-empty `TEST_SHARD_SALT` overrides it. Use `Shard.MaybeSkip(t)`
to apply the selection; selected tests and their descendants retain the same
shard across subsequent checks.
