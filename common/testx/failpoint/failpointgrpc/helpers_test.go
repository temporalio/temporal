package failpointgrpc_test

import "go.temporal.io/testx/failpoint"

// Keys shared by the tests in this package. NewKey rejects duplicate names, so each key is
// declared once.
var (
	key1   = failpoint.NewKey[error]("1")
	key2   = failpoint.NewKey[error]("2")
	key3   = failpoint.NewKey[error]("3")
	key100 = failpoint.NewKey[error]("100")

	// ruleKey is the key of the rule that some tests apply, and intKey a key of another type.
	ruleKey = failpoint.NewKey[error]("rule")
	intKey  = failpoint.NewKey[int]("test.int")
)
