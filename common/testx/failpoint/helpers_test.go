package failpoint_test

import "go.temporal.io/testx/failpoint"

// Keys shared by the tests in this package. NewKey rejects duplicate names, so each key is
// declared once.
var (
	key1   = failpoint.NewKey[error]("test.key1")
	key2   = failpoint.NewKey[error]("test.key2")
	key3   = failpoint.NewKey[error]("test.key3")
	key4   = failpoint.NewKey[error]("test.key4")
	key5   = failpoint.NewKey[error]("test.key5")
	key100 = failpoint.NewKey[error]("test.key100")

	// Keys of failpoints with other value types.
	intKey    = failpoint.NewKey[int]("test.int")
	stringKey = failpoint.NewKey[string]("test.string")
	anyKey    = failpoint.NewKey[any]("test.any")
	unitKey   = failpoint.NewKey[struct{}]("test.unit")
)
