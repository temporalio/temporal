package failpoint_test

import "go.temporal.io/testx/failpoint"

// Keys shared by the tests in this package. NewKey rejects duplicate names, so each key is
// declared once.
var (
	key1   = failpoint.NewKey[error]("test.key1")
	intKey = failpoint.NewKey[int]("test.int")
	anyKey = failpoint.NewKey[any]("test.any")
)
