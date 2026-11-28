package failpoint_test

import "go.temporal.io/testx/failpoint"

// Keys shared by the tests in this package. NewKey rejects duplicate names, so each key is
// declared once.
var (
	errorKey = failpoint.NewKey[error]("test.error")
	key2     = failpoint.NewKey[error]("test.key2")
	key3     = failpoint.NewKey[error]("test.key3")
	key4     = failpoint.NewKey[error]("test.key4")
	intKey   = failpoint.NewKey[int]("test.int")
)
