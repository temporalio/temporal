package failpoint_test

import "go.temporal.io/testx/failpoint"

// Keys shared by the tests in this package. NewKey rejects duplicate names, so each key is
// declared once.
var (
	errorKey = failpoint.NewKey[error]("test.error")
	intKey   = failpoint.NewKey[int]("test.int")
)
