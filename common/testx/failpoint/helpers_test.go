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

// executeError returns the error that the failpoint for key in registry injects, or nil.
func executeError(registry *failpoint.Registry, key failpoint.Key[error]) error {
	err, _ := key.Execute(registry)
	return err
}
