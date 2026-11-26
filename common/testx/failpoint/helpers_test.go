package failpoint_test

import "go.temporal.io/testx/failpoint"

// Keys shared by the tests in this package. NewKey rejects duplicate names, so each key is
// declared once.
var (
	errorKey = failpoint.NewKey[error]("test.error")
	intKey   = failpoint.NewKey[int]("test.int")
)

// executeError returns the error that the failpoint for key in registry injects, or nil.
func executeError(registry *failpoint.Registry, key failpoint.Key[error]) error {
	err, _ := key.Execute(registry)
	return err
}
