//go:build test_dep

package failpointserver_test

import "go.temporal.io/testx/failpoint"

// Keys shared by the tests in this package. NewKey rejects duplicate names, so each key is
// declared once.
var (
	key1   = failpoint.NewKey("1")
	key2   = failpoint.NewKey("2")
	key3   = failpoint.NewKey("3")
	key100 = failpoint.NewKey("100")
)
