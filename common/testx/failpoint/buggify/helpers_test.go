package buggify_test

import (
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
)

// Keys shared by the tests in this package. NewKey rejects duplicate names, so each key is
// declared once.
var (
	key1   = failpoint.NewKey[error]("1")
	key2   = failpoint.NewKey[error]("2")
	key3   = failpoint.NewKey[error]("3")
	key100 = failpoint.NewKey[error]("100")

	// Keys of failpoints with other value types.
	anyKey    = failpoint.NewKey[any]("any")
	stringKey = failpoint.NewKey[string]("string")
)

// allKeys returns an allowlist with one entry that matches every key name.
func allKeys(activation, fire float64, errorName string) []buggify.Key {
	return []buggify.Key{{Match: "*", ActivationProbability: activation, FireProbability: fire, Error: errorName}}
}
