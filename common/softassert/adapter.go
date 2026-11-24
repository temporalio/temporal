// Package softassert adapts [go.temporal.io/testx/softassert] to Temporal's logger
// and attaches [tag.FailedAssertion] to each failed assertion.
package softassert

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/testx/softassert"
)

// That returns condition and calls [Fail] when it is false.
func That(logger log.Logger, condition bool, staticMessage string, tags ...tag.Tag) bool {
	if !condition {
		Fail(logger, staticMessage, tags...)
	}
	return condition
}

// Fail logs a failed assertion with [tag.FailedAssertion]. Pass dynamic details as tags.
func Fail(logger log.Logger, staticMessage string, tags ...tag.Tag) {
	softassert.Fail(logger, staticMessage, append([]tag.Tag{tag.FailedAssertion}, tags...)...)
}
