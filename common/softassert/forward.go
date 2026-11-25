// TODO: Remove this forwarding file once all callers use go.temporal.io/testx/softassert.
package softassert

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/testx/softassert"
)

// That returns condition and logs an error when it is false. Pass dynamic details as tags.
func That(logger log.Logger, condition bool, staticMessage string, tags ...tag.Tag) bool {
	return softassert.That(logger, condition, staticMessage, tags...)
}

// Fail logs a failed assertion with the shared failure marker. Pass dynamic details as tags.
func Fail(logger log.Logger, staticMessage string, tags ...tag.Tag) {
	softassert.Fail(logger, staticMessage, tags...)
}
