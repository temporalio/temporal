package softassert

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// That performs a soft assertion by logging an error if the given condition is false.
// It is meant to indicate a condition that are always expected to be true.
// Returns true if the condition is met, otherwise false.
//
// Example:
// softassert.That(logger, object.state == "ready", "object is not ready")
//
// Best practices:
// - Use it to check for programming errors and invariants.
// - Use it to communicate assumptions about the code.
// - Use it to abort or recover from an unexpected state.
// - Never use it as a substitute for regular error handling, validation, or control flow.
func That(logger log.Logger, condition bool, msg string) bool {
	if !condition {
		// By using the same prefix for all assertions, they can be reliably found in logs.
		logger.Error("failed assertion: "+msg, tag.FailedAssertion)
	}
	return condition
}

func Fail(logger log.Logger, msg string, tags ...tag.Tag) {
	// By using the same prefix for all assertions, they can be reliably found in logs.
	logger.Error("failed assertion: "+msg, append(tags, tag.FailedAssertion)...)
}
