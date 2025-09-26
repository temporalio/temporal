package softassert

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// That performs a soft assertion by logging an error if the given condition is false.
// It is meant to indicate a condition is always expected to be true.
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
func That(logger log.Logger, condition bool, staticMessage string, tags ...tag.Tag) bool {
	if !condition {
		// By using the same prefix for all assertions, they can be reliably found in logs.
		logger.Error("failed assertion: "+staticMessage, append([]tag.Tag{tag.FailedAssertion}, tags...)...)
	}
	return condition
}

// ThatSometimes is used to conditionally log a debug message of a noteworthy but non-problematic event.
func ThatSometimes(logger log.Logger, condition bool, staticMessage string, tags ...tag.Tag) bool {
	if !condition {
		logger.Debug(staticMessage, tags...)
	}
	return condition
}

// Sometimes is used to log a debug message of a noteworthy but non-problematic event.
func Sometimes(logger log.Logger, staticMessage string, tags ...tag.Tag) {
	logger.Debug(staticMessage, tags...)
}

// Unexpected is used to emit an error log for an unexpected error condition.
// It returns a FailedAssertion error that can be returned; or converted to a service error.
//
// Example:
// return softassert.Unexpected(logger, "failed to process request", err.Error(), tag.Key("request-id", requestID))
func Unexpected(logger log.Logger, staticMessage string, errorDetails string, tags ...tag.Tag) *FailedAssertion {
	fa := FailedAssertion{message: staticMessage, errorDetails: errorDetails}
	combinedTags := append([]tag.Tag{tag.FailedAssertion}, tags...)
	if errorDetails != "" {
		combinedTags = append(combinedTags, tag.NewStringTag("error-details", errorDetails))
	}
	logger.Error("failed assertion: "+fa.Error(), combinedTags...)
	return &fa
}
