// softassert provides utilities for performing assertions.
//
// Unlike traditional assertions that may terminate the program or panic, soft assertions log an
// error message when a condition is not met but allow the program to continue running. The runner
// of the program, such as a Go test, is expected to flag failed assertions.
//
// Best practices:
// - Use it to check for programming errors and invariants.
// - Use it to communicate assumptions about the code.
// - Use it to abort or recover from an unexpected state.
// - Never use it as a substitute for regular error handling, validation, or control flow.
package softassert

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// That performs a soft assertion by logging an error if the given condition is false.
// It is meant to indicate a condition is always expected to be true.
// Returns true if the condition is met, otherwise false.
//
// `staticMessage` is expected to be a static string to help with grouping and searching logs.
// Dynamic information should be passed via `tags`.
//
// Example:
// softassert.That(logger, object.state == "ready", "object is not ready")
func That(logger log.Logger, condition bool, staticMessage string, tags ...tag.Tag) bool {
	if !condition {
		// By using the same prefix for all assertions, they can be reliably found in logs.
		logger.Error("failed assertion: "+staticMessage, append([]tag.Tag{tag.FailedAssertion}, tags...)...)
	}
	return condition
}

// Fail logs an error using `staticMessage` to indicate a failed assertion.
//
// `staticMessage` is expected to be a static string to help with grouping and searching logs.
// Dynamic information should be passed via `tags`.
//
// Example:
// softassert.Fail(logger, "unreachable code reached", tag.NewStringTag("state", object.state))
func Fail(logger log.Logger, staticMessage string, tags ...tag.Tag) {
	logger.Error("failed assertion: "+staticMessage, append([]tag.Tag{tag.FailedAssertion}, tags...)...)
}

// ThatSometimes is used to conditionally log a debug message of a noteworthy but non-problematic event.
//
// `staticMessage` is expected to be a static string to help with grouping and searching logs.
// Dynamic information should be passed via `tags`.
//
// Example:
// softassert.ThatSometimes(logger, object.state == "terminated", "termination event", tag.NewStringTag("state", object.state))
func ThatSometimes(logger log.Logger, condition bool, staticMessage string, tags ...tag.Tag) bool {
	if !condition {
		logger.Debug(staticMessage, tags...)
	}
	return condition
}

// Sometimes is used to log a debug message of a noteworthy but non-problematic event.
//
// `staticMessage` is expected to be a static string to help with grouping and searching logs.
// Dynamic information should be passed via `tags`.
//
// Example:
// softassert.Sometimes(logger, "termination event", tag.NewStringTag("state", object.state))
func Sometimes(logger log.Logger, staticMessage string, tags ...tag.Tag) {
	logger.Debug(staticMessage, tags...)
}
