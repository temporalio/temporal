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

type sometimesLogger struct {
	logger log.Logger
}

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

// Sometimes is used to log a message of a noteworthy but non-problematic event.
//
// Example:
// softassert.Sometimes(logger).Warn("termination event", tag.NewStringTag("state", object.state))
func Sometimes(logger log.Logger) *sometimesLogger {
	return &sometimesLogger{logger: logger}
}

// Debug logs a message at debug level.
//
// `staticMessage` is expected to be a static string to help with grouping and searching logs.
// Dynamic information should be passed via `tags`.
func (s *sometimesLogger) Debug(staticMessage string, tags ...tag.Tag) {
	s.logger.Debug(staticMessage, tags...)
}

// Info logs a message at info level.
//
// `staticMessage` is expected to be a static string to help with grouping and searching logs.
// Dynamic information should be passed via `tags`.
func (s *sometimesLogger) Info(staticMessage string, tags ...tag.Tag) {
	s.logger.Info(staticMessage, tags...)
}

// Warn logs a message at warn level.
//
// `staticMessage` is expected to be a static string to help with grouping and searching logs.
// Dynamic information should be passed via `tags`.
func (s *sometimesLogger) Warn(staticMessage string, tags ...tag.Tag) {
	s.logger.Warn(staticMessage, tags...)
}

// Error logs a message at error level.
//
// `staticMessage` is expected to be a static string to help with grouping and searching logs.
// Dynamic information should be passed via `tags`.
func (s *sometimesLogger) Error(staticMessage string, tags ...tag.Tag) {
	s.logger.Error(staticMessage, tags...)
}
