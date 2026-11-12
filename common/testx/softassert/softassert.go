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

// Logger is the error-logging method needed by soft assertions.
type Logger[F any] interface {
	Error(string, ...F)
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
func That[F any](logger Logger[F], condition bool, staticMessage string, tags ...F) bool {
	if !condition {
		// By using the same prefix for all assertions, they can be reliably found in logs.
		logger.Error("failed assertion: "+staticMessage, tags...)
	}
	return condition
}

// Fail logs an error using `staticMessage` to indicate a failed assertion.
//
// `staticMessage` is expected to be a static string to help with grouping and searching logs.
// Dynamic information should be passed via `tags`.
//
// Example:
// softassert.Fail(logger, "unreachable code reached", tag.String("state", object.state))
func Fail[F any](logger Logger[F], staticMessage string, tags ...F) {
	logger.Error("failed assertion: "+staticMessage, tags...)
}
