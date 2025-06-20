package util

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// typedError is an error that has a type name.
// It is useful for attaching low-cardinality type names to errors that are suitable for telemetry tags.
// See ErrorType for more details.
type typedError interface {
	ErrorTypeName() string
}

var wrapperErrorTypes = map[string]bool{
	"*fmt.wrapError":      true,
	"*errors.joinError":   true,
	"*errors.withMessage": true,
	"*errors.withStack":   true,
}

// ErrorType returns a best effort guess at the most meaningful type name for the given error.
// If any of err's underlying errors implement TypedError, then the type name of the first such error is returned.
// This allows us to be explicit about the tag values we want to use for telemetry.
// Otherwise, the type name of the first non-wrapper error in the depth-first traversal of err's tree is returned.
// We consider errors wrapped via [fmt.Errorf], [errors.Join] and some pkg/errors functions to be wrapper errors.
func ErrorType(err error) string {
	// If any error in the tree has an explicit type name, use it, preferring the first one in the DFS traversal.
	var typedErr typedError
	if errors.As(err, &typedErr) {
		return typedErr.ErrorTypeName()
	}

	// Special case for context.Cancel error. It is of type errorString, which is not very useful.
	if errors.Is(err, context.Canceled) {
		return "context.Canceled"
	}
	// Special case for context.DeadlineExceeded error. It is of unexported type deadlineExceededError.
	if errors.Is(err, context.DeadlineExceeded) {
		return "context.DeadlineExceeded"
	}

	// Otherwise, do a DFS traversal of the error tree, ignoring wrapper errors.
	q := []error{err}
	for len(q) > 0 {
		err = q[len(q)-1]
		q = q[:len(q)-1]
		errType := fmt.Sprintf("%T", err)
		if !wrapperErrorTypes[errType] {
			return strings.TrimPrefix(errType, "*")
		}
		// The error could implement zero or one of the unary or multi-error wrapper interfaces. It's impossible to
		// implement both because they have the same method name. As a result, this is still deterministic.
		// In any case, add the unwrapped error(s) to the DFS stack.
		switch t := err.(type) {
		case interface{ Unwrap() error }:
			q = append(q, t.Unwrap())
		case interface{ Unwrap() []error }:
			q = append(q, t.Unwrap()...)
		}
	}
	// This should never happen, but it could if the error is non-nil, the error does not implement TypedError, and
	// there is no non-wrapper error in the error tree. For example, if there is a bug in `errors.Join()` where it
	// accepts an empty slice of errors, then this function will return "unknown" for that error.
	return "unknown"
}
