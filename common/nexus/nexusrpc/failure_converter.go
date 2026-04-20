package nexusrpc

import (
	"encoding/json"
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
)

// FailureConverter is used by the framework to transform [error] instances to and from [Failure] instances.
// To customize conversion logic, implement this interface and provide your implementation to framework methods such as
// [NewHTTPClient] and [NewHTTPHandler].
// By default the SDK translates [HandlerError], [OperationError] and [FailureError] to and from [Failure]
// objects maintaining their cause chain.
// Arbitrary errors are translated to a [Failure] object with its Message set to the Error() string, losing the cause
// chain.
type FailureConverter interface {
	// ErrorToFailure converts an [error] to a [Failure].
	// Note that the provided error may be nil.
	ErrorToFailure(error) (nexus.Failure, error)
	// FailureToError converts a [Failure] to an [error].
	FailureToError(nexus.Failure) (error, error)
}

type knownErrorFailureConverter struct{}

type serializedHandlerError struct {
	Type              string `json:"type,omitempty"`
	RetryableOverride *bool  `json:"retryableOverride,omitempty"`
}

func (e serializedHandlerError) RetryBehavior() nexus.HandlerErrorRetryBehavior {
	if e.RetryableOverride == nil {
		return nexus.HandlerErrorRetryBehaviorUnspecified
	}
	if *e.RetryableOverride {
		return nexus.HandlerErrorRetryBehaviorRetryable
	}
	return nexus.HandlerErrorRetryBehaviorNonRetryable
}

type serializedOperationError struct {
	State string `json:"state,omitempty"`
}

// ErrorToFailure implements FailureConverter.
// nolint:revive // Keeping all of the logic together for readability, even if it means the function is long.
func (e knownErrorFailureConverter) ErrorToFailure(err error) (nexus.Failure, error) {
	if err == nil {
		return nexus.Failure{}, nil
	}
	// NOTE: not using errors.Unwrap here we are intentionally only supporting unwrapping known errors.
	switch typedErr := err.(type) {
	case *nexus.FailureError:
		f := typedErr.Failure
		// FailureError is has both a Cause error and an underlying Failure Cause.
		// When instantiated directly, there are cases where only the Go error cause is set.
		// Preserve the embedded failure's cause, as the embedded failure is treated similarly to the OriginalFailure field for well-known other error types.
		if typedErr.Cause != nil && f.Cause == nil {
			c, err := e.ErrorToFailure(typedErr.Cause)
			if err != nil {
				return nexus.Failure{}, err
			}
			f.Cause = &c
		}
		return f, nil
	case *nexus.HandlerError:
		if typedErr.OriginalFailure != nil {
			return *typedErr.OriginalFailure, nil
		}
		data := serializedHandlerError{
			Type:              string(typedErr.Type),
			RetryableOverride: retryBehaviorAsOptionalBool(typedErr),
		}
		var details []byte
		details, err := json.Marshal(data)
		if err != nil {
			return nexus.Failure{}, err
		}
		f := nexus.Failure{
			Message:    typedErr.Message,
			StackTrace: typedErr.StackTrace,
			Metadata: map[string]string{
				"type": "nexus.HandlerError",
			},
			Details: details,
		}

		if typedErr.Cause != nil {
			c, err := e.ErrorToFailure(typedErr.Cause)
			if err != nil {
				return nexus.Failure{}, err
			}
			f.Cause = &c
		}
		return f, nil
	case *nexus.OperationError:
		if typedErr.OriginalFailure != nil {
			return *typedErr.OriginalFailure, nil
		}
		data := serializedOperationError{
			State: string(typedErr.State),
		}
		details, err := json.Marshal(data)
		if err != nil {
			return nexus.Failure{}, err
		}
		f := nexus.Failure{
			Message:    typedErr.Message,
			StackTrace: typedErr.StackTrace,
			Metadata: map[string]string{
				"type": "nexus.OperationError",
			},
			Details: details,
		}

		if typedErr.Cause != nil {
			c, err := e.ErrorToFailure(typedErr.Cause)
			if err != nil {
				return nexus.Failure{}, err
			}
			f.Cause = &c
		}
		return f, nil
	default:
		return nexus.Failure{
			Message: typedErr.Error(),
		}, nil
	}
}

// FailureToError implements FailureConverter.
// nolint:revive // Keeping all of the logic together for readability, even if it means the function is long.
func (e knownErrorFailureConverter) FailureToError(f nexus.Failure) (error, error) {
	if f.Metadata != nil {
		switch f.Metadata["type"] {
		case "nexus.HandlerError":
			var se serializedHandlerError
			err := json.Unmarshal(f.Details, &se)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize HandlerError: %w", err)
			}
			he := &nexus.HandlerError{
				Message:         f.Message,
				StackTrace:      f.StackTrace,
				Type:            nexus.HandlerErrorType(se.Type),
				RetryBehavior:   se.RetryBehavior(),
				OriginalFailure: &f,
			}
			if f.Cause != nil {
				he.Cause, err = e.FailureToError(*f.Cause)
				if err != nil {
					return nil, err
				}
			}
			return he, nil
		case "nexus.OperationError":
			var se serializedOperationError
			err := json.Unmarshal(f.Details, &se)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize OperationError: %w", err)
			}
			oe := &nexus.OperationError{
				Message:         f.Message,
				StackTrace:      f.StackTrace,
				State:           nexus.OperationState(se.State),
				OriginalFailure: &f,
			}
			if f.Cause != nil {
				oe.Cause, err = e.FailureToError(*f.Cause)
				if err != nil {
					return nil, err
				}
			}
			return oe, nil
		}
	}
	// Note that the original failure cause is retained on the FailureError's failure object.
	fe := &nexus.FailureError{Failure: f}
	if f.Cause != nil {
		c, err := e.FailureToError(*f.Cause)
		if err != nil {
			return nil, err
		}
		fe.Cause = c
	}
	return fe, nil
}

var defaultFailureConverter FailureConverter = knownErrorFailureConverter{}

// DefaultFailureConverter returns the package's default [FailureConverter] implementation. Translates [HandlerError],
// [OperationError] and [FailureError] to and from [Failure] objects maintaining their cause chain.
// Arbitrary errors are translated to a [Failure] object with its Message set to the Error() string, losing the cause
// chain.
// [Failure] instances are converted to [FailureError] to allow access to the full failure metadata and details if
// available.
func DefaultFailureConverter() FailureConverter {
	return defaultFailureConverter
}

func retryBehaviorAsOptionalBool(e *nexus.HandlerError) *bool {
	// nolint:exhaustive // this is a simple optional boolean.
	switch e.RetryBehavior {
	case nexus.HandlerErrorRetryBehaviorRetryable:
		ret := true
		return &ret
	case nexus.HandlerErrorRetryBehaviorNonRetryable:
		ret := false
		return &ret
	}
	return nil
}
