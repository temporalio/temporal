package grpcfaults

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/faultinjection"
)

type (
	// Outcome contains the result returned when a gRPC fault matches.
	Outcome = faultinjection.Outcome[any]
	// RequestCallback is a callback function for pre-handler gRPC fault injection.
	RequestCallback = faultinjection.RequestCallback[any, any]
	// ResponseCallback is a callback function for post-handler gRPC fault injection.
	ResponseCallback = faultinjection.ResponseCallback[any, any]
	// Generator checks for gRPC faults before and after a handler runs.
	Generator = faultinjection.Generator[any, any]
	// Hooks connects a CallbackGenerator to externally managed fault callbacks.
	Hooks = faultinjection.Hooks[any, any]
	// Scope identifies a namespace by ID, name, or both. An empty scope applies globally.
	Scope = faultinjection.Scope
	// CallbackGenerator handles fault injection for gRPC requests and responses.
	CallbackGenerator = faultinjection.CallbackGenerator[any, any]
)

// NewCallbackGenerator creates a new CallbackGenerator instance.
func NewCallbackGenerator() *CallbackGenerator {
	return faultinjection.NewCallbackGenerator[any, any]()
}

// NewCallbackGeneratorWithHooks creates a CallbackGenerator connected to external fault hooks.
func NewCallbackGeneratorWithHooks(hooks Hooks) *CallbackGenerator {
	return faultinjection.NewCallbackGeneratorWithHooks[any, any](hooks)
}

// NewOutcome maps a configured transient fault to a service error.
func NewOutcome(name string, _ float64) *Outcome {
	message := "fault injection: " + name
	var err error
	switch name {
	case "Unavailable":
		err = serviceerror.NewUnavailable(message)
	case "Internal":
		err = serviceerror.NewInternal(message)
	case "ResourceExhausted":
		err = &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			Message: message,
		}
	default:
		return nil
	}
	return &Outcome{Error: err}
}
