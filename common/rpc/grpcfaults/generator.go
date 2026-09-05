package grpcfaults

import "go.temporal.io/server/common/rpc/faultinjection"

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
