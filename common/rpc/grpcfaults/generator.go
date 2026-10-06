package grpcfaults

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/rpc/faults"
)

type (
	// Generators contains inbound and outbound fault hooks.
	Generators = faults.Generators[any, any]
	// Outcome contains the result returned when a gRPC fault matches.
	Outcome = faults.Outcome[any]
	// RequestCallback is a callback function for pre-handler gRPC fault injection.
	RequestCallback = faults.RequestCallback[any, any]
	// ResponseCallback is a callback function for post-handler gRPC fault injection.
	ResponseCallback = faults.ResponseCallback[any, any]
	// Generator checks for gRPC faults before and after a handler runs.
	Generator = faults.Generator[any, any]
	// Hooks connects a CallbackGenerator to externally managed fault callbacks.
	Hooks = faults.Hooks[any, any]
	// Scope identifies a namespace by ID, name, or both. An empty scope applies globally.
	Scope = faults.Scope
	// CallbackGenerator handles fault injection for gRPC requests and responses.
	CallbackGenerator = faults.CallbackGenerator[any, any]
)

// NewCallbackGenerator creates a new CallbackGenerator instance.
func NewCallbackGenerator() *CallbackGenerator {
	return faults.NewCallbackGenerator[any, any]()
}

// NewCallbackGeneratorWithHooks creates a CallbackGenerator connected to external fault hooks.
func NewCallbackGeneratorWithHooks(hooks Hooks) *CallbackGenerator {
	return faults.NewCallbackGeneratorWithHooks[any, any](hooks)
}

func NewConfiguredGenerators(cfg *config.TransportFaultInjection, fallback Generators) (Generators, error) {
	return faults.NewConfiguredGenerators(cfg, configuredOutcome, fallback, func(_ any, err error) bool { return err == nil })
}

func configuredOutcome(name string) *Outcome {
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
