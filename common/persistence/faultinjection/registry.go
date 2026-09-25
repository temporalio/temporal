package faultinjection

import (
	"context"

	"go.temporal.io/server/common/config"
	commonfaults "go.temporal.io/server/common/faultinjection"
)

type (
	// Target identifies a persistence operation.
	Target = config.FaultInjectionTarget

	// Callback returns the error to inject. Return nil to run the operation.
	// Callbacks must be safe for concurrent use.
	Callback func(Target) error

	// A faultCallback supports faults that must run the operation first.
	faultCallback func(Target) *fault

	// FaultRegistry stores fault callbacks in registration order.
	// It is safe for concurrent use.
	FaultRegistry struct {
		callbacks *commonfaults.CallbackGenerator[Target, *fault]
	}
)

// NewFaultRegistry returns an empty FaultRegistry.
func NewFaultRegistry() *FaultRegistry {
	return &FaultRegistry{
		callbacks: commonfaults.NewCallbackGenerator[Target, *fault](),
	}
}

// RegisterCallback adds a callback. The returned function removes it.
func (r *FaultRegistry) RegisterCallback(cb Callback) func() {
	return r.register(func(t Target) *fault {
		err := cb(t)
		if err == nil {
			return nil
		}
		f := newFaultFromError(err, 1.0)
		return &f
	})
}

func (r *FaultRegistry) register(cb faultCallback) func() {
	if r == nil {
		return func() {}
	}
	return r.callbacks.RegisterRequestCallback(commonfaults.Scope{}, func(_ context.Context, _ string, target Target) *commonfaults.Outcome[*fault] {
		if generated := cb(target); generated != nil {
			return &commonfaults.Outcome[*fault]{Response: generated}
		}
		return nil
	})
}

func (r *FaultRegistry) generate(t Target) *fault {
	if r == nil {
		return nil
	}
	outcome := r.callbacks.GenerateRequest(context.Background(), t.Method, t)
	if outcome == nil {
		return nil
	}
	return outcome.Response
}

// Inject implements config.FaultInjector.
func (r *FaultRegistry) Inject(t Target) error {
	f := r.generate(t)
	if f == nil {
		return nil
	}
	return f.err
}
