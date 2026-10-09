package testcore

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/server/common/config"
)

// NewPersistenceCallGate returns a gate and a TestOption that installs it.
// Matching persistence calls block at the gate until it is released.
// The test fails if no matching call reaches the gate.
func NewPersistenceCallGate(t testing.TB, opts ...PersistenceFaultOption) (*Gate, TestOption) {
	t.Helper()

	options := applyPersistenceFaultOptions(opts)

	gate := NewGate()

	// Open the gate before cluster cleanup starts.
	go func() {
		<-t.Context().Done()
		gate.Release()
	}()

	return gate, func(o *testOptions) {
		o.addPersistenceFault(func(target config.FaultInjectionTarget) error {
			if !options.matches(target) {
				return nil
			}
			// The injector does not get the context of the persistence call.
			// Use context.Background so that only a gate release can unblock the call.
			// At test end, blocked calls then run and do not fail with an error.
			return gate.Arrive(context.Background())
		})

		t.Cleanup(func() {
			if !t.Skipped() && gate.NumArrived() == 0 {
				t.Error("persistence call block was registered but no matching call arrived")
			}
		})
	}
}

// WaitForPersistenceCall waits for one call to reach the gate.
func WaitForPersistenceCall(t testing.TB, gate *Gate, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	if err := gate.WaitForArrived(ctx, 1); err != nil {
		t.Fatalf("gate: no matching persistence call arrived within %s: %v", timeout, err)
	}
}
