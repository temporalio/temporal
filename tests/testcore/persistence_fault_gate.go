package testcore

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/server/common/faultinjection"
	persistencefaults "go.temporal.io/server/common/persistence/faultinjection"
)

// BlockPersistenceCall blocks matching calls before cluster startup.
// The test fails if no matching call reaches the gate.
func BlockPersistenceCall(t testing.TB, opts ...PersistenceFaultOption) (*faultinjection.Gate, TestOption) {
	t.Helper()

	options := applyPersistenceFaultOptions(opts)

	gate := faultinjection.NewGate()

	// Open the gate before cluster cleanup starts.
	go func() {
		<-t.Context().Done()
		gate.Release()
	}()

	return gate, func(o *testOptions) {
		unregister := o.getPersistenceFaultRegistry().RegisterCallback(func(target persistencefaults.Target) error {
			if !options.matches(target) {
				return nil
			}
			return gate.Wait(context.Background())
		})

		t.Cleanup(func() {
			unregister()
			if !t.Skipped() && gate.Arrived() == 0 {
				t.Error("persistence call block was registered but no matching call arrived")
			}
		})
	}
}

// WaitUntilBlocked waits for one call to reach the gate.
func WaitUntilBlocked(t testing.TB, gate *faultinjection.Gate, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	if err := gate.WaitForArrived(ctx, 1); err != nil {
		t.Fatalf("gate: no matching persistence call arrived within %s: %v", timeout, err)
	}
}
