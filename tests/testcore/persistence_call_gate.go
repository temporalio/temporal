package testcore

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/server/common/config"
)

// PersistenceCallGate blocks matching persistence calls until it is released.
// A released gate does not block new calls.
type PersistenceCallGate struct {
	gate *gate
}

// NewPersistenceCallGate returns a gate and a TestOption that installs it.
// Matching persistence calls block at the gate until it is released.
// The test fails if no matching call reaches the gate.
func NewPersistenceCallGate(opts ...PersistenceFaultOption) (*PersistenceCallGate, TestOption) {
	options := applyPersistenceFaultOptions(opts)

	g := newGate()

	return &PersistenceCallGate{gate: g}, func(o *testOptions) {
		t := o.t

		// Open the gate before cluster cleanup starts.
		go func() {
			<-t.Context().Done()
			g.release()
		}()

		t.Cleanup(func() {
			if !t.Skipped() && g.numArrived() == 0 {
				t.Error("persistence call block was registered but no matching call arrived")
			}
		})
		o.addPersistenceFault(func(target config.FaultInjectionTarget) error {
			if !options.matches(target) {
				return nil
			}
			// The injector does not get the context of the persistence call.
			// Use context.Background so that only a gate release can unblock the call.
			// At test end, blocked calls then run and do not fail with an error.
			return g.arrive(context.Background())
		})
	}
}

// Release lets the blocked calls and all later matching calls run.
func (g *PersistenceCallGate) Release() {
	g.gate.release()
}

// ReleaseWithError makes each blocked call return err.
// Later matching calls run without an error.
func (g *PersistenceCallGate) ReleaseWithError(err error) {
	g.gate.releaseWithError(err)
}

// NumArrived returns the number of matching calls that reached the gate before release.
func (g *PersistenceCallGate) NumArrived() int {
	return g.gate.numArrived()
}

// WaitForPersistenceCall waits for one call to reach the gate.
func WaitForPersistenceCall(t testing.TB, g *PersistenceCallGate, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	if err := g.gate.waitForArrived(ctx, 1); err != nil {
		t.Fatalf("gate: no matching persistence call arrived within %s: %v", timeout, err)
	}
}
