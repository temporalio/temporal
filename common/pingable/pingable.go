package pingable

import "time"

type (
	// Pingable is interface to check for liveness of a component, to detect deadlocks.
	// This call should not block.
	Pingable interface {
		GetPingChecks() []Check
	}

	Check struct {
		// Name of this component.
		Name string
		// The longest time that Ping can take. If it doesn't return in that much time, that's
		// considered a deadlock and the deadlock detector may take actions to recover, like
		// killing the process.
		Timeout time.Duration
		// Perform the check. The typical implementation will just be Lock() and then Unlock()
		// on a mutex, returning nil. Ping can also return more Pingables for sub-components
		// that will be checked independently. These should form a tree and not lead to cycles.
		Ping func() []Pingable

		// Metrics recording:
		// Timer id within DeadlockDetectorScope (or zero for no metrics)
		MetricsName string
	}
)
