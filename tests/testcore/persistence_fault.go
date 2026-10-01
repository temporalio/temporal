package testcore

import (
	"testing"

	"go.temporal.io/server/common/config"
)

// PersistenceFault returns the error to inject. Return nil to run the operation.
type PersistenceFault func(target config.FaultInjectionTarget) error

// PersistenceFaultOption limits which operations receive a fault.
type PersistenceFaultOption func(*persistenceFaultOptions)

// InjectPersistenceFault adds a fault and enables it before cluster startup.
// The test fails if the fault does not fire.
func InjectPersistenceFault(t testing.TB, fault PersistenceFault, opts ...PersistenceFaultOption) TestOption {
	t.Helper()

	options := applyPersistenceFaultOptions(opts)
	tracker := newFaultTracker(t)
	return func(o *testOptions) {
		o.addPersistenceFault(func(target config.FaultInjectionTarget) error {
			if !options.matches(target) {
				return nil
			}
			injectedErr := fault(target)
			if injectedErr == nil {
				return nil
			}
			tracker.markFired(target)
			return injectedErr
		})
		tracker.attach(func() {})
	}
}

// WithStore limits a fault to one data store.
func WithStore(store config.DataStoreName) PersistenceFaultOption {
	return func(o *persistenceFaultOptions) {
		o.store = store
	}
}

// WithMethod limits a fault to one method.
func WithMethod(method string) PersistenceFaultOption {
	return func(o *persistenceFaultOptions) {
		o.method = method
	}
}

type persistenceFaultOptions struct {
	store  config.DataStoreName
	method string
}

func (o persistenceFaultOptions) matches(target config.FaultInjectionTarget) bool {
	if o.store != "" && target.Store != o.store {
		return false
	}
	if o.method != "" && target.Method != o.method {
		return false
	}
	return true
}

func applyPersistenceFaultOptions(opts []PersistenceFaultOption) persistenceFaultOptions {
	var options persistenceFaultOptions
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

// addPersistenceFault sets one runtime injector for all faults of the test.
func (o *testOptions) addPersistenceFault(fault PersistenceFault) {
	if len(o.persistenceFaults) == 0 {
		o.dedicatedCluster = true
		o.clusterOptions = append(o.clusterOptions, WithFaultInjectionConfig(&config.FaultInjection{
			Injector: o.chainedPersistenceFaultInjector,
		}))
		o.dedicatedReason = "fault injection config used"
	}
	o.persistenceFaults = append(o.persistenceFaults, fault)
}

// chainedPersistenceFaultInjector returns the error of the first fault that returns one.
// This allows us to inject multiple faults in test without overwriting one another (although
// an implicit order exists here, where the faults are added FIFO so the first fault error
// depends on injection order).
func (o *testOptions) chainedPersistenceFaultInjector(target config.FaultInjectionTarget) error {
	for _, fault := range o.persistenceFaults {
		if err := fault(target); err != nil {
			return err
		}
	}
	return nil
}
