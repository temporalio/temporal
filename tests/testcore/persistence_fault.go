package testcore

import (
	"sync/atomic"
	"testing"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/faultinjection"
)

// PersistenceFault determines whether a fault should be injected for a persistence
// operation. It receives the target (data store, method, and request) and returns the
// error to inject, or nil to let the operation reach the real data store.
type PersistenceFault func(target faultinjection.Target) error

// PersistenceFaultOption configures the behavior of [InjectPersistenceFault].
type PersistenceFaultOption func(*persistenceFaultOptions)

type persistenceFaultOptions struct {
	store  config.DataStoreName
	method string
}

// WithStore filters faults to only fire for the given data store,
// e.g. config.ShardStoreName.
func WithStore(store config.DataStoreName) PersistenceFaultOption {
	return func(o *persistenceFaultOptions) {
		o.store = store
	}
}

// WithMethod filters faults to only fire for the given data store method,
// e.g. "UpdateShard".
func WithMethod(method string) PersistenceFaultOption {
	return func(o *persistenceFaultOptions) {
		o.method = method
	}
}

// InjectPersistenceFault registers a programmable persistence fault on reg and returns a
// [TestOption] that wires reg into the test cluster (via the existing config.Injector
// seam). The fault function decides which operations fail and with what error; the
// optional WithStore/WithMethod filters narrow the target. The test fails during cleanup
// if the fault never fired.
//
// Because the fault is wired through config, it is active from the first persistence call
// (including server startup), so it can also exercise startup-time paths such as shard
// acquisition.
//
// Example:
//
//	reg := faultinjection.NewFaultRegistry()
//	env := testcore.NewEnv(s.T(),
//	    testcore.WithHistoryShardCount(1),
//	    testcore.InjectPersistenceFault(s.T(), reg,
//	        func(faultinjection.Target) error {
//	            return &persistence.ShardOwnershipLostError{Msg: "injected fault"}
//	        },
//	        testcore.WithStore(config.ShardStoreName),
//	        testcore.WithMethod("UpdateShard")))
func InjectPersistenceFault(t testing.TB, reg *faultinjection.FaultRegistry, fault PersistenceFault, opts ...PersistenceFaultOption) TestOption {
	t.Helper()

	var options persistenceFaultOptions
	for _, opt := range opts {
		opt(&options)
	}

	var fired atomic.Bool
	reg.RegisterCallback(func(target faultinjection.Target) error {
		if options.store != "" && target.Store != options.store {
			return nil
		}
		if options.method != "" && target.Method != options.method {
			return nil
		}
		if injectedErr := fault(target); injectedErr != nil {
			fired.Store(true)
			t.Logf("Persistence fault injection fired: %s.%s", target.Store, target.Method)
			return injectedErr
		}
		return nil
	})

	t.Cleanup(func() {
		if !fired.Load() {
			t.Error("persistence fault injection was registered but never fired - the fault was never injected")
		}
	})

	return WithPersistenceFaultInjection(&config.FaultInjection{Injector: reg.Inject})
}
