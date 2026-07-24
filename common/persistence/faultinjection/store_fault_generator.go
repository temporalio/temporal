package faultinjection

import (
	"go.temporal.io/server/common/config"
)

type (
	// storeFaultInjector is an implementation of faultGenerator that injects errors into
	// the persistence layer using a FaultRegistry. Both the per-method YAML configuration
	// and the runtime config.Injector are registered as regular callbacks, so config-based
	// fault injection is a special case of the generic programmable registry.
	storeFaultInjector struct {
		storeName config.DataStoreName
		registry  *FaultRegistry
	}
)

// newStoreFaultInjector returns a new instance of a data store fault injector that will
// inject errors into the persistence layer based on the provided configuration and/or
// runtime injector. It returns false when neither is configured.
func newStoreFaultInjector(
	storeName config.DataStoreName,
	cfg *config.FaultInjectionDataStoreConfig,
	injector config.FaultInjector,
) (*storeFaultInjector, bool) {
	registry := NewFaultRegistry()
	if len(cfg.Methods) > 0 {
		registry.register(configCallback(cfg))
	}
	if injector != nil {
		registry.register(injectorCallback(injector))
	}
	if !registry.HasCallbacks() {
		return nil, false
	}
	return &storeFaultInjector{
		storeName: storeName,
		registry:  registry,
	}, true
}

// configCallback builds a faultCallback from per-method YAML configuration.
func configCallback(cfg *config.FaultInjectionDataStoreConfig) faultCallback {
	methodFaultGenerators := make(map[string]faultGenerator, len(cfg.Methods))
	for methodName, methodConfig := range cfg.Methods {
		var faults []fault
		for errName, errRate := range methodConfig.Errors {
			faults = append(faults, newFault(errName, errRate, methodName))
		}
		methodFaultGenerators[methodName] = newMethodFaultGenerator(faults, methodConfig.Seed)
	}
	return func(target Target) *fault {
		methodGenerator, ok := methodFaultGenerators[target.Method]
		if !ok {
			return nil
		}
		return methodGenerator.generate(target.Method)
	}
}

// injectorCallback adapts a config.FaultInjector (a programmable hook set through config)
// into a faultCallback.
func injectorCallback(injector config.FaultInjector) faultCallback {
	return func(target Target) *fault {
		err := injector(target)
		if err == nil {
			return nil
		}
		f := newFaultFromError(err, 1.0)
		return &f
	}
}

// generate returns a fault from the registry. When this method returns nil, the
// persistence layer uses the real implementation.
func (d *storeFaultInjector) generate(methodName string, requests ...any) *fault {
	target := Target{
		Store:  d.storeName,
		Method: methodName,
	}
	if len(requests) > 0 {
		target.Request = requests[0]
	}
	return d.registry.Generate(target)
}
