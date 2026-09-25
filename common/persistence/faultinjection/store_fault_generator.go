package faultinjection

import (
	"go.temporal.io/server/common/config"
)

type (
	// storeFaultInjector injects configured and runtime faults.
	storeFaultInjector struct {
		storeName config.DataStoreName
		registry  *FaultRegistry
	}
)

// newStoreFaultInjector returns false when the store has no fault source.
func newStoreFaultInjector(
	storeName config.DataStoreName,
	cfg *config.FaultInjectionDataStoreConfig,
	injector config.FaultInjector,
) (*storeFaultInjector, bool) {
	if injector == nil && len(cfg.Methods) == 0 {
		return nil, false
	}

	registry := NewFaultRegistry()
	// Runtime faults take priority over configured faults.
	if injector != nil {
		registry.register(injectorCallback(injector))
	}
	if len(cfg.Methods) > 0 {
		registry.register(configCallback(cfg))
	}
	return &storeFaultInjector{
		storeName: storeName,
		registry:  registry,
	}, true
}

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

func (d *storeFaultInjector) generate(methodName string, requests ...any) *fault {
	target := Target{
		Store:  d.storeName,
		Method: methodName,
	}
	if len(requests) > 0 {
		target.Request = requests[0]
	}
	return d.registry.generate(target)
}
