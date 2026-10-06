package faultinjection

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/probability"
)

type (
	// storeFaultInjector is an implementation of faultGenerator that will inject errors into the persistence layer
	// using runtime injectors or per-method configuration.
	storeFaultInjector struct {
		storeName config.DataStoreName
		injectors []faultInjector
	}

	faultInjector func(config.FaultInjectionTarget) *fault
)

// newStoreFaultInjector returns a new instance of a data store fault injector that will inject errors
// into the persistence layer based on the provided configuration.
func newStoreFaultInjector(
	storeName config.DataStoreName,
	cfg *config.FaultInjectionDataStoreConfig,
	injector config.FaultInjector,
) (*storeFaultInjector, bool) {
	var injectors []faultInjector
	if injector != nil {
		injectors = append(injectors, runtimeFaultInjector(injector))
	}
	if len(cfg.Methods) > 0 {
		injectors = append(injectors, configuredFaultInjector(cfg))
	}
	if len(injectors) == 0 {
		return nil, false
	}
	return &storeFaultInjector{
		storeName: storeName,
		injectors: injectors,
	}, true
}

func runtimeFaultInjector(injector config.FaultInjector) faultInjector {
	return func(target config.FaultInjectionTarget) *fault {
		err := injector(target)
		if err == nil {
			return nil
		}
		return &fault{err: err}
	}
}

func configuredFaultInjector(cfg *config.FaultInjectionDataStoreConfig) faultInjector {
	methodFaultGenerators := make(map[string]*probability.Sampler[*fault], len(cfg.Methods))
	for methodName, methodConfig := range cfg.Methods {
		methodFaultGenerators[methodName] = probability.NewSampler(methodConfig.Errors, methodConfig.Seed, func(name string, rate float64) *fault {
			return newFault(name, rate, methodName)
		})
	}
	return func(target config.FaultInjectionTarget) *fault {
		methodGenerator, ok := methodFaultGenerators[target.Method]
		if !ok {
			return nil
		}
		f, _ := methodGenerator.Sample()
		return f
	}
}

// generate returns a fault from the first injector that chooses to inject one.
// When this method returns nil, the persistence layer uses the real implementation.
func (d *storeFaultInjector) generate(methodName string, requests ...any) *fault {
	target := config.FaultInjectionTarget{
		Store:  d.storeName,
		Method: methodName,
	}
	if len(requests) > 0 {
		target.Request = requests[0]
	}
	for _, injector := range d.injectors {
		if f := injector(target); f != nil {
			return f
		}
	}
	return nil
}
