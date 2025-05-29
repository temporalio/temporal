package faultinjection

import (
	"go.temporal.io/server/common/config"
)

type (
	// storeFaultGenerator is an implementation of faultGenerator that will inject errors into the persistence layer
	// using a per-method configuration.
	storeFaultGenerator struct {
		methodFaultGenerators map[string]faultGenerator
	}
)

// newStoreFaultGenerator returns a new instance of a data store error generator that will inject errors
// into the persistence layer based on the provided configuration.
func newStoreFaultGenerator(cfg *config.FaultInjectionDataStoreConfig) *storeFaultGenerator {
	methodFaultGenerators := make(map[string]faultGenerator, len(cfg.Methods))
	for methodName, methodConfig := range cfg.Methods {
		var faults []fault
		for errName, errRate := range methodConfig.Errors {
			faults = append(faults, newFault(errName, errRate, methodName))
		}
		methodFaultGenerators[methodName] = newMethodFaultGenerator(faults, methodConfig.Seed)
	}
	return &storeFaultGenerator{
		methodFaultGenerators: methodFaultGenerators,
	}
}

// Generate returns an error from the configured error types and rates for this method.
// If no errors are configured for the method, or if there are some errors configured for this method,
// but no error is sampled, then this method returns nil.
// When this method returns nil, this causes the persistence layer to use the real implementation.
func (d *storeFaultGenerator) generate(methodName string) *fault {
	methodGenerator, ok := d.methodFaultGenerators[methodName]
	if !ok {
		return nil
	}
	return methodGenerator.generate(methodName)
}
