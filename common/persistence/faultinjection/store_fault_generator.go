// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
