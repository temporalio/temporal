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

package client

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
)

// NewTargetedDataStoreErrorGenerator returns a new instance of a data store error generator that will inject errors
// into the persistence layer based on the provided configuration.
func NewTargetedDataStoreErrorGenerator(cfg *config.FaultInjectionDataStoreConfig) ErrorGenerator {
	methods := make(map[string]ErrorGenerator, len(cfg.Methods))
	for methodName, methodConfig := range cfg.Methods {
		var faultWeights []FaultWeight
		methodErrRate := 0.0
		for errName, errRate := range methodConfig.Errors {
			err := newError(errName, errRate)
			faultWeights = append(faultWeights, FaultWeight{
				errFactory: func(data string) error {
					return err
				},
				weight: errRate,
			})
			methodErrRate += errRate
		}
		errGenerator := NewDefaultErrorGenerator(methodErrRate, faultWeights)
		seed := methodConfig.Seed
		if seed == 0 {
			seed = time.Now().UnixNano()
		}
		errGenerator.r = rand.New(rand.NewSource(seed))
		methods[methodName] = errGenerator
	}
	return &dataStoreErrorGenerator{MethodErrorGenerators: methods}
}

// dataStoreErrorGenerator is an implementation of ErrorGenerator that will inject errors into the persistence layer
// using a per-method configuration.
type dataStoreErrorGenerator struct {
	MethodErrorGenerators map[string]ErrorGenerator
}

// Generate returns an error from the configured error types and rates for this method.
// This method infers the fault injection target's method name from the function name of the caller.
// As a result, this method should only be called from the persistence layer.
// This method will panic if the method name cannot be inferred.
// If no errors are configured for the method, or if there are some errors configured for this method,
// but no error is sampled, then this method returns nil.
// When this method returns nil, this causes the persistence layer to use the real implementation.
func (d *dataStoreErrorGenerator) Generate() error {
	pc, _, _, ok := runtime.Caller(1)
	if !ok {
		panic("failed to get caller info")
	}
	runtimeFunc := runtime.FuncForPC(pc)
	if runtimeFunc == nil {
		panic("failed to get runtime function")
	}
	parts := strings.Split(runtimeFunc.Name(), ".")
	methodName := parts[len(parts)-1]
	methodErrorGenerator, ok := d.MethodErrorGenerators[methodName]
	if !ok {
		return nil
	}
	return methodErrorGenerator.Generate()
}

// newError returns an error based on the provided name. If the name is not recognized, then this method will
// panic.
func newError(errName string, errRate float64) error {
	switch errName {
	case "ShardOwnershipLost":
		return &persistence.ShardOwnershipLostError{Msg: fmt.Sprintf("fault injection error (%f): persistence.ShardOwnershipLostError", errRate)}
	case "DeadlineExceeded":
		return fmt.Errorf("fault injection error (%f): %w", errRate, context.DeadlineExceeded)
	case "ResourceExhausted":
		return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, fmt.Sprintf("fault injection error (%f): serviceerror.ResourceExhausted", errRate))
	case "Unavailable":
		return serviceerror.NewUnavailable(fmt.Sprintf("fault injection error (%f): serviceerror.Unavailable", errRate))
	default:
		panic(fmt.Sprintf("unknown error type: %v", errName))
	}
}

// UpdateRate should not be called for the data store error generator since the rate is defined on a per-method basis.
func (d *dataStoreErrorGenerator) UpdateRate(rate float64) {
	panic("UpdateRate not supported for data store error generators")
}

// UpdateWeights should not be called for the data store error generator since the weights are defined on a per-method
// basis.
func (d *dataStoreErrorGenerator) UpdateWeights(weights []FaultWeight) {
	panic("UpdateWeights not supported for data store error generators")
}

// Rate should not be called for the data store error generator since there is no global rate for the data store, only
// per-method rates.
func (d *dataStoreErrorGenerator) Rate() float64 {
	panic("Rate not supported for data store error generators")
}
