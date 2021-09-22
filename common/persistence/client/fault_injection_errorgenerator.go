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
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"go.uber.org/atomic"
)

type (
	DefaultErrorGenerator struct {
		sync.Mutex
		rate          atomic.Float64  // chance for error to be returned
		r             *rand.Rand      // rand is not thread-safe
		faultMetadata []FaultMetadata //
		faultWeights  []FaultWeight
	}

	ErrorGenerator interface {
		Generate() error
		UpdateRate(rate float64)
		UpdateWeights(weights []FaultWeight)
		Rate() float64
	}

	FaultWeight struct {
		errFactory ErrorFactory
		weight     float64
	}

	ErrorFactory func(string) error

	FaultMetadata struct {
		errFactory ErrorFactory
		threshold  float64
	}

	NoopErrorGenerator struct{}
)

func calculateErrorRates(rate float64, weights []FaultWeight) []FaultMetadata {
	totalCount := 0.0
	for _, w := range weights {
		totalCount += w.weight
	}

	faultMeta := make([]FaultMetadata, len(weights))
	count := 0
	weightSum := 0.0
	for _, w := range weights {
		weightSum += w.weight
		faultMeta[count] = FaultMetadata{
			errFactory: w.errFactory,
			threshold:  weightSum / totalCount * rate,
		}
		count++
	}

	sort.Slice(
		faultMeta,
		func(left int, right int) bool { return faultMeta[left].threshold < faultMeta[right].threshold },
	)

	return faultMeta
}

func (p *DefaultErrorGenerator) Rate() float64 {
	return p.rate.Load()
}

func (p *DefaultErrorGenerator) UpdateRate(rate float64) {
	if rate > 1 {
		rate = 1
	}

	if rate < 0 {
		rate = 0
	}

	newFaultMetadata := calculateErrorRates(rate, p.faultWeights)

	p.Lock()
	defer p.Unlock()

	p.rate.Store(rate)
	p.faultMetadata = newFaultMetadata
}

func (p *DefaultErrorGenerator) UpdateWeights(errorWeights []FaultWeight) {
	updatedRates := calculateErrorRates(p.rate.Load(), errorWeights)
	p.Lock()
	defer p.Unlock()
	p.faultMetadata = updatedRates
	p.faultWeights = errorWeights
}

func NewDefaultErrorGenerator(rate float64, errorWeights []FaultWeight) *DefaultErrorGenerator {
	result := &DefaultErrorGenerator{
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	result.UpdateRate(rate)
	result.UpdateWeights(errorWeights)
	return result
}

func (p *DefaultErrorGenerator) Generate() error {
	if p.rate.Load() <= 0 {
		return nil
	}

	p.Lock()
	defer p.Unlock()

	if roll := p.r.Float64(); roll < p.rate.Load() {
		var result error
		for _, fm := range p.faultMetadata {
			if roll < fm.threshold {
				msg := fmt.Sprintf(
					"FaultInjectionGenerator. rate %f, roll: %f, treshold: %f",
					p.rate.Load(),
					roll,
					fm.threshold,
				)
				result = fm.errFactory(msg)
				break
			}
		}
		return result
	}
	return nil
}

func NewNoopErrorGenerator() *NoopErrorGenerator {
	return &NoopErrorGenerator{}
}

func (p *NoopErrorGenerator) Generate() error {
	return nil
}

func (p *NoopErrorGenerator) Rate() float64 { return 0 }

func (p *NoopErrorGenerator) UpdateRate(rate float64) {}

func (p *NoopErrorGenerator) UpdateWeights(weights []FaultWeight) {}
