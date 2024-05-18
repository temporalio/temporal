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
	"math/rand"
	"sync"
	"time"
)

type (
	faultMetadata struct {
		fault     fault
		threshold float64 // threshold for this fault to be returned
	}

	methodFaultGenerator struct {
		rndMu sync.Mutex
		rnd   *rand.Rand // rand is not thread-safe

		rate           float64         // chance for one of the errors for this method to be returned
		faultsMetadata []faultMetadata // faults with their thresholds that might be generated for this method
	}
)

func newMethodFaultGenerator(faults []fault, seed int64) *methodFaultGenerator {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	fm := make([]faultMetadata, 0, len(faults))
	totalRate := 0.0
	for _, f := range faults {
		totalRate += f.rate
		fm = append(fm, faultMetadata{
			fault:     f,
			threshold: totalRate,
		})
	}

	return &methodFaultGenerator{
		rate:           totalRate,
		faultsMetadata: fm,
		rnd:            rand.New(rand.NewSource(seed)),
	}
}

func (p *methodFaultGenerator) generate() *fault {
	if p.rate <= 0 {
		return nil
	}

	p.rndMu.Lock()
	roll := p.rnd.Float64()
	p.rndMu.Unlock()

	if roll < p.rate {
		// Yes, this method call should be failed.
		// Let's find out with what fault.
		for i := range p.faultsMetadata {
			if roll < p.faultsMetadata[i].threshold {
				return &p.faultsMetadata[i].fault
			}
		}
	}
	return nil
}
