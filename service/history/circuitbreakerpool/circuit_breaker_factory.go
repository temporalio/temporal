// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package circuitbreakerpool

import (
	"go.temporal.io/server/common/circuitbreaker"
	"go.temporal.io/server/common/collection"
)

type CircuitBreakerPool[K comparable] struct {
	m *collection.OnceMap[K, circuitbreaker.TwoStepCircuitBreaker]
}

func (p *CircuitBreakerPool[K]) Get(key K) circuitbreaker.TwoStepCircuitBreaker {
	return p.m.Get(key)
}

func NewCircuitBreakerPool[K comparable](
	constructor func(key K) circuitbreaker.TwoStepCircuitBreaker,
) *CircuitBreakerPool[K] {
	return &CircuitBreakerPool[K]{
		m: collection.NewOnceMap(constructor),
	}
}
