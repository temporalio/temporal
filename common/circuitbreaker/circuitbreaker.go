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

package circuitbreaker

import (
	"sync"
	"sync/atomic"

	"github.com/sony/gobreaker"

	"go.temporal.io/server/common/dynamicconfig"
)

type (
	TwoStepCircuitBreaker interface {
		Name() string
		State() gobreaker.State
		Counts() gobreaker.Counts
		Allow() (done func(success bool), err error)
	}

	// TwoStepCircuitBreakerWithDynamicSettings is a wrapper of gobreaker.TwoStepCircuitBreaker
	// that calls the settingsFn everytime the Allow function is called and replaces the circuit
	// breaker if there is a change in the settings object. Note that in this case, the previous
	// state of the circuit breaker is lost.
	TwoStepCircuitBreakerWithDynamicSettings struct {
		name          string
		readyToTrip   func(counts gobreaker.Counts) bool
		onStateChange func(name string, from gobreaker.State, to gobreaker.State)

		cb       atomic.Pointer[gobreaker.TwoStepCircuitBreaker]
		cbLock   sync.Mutex
		settings dynamicconfig.CircuitBreakerSettings
	}

	Settings struct {
		// For the following options, check gobreaker docs for details.
		Name          string
		ReadyToTrip   func(counts gobreaker.Counts) bool
		OnStateChange func(name string, from gobreaker.State, to gobreaker.State)
	}
)

var _ TwoStepCircuitBreaker = (*TwoStepCircuitBreakerWithDynamicSettings)(nil)

// Caller must call UpdateSettings once before using this object.
func NewTwoStepCircuitBreakerWithDynamicSettings(
	settings Settings,
) *TwoStepCircuitBreakerWithDynamicSettings {
	return &TwoStepCircuitBreakerWithDynamicSettings{
		name:          settings.Name,
		readyToTrip:   settings.ReadyToTrip,
		onStateChange: settings.OnStateChange,
	}
}

func (c *TwoStepCircuitBreakerWithDynamicSettings) UpdateSettings(
	ds dynamicconfig.CircuitBreakerSettings,
) {
	c.cbLock.Lock()
	defer c.cbLock.Unlock()
	if c.cb.Load() != nil && ds == c.settings {
		return // no change
	}
	c.settings = ds
	c.cb.Store(gobreaker.NewTwoStepCircuitBreaker(gobreaker.Settings{
		Name:          c.name,
		MaxRequests:   uint32(ds.MaxRequests),
		Interval:      ds.Interval,
		Timeout:       ds.Timeout,
		ReadyToTrip:   c.readyToTrip,
		OnStateChange: c.onStateChange,
	}))
}

func (c *TwoStepCircuitBreakerWithDynamicSettings) Name() string {
	return c.cb.Load().Name()
}

func (c *TwoStepCircuitBreakerWithDynamicSettings) State() gobreaker.State {
	return c.cb.Load().State()
}

func (c *TwoStepCircuitBreakerWithDynamicSettings) Counts() gobreaker.Counts {
	return c.cb.Load().Counts()
}

func (c *TwoStepCircuitBreakerWithDynamicSettings) Allow() (done func(success bool), err error) {
	return c.cb.Load().Allow()
}
