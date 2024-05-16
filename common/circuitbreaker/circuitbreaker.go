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
	"time"

	"github.com/sony/gobreaker"
	"go.temporal.io/server/common/number"
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
		settingsFn           func() map[string]any
		settingsEvalInterval time.Duration
		settingsLastCheck    atomic.Pointer[time.Time]

		name          string
		readyToTrip   func(counts gobreaker.Counts) bool
		onStateChange func(name string, from gobreaker.State, to gobreaker.State)

		cb              *gobreaker.TwoStepCircuitBreaker
		cbLock          *sync.RWMutex
		dynamicSettings dynamicSettings
	}

	dynamicSettings struct {
		MaxRequests uint32
		Interval    time.Duration
		Timeout     time.Duration
	}

	Settings struct {
		// Function to get the dynamic settings. Accepted keys:
		// - maxRequests: Maximum number of requests allowed to pass through when
		//   it is in half-open state (default 1).
		// - interval (seconds): Cyclic period in closed state to clear the internal counts;
		//   if interval is 0, then it never clears the internal counts (default 0).
		// - timeout (seconds): Period of open state before changing to half-open state (default 60).`
		SettingsFn func() map[string]any
		// Min interval time between calls to SettingsFn. If not set or zero, then it defaults
		// to 1 minute.
		SettingsEvalInterval time.Duration

		// For the following options, check gobreaker docs for details.

		Name          string
		ReadyToTrip   func(counts gobreaker.Counts) bool
		OnStateChange func(name string, from gobreaker.State, to gobreaker.State)
	}
)

var _ TwoStepCircuitBreaker = (*TwoStepCircuitBreakerWithDynamicSettings)(nil)

const (
	maxRequestsKey = "maxRequests"
	intervalKey    = "interval"
	timeoutKey     = "timeout"

	// Zero values indicate to use the default values from gobreaker.Settings.
	defaultMaxRequests = uint32(0)
	defaultInterval    = 0 * time.Second
	defaultTimeout     = 0 * time.Second

	defaultSettingsEvalInterval = 1 * time.Minute
)

func NewTwoStepCircuitBreakerWithDynamicSettings(
	settings Settings,
) *TwoStepCircuitBreakerWithDynamicSettings {
	c := &TwoStepCircuitBreakerWithDynamicSettings{
		settingsEvalInterval: settings.SettingsEvalInterval,

		name:          settings.Name,
		settingsFn:    settings.SettingsFn,
		readyToTrip:   settings.ReadyToTrip,
		onStateChange: settings.OnStateChange,

		cbLock: &sync.RWMutex{},
	}
	if c.settingsEvalInterval <= 0 {
		c.settingsEvalInterval = defaultSettingsEvalInterval
	}
	c.settingsLastCheck.Store(&time.Time{})
	_ = c.getInternalCircuitBreaker()
	return c
}

func (c *TwoStepCircuitBreakerWithDynamicSettings) Name() string {
	return c.cb.Name()
}

func (c *TwoStepCircuitBreakerWithDynamicSettings) State() gobreaker.State {
	return c.cb.State()
}

func (c *TwoStepCircuitBreakerWithDynamicSettings) Counts() gobreaker.Counts {
	return c.cb.Counts()
}

func (c *TwoStepCircuitBreakerWithDynamicSettings) Allow() (done func(success bool), err error) {
	cb := c.getInternalCircuitBreaker()
	return cb.Allow()
}

func (c *TwoStepCircuitBreakerWithDynamicSettings) getDynamicSettings() dynamicSettings {
	settingsMap := c.settingsFn()
	settings := dynamicSettings{
		MaxRequests: defaultMaxRequests,
		Interval:    defaultInterval,
		Timeout:     defaultTimeout,
	}

	if maxRequests, ok := settingsMap[maxRequestsKey]; ok {
		settings.MaxRequests = uint32(
			number.NewNumber(maxRequests).GetUintOrDefault(uint(defaultMaxRequests)),
		)
	}
	if interval, ok := settingsMap[intervalKey]; ok {
		settings.Interval = time.Duration(
			number.NewNumber(interval).GetIntOrDefault(int(defaultInterval.Seconds())),
		) * time.Second
	}
	if timeout, ok := settingsMap[timeoutKey]; ok {
		settings.Timeout = time.Duration(
			number.NewNumber(timeout).GetIntOrDefault(int(defaultTimeout.Seconds())),
		) * time.Second
	}

	return settings
}

// getInternalCircuitBreaker checks if the dynamic settings changed, updating
// the cirbuit breaker if necessary, and returns the up-to-date reference to
// the circuit breaker.
func (c *TwoStepCircuitBreakerWithDynamicSettings) getInternalCircuitBreaker() *gobreaker.TwoStepCircuitBreaker {
	c.cbLock.RLock()
	currentCb := c.cb
	currentDs := c.dynamicSettings
	c.cbLock.RUnlock()

	now := time.Now()
	lastCheck := c.settingsLastCheck.Load()
	if now.Sub(*lastCheck) < c.settingsEvalInterval {
		return currentCb
	}
	if !c.settingsLastCheck.CompareAndSwap(lastCheck, &now) {
		return currentCb
	}

	ds := c.getDynamicSettings()
	if currentCb != nil && ds == currentDs {
		return currentCb
	}

	c.cbLock.Lock()
	defer c.cbLock.Unlock()
	if c.cb != nil && ds == c.dynamicSettings {
		return c.cb
	}
	c.dynamicSettings = ds
	c.cb = gobreaker.NewTwoStepCircuitBreaker(gobreaker.Settings{
		Name:          c.name,
		MaxRequests:   ds.MaxRequests,
		Interval:      ds.Interval,
		Timeout:       ds.Timeout,
		ReadyToTrip:   c.readyToTrip,
		OnStateChange: c.onStateChange,
	})
	return c.cb
}
