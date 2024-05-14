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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TSCBWithDynamicSettingsTestSuite struct {
	suite.Suite
}

func TestTSCBWithDynamicSettings(t *testing.T) {
	suite.Run(t, &TSCBWithDynamicSettingsTestSuite{})
}

func TestBasic(t *testing.T) {
	s := assert.New(t)

	name := "test-tscb"
	tscb := NewTwoStepCircuitBreakerWithDynamicSettings(
		Settings{
			SettingsFn: func() map[string]any {
				return map[string]any{}
			},
			Name: name,
		},
	)
	s.Equal(name, tscb.Name())

	doneFn, err := tscb.Allow()
	s.NoError(err)
	doneFn(true)
}

func TestDynamicSettings(t *testing.T) {
	s := assert.New(t)

	settingsCallCount := 0
	tscb := NewTwoStepCircuitBreakerWithDynamicSettings(
		Settings{
			SettingsFn: func() map[string]any {
				settingsCallCount += 1
				if settingsCallCount > 2 {
					return map[string]any{
						maxRequestsKey: 2,
						intervalKey:    3600,
						timeoutKey:     30,
					}
				}
				return map[string]any{}
			},
		},
	)
	s.Equal(1, settingsCallCount)

	// settingsCallCount = 2
	ds := tscb.getDynamicSettings()
	s.Equal(2, settingsCallCount)
	s.Equal(
		dynamicSettings{
			MaxRequests: defaultMaxRequests,
			Interval:    defaultInterval,
			Timeout:     defaultTimeout,
		},
		ds,
	)

	// settingsCallCount = 3
	ds = tscb.getDynamicSettings()
	s.Equal(3, settingsCallCount)
	s.Equal(
		dynamicSettings{
			MaxRequests: 2,
			Interval:    1 * time.Hour,
			Timeout:     30 * time.Second,
		},
		ds,
	)
}

func TestGetInternalCircuitBreaker(t *testing.T) {
	s := assert.New(t)

	settingsCallCount := 0
	tscb := NewTwoStepCircuitBreakerWithDynamicSettings(
		Settings{
			SettingsFn: func() map[string]any {
				settingsCallCount += 1
				if settingsCallCount > 2 {
					return map[string]any{
						maxRequestsKey: 2,
						intervalKey:    3600,
						timeoutKey:     30,
					}
				}
				return map[string]any{}
			},
			SettingsEvalInterval: 2 * time.Second,
		},
	)
	s.Equal(1, settingsCallCount)

	workerFn := func(wg *sync.WaitGroup) {
		defer wg.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		timer := time.NewTimer(9 * time.Second)
		for {
			select {
			case <-ticker.C:
				_ = tscb.getInternalCircuitBreaker()
			case <-timer.C:
				return
			}
		}
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go workerFn(wg)
	}
	wg.Wait()
	// Only one thread updates the settings at regular intervals
	s.Equal(5, settingsCallCount)
}
