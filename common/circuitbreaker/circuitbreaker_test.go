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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/dynamicconfig"
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
	tscb := NewTwoStepCircuitBreakerWithDynamicSettings(Settings{Name: name})
	tscb.UpdateSettings(dynamicconfig.CircuitBreakerSettings{})
	s.Equal(name, tscb.Name())

	doneFn, err := tscb.Allow()
	s.NoError(err)
	doneFn(true)
}

func TestDynamicSettings(t *testing.T) {
	s := assert.New(t)

	tscb := NewTwoStepCircuitBreakerWithDynamicSettings(Settings{})
	tscb.UpdateSettings(dynamicconfig.CircuitBreakerSettings{})
	cb1 := tscb.cb.Load()

	// should not change
	tscb.UpdateSettings(dynamicconfig.CircuitBreakerSettings{})
	cb2 := tscb.cb.Load()
	s.Equal(cb2, cb1)

	// should change
	tscb.UpdateSettings(dynamicconfig.CircuitBreakerSettings{
		MaxRequests: 2,
		Interval:    3600 * time.Second,
		Timeout:     30 * time.Second,
	})
	cb3 := tscb.cb.Load()
	s.NotEqual(cb3, cb2)
}
