// Copyright (c) 2017 Uber Technologies, Inc.
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

package history

import (
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/uber/cadence/common/persistence"
)

// MockTimerQueueProcessor is used as mock implementation for Processor
type MockTimerQueueProcessor struct {
	mock.Mock
}

// Start is mock implementation for Start of Processor
func (_m *MockTimerQueueProcessor) Start() {
	_m.Called()
}

// Stop is mock implementation for Stop of Processor
func (_m *MockTimerQueueProcessor) Stop() {
	_m.Called()
}

// NotifyNewTimers is mock implementation for NotifyNewTimers of Processor
func (_m *MockTimerQueueProcessor) NotifyNewTimers(clusterName string, currentTime time.Time, timerTask []persistence.Task) {
	_m.Called(clusterName, timerTask)
}
