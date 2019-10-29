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
	"github.com/stretchr/testify/mock"

	"github.com/uber/cadence/common/persistence"
)

// MockTimerProcessor is used as mock implementation for timerProcessor
type MockTimerProcessor struct {
	mock.Mock
}

var _ timerProcessor = (*MockTimerProcessor)(nil)

// notifyNewTimers is mock implementation for notifyNewTimers of timerProcessor
func (_m *MockTimerProcessor) notifyNewTimers(timerTask []persistence.Task) {
	_m.Called(timerTask)
}

// process is mock implementation for process of timerProcessor
func (_m *MockTimerProcessor) process(task *taskInfo) (int, error) {
	ret := _m.Called(task)

	var r0 int
	if rf, ok := ret.Get(0).(func(*taskInfo) int); ok {
		r0 = rf(task)
	} else {
		r0 = ret.Get(0).(int)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*taskInfo) error); ok {
		r1 = rf(task)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// complete is mock implementation for complete of timerProcessor
func (_m *MockTimerProcessor) complete(task *taskInfo) {
	_m.Called(task)
}

// getTaskFilter is mock implementation for process of timerProcessor
func (_m *MockTimerProcessor) getTaskFilter() taskFilter {
	ret := _m.Called()

	var r0 taskFilter
	if rf, ok := ret.Get(0).(func() taskFilter); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(taskFilter)
		}
	}

	return r0
}
