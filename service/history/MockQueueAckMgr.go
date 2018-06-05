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
)

// MockQueueAckMgr is used as mock implementation for QueueAckMgr
type MockQueueAckMgr struct {
	mock.Mock
}

// getFinishedChan is mock implementation for getFinishedChan of QueueAckMgr
func (_m *MockQueueAckMgr) getFinishedChan() <-chan struct{} {
	ret := _m.Called()

	var r0 <-chan struct{}
	if rf, ok := ret.Get(0).(func() <-chan struct{}); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan struct{})
		}
	}
	return r0
}

// readQueueTasks is mock implementation for readQueueTasks of QueueAckMgr
func (_m *MockQueueAckMgr) readQueueTasks() ([]queueTaskInfo, bool, error) {
	ret := _m.Called()

	var r0 []queueTaskInfo
	if rf, ok := ret.Get(0).(func() []queueTaskInfo); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]queueTaskInfo)
		}
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func() bool); ok {
		r1 = rf()
	} else {
		r1 = ret.Get(1).(bool)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func() error); ok {
		r2 = rf()
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// completeTask is mock implementation for completeTask of QueueAckMgr
func (_m *MockQueueAckMgr) completeTask(taskID int64) {
	_m.Called(taskID)
}

// getAckLevel is mock implementation for getAckLevel of QueueAckMgr
func (_m *MockQueueAckMgr) getAckLevel() int64 {
	ret := _m.Called()

	var r0 int64
	if rf, ok := ret.Get(0).(func() int64); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(int64)
		}
	}
	return r0
}

// getReadLevel is mock implementation for getReadLevel of QueueAckMgr
func (_m *MockQueueAckMgr) getReadLevel() int64 {
	ret := _m.Called()

	var r0 int64
	if rf, ok := ret.Get(0).(func() int64); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(int64)
		}
	}
	return r0
}

// updateAckLevel is mock implementation for updateAckLevel of QueueAckMgr
func (_m *MockQueueAckMgr) updateAckLevel() {
	_m.Called()
}
