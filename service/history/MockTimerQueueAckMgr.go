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

package history

import (
	"github.com/stretchr/testify/mock"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// MockTimerQueueAckMgr is used as mock implementation for TimerQueueAckMgr
type MockTimerQueueAckMgr struct {
	mock.Mock
}

var _ timerQueueAckMgr = (*MockTimerQueueAckMgr)(nil)

// getFinishedChan is mock implementation for readTimerTasks of TimerQueueAckMgr
func (_m *MockTimerQueueAckMgr) getFinishedChan() <-chan struct{} {
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

// readTimerTasks is mock implementation for readTimerTasks of TimerQueueAckMgr
func (_m *MockTimerQueueAckMgr) readTimerTasks() ([]*persistencespb.TimerTaskInfo, *persistencespb.TimerTaskInfo, bool, error) {
	ret := _m.Called()

	var r0 []*persistencespb.TimerTaskInfo
	if rf, ok := ret.Get(0).(func() []*persistencespb.TimerTaskInfo); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*persistencespb.TimerTaskInfo)
		}
	}

	var r1 *persistencespb.TimerTaskInfo
	if rf, ok := ret.Get(1).(func() *persistencespb.TimerTaskInfo); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(*persistencespb.TimerTaskInfo)
		}
	}

	var r2 bool
	if rf, ok := ret.Get(2).(func() bool); ok {
		r2 = rf()
	} else {
		r2 = ret.Get(2).(bool)
	}

	var r3 error
	if rf, ok := ret.Get(3).(func() error); ok {
		r3 = rf()
	} else {
		r3 = ret.Error(3)
	}

	return r0, r1, r2, r3
}

func (_m *MockTimerQueueAckMgr) completeTimerTask(timerTask *persistencespb.TimerTaskInfo) {
	_m.Called(timerTask)
}

func (_m *MockTimerQueueAckMgr) getAckLevel() timerKey {
	ret := _m.Called()

	var r0 timerKey
	if rf, ok := ret.Get(0).(func() timerKey); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(timerKey)
		}
	}
	return r0
}

func (_m *MockTimerQueueAckMgr) getReadLevel() timerKey {
	ret := _m.Called()

	var r0 timerKey
	if rf, ok := ret.Get(0).(func() timerKey); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(timerKey)
		}
	}
	return r0
}

func (_m *MockTimerQueueAckMgr) updateAckLevel() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}
	return r0
}
