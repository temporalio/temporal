package history

import (
	"github.com/stretchr/testify/mock"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
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
func (_m *MockTimerQueueAckMgr) readTimerTasks() ([]*persistenceblobs.TimerTaskInfo, *persistenceblobs.TimerTaskInfo, bool, error) {
	ret := _m.Called()

	var r0 []*persistenceblobs.TimerTaskInfo
	if rf, ok := ret.Get(0).(func() []*persistenceblobs.TimerTaskInfo); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*persistenceblobs.TimerTaskInfo)
		}
	}

	var r1 *persistenceblobs.TimerTaskInfo
	if rf, ok := ret.Get(1).(func() *persistenceblobs.TimerTaskInfo); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(*persistenceblobs.TimerTaskInfo)
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

func (_m *MockTimerQueueAckMgr) completeTimerTask(timerTask *persistenceblobs.TimerTaskInfo) {
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

func (_m *MockTimerQueueAckMgr) updateAckLevel() {
	_m.Called()
}
