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

// completeQueueTask is mock implementation for completeQueueTask of QueueAckMgr
func (_m *MockQueueAckMgr) completeQueueTask(taskID int64) {
	_m.Called(taskID)
}

// getQueueAckLevel is mock implementation for getQueueAckLevel of QueueAckMgr
func (_m *MockQueueAckMgr) getQueueAckLevel() int64 {
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

// getQueueReadLevel is mock implementation for getReadLevel of QueueAckMgr
func (_m *MockQueueAckMgr) getQueueReadLevel() int64 {
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

// updateQueueAckLevel is mock implementation for updateQueueAckLevel of QueueAckMgr
func (_m *MockQueueAckMgr) updateQueueAckLevel() {
	_m.Called()
}
