package history

import (
	"github.com/stretchr/testify/mock"
)

// MockProcessor is used as mock implementation for Processor
type MockProcessor struct {
	mock.Mock
}

// process is mock implementation for process of Processor
func (_m *MockProcessor) process(task *taskInfo) (int, error) {
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

// complete is mock implementation for complete of Processor
func (_m *MockProcessor) complete(task *taskInfo) {
	_m.Called(task)
}

// getTaskFilter is mock implementation for process of Processor
func (_m *MockProcessor) getTaskFilter() taskFilter {
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

// readTasks is mock implementation for readTasks of Processor
func (_m *MockProcessor) readTasks(readLevel int64) ([]queueTaskInfo, bool, error) {
	ret := _m.Called(readLevel)

	var r0 []queueTaskInfo
	if rf, ok := ret.Get(0).(func(int64) []queueTaskInfo); ok {
		r0 = rf(readLevel)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]queueTaskInfo)
		}
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func(int64) bool); ok {
		r1 = rf(readLevel)
	} else {
		r1 = ret.Get(1).(bool)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(int64) error); ok {
		r2 = rf(readLevel)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// updateAckLevel is mock implementation for updateAckLevel of Processor
func (_m *MockProcessor) updateAckLevel(taskID int64) error {
	ret := _m.Called(taskID)

	var r0 error
	if rf, ok := ret.Get(0).(func(int64) error); ok {
		r0 = rf(taskID)
	} else {
		r0 = ret.Error(0)
	}
	return r0
}

// queueShutdown is mock implementation for queueShutdown of Processor
func (_m *MockProcessor) queueShutdown() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}
	return r0
}
