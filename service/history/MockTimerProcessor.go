package history

import (
	"github.com/stretchr/testify/mock"

	"github.com/temporalio/temporal/common/persistence"
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
