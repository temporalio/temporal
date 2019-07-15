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
	"context"
	"time"

	"github.com/uber/cadence/common/log"

	"github.com/stretchr/testify/mock"
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
)

// mockWorkflowExecutionContext is used as mock implementation for workflowExecutionContext
type mockWorkflowExecutionContext struct {
	mock.Mock
}

var _ workflowExecutionContext = (*mockWorkflowExecutionContext)(nil)

func (_m *mockWorkflowExecutionContext) appendFirstBatchEventsForActive(_a0 mutableState, _a1 bool) (int64, persistence.Task, error) {
	ret := _m.Called(_a0, _a1)

	var r0 int64
	if rf, ok := ret.Get(0).(func(mutableState, bool) int64); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 persistence.Task
	if rf, ok := ret.Get(1).(func(mutableState, bool) persistence.Task); ok {
		r1 = rf(_a0, _a1)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(persistence.Task)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(mutableState, bool) error); ok {
		r2 = rf(_a0, _a1)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

func (_m *mockWorkflowExecutionContext) appendFirstBatchEventsForStandby(_a0 mutableState, _a1 []*workflow.HistoryEvent) (int64, persistence.Task, error) {
	ret := _m.Called(_a0, _a1)

	var r0 int64
	if rf, ok := ret.Get(0).(func(mutableState, []*workflow.HistoryEvent) int64); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 persistence.Task
	if rf, ok := ret.Get(1).(func(mutableState, []*workflow.HistoryEvent) persistence.Task); ok {
		r1 = rf(_a0, _a1)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(persistence.Task)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(mutableState, []*workflow.HistoryEvent) error); ok {
		r2 = rf(_a0, _a1)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

func (_m *mockWorkflowExecutionContext) clear() {
	_m.Called()
}

func (_m *mockWorkflowExecutionContext) createWorkflowExecution(_a0 *persistence.WorkflowSnapshot, _a1 int64, _a2 time.Time, _a3 int, _a4 string, _a5 int64) error {

	ret := _m.Called(_a0, _a1, _a2, _a3, _a4, _a5)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.WorkflowSnapshot, int64, time.Time, int, string, int64) error); ok {
		r0 = rf(_a0, _a1, _a2, _a3, _a4, _a5)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *mockWorkflowExecutionContext) getDomainID() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(string)
		}
	}

	return r0
}

func (_m *mockWorkflowExecutionContext) getExecution() *workflow.WorkflowExecution {
	ret := _m.Called()

	var r0 *workflow.WorkflowExecution
	if rf, ok := ret.Get(0).(func() *workflow.WorkflowExecution); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*workflow.WorkflowExecution)
		}
	}

	return r0
}

func (_m *mockWorkflowExecutionContext) getLogger() log.Logger {
	ret := _m.Called()

	var r0 log.Logger
	if rf, ok := ret.Get(0).(func() log.Logger); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(log.Logger)
		}
	}

	return r0
}

func (_m *mockWorkflowExecutionContext) loadWorkflowExecution() (mutableState, error) {
	ret := _m.Called()

	var r0 mutableState
	if rf, ok := ret.Get(0).(func() mutableState); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(mutableState)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *mockWorkflowExecutionContext) loadExecutionStats() (*persistence.ExecutionStats, error) {
	ret := _m.Called()

	var r0 *persistence.ExecutionStats
	if rf, ok := ret.Get(0).(func() *persistence.ExecutionStats); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.ExecutionStats)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *mockWorkflowExecutionContext) lock(_a0 context.Context) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *mockWorkflowExecutionContext) appendFirstBatchHistoryForContinueAsNew(_a0 mutableState, _a1 int64) (int64, error) {
	ret := _m.Called(_a0, _a1)

	var r0 int64
	if rf, ok := ret.Get(0).(func(mutableState, int64) int64); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(mutableState, int64) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *mockWorkflowExecutionContext) replicateWorkflowExecution(_a0 *h.ReplicateEventsRequest, _a1 []persistence.Task, _a2 []persistence.Task, _a3 int64, _a4 time.Time) error {
	ret := _m.Called(_a0, _a1, _a2, _a3, _a4)

	var r0 error
	if rf, ok := ret.Get(0).(func(*h.ReplicateEventsRequest, []persistence.Task, []persistence.Task, int64, time.Time) error); ok {
		r0 = rf(_a0, _a1, _a2, _a3, _a4)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *mockWorkflowExecutionContext) resetMutableState(_a0 string, _a1 int64, _a2 int, _a3 []persistence.Task, _a4 []persistence.Task, _a5 []persistence.Task, _a6 mutableState, _a7 int64) (mutableState, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3, _a4, _a5, _a6, _a7)

	var r0 mutableState
	if rf, ok := ret.Get(0).(func(string, int64, int, []persistence.Task, []persistence.Task, []persistence.Task, mutableState, int64) mutableState); ok {
		r0 = rf(_a0, _a1, _a2, _a3, _a4, _a5, _a6, _a7)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(mutableState)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, int64, int, []persistence.Task, []persistence.Task, []persistence.Task, mutableState, int64) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3, _a4, _a5, _a6, _a7)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *mockWorkflowExecutionContext) resetWorkflowExecution(_a0 mutableState, _a1 bool, _a2, _a3 persistence.Task, _a4 mutableState, _a5 int64, _a6, _a7, _a8, _a9 []persistence.Task, _a10 string, _a11 int64) error {
	ret := _m.Called(_a0, _a1, _a2, _a3, _a4, _a5, _a6, _a7, _a8, _a9, _a10, _a11)
	var r0 error
	if rf, ok := ret.Get(1).(func(mutableState, bool, persistence.Task, persistence.Task, mutableState, int64, []persistence.Task, []persistence.Task, []persistence.Task, []persistence.Task, string, int64) error); ok {
		r0 = rf(_a0, _a1, _a2, _a3, _a4, _a5, _a6, _a7, _a8, _a9, _a10, _a11)
	} else {
		r0 = ret.Error(1)
	}

	return r0
}

func (_m *mockWorkflowExecutionContext) scheduleNewDecision(_a0 []persistence.Task, _a1 []persistence.Task) ([]persistence.Task, []persistence.Task, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []persistence.Task
	if rf, ok := ret.Get(0).(func([]persistence.Task, []persistence.Task) []persistence.Task); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]persistence.Task)
		}
	}

	var r1 []persistence.Task
	if rf, ok := ret.Get(1).(func([]persistence.Task, []persistence.Task) []persistence.Task); ok {
		r1 = rf(_a0, _a1)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).([]persistence.Task)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func([]persistence.Task, []persistence.Task) error); ok {
		r2 = rf(_a0, _a1)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

func (_m *mockWorkflowExecutionContext) unlock() {
	_m.Called()
}

func (_m *mockWorkflowExecutionContext) updateAsPassive(_a0 []persistence.Task, _a1 []persistence.Task, _a2 int64, _a3 time.Time, _a4 bool, _a5 *historyBuilder, _a6 string) error {
	ret := _m.Called(_a0, _a1, _a2, _a3, _a4, _a5, _a6)

	var r0 error
	if rf, ok := ret.Get(0).(func([]persistence.Task, []persistence.Task, int64, time.Time, bool, *historyBuilder, string) error); ok {
		r0 = rf(_a0, _a1, _a2, _a3, _a4, _a5, _a6)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *mockWorkflowExecutionContext) updateAsActive(_a0 []persistence.Task, _a1 []persistence.Task, _a2 int64) error {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 error
	if rf, ok := ret.Get(0).(func([]persistence.Task, []persistence.Task, int64) error); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *mockWorkflowExecutionContext) updateAsActiveWithNew(_a0 []persistence.Task, _a1 []persistence.Task, _a2 int64, _a3 mutableState) error {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	var r0 error
	if rf, ok := ret.Get(0).(func([]persistence.Task, []persistence.Task, int64, mutableState) error); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *mockWorkflowExecutionContext) persistFirstWorkflowEvents(_a0 *persistence.WorkflowEvents) (int64, error) {
	ret := _m.Called(_a0)

	var r0 int64
	if rf, ok := ret.Get(0).(func(*persistence.WorkflowEvents) int64); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.WorkflowEvents) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *mockWorkflowExecutionContext) persistNonFirstWorkflowEvents(_a0 *persistence.WorkflowEvents) (int64, error) {
	ret := _m.Called(_a0)

	var r0 int64
	if rf, ok := ret.Get(0).(func(*persistence.WorkflowEvents) int64); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.WorkflowEvents) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
