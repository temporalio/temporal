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

package mocks

import "github.com/uber/cadence/common/persistence"
import "github.com/stretchr/testify/mock"

// ExecutionManager mock implementation
type ExecutionManager struct {
	mock.Mock
}

// GetName provides a mock function with given fields:
func (_m *ExecutionManager) GetName() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// GetShardID provides a mock function with given fields:
func (_m *ExecutionManager) GetShardID() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// CreateWorkflowExecution provides a mock function with given fields: request
func (_m *ExecutionManager) CreateWorkflowExecution(request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
	ret := _m.Called(request)

	var r0 *persistence.CreateWorkflowExecutionResponse
	if rf, ok := ret.Get(0).(func(*persistence.CreateWorkflowExecutionRequest) *persistence.CreateWorkflowExecutionResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.CreateWorkflowExecutionResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.CreateWorkflowExecutionRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetWorkflowExecution provides a mock function with given fields: request
func (_m *ExecutionManager) GetWorkflowExecution(request *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
	ret := _m.Called(request)

	var r0 *persistence.GetWorkflowExecutionResponse
	if rf, ok := ret.Get(0).(func(*persistence.GetWorkflowExecutionRequest) *persistence.GetWorkflowExecutionResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.GetWorkflowExecutionResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.GetWorkflowExecutionRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateWorkflowExecution provides a mock function with given fields: request
func (_m *ExecutionManager) UpdateWorkflowExecution(request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
	ret := _m.Called(request)

	var r0 *persistence.UpdateWorkflowExecutionResponse
	if rf, ok := ret.Get(0).(func(*persistence.UpdateWorkflowExecutionRequest) *persistence.UpdateWorkflowExecutionResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.UpdateWorkflowExecutionResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.UpdateWorkflowExecutionRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ConflictResolveWorkflowExecution provides a mock function with given fields: request
func (_m *ExecutionManager) ConflictResolveWorkflowExecution(request *persistence.ConflictResolveWorkflowExecutionRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.ConflictResolveWorkflowExecutionRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ResetWorkflowExecution provides a mock function with given fields: request
func (_m *ExecutionManager) ResetWorkflowExecution(request *persistence.ResetWorkflowExecutionRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.ResetWorkflowExecutionRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteTask provides a mock function with given fields: request
func (_m *ExecutionManager) DeleteTask(request *persistence.DeleteTaskRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(executionRequest *persistence.DeleteTaskRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteWorkflowExecution provides a mock function with given fields: request
func (_m *ExecutionManager) DeleteWorkflowExecution(request *persistence.DeleteWorkflowExecutionRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.DeleteWorkflowExecutionRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteCurrentWorkflowExecution provides a mock function with given fields: request
func (_m *ExecutionManager) DeleteCurrentWorkflowExecution(request *persistence.DeleteCurrentWorkflowExecutionRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.DeleteCurrentWorkflowExecutionRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetCurrentExecution provides a mock function with given fields: request
func (_m *ExecutionManager) GetCurrentExecution(request *persistence.GetCurrentExecutionRequest) (*persistence.GetCurrentExecutionResponse, error) {
	ret := _m.Called(request)

	var r0 *persistence.GetCurrentExecutionResponse
	if rf, ok := ret.Get(0).(func(*persistence.GetCurrentExecutionRequest) *persistence.GetCurrentExecutionResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.GetCurrentExecutionResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.GetCurrentExecutionRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetTransferTasks provides a mock function with given fields: request
func (_m *ExecutionManager) GetTransferTasks(request *persistence.GetTransferTasksRequest) (*persistence.GetTransferTasksResponse, error) {
	ret := _m.Called(request)

	var r0 *persistence.GetTransferTasksResponse
	if rf, ok := ret.Get(0).(func(*persistence.GetTransferTasksRequest) *persistence.GetTransferTasksResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.GetTransferTasksResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.GetTransferTasksRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CompleteTransferTask provides a mock function with given fields: request
func (_m *ExecutionManager) CompleteTransferTask(request *persistence.CompleteTransferTaskRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.CompleteTransferTaskRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RangeCompleteTransferTask provides a mock function with given fields: request
func (_m *ExecutionManager) RangeCompleteTransferTask(request *persistence.RangeCompleteTransferTaskRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.RangeCompleteTransferTaskRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetReplicationTasks provides a mock function with given fields: request
func (_m *ExecutionManager) GetReplicationTasks(request *persistence.GetReplicationTasksRequest) (*persistence.GetReplicationTasksResponse, error) {
	ret := _m.Called(request)

	var r0 *persistence.GetReplicationTasksResponse
	if rf, ok := ret.Get(0).(func(*persistence.GetReplicationTasksRequest) *persistence.GetReplicationTasksResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.GetReplicationTasksResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.GetReplicationTasksRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CompleteReplicationTask provides a mock function with given fields: request
func (_m *ExecutionManager) CompleteReplicationTask(request *persistence.CompleteReplicationTaskRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.CompleteReplicationTaskRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RangeCompleteReplicationTask provides a mock function with given fields: request
func (_m *ExecutionManager) RangeCompleteReplicationTask(request *persistence.RangeCompleteReplicationTaskRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.RangeCompleteReplicationTaskRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PutReplicationTaskToDLQ provides a mock function with given fields: request
func (_m *ExecutionManager) PutReplicationTaskToDLQ(request *persistence.PutReplicationTaskToDLQRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.PutReplicationTaskToDLQRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetReplicationTasksFromDLQ provides a mock function with given fields: request
func (_m *ExecutionManager) GetReplicationTasksFromDLQ(request *persistence.GetReplicationTasksFromDLQRequest) (*persistence.GetReplicationTasksFromDLQResponse, error) {
	ret := _m.Called(request)

	var r0 *persistence.GetReplicationTasksFromDLQResponse
	if rf, ok := ret.Get(0).(func(*persistence.GetReplicationTasksFromDLQRequest) *persistence.GetReplicationTasksFromDLQResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.GetReplicationTasksFromDLQResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.GetReplicationTasksFromDLQRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// DeleteReplicationTaskFromDLQ provides a mock function with given fields: request
func (_m *ExecutionManager) DeleteReplicationTaskFromDLQ(
	request *persistence.DeleteReplicationTaskFromDLQRequest,
) error {

	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.DeleteReplicationTaskFromDLQRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RangeDeleteReplicationTaskFromDLQ provides a mock function with given fields: request
func (_m *ExecutionManager) RangeDeleteReplicationTaskFromDLQ(
	request *persistence.RangeDeleteReplicationTaskFromDLQRequest,
) error {

	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.RangeDeleteReplicationTaskFromDLQRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetTimerIndexTasks provides a mock function with given fields: request
func (_m *ExecutionManager) GetTimerIndexTasks(request *persistence.GetTimerIndexTasksRequest) (*persistence.GetTimerIndexTasksResponse, error) {
	ret := _m.Called(request)

	var r0 *persistence.GetTimerIndexTasksResponse
	if rf, ok := ret.Get(0).(func(*persistence.GetTimerIndexTasksRequest) *persistence.GetTimerIndexTasksResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.GetTimerIndexTasksResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.GetTimerIndexTasksRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CompleteTimerTask provides a mock function with given fields: request
func (_m *ExecutionManager) CompleteTimerTask(request *persistence.CompleteTimerTaskRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.CompleteTimerTaskRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RangeCompleteTimerTask provides a mock function with given fields: request
func (_m *ExecutionManager) RangeCompleteTimerTask(request *persistence.RangeCompleteTimerTaskRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.RangeCompleteTimerTaskRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Close provides a mock function with given fields:
func (_m *ExecutionManager) Close() {
	_m.Called()
}

var _ persistence.ExecutionManager = (*ExecutionManager)(nil)
