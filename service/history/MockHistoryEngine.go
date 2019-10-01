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

	"github.com/stretchr/testify/mock"
	gohistory "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
)

var _ Engine = (*MockHistoryEngine)(nil)

// MockHistoryEngine is used as mock implementation for HistoryEngine
type MockHistoryEngine struct {
	mock.Mock
}

// Start is mock implementation for Start for HistoryEngine
func (_m *MockHistoryEngine) Start() {
	_m.Called()
}

// Stop is mock implementation for Stop of HistoryEngine
func (_m *MockHistoryEngine) Stop() {
	_m.Called()
}

// StartWorkflowExecution is mock implementation for StartWorkflowExecution of HistoryEngine
func (_m *MockHistoryEngine) StartWorkflowExecution(ctx context.Context, request *gohistory.StartWorkflowExecutionRequest) (*shared.StartWorkflowExecutionResponse, error) {
	ret := _m.Called(ctx, request)

	var r0 *shared.StartWorkflowExecutionResponse
	if rf, ok := ret.Get(0).(func(*gohistory.StartWorkflowExecutionRequest) *shared.StartWorkflowExecutionResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.StartWorkflowExecutionResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*gohistory.StartWorkflowExecutionRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// DescribeMutableState is mock implementation for DescribeMutableState of HistoryEngine
func (_m *MockHistoryEngine) DescribeMutableState(ctx context.Context, request *gohistory.DescribeMutableStateRequest) (*gohistory.DescribeMutableStateResponse, error) {
	ret := _m.Called(ctx, request)

	var r0 *gohistory.DescribeMutableStateResponse
	if rf, ok := ret.Get(0).(func(*gohistory.DescribeMutableStateRequest) *gohistory.DescribeMutableStateResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gohistory.DescribeMutableStateResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*gohistory.DescribeMutableStateRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// QueryWorkflow is mock implementation for QueryWorkflow of HistoryEngine
func (_m *MockHistoryEngine) QueryWorkflow(ctx context.Context, request *gohistory.QueryWorkflowRequest) (*gohistory.QueryWorkflowResponse, error) {
	ret := _m.Called(ctx, request)

	var r0 *gohistory.QueryWorkflowResponse
	if rf, ok := ret.Get(0).(func(*gohistory.QueryWorkflowRequest) *gohistory.QueryWorkflowResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gohistory.QueryWorkflowResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*gohistory.QueryWorkflowRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetMutableState is mock implementation for GetMutableState of HistoryEngine
func (_m *MockHistoryEngine) GetMutableState(ctx context.Context, request *gohistory.GetMutableStateRequest) (*gohistory.GetMutableStateResponse, error) {
	ret := _m.Called(ctx, request)

	var r0 *gohistory.GetMutableStateResponse
	if rf, ok := ret.Get(0).(func(*gohistory.GetMutableStateRequest) *gohistory.GetMutableStateResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gohistory.GetMutableStateResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*gohistory.GetMutableStateRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// PollMutableState is mock implementation for GetMutableState of HistoryEngine
func (_m *MockHistoryEngine) PollMutableState(ctx context.Context, request *gohistory.PollMutableStateRequest) (*gohistory.PollMutableStateResponse, error) {
	ret := _m.Called(ctx, request)

	var r0 *gohistory.PollMutableStateResponse
	if rf, ok := ret.Get(0).(func(*gohistory.PollMutableStateRequest) *gohistory.PollMutableStateResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gohistory.PollMutableStateResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*gohistory.PollMutableStateRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ResetStickyTaskList is mock implementation for ResetStickyTaskList of HistoryEngine
func (_m *MockHistoryEngine) ResetStickyTaskList(ctx context.Context, request *gohistory.ResetStickyTaskListRequest) (*gohistory.ResetStickyTaskListResponse, error) {
	ret := _m.Called(request)

	var r0 *gohistory.ResetStickyTaskListResponse
	if rf, ok := ret.Get(0).(func(*gohistory.ResetStickyTaskListRequest) *gohistory.ResetStickyTaskListResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gohistory.ResetStickyTaskListResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*gohistory.ResetStickyTaskListRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// DescribeWorkflowExecution is mock implementation for DescribeWorkflowExecution of HistoryEngine
func (_m *MockHistoryEngine) DescribeWorkflowExecution(ctx context.Context, request *gohistory.DescribeWorkflowExecutionRequest) (*shared.DescribeWorkflowExecutionResponse, error) {
	ret := _m.Called(request)

	var r0 *shared.DescribeWorkflowExecutionResponse
	if rf, ok := ret.Get(0).(func(*gohistory.DescribeWorkflowExecutionRequest) *shared.DescribeWorkflowExecutionResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.DescribeWorkflowExecutionResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*gohistory.DescribeWorkflowExecutionRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RecordDecisionTaskStarted is mock implementation for RecordDecisionTaskStarted of HistoryEngine
func (_m *MockHistoryEngine) RecordDecisionTaskStarted(ctx context.Context, request *gohistory.RecordDecisionTaskStartedRequest) (*gohistory.RecordDecisionTaskStartedResponse, error) {
	ret := _m.Called(request)

	var r0 *gohistory.RecordDecisionTaskStartedResponse
	if rf, ok := ret.Get(0).(func(*gohistory.RecordDecisionTaskStartedRequest) *gohistory.RecordDecisionTaskStartedResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gohistory.RecordDecisionTaskStartedResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*gohistory.RecordDecisionTaskStartedRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RecordActivityTaskStarted is mock implementation for RecordActivityTaskStarted of HistoryEngine
func (_m *MockHistoryEngine) RecordActivityTaskStarted(ctx context.Context, request *gohistory.RecordActivityTaskStartedRequest) (*gohistory.RecordActivityTaskStartedResponse, error) {
	ret := _m.Called(request)

	var r0 *gohistory.RecordActivityTaskStartedResponse
	if rf, ok := ret.Get(0).(func(*gohistory.RecordActivityTaskStartedRequest) *gohistory.RecordActivityTaskStartedResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gohistory.RecordActivityTaskStartedResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*gohistory.RecordActivityTaskStartedRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RespondDecisionTaskCompleted is mock implementation for RespondDecisionTaskCompleted of HistoryEngine
func (_m *MockHistoryEngine) RespondDecisionTaskCompleted(ctx context.Context, request *gohistory.RespondDecisionTaskCompletedRequest) (*gohistory.RespondDecisionTaskCompletedResponse, error) {
	ret := _m.Called(ctx, request)

	var r0 *gohistory.RespondDecisionTaskCompletedResponse
	if rf, ok := ret.Get(0).(func(*gohistory.RespondDecisionTaskCompletedRequest) *gohistory.RespondDecisionTaskCompletedResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gohistory.RespondDecisionTaskCompletedResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(0).(func(*gohistory.RespondDecisionTaskCompletedRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(0)
	}

	return r0, r1
}

// RespondDecisionTaskFailed is mock implementation for RespondDecisionTaskFailed of HistoryEngine
func (_m *MockHistoryEngine) RespondDecisionTaskFailed(ctx context.Context, request *gohistory.RespondDecisionTaskFailedRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(failedRequest *gohistory.RespondDecisionTaskFailedRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RespondActivityTaskCompleted is mock implementation for RespondActivityTaskCompleted of HistoryEngine
func (_m *MockHistoryEngine) RespondActivityTaskCompleted(ctx context.Context, request *gohistory.RespondActivityTaskCompletedRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gohistory.RespondActivityTaskCompletedRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RespondActivityTaskFailed is mock implementation for RespondActivityTaskFailed of HistoryEngine
func (_m *MockHistoryEngine) RespondActivityTaskFailed(ctx context.Context, request *gohistory.RespondActivityTaskFailedRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gohistory.RespondActivityTaskFailedRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RespondActivityTaskCanceled is mock implementation for RespondActivityTaskCanceled of HistoryEngine
func (_m *MockHistoryEngine) RespondActivityTaskCanceled(ctx context.Context, request *gohistory.RespondActivityTaskCanceledRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gohistory.RespondActivityTaskCanceledRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RecordActivityTaskHeartbeat is mock implementation for RecordActivityTaskHeartbeat of HistoryEngine
func (_m *MockHistoryEngine) RecordActivityTaskHeartbeat(ctx context.Context, request *gohistory.RecordActivityTaskHeartbeatRequest) (*shared.RecordActivityTaskHeartbeatResponse, error) {
	ret := _m.Called(request)

	var r0 *shared.RecordActivityTaskHeartbeatResponse
	if rf, ok := ret.Get(0).(func(*gohistory.RecordActivityTaskHeartbeatRequest) *shared.RecordActivityTaskHeartbeatResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.RecordActivityTaskHeartbeatResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*gohistory.RecordActivityTaskHeartbeatRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RequestCancelWorkflowExecution is mock implementation for RequestCancelWorkflowExecution of HistoryEngine
func (_m *MockHistoryEngine) RequestCancelWorkflowExecution(ctx context.Context, request *gohistory.RequestCancelWorkflowExecutionRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gohistory.RequestCancelWorkflowExecutionRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SignalWorkflowExecution is mock implementation for SignalWorkflowExecution of HistoryEngine
func (_m *MockHistoryEngine) SignalWorkflowExecution(ctx context.Context, request *gohistory.SignalWorkflowExecutionRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gohistory.SignalWorkflowExecutionRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SignalWithStartWorkflowExecution is mock implementation for SignalWithStartWorkflowExecution of HistoryEngine
func (_m *MockHistoryEngine) SignalWithStartWorkflowExecution(ctx context.Context, request *gohistory.SignalWithStartWorkflowExecutionRequest) (
	*shared.StartWorkflowExecutionResponse, error) {
	ret := _m.Called(request)

	var r0 *shared.StartWorkflowExecutionResponse
	if rf, ok := ret.Get(0).(func(*gohistory.SignalWithStartWorkflowExecutionRequest) *shared.StartWorkflowExecutionResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.StartWorkflowExecutionResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*gohistory.SignalWithStartWorkflowExecutionRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RemoveSignalMutableState is mock implementation for RemoveSignalMutableState of HistoryEngine
func (_m *MockHistoryEngine) RemoveSignalMutableState(ctx context.Context, request *gohistory.RemoveSignalMutableStateRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gohistory.RemoveSignalMutableStateRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// TerminateWorkflowExecution is mock implementation for TerminateWorkflowExecution of HistoryEngine
func (_m *MockHistoryEngine) TerminateWorkflowExecution(ctx context.Context, request *gohistory.TerminateWorkflowExecutionRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gohistory.TerminateWorkflowExecutionRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ResetWorkflowExecution is mock implementation for TerminateWorkflowExecution of HistoryEngine
func (_m *MockHistoryEngine) ResetWorkflowExecution(ctx context.Context, request *gohistory.ResetWorkflowExecutionRequest) (*shared.ResetWorkflowExecutionResponse, error) {
	ret := _m.Called(request)

	var r0 *shared.ResetWorkflowExecutionResponse
	if rf, ok := ret.Get(0).(func(*gohistory.ResetWorkflowExecutionRequest) *shared.ResetWorkflowExecutionResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ResetWorkflowExecutionResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(0).(func(*gohistory.ResetWorkflowExecutionRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(0)
	}

	return r0, r1
}

// ScheduleDecisionTask is mock implementation for ScheduleDecisionTask of HistoryEngine
func (_m *MockHistoryEngine) ScheduleDecisionTask(ctx context.Context, request *gohistory.ScheduleDecisionTaskRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gohistory.ScheduleDecisionTaskRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RecordChildExecutionCompleted is mock implementation for CompleteChildExecution of HistoryEngine
func (_m *MockHistoryEngine) RecordChildExecutionCompleted(ctx context.Context, request *gohistory.RecordChildExecutionCompletedRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gohistory.RecordChildExecutionCompletedRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReplicateEvents is mock implementation for ReplicateEvents of HistoryEngine
func (_m *MockHistoryEngine) ReplicateEvents(ctx context.Context, request *gohistory.ReplicateEventsRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gohistory.ReplicateEventsRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReplicateRawEvents is mock implementation for ReplicateRawEvents of HistoryEngine
func (_m *MockHistoryEngine) ReplicateRawEvents(ctx context.Context, request *gohistory.ReplicateRawEventsRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gohistory.ReplicateRawEventsRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReplicateEventsV2 is mock implementation for ReplicateEventsV2 of HistoryEngine
func (_m *MockHistoryEngine) ReplicateEventsV2(ctx context.Context, request *gohistory.ReplicateEventsV2Request) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gohistory.ReplicateEventsV2Request) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SyncShardStatus is mock implementation for SyncShardStatus of HistoryEngine
func (_m *MockHistoryEngine) SyncShardStatus(ctx context.Context, request *gohistory.SyncShardStatusRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gohistory.SyncShardStatusRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SyncActivity is mock implementation for SyncActivity of HistoryEngine
func (_m *MockHistoryEngine) SyncActivity(ctx context.Context, request *gohistory.SyncActivityRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gohistory.SyncActivityRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NotifyNewHistoryEvent is mock implementation for NotifyNewHistoryEvent of HistoryEngine
func (_m *MockHistoryEngine) NotifyNewHistoryEvent(event *historyEventNotification) {
	_m.Called(event)
}

// NotifyNewTransferTasks is mock implementation for NotifyNewTransferTasks of HistoryEngine
func (_m *MockHistoryEngine) NotifyNewTransferTasks(tasks []persistence.Task) {
	_m.Called(tasks)
}

// NotifyNewReplicationTasks is mock implementation for NotifyNewReplicationTasks of HistoryEngine
func (_m *MockHistoryEngine) NotifyNewReplicationTasks(tasks []persistence.Task) {
	_m.Called(tasks)
}

// NotifyNewTimerTasks is mock implementation for SyncActivity of HistoryEngine
func (_m *MockHistoryEngine) NotifyNewTimerTasks(tasks []persistence.Task) {
	_m.Called(tasks)
}

// GetReplicationMessages is mock implementation for GetReplicationTasks of HistoryEngine
func (_m *MockHistoryEngine) GetReplicationMessages(ctx context.Context, taskID int64) (*replicator.ReplicationMessages, error) {
	ret := _m.Called(ctx, taskID)

	var r0 *replicator.ReplicationMessages
	if rf, ok := ret.Get(0).(func(int64) *replicator.ReplicationMessages); ok {
		r0 = rf(taskID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*replicator.ReplicationMessages)
		}
	}

	var r1 error
	if rf, ok := ret.Get(0).(func(int64) error); ok {
		r1 = rf(taskID)
	} else {
		r1 = ret.Error(0)
	}

	return r0, r1
}

// ReapplyEvents is mock implementation for ReapplyEvents of HistoryEngine
func (_m *MockHistoryEngine) ReapplyEvents(ctx context.Context, domainUUID string, workflowID string, events []*shared.HistoryEvent) error {
	ret := _m.Called(domainUUID, workflowID, events)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, []*shared.HistoryEvent) error); ok {
		r0 = rf(domainUUID, workflowID, events)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
