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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination interface_mock.go

package engine

import (
	"context"

	h "github.com/uber/cadence/.gen/go/history"
	r "github.com/uber/cadence/.gen/go/replicator"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/events"
)

type (
	// Engine represents an interface for managing workflow execution history.
	Engine interface {
		common.Daemon

		StartWorkflowExecution(ctx context.Context, request *h.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error)
		GetMutableState(ctx context.Context, request *h.GetMutableStateRequest) (*h.GetMutableStateResponse, error)
		PollMutableState(ctx context.Context, request *h.PollMutableStateRequest) (*h.PollMutableStateResponse, error)
		DescribeMutableState(ctx context.Context, request *h.DescribeMutableStateRequest) (*h.DescribeMutableStateResponse, error)
		ResetStickyTaskList(ctx context.Context, resetRequest *h.ResetStickyTaskListRequest) (*h.ResetStickyTaskListResponse, error)
		DescribeWorkflowExecution(ctx context.Context, request *h.DescribeWorkflowExecutionRequest) (*workflow.DescribeWorkflowExecutionResponse, error)
		RecordDecisionTaskStarted(ctx context.Context, request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error)
		RecordActivityTaskStarted(ctx context.Context, request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error)
		RespondDecisionTaskCompleted(ctx context.Context, request *h.RespondDecisionTaskCompletedRequest) (*h.RespondDecisionTaskCompletedResponse, error)
		RespondDecisionTaskFailed(ctx context.Context, request *h.RespondDecisionTaskFailedRequest) error
		RespondActivityTaskCompleted(ctx context.Context, request *h.RespondActivityTaskCompletedRequest) error
		RespondActivityTaskFailed(ctx context.Context, request *h.RespondActivityTaskFailedRequest) error
		RespondActivityTaskCanceled(ctx context.Context, request *h.RespondActivityTaskCanceledRequest) error
		RecordActivityTaskHeartbeat(ctx context.Context, request *h.RecordActivityTaskHeartbeatRequest) (*workflow.RecordActivityTaskHeartbeatResponse, error)
		RequestCancelWorkflowExecution(ctx context.Context, request *h.RequestCancelWorkflowExecutionRequest) error
		SignalWorkflowExecution(ctx context.Context, request *h.SignalWorkflowExecutionRequest) error
		SignalWithStartWorkflowExecution(ctx context.Context, request *h.SignalWithStartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error)
		RemoveSignalMutableState(ctx context.Context, request *h.RemoveSignalMutableStateRequest) error
		TerminateWorkflowExecution(ctx context.Context, request *h.TerminateWorkflowExecutionRequest) error
		ResetWorkflowExecution(ctx context.Context, request *h.ResetWorkflowExecutionRequest) (*workflow.ResetWorkflowExecutionResponse, error)
		ScheduleDecisionTask(ctx context.Context, request *h.ScheduleDecisionTaskRequest) error
		RecordChildExecutionCompleted(ctx context.Context, request *h.RecordChildExecutionCompletedRequest) error
		ReplicateEvents(ctx context.Context, request *h.ReplicateEventsRequest) error
		ReplicateRawEvents(ctx context.Context, request *h.ReplicateRawEventsRequest) error
		ReplicateEventsV2(ctx context.Context, request *h.ReplicateEventsV2Request) error
		SyncShardStatus(ctx context.Context, request *h.SyncShardStatusRequest) error
		SyncActivity(ctx context.Context, request *h.SyncActivityRequest) error
		GetReplicationMessages(ctx context.Context, pollingCluster string, lastReadMessageID int64) (*r.ReplicationMessages, error)
		GetDLQReplicationMessages(ctx context.Context, taskInfos []*r.ReplicationTaskInfo) ([]*r.ReplicationTask, error)
		QueryWorkflow(ctx context.Context, request *h.QueryWorkflowRequest) (*h.QueryWorkflowResponse, error)
		ReapplyEvents(ctx context.Context, domainUUID string, workflowID string, runID string, events []*workflow.HistoryEvent) error
		ReadDLQMessages(ctx context.Context, messagesRequest *r.ReadDLQMessagesRequest) (*r.ReadDLQMessagesResponse, error)
		PurgeDLQMessages(ctx context.Context, messagesRequest *r.PurgeDLQMessagesRequest) error
		MergeDLQMessages(ctx context.Context, messagesRequest *r.MergeDLQMessagesRequest) (*r.MergeDLQMessagesResponse, error)
		RefreshWorkflowTasks(ctx context.Context, domainUUID string, execution workflow.WorkflowExecution) error
		ResetTransferQueue(ctx context.Context, clusterName string) error
		ResetTimerQueue(ctx context.Context, clusterName string) error
		DescribeTransferQueue(ctx context.Context, clusterName string) (*workflow.DescribeQueueResponse, error)
		DescribeTimerQueue(ctx context.Context, clusterName string) (*workflow.DescribeQueueResponse, error)

		NotifyNewHistoryEvent(event *events.Notification)
		NotifyNewTransferTasks(tasks []persistence.Task)
		NotifyNewTimerTasks(tasks []persistence.Task)
	}
)
