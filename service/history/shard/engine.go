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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination engine_mock.go

package shard

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// Engine represents an interface for managing workflow execution history.
	Engine interface {
		common.Daemon

		StartWorkflowExecution(ctx context.Context, request *historyservice.StartWorkflowExecutionRequest) (*historyservice.StartWorkflowExecutionResponse, error)
		GetMutableState(ctx context.Context, request *historyservice.GetMutableStateRequest) (*historyservice.GetMutableStateResponse, error)
		PollMutableState(ctx context.Context, request *historyservice.PollMutableStateRequest) (*historyservice.PollMutableStateResponse, error)
		DescribeMutableState(ctx context.Context, request *historyservice.DescribeMutableStateRequest) (*historyservice.DescribeMutableStateResponse, error)
		ResetStickyTaskQueue(ctx context.Context, resetRequest *historyservice.ResetStickyTaskQueueRequest) (*historyservice.ResetStickyTaskQueueResponse, error)
		DescribeWorkflowExecution(ctx context.Context, request *historyservice.DescribeWorkflowExecutionRequest) (*historyservice.DescribeWorkflowExecutionResponse, error)
		RecordWorkflowTaskStarted(ctx context.Context, request *historyservice.RecordWorkflowTaskStartedRequest) (*historyservice.RecordWorkflowTaskStartedResponse, error)
		RecordActivityTaskStarted(ctx context.Context, request *historyservice.RecordActivityTaskStartedRequest) (*historyservice.RecordActivityTaskStartedResponse, error)
		RespondWorkflowTaskCompleted(ctx context.Context, request *historyservice.RespondWorkflowTaskCompletedRequest) (*historyservice.RespondWorkflowTaskCompletedResponse, error)
		RespondWorkflowTaskFailed(ctx context.Context, request *historyservice.RespondWorkflowTaskFailedRequest) error
		RespondActivityTaskCompleted(ctx context.Context, request *historyservice.RespondActivityTaskCompletedRequest) error
		RespondActivityTaskFailed(ctx context.Context, request *historyservice.RespondActivityTaskFailedRequest) error
		RespondActivityTaskCanceled(ctx context.Context, request *historyservice.RespondActivityTaskCanceledRequest) error
		RecordActivityTaskHeartbeat(ctx context.Context, request *historyservice.RecordActivityTaskHeartbeatRequest) (*historyservice.RecordActivityTaskHeartbeatResponse, error)
		RequestCancelWorkflowExecution(ctx context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest) error
		SignalWorkflowExecution(ctx context.Context, request *historyservice.SignalWorkflowExecutionRequest) error
		SignalWithStartWorkflowExecution(ctx context.Context, request *historyservice.SignalWithStartWorkflowExecutionRequest) (*historyservice.SignalWithStartWorkflowExecutionResponse, error)
		RemoveSignalMutableState(ctx context.Context, request *historyservice.RemoveSignalMutableStateRequest) error
		TerminateWorkflowExecution(ctx context.Context, request *historyservice.TerminateWorkflowExecutionRequest) error
		DeleteWorkflowExecution(ctx context.Context, deleteRequest *historyservice.DeleteWorkflowExecutionRequest) error
		ResetWorkflowExecution(ctx context.Context, request *historyservice.ResetWorkflowExecutionRequest) (*historyservice.ResetWorkflowExecutionResponse, error)
		ScheduleWorkflowTask(ctx context.Context, request *historyservice.ScheduleWorkflowTaskRequest) error
		VerifyFirstWorkflowTaskScheduled(ctx context.Context, request *historyservice.VerifyFirstWorkflowTaskScheduledRequest) error
		RecordChildExecutionCompleted(ctx context.Context, request *historyservice.RecordChildExecutionCompletedRequest) error
		VerifyChildExecutionCompletionRecorded(ctx context.Context, request *historyservice.VerifyChildExecutionCompletionRecordedRequest) error
		ReplicateEventsV2(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error
		ReplicateWorkflowState(ctx context.Context, request *historyservice.ReplicateWorkflowStateRequest) error
		SyncShardStatus(ctx context.Context, request *historyservice.SyncShardStatusRequest) error
		SyncActivity(ctx context.Context, request *historyservice.SyncActivityRequest) error
		GetReplicationMessages(ctx context.Context, pollingCluster string, ackMessageID int64, ackTimestamp time.Time, queryMessageID int64) (*replicationspb.ReplicationMessages, error)
		GetDLQReplicationMessages(ctx context.Context, taskInfos []*replicationspb.ReplicationTaskInfo) ([]*replicationspb.ReplicationTask, error)
		QueryWorkflow(ctx context.Context, request *historyservice.QueryWorkflowRequest) (*historyservice.QueryWorkflowResponse, error)
		ReapplyEvents(ctx context.Context, namespaceUUID namespace.ID, workflowID string, runID string, events []*historypb.HistoryEvent) error
		GetDLQMessages(ctx context.Context, messagesRequest *historyservice.GetDLQMessagesRequest) (*historyservice.GetDLQMessagesResponse, error)
		PurgeDLQMessages(ctx context.Context, messagesRequest *historyservice.PurgeDLQMessagesRequest) error
		MergeDLQMessages(ctx context.Context, messagesRequest *historyservice.MergeDLQMessagesRequest) (*historyservice.MergeDLQMessagesResponse, error)
		RebuildMutableState(ctx context.Context, namespaceUUID namespace.ID, execution commonpb.WorkflowExecution) error
		RefreshWorkflowTasks(ctx context.Context, namespaceUUID namespace.ID, execution commonpb.WorkflowExecution) error
		GenerateLastHistoryReplicationTasks(ctx context.Context, request *historyservice.GenerateLastHistoryReplicationTasksRequest) (*historyservice.GenerateLastHistoryReplicationTasksResponse, error)
		GetReplicationStatus(ctx context.Context, request *historyservice.GetReplicationStatusRequest) (*historyservice.ShardReplicationStatus, error)
		UpdateWorkflow(ctx context.Context, request *historyservice.UpdateWorkflowRequest) (*historyservice.UpdateWorkflowResponse, error)

		NotifyNewHistoryEvent(event *events.Notification)
		NotifyNewTasks(clusterName string, tasks map[tasks.Category][]tasks.Task)
	}
)
