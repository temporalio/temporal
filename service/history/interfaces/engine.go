//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination engine_mock.go

package interfaces

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// Engine represents an interface for managing workflow execution history.
	Engine interface {
		StartWorkflowExecution(ctx context.Context, request *historyservice.StartWorkflowExecutionRequest) (*historyservice.StartWorkflowExecutionResponse, error)
		GetMutableState(ctx context.Context, request *historyservice.GetMutableStateRequest) (*historyservice.GetMutableStateResponse, error)
		PollMutableState(ctx context.Context, request *historyservice.PollMutableStateRequest) (*historyservice.PollMutableStateResponse, error)
		DescribeMutableState(ctx context.Context, request *historyservice.DescribeMutableStateRequest) (*historyservice.DescribeMutableStateResponse, error)
		ResetStickyTaskQueue(ctx context.Context, resetRequest *historyservice.ResetStickyTaskQueueRequest) (*historyservice.ResetStickyTaskQueueResponse, error)
		DescribeWorkflowExecution(ctx context.Context, request *historyservice.DescribeWorkflowExecutionRequest) (*historyservice.DescribeWorkflowExecutionResponse, error)
		RecordWorkflowTaskStarted(ctx context.Context, request *historyservice.RecordWorkflowTaskStartedRequest) (*historyservice.RecordWorkflowTaskStartedResponseWithRawHistory, error)
		RecordActivityTaskStarted(ctx context.Context, request *historyservice.RecordActivityTaskStartedRequest) (*historyservice.RecordActivityTaskStartedResponse, error)
		RespondWorkflowTaskCompleted(ctx context.Context, request *historyservice.RespondWorkflowTaskCompletedRequest) (*historyservice.RespondWorkflowTaskCompletedResponse, error)
		RespondWorkflowTaskFailed(ctx context.Context, request *historyservice.RespondWorkflowTaskFailedRequest) error
		RespondActivityTaskCompleted(ctx context.Context, request *historyservice.RespondActivityTaskCompletedRequest) (*historyservice.RespondActivityTaskCompletedResponse, error)
		RespondActivityTaskFailed(ctx context.Context, request *historyservice.RespondActivityTaskFailedRequest) (*historyservice.RespondActivityTaskFailedResponse, error)
		RespondActivityTaskCanceled(ctx context.Context, request *historyservice.RespondActivityTaskCanceledRequest) (*historyservice.RespondActivityTaskCanceledResponse, error)
		RecordActivityTaskHeartbeat(ctx context.Context, request *historyservice.RecordActivityTaskHeartbeatRequest) (*historyservice.RecordActivityTaskHeartbeatResponse, error)
		RequestCancelWorkflowExecution(ctx context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest) (*historyservice.RequestCancelWorkflowExecutionResponse, error)
		SignalWorkflowExecution(ctx context.Context, request *historyservice.SignalWorkflowExecutionRequest) (*historyservice.SignalWorkflowExecutionResponse, error)
		SignalWithStartWorkflowExecution(ctx context.Context, request *historyservice.SignalWithStartWorkflowExecutionRequest) (*historyservice.SignalWithStartWorkflowExecutionResponse, error)
		ExecuteMultiOperation(ctx context.Context, request *historyservice.ExecuteMultiOperationRequest) (*historyservice.ExecuteMultiOperationResponse, error)
		RemoveSignalMutableState(ctx context.Context, request *historyservice.RemoveSignalMutableStateRequest) (*historyservice.RemoveSignalMutableStateResponse, error)
		TerminateWorkflowExecution(ctx context.Context, request *historyservice.TerminateWorkflowExecutionRequest) (*historyservice.TerminateWorkflowExecutionResponse, error)
		DeleteWorkflowExecution(ctx context.Context, deleteRequest *historyservice.DeleteWorkflowExecutionRequest) (*historyservice.DeleteWorkflowExecutionResponse, error)
		ResetWorkflowExecution(ctx context.Context, request *historyservice.ResetWorkflowExecutionRequest) (*historyservice.ResetWorkflowExecutionResponse, error)
		UpdateWorkflowExecutionOptions(ctx context.Context, request *historyservice.UpdateWorkflowExecutionOptionsRequest) (*historyservice.UpdateWorkflowExecutionOptionsResponse, error)
		ScheduleWorkflowTask(ctx context.Context, request *historyservice.ScheduleWorkflowTaskRequest) error
		IsActivityTaskValid(ctx context.Context, request *historyservice.IsActivityTaskValidRequest) (*historyservice.IsActivityTaskValidResponse, error)
		IsWorkflowTaskValid(ctx context.Context, request *historyservice.IsWorkflowTaskValidRequest) (*historyservice.IsWorkflowTaskValidResponse, error)
		VerifyFirstWorkflowTaskScheduled(ctx context.Context, request *historyservice.VerifyFirstWorkflowTaskScheduledRequest) error
		RecordChildExecutionCompleted(ctx context.Context, request *historyservice.RecordChildExecutionCompletedRequest) (*historyservice.RecordChildExecutionCompletedResponse, error)
		VerifyChildExecutionCompletionRecorded(ctx context.Context, request *historyservice.VerifyChildExecutionCompletionRecordedRequest) (*historyservice.VerifyChildExecutionCompletionRecordedResponse, error)
		ReplicateHistoryEvents(
			ctx context.Context,
			workflowKey definition.WorkflowKey,
			baseExecutionInfo *workflowspb.BaseExecutionInfo,
			versionHistoryItems []*historyspb.VersionHistoryItem,
			historyEvents [][]*historypb.HistoryEvent,
			newEvents []*historypb.HistoryEvent,
			newRunID string,
		) error
		ReplicateEventsV2(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error
		ReplicateWorkflowState(ctx context.Context, request *historyservice.ReplicateWorkflowStateRequest) error
		ReplicateVersionedTransition(ctx context.Context, archetypeID chasm.ArchetypeID, artifact *replicationspb.VersionedTransitionArtifact, sourceClusterName string) error
		SyncShardStatus(ctx context.Context, request *historyservice.SyncShardStatusRequest) error
		SyncActivity(ctx context.Context, request *historyservice.SyncActivityRequest) error
		SyncActivities(ctx context.Context, request *historyservice.SyncActivitiesRequest) error
		SyncHSM(ctx context.Context, request *SyncHSMRequest) error
		BackfillHistoryEvents(ctx context.Context, request *BackfillHistoryEventsRequest) error
		GetReplicationMessages(ctx context.Context, pollingCluster string, ackMessageID int64, ackTimestamp time.Time, queryMessageID int64) (*replicationspb.ReplicationMessages, error)
		GetDLQReplicationMessages(ctx context.Context, taskInfos []*replicationspb.ReplicationTaskInfo) ([]*replicationspb.ReplicationTask, error)
		QueryWorkflow(ctx context.Context, request *historyservice.QueryWorkflowRequest) (*historyservice.QueryWorkflowResponse, error)
		ReapplyEvents(ctx context.Context, namespaceUUID namespace.ID, workflowID string, runID string, events []*historypb.HistoryEvent) error
		GetDLQMessages(ctx context.Context, messagesRequest *historyservice.GetDLQMessagesRequest) (*historyservice.GetDLQMessagesResponse, error)
		PurgeDLQMessages(ctx context.Context, messagesRequest *historyservice.PurgeDLQMessagesRequest) (*historyservice.PurgeDLQMessagesResponse, error)
		MergeDLQMessages(ctx context.Context, messagesRequest *historyservice.MergeDLQMessagesRequest) (*historyservice.MergeDLQMessagesResponse, error)
		RebuildMutableState(ctx context.Context, namespaceUUID namespace.ID, execution *commonpb.WorkflowExecution) error
		ImportWorkflowExecution(ctx context.Context, request *historyservice.ImportWorkflowExecutionRequest) (*historyservice.ImportWorkflowExecutionResponse, error)
		RefreshWorkflowTasks(ctx context.Context, namespaceUUID namespace.ID, execution *commonpb.WorkflowExecution, archetypeID chasm.ArchetypeID) error
		GenerateLastHistoryReplicationTasks(ctx context.Context, request *historyservice.GenerateLastHistoryReplicationTasksRequest) (*historyservice.GenerateLastHistoryReplicationTasksResponse, error)
		GetReplicationStatus(ctx context.Context, request *historyservice.GetReplicationStatusRequest) (*historyservice.ShardReplicationStatus, error)
		UpdateWorkflowExecution(ctx context.Context, request *historyservice.UpdateWorkflowExecutionRequest) (*historyservice.UpdateWorkflowExecutionResponse, error)
		PollWorkflowExecutionUpdate(ctx context.Context, request *historyservice.PollWorkflowExecutionUpdateRequest) (*historyservice.PollWorkflowExecutionUpdateResponse, error)
		GetWorkflowExecutionHistory(ctx context.Context, request *historyservice.GetWorkflowExecutionHistoryRequest) (*historyservice.GetWorkflowExecutionHistoryResponseWithRaw, error)
		GetWorkflowExecutionHistoryReverse(ctx context.Context, request *historyservice.GetWorkflowExecutionHistoryReverseRequest) (*historyservice.GetWorkflowExecutionHistoryReverseResponse, error)
		GetWorkflowExecutionRawHistory(ctx context.Context, request *historyservice.GetWorkflowExecutionRawHistoryRequest) (*historyservice.GetWorkflowExecutionRawHistoryResponse, error)
		GetWorkflowExecutionRawHistoryV2(ctx context.Context, request *historyservice.GetWorkflowExecutionRawHistoryV2Request) (*historyservice.GetWorkflowExecutionRawHistoryV2Response, error)
		AddTasks(ctx context.Context, request *historyservice.AddTasksRequest) (*historyservice.AddTasksResponse, error)
		ListTasks(ctx context.Context, request *historyservice.ListTasksRequest) (*historyservice.ListTasksResponse, error)
		SyncWorkflowState(ctx context.Context, request *historyservice.SyncWorkflowStateRequest) (*historyservice.SyncWorkflowStateResponse, error)
		UpdateActivityOptions(ctx context.Context, request *historyservice.UpdateActivityOptionsRequest) (*historyservice.UpdateActivityOptionsResponse, error)
		PauseActivity(ctx context.Context, request *historyservice.PauseActivityRequest) (*historyservice.PauseActivityResponse, error)
		UnpauseActivity(ctx context.Context, request *historyservice.UnpauseActivityRequest) (*historyservice.UnpauseActivityResponse, error)
		ResetActivity(ctx context.Context, request *historyservice.ResetActivityRequest) (*historyservice.ResetActivityResponse, error)
		PauseWorkflowExecution(ctx context.Context, request *historyservice.PauseWorkflowExecutionRequest) (*historyservice.PauseWorkflowExecutionResponse, error)
		UnpauseWorkflowExecution(ctx context.Context, request *historyservice.UnpauseWorkflowExecutionRequest) (*historyservice.UnpauseWorkflowExecutionResponse, error)

		NotifyNewHistoryEvent(event *events.Notification)
		NotifyNewTasks(tasks map[tasks.Category][]tasks.Task)
		NotifyChasmExecution(executionKey chasm.ExecutionKey, componentRef []byte)
		// TODO(bergundy): This Environment should be host level once shard level workflow cache is deprecated.
		StateMachineEnvironment(operationTag metrics.Tag) hsm.Environment

		ReplicationStream
		Start()
		Stop()
	}
)

type (
	SyncHSMRequest struct {
		definition.WorkflowKey

		StateMachineNode    *persistencespb.StateMachineNode
		EventVersionHistory *historyspb.VersionHistory
	}

	BackfillHistoryEventsRequest struct {
		definition.WorkflowKey

		SourceClusterName   string
		VersionedHistory    *persistencespb.VersionedTransition
		BaseExecutionInfo   *workflowspb.BaseExecutionInfo
		VersionHistoryItems []*historyspb.VersionHistoryItem

		Events    [][]*historypb.HistoryEvent
		NewEvents []*historypb.HistoryEvent
		NewRunID  string
	}
)
