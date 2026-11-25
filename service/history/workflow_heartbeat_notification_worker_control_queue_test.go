package history

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

func (s *engine2Suite) TestRecordWorkflowTaskStarted_StoresWorkerControlQueue() {
	for _, tc := range []struct {
		name          string
		capabilities  *workflowservice.PollWorkflowTaskQueueRequest_Capabilities
		previousQueue string
		expectedQueue string
	}{
		{
			name:          "supports workflow heartbeat notification",
			capabilities:  &workflowservice.PollWorkflowTaskQueueRequest_Capabilities{SupportsWorkflowHeartbeatNotification: true},
			expectedQueue: "worker-control",
		},
		{
			name: "no capability",
		},
		{
			name:          "no capability clears previous queue",
			previousQueue: "previous-worker-control",
		},
	} {
		s.Run(tc.name, func() {
			we := &commonpb.WorkflowExecution{WorkflowId: "wId-" + tc.name, RunId: tests.RunID}
			ms := s.createExecutionStartedState(we, "testTaskQueue", "testIdentity", true, false)
			ms.GetExecutionInfo().WorkflowTaskWorkerControlQueue = tc.previousQueue
			s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(
				&persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(context.Background(), ms)},
				nil,
			)
			var persisted *persistencespb.WorkflowExecutionInfo
			s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
					persisted = common.CloneProto(request.UpdateWorkflowMutation.ExecutionInfo)
					return tests.UpdateWorkflowExecutionResponse, nil
				},
			)
			s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).
				Return(&persistence.ReadHistoryBranchResponse{}, nil).AnyTimes()
			s.mockShard.Resource.SearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).
				Return(searchattribute.TestNameTypeMap(), nil).AnyTimes()
			s.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).
				Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()

			_, err := s.historyEngine.RecordWorkflowTaskStarted(
				metrics.AddMetricsContext(context.Background()),
				&historyservice.RecordWorkflowTaskStartedRequest{
					NamespaceId:       tests.NamespaceID.String(),
					WorkflowExecution: we,
					ScheduledEventId:  ms.GetPendingWorkflowTask().ScheduledEventID,
					RequestId:         "reqId",
					PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
						TaskQueue:              &taskqueuepb.TaskQueue{Name: "testTaskQueue"},
						Identity:               "testIdentity",
						WorkerControlTaskQueue: "worker-control",
						Capabilities:           tc.capabilities,
					},
				},
			)
			s.NoError(err)
			s.Equal(tc.expectedQueue, persisted.GetWorkflowTaskWorkerControlQueue())
		})
	}
}

func (s *engineSuite) TestEagerWorkflowStart_StoresWorkerControlQueue() {
	var persisted *persistencespb.WorkflowExecutionInfo
	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
			persisted = request.NewWorkflowSnapshot.ExecutionInfo
			return &persistence.CreateWorkflowExecutionResponse{NewMutableStateStats: tests.CreateWorkflowExecutionResponse.NewMutableStateStats}, nil
		},
	)

	resp, err := s.historyEngine.StartWorkflowExecution(
		metrics.AddMetricsContext(context.Background()),
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: tests.NamespaceID.String(),
			Attempt:     1,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId:                  "test",
				Namespace:                   tests.Namespace.String(),
				WorkflowType:                &commonpb.WorkflowType{Name: "test"},
				TaskQueue:                   &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "test"},
				Identity:                    "test",
				RequestId:                   "test",
				RequestEagerExecution:       true,
				EagerWorkerControlTaskQueue: "worker-control",
				EagerWorkerCapabilities: &workflowservice.StartWorkflowExecutionRequest_Capabilities{
					SupportsWorkflowHeartbeatNotification: true,
				},
			},
		},
	)
	s.NoError(err)
	s.NotNil(resp.GetEagerWorkflowTask())
	s.Equal("worker-control", persisted.GetWorkflowTaskWorkerControlQueue())
}

func (s *engine2Suite) TestRecordWorkflowTaskStarted_RetryOnDifferentWorkerReplacesWorkerControlQueue() {
	for _, tc := range []struct {
		name          string
		capabilities  *workflowservice.PollWorkflowTaskQueueRequest_Capabilities
		expectedQueue string
	}{
		{
			name:          "supports workflow heartbeat notification",
			capabilities:  &workflowservice.PollWorkflowTaskQueueRequest_Capabilities{SupportsWorkflowHeartbeatNotification: true},
			expectedQueue: "worker-b-control",
		},
		{
			name: "no capability",
		},
	} {
		s.Run(tc.name, func() {
			we := &commonpb.WorkflowExecution{WorkflowId: "wId-" + tc.name, RunId: tests.RunID}
			s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).
				Return(cluster.TestCurrentClusterName).AnyTimes()
			ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
				we.GetWorkflowId(), we.GetRunId(), s.logger)
			s.NoError(ms.UpdateCurrentVersion(tests.GlobalNamespaceEntry.FailoverVersion(we.GetWorkflowId()), true))
			addWorkflowExecutionStartedEvent(ms, we, "wType", "testTaskQueue", payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, "testIdentity")
			wt := addWorkflowTaskScheduledEvent(ms)
			addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, "testTaskQueue", "worker-a")
			ms.GetExecutionInfo().WorkflowTaskWorkerControlQueue = "worker-a-control"
			_, err := ms.AddWorkflowTaskTimedOutEvent(ms.GetStartedWorkflowTask())
			s.NoError(err)
			s.NoError(ms.SetHistoryTree(nil, nil, we.GetRunId()))
			versionHistory, err := versionhistory.GetCurrentVersionHistory(ms.GetExecutionInfo().VersionHistories)
			s.NoError(err)
			s.NoError(versionhistory.AddOrUpdateVersionHistoryItem(
				versionHistory,
				versionhistory.NewVersionHistoryItem(ms.GetNextEventID()-1, ms.GetCurrentVersion()),
			))
			wt = addWorkflowTaskScheduledEvent(ms)
			s.Equal(int32(2), wt.Attempt)

			s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(
				&persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(context.Background(), ms)},
				nil,
			)
			var persisted *persistencespb.WorkflowExecutionInfo
			s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
					persisted = common.CloneProto(request.UpdateWorkflowMutation.ExecutionInfo)
					return tests.UpdateWorkflowExecutionResponse, nil
				},
			)
			s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).
				Return(&persistence.ReadHistoryBranchResponse{}, nil).AnyTimes()
			s.mockShard.Resource.SearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).
				Return(searchattribute.TestNameTypeMap(), nil).AnyTimes()
			s.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).
				Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()

			resp, err := s.historyEngine.RecordWorkflowTaskStarted(
				metrics.AddMetricsContext(context.Background()),
				&historyservice.RecordWorkflowTaskStartedRequest{
					NamespaceId:       tests.NamespaceID.String(),
					WorkflowExecution: we,
					ScheduledEventId:  wt.ScheduledEventID,
					RequestId:         "reqId",
					PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
						TaskQueue:              &taskqueuepb.TaskQueue{Name: "testTaskQueue"},
						Identity:               "worker-b",
						WorkerControlTaskQueue: "worker-b-control",
						Capabilities:           tc.capabilities,
					},
				},
			)
			s.NoError(err)
			s.Equal(int32(2), resp.GetAttempt())
			s.Equal(tc.expectedQueue, persisted.GetWorkflowTaskWorkerControlQueue())
		})
	}
}
