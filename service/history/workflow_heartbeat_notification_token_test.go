package history

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/effect"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/protorequire"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

// matchingWorkflowTaskToken builds the token the same way Matching does from a RecordWorkflowTaskStarted response.
func matchingWorkflowTaskToken(
	we *commonpb.WorkflowExecution,
	resp *historyservice.RecordWorkflowTaskStartedResponseWithRawHistory,
) *tokenspb.Task {
	return tasktoken.NewWorkflowTaskToken(
		tests.NamespaceID.String(),
		we.GetWorkflowId(),
		we.GetRunId(),
		resp.GetScheduledEventId(),
		resp.GetStartedEventId(),
		resp.GetStartedTime(),
		resp.GetAttempt(),
		resp.GetClock(),
		resp.GetVersion(),
	)
}

func workflowKeyOf(we *commonpb.WorkflowExecution) definition.WorkflowKey {
	return definition.NewWorkflowKey(tests.NamespaceID.String(), we.GetWorkflowId(), we.GetRunId())
}

// expectWorkflowTaskStartPersisted returns a getter for the ExecutionInfo written by the next update.
func (s *engine2Suite) expectWorkflowTaskStartPersisted(ms historyi.MutableState) func() *persistencespb.WorkflowExecutionInfo {
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
	s.expectHistoryReads()
	return func() *persistencespb.WorkflowExecutionInfo { return persisted }
}

func (s *engine2Suite) expectHistoryReads() {
	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).
		Return(&persistence.ReadHistoryBranchResponse{}, nil).AnyTimes()
	s.mockShard.Resource.SearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).
		Return(searchattribute.TestNameTypeMap(), nil).AnyTimes()
	s.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()
}

func (s *engine2Suite) recordWorkflowTaskStarted(
	we *commonpb.WorkflowExecution,
	scheduledEventID int64,
	requestID string,
) *historyservice.RecordWorkflowTaskStartedResponseWithRawHistory {
	resp, err := s.historyEngine.RecordWorkflowTaskStarted(
		metrics.AddMetricsContext(context.Background()),
		&historyservice.RecordWorkflowTaskStartedRequest{
			NamespaceId:       tests.NamespaceID.String(),
			WorkflowExecution: we,
			ScheduledEventId:  scheduledEventID,
			RequestId:         requestID,
			PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{Name: "testTaskQueue"},
				Identity:  "testIdentity",
			},
		},
	)
	s.Require().NoError(err)
	return resp
}

func (s *engine2Suite) TestWorkflowTaskTokenRebuild_RecordWorkflowTaskStarted() {
	s.config.EnableWorkflowTaskStartedClock = dynamicconfig.GetBoolPropertyFn(true)
	we := &commonpb.WorkflowExecution{WorkflowId: "wId", RunId: tests.RunID}
	ms := s.createExecutionStartedState(we, "testTaskQueue", "testIdentity", true, false)
	scheduledEventID := ms.GetPendingWorkflowTask().ScheduledEventID
	persisted := s.expectWorkflowTaskStartPersisted(ms)

	workerToken := matchingWorkflowTaskToken(we, s.recordWorkflowTaskStarted(we, scheduledEventID, "reqId"))
	s.NotNil(workerToken.GetClock())
	protorequire.ProtoEqual(s.T(), workerToken, workflow.NewStartedWorkflowTaskToken(workflowKeyOf(we), persisted()))

	// A retry with the same request ID must hand out the same token.
	retryToken := matchingWorkflowTaskToken(we, s.recordWorkflowTaskStarted(we, scheduledEventID, "reqId"))
	protorequire.ProtoEqual(s.T(), workerToken, retryToken)
}

func (s *engine2Suite) TestWorkflowTaskTokenRebuild_TransientWorkflowTask() {
	s.config.EnableWorkflowTaskStartedClock = dynamicconfig.GetBoolPropertyFn(true)
	we := &commonpb.WorkflowExecution{WorkflowId: "wId", RunId: tests.RunID}
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).
		Return(cluster.TestCurrentClusterName).AnyTimes()
	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
		we.GetWorkflowId(), we.GetRunId(), s.logger)
	// Match the version the engine loads the workflow with, so the transient task stays transient on start.
	s.NoError(ms.UpdateCurrentVersion(tests.GlobalNamespaceEntry.FailoverVersion(we.GetWorkflowId()), true))
	addWorkflowExecutionStartedEvent(ms, we, "wType", "testTaskQueue", payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, "testIdentity")
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, "testTaskQueue", "testIdentity")
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
	persisted := s.expectWorkflowTaskStartPersisted(ms)

	workerToken := matchingWorkflowTaskToken(we, s.recordWorkflowTaskStarted(we, wt.ScheduledEventID, "reqId"))
	s.NotNil(workerToken.GetClock())
	s.Equal(int32(2), workerToken.GetAttempt())
	protorequire.ProtoEqual(s.T(), workerToken, workflow.NewStartedWorkflowTaskToken(workflowKeyOf(we), persisted()))
}

func (s *engine2Suite) TestWorkflowTaskTokenRebuild_CleanSpeculativeWorkflowTaskInMemory() {
	s.config.EnableWorkflowTaskStartedClock = dynamicconfig.GetBoolPropertyFn(true)
	we := &commonpb.WorkflowExecution{WorkflowId: "wId", RunId: tests.RunID}
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, tests.Version).
		Return(cluster.TestCurrentClusterName).AnyTimes()
	ms := s.createExecutionStartedState(we, "testTaskQueue", "testIdentity", false, false)
	s.NoError(ms.UpdateCurrentVersion(tests.GlobalNamespaceEntry.FailoverVersion(we.GetWorkflowId()), true))
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		&persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(context.Background(), ms)},
		nil,
	)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Times(0)
	s.expectHistoryReads()

	wfCtx, release, err := s.workflowCache.GetOrCreateWorkflowExecution(
		metrics.AddMetricsContext(context.Background()),
		s.mockShard,
		tests.NamespaceID,
		we,
		locks.PriorityHigh,
	)
	s.NoError(err)
	loadedMS, err := wfCtx.LoadMutableState(context.Background(), s.mockShard)
	s.NoError(err)
	upd, _, err := wfCtx.UpdateRegistry(context.Background()).FindOrCreate(context.Background(), "update-id")
	s.NoError(err)
	s.NoError(upd.Admit(
		&updatepb.Request{Meta: &updatepb.Meta{UpdateId: "update-id"}, Input: &updatepb.Input{Name: "update"}},
		workflow.WithEffects(effect.Immediate(context.Background()), loadedMS),
	))
	wft, err := loadedMS.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE)
	s.NoError(err)
	release(nil)

	workerToken := matchingWorkflowTaskToken(we, s.recordWorkflowTaskStarted(we, wft.ScheduledEventID, "reqId"))
	s.NotNil(workerToken.GetClock())

	inMemory := s.getMutableState(tests.NamespaceID, we).GetExecutionInfo()
	s.Equal(enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE, inMemory.GetWorkflowTaskType())
	protorequire.ProtoEqual(s.T(), workerToken, workflow.NewStartedWorkflowTaskToken(workflowKeyOf(we), inMemory))
}

func (s *engine2Suite) TestWorkflowTaskTokenRebuild_RespondWorkflowTaskCompletedReturnsNewWorkflowTask() {
	s.config.EnableWorkflowTaskStartedClock = dynamicconfig.GetBoolPropertyFn(true)
	we := &commonpb.WorkflowExecution{WorkflowId: "wId", RunId: tests.RunID}
	ms := s.createExecutionStartedState(we, "testTaskQueue", "testIdentity", true, true)
	completingToken, err := tasktoken.NewSerializer().Serialize(
		workflow.NewStartedWorkflowTaskToken(workflowKeyOf(we), ms.GetExecutionInfo()),
	)
	s.NoError(err)
	persisted := s.expectWorkflowTaskStartPersisted(ms)

	resp, err := s.historyEngine.RespondWorkflowTaskCompleted(
		metrics.AddMetricsContext(context.Background()),
		&historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tests.NamespaceID.String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken:                  completingToken,
				Identity:                   "testIdentity",
				ForceCreateNewWorkflowTask: true,
				ReturnNewWorkflowTask:      true,
			},
		},
	)
	s.NoError(err)
	s.Require().NotNil(resp.GetNewWorkflowTask())
	workerToken, err := tasktoken.NewSerializer().Deserialize(resp.GetNewWorkflowTask().GetTaskToken())
	s.NoError(err)
	s.NotNil(workerToken.GetClock())
	protorequire.ProtoEqual(s.T(), workerToken, workflow.NewStartedWorkflowTaskToken(workflowKeyOf(we), persisted()))
}

func (s *engineSuite) TestWorkflowTaskTokenRebuild_EagerWorkflowStart() {
	s.config.EnableWorkflowTaskStartedClock = dynamicconfig.GetBoolPropertyFn(true)
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
				WorkflowId:            "test",
				Namespace:             tests.Namespace.String(),
				WorkflowType:          &commonpb.WorkflowType{Name: "test"},
				TaskQueue:             &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "test"},
				Identity:              "test",
				RequestId:             "test",
				RequestEagerExecution: true,
			},
		},
	)
	s.NoError(err)
	s.Require().NotNil(resp.GetEagerWorkflowTask())
	workerToken, err := tasktoken.NewSerializer().Deserialize(resp.GetEagerWorkflowTask().GetTaskToken())
	s.NoError(err)
	s.NotNil(workerToken.GetClock())
	we := &commonpb.WorkflowExecution{WorkflowId: "test", RunId: resp.GetRunId()}
	protorequire.ProtoEqual(s.T(), workerToken, workflow.NewStartedWorkflowTaskToken(workflowKeyOf(we), persisted))
}

func (s *engine2Suite) TestWorkflowTaskStartedClockDisabled_RespondWorkflowTaskCompletedKeepsNilClock() {
	we := &commonpb.WorkflowExecution{WorkflowId: "wId", RunId: tests.RunID}
	ms := s.createExecutionStartedState(we, "testTaskQueue", "testIdentity", true, true)
	completingToken, err := tasktoken.NewSerializer().Serialize(&tokenspb.Task{
		NamespaceId:      tests.NamespaceID.String(),
		WorkflowId:       we.GetWorkflowId(),
		RunId:            we.GetRunId(),
		ScheduledEventId: ms.GetStartedWorkflowTask().ScheduledEventID,
		Attempt:          1,
	})
	s.NoError(err)
	persisted := s.expectWorkflowTaskStartPersisted(ms)

	resp, err := s.historyEngine.RespondWorkflowTaskCompleted(
		metrics.AddMetricsContext(context.Background()),
		&historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tests.NamespaceID.String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken:                  completingToken,
				Identity:                   "testIdentity",
				ForceCreateNewWorkflowTask: true,
				ReturnNewWorkflowTask:      true,
			},
		},
	)
	s.NoError(err)
	s.Require().NotNil(resp.GetNewWorkflowTask())
	workerToken, err := tasktoken.NewSerializer().Deserialize(resp.GetNewWorkflowTask().GetTaskToken())
	s.NoError(err)
	s.Nil(workerToken.GetClock())
	s.Nil(persisted().GetWorkflowTaskStartedClock())
	s.Nil(workflow.NewStartedWorkflowTaskToken(workflowKeyOf(we), persisted()))
}
