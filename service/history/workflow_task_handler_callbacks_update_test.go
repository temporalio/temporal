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

package history

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/testing/updateutils"
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/history/workflow/update"
)

type (
	WorkflowTaskHandlerCallbacksUpdateSuite struct {
		suite.Suite
		*require.Assertions
		protorequire.ProtoAssertions
		historyrequire.HistoryRequire
		updateutils.UpdateUtils

		controller         *gomock.Controller
		mockEventsCache    *events.MockCache
		mockExecutionMgr   *persistence.MockExecutionManager
		workflowCache      wcache.Cache
		mockNamespaceCache *namespace.MockRegistry

		logger log.Logger

		workflowTaskHandlerCallback *workflowTaskHandlerCallbacksImpl
	}
)

func TestWorkflowTaskHandlerCallbacksUpdateSuite(t *testing.T) {
	suite.Run(t, new(WorkflowTaskHandlerCallbacksUpdateSuite))
}

func (s *WorkflowTaskHandlerCallbacksUpdateSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
	s.HistoryRequire = historyrequire.New(s.T())
	s.UpdateUtils = updateutils.New(s.T())

	s.controller = gomock.NewController(s.T())
	config := tests.NewDynamicConfig()
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		config,
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	mockShard.SetStateMachineRegistry(reg)

	mockShard.Resource.ShardMgr.EXPECT().AssertShardOwnership(gomock.Any(), gomock.Any()).AnyTimes()
	mockShard.Resource.ShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	s.mockNamespaceCache = mockShard.Resource.NamespaceCache
	s.mockExecutionMgr = mockShard.Resource.ExecutionMgr
	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(nil).AnyTimes()

	s.mockEventsCache = mockShard.MockEventsCache
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	s.logger = mockShard.GetLogger()

	s.workflowCache = wcache.NewHostLevelCache(mockShard.GetConfig(), metrics.NoopMetricsHandler)

	mockTxProcessor := queues.NewMockQueue(s.controller)
	mockTimerProcessor := queues.NewMockQueue(s.controller)
	mockVisibilityProcessor := queues.NewMockQueue(s.controller)
	mockArchivalProcessor := queues.NewMockQueue(s.controller)
	mockMemoryScheduledQueue := queues.NewMockQueue(s.controller)
	mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	mockVisibilityProcessor.EXPECT().Category().Return(tasks.CategoryVisibility).AnyTimes()
	mockArchivalProcessor.EXPECT().Category().Return(tasks.CategoryArchival).AnyTimes()
	mockMemoryScheduledQueue.EXPECT().Category().Return(tasks.CategoryMemoryTimer).AnyTimes()
	mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockVisibilityProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockArchivalProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockMemoryScheduledQueue.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()

	h := &historyEngineImpl{
		currentClusterName: mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       mockShard,
		clusterMetadata:    mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		logger:             s.logger,
		throttledLogger:    s.logger,
		metricsHandler:     metrics.NoopMetricsHandler,
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		config:             config,
		timeSource:         mockShard.GetTimeSource(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			mockArchivalProcessor.Category():    mockArchivalProcessor,
			mockTxProcessor.Category():          mockTxProcessor,
			mockTimerProcessor.Category():       mockTimerProcessor,
			mockVisibilityProcessor.Category():  mockVisibilityProcessor,
			mockMemoryScheduledQueue.Category(): mockMemoryScheduledQueue,
		},
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(mockShard, s.workflowCache),
	}
	mockShard.SetEngineForTesting(h)

	s.workflowTaskHandlerCallback = newWorkflowTaskHandlerCallback(h)
}

func (s *WorkflowTaskHandlerCallbacksUpdateSuite) TearDownTest() {
}

func (s *WorkflowTaskHandlerCallbacksUpdateSuite) TestAcceptComplete() {

	tv := testvars.New(s.T().Name())

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()

	_, serializedTaskToken := s.createStartedWorkflow(tv)

	writtenHistoryCh := s.writtenHistoryCh(1)

	updRequestMsg, upd := s.createSentUpdate(tv, "1")
	s.NotNil(upd)

	_, err := s.workflowTaskHandlerCallback.handleWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tv.NamespaceID().String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: serializedTaskToken,
			Commands:  s.UpdateAcceptCompleteCommands(tv, "1"),
			Messages:  s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"),
			Identity:  tv.Any().String(),
		},
	})
	s.NoError(err)

	updStatus, err := upd.WaitLifecycleStage(context.Background(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, time.Duration(0))
	s.NoError(err)
	s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED.String(), updStatus.Stage.String())
	s.ProtoEqual(payloads.EncodeString("success-result-of-"+tv.UpdateID("1")), updStatus.Outcome.GetSuccess())

	s.EqualHistoryEvents(`
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted`, <-writtenHistoryCh)
}

func (s *WorkflowTaskHandlerCallbacksUpdateSuite) TestReject() {

	tv := testvars.New(s.T().Name())

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()

	_, serializedTaskToken := s.createStartedWorkflow(tv)

	writtenHistoryCh := s.writtenHistoryCh(1)

	updRequestMsg, upd := s.createSentUpdate(tv, "1")
	s.NotNil(upd)

	_, err := s.workflowTaskHandlerCallback.handleWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tv.NamespaceID().String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: serializedTaskToken,
			Messages:  s.UpdateRejectMessages(tv, updRequestMsg, "1"),
			Identity:  tv.Any().String(),
		},
	})
	s.NoError(err)

	updStatus, err := upd.WaitLifecycleStage(context.Background(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, time.Duration(0))
	s.NoError(err)
	s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED.String(), updStatus.Stage.String())
	s.Equal("rejection-of-"+tv.UpdateID("1"), updStatus.Outcome.GetFailure().GetMessage())

	s.EqualHistoryEvents(`
  4 WorkflowTaskCompleted`, <-writtenHistoryCh)
}

func (s *WorkflowTaskHandlerCallbacksUpdateSuite) TestWriteFailed() {

	tv := testvars.New(s.T().Name())

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()

	_, serializedTaskToken := s.createStartedWorkflow(tv)

	writeErr := errors.New("write failed")
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, writeErr)

	updRequestMsg, upd := s.createSentUpdate(tv, "1")
	s.NotNil(upd)

	_, err := s.workflowTaskHandlerCallback.handleWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tv.NamespaceID().String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: serializedTaskToken,
			Commands:  s.UpdateAcceptCompleteCommands(tv, "1"),
			Messages:  s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"),
			Identity:  tv.Any().String(),
		},
	})
	s.ErrorIs(err, writeErr)

	updStatus, err := upd.WaitLifecycleStage(context.Background(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, time.Duration(0))
	s.NoError(err)
	s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED.String(), updStatus.Stage.String())
	s.Nil(updStatus.Outcome.GetSuccess())
}

func (s *WorkflowTaskHandlerCallbacksUpdateSuite) TestGetHistoryFailed() {

	tv := testvars.New(s.T().Name())

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()

	_, serializedTaskToken := s.createStartedWorkflow(tv)

	writtenHistoryCh := s.writtenHistoryCh(1)

	updRequestMsg, upd := s.createSentUpdate(tv, "1")
	s.NotNil(upd)

	readHistoryErr := errors.New("get history failed")
	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(nil, readHistoryErr)

	_, err := s.workflowTaskHandlerCallback.handleWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tv.NamespaceID().String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken:                  serializedTaskToken,
			Commands:                   s.UpdateAcceptCompleteCommands(tv, "1"),
			Messages:                   s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"),
			Identity:                   tv.Any().String(),
			ReturnNewWorkflowTask:      true,
			ForceCreateNewWorkflowTask: true,
		},
	})
	s.ErrorIs(err, readHistoryErr)

	updStatus, err := upd.WaitLifecycleStage(context.Background(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, time.Duration(0))
	s.NoError(err)
	s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED.String(), updStatus.Stage.String())
	s.ProtoEqual(payloads.EncodeString("success-result-of-"+tv.UpdateID("1")), updStatus.Outcome.GetSuccess())

	s.EqualHistoryEvents(`
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted`, <-writtenHistoryCh)
}

func (s *WorkflowTaskHandlerCallbacksUpdateSuite) writtenHistoryCh(expectedUpdateWorkflowExecutionCalls int) <-chan []*historypb.HistoryEvent {
	writtenHistoryCh := make(chan []*historypb.HistoryEvent, expectedUpdateWorkflowExecutionCalls)
	var historyEvents []*historypb.HistoryEvent
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		var wfEvents []*persistence.WorkflowEvents
		if len(request.UpdateWorkflowEvents) > 0 {
			wfEvents = request.UpdateWorkflowEvents
		} else {
			wfEvents = request.NewWorkflowEvents
		}

		for _, uwe := range wfEvents {
			for _, event := range uwe.Events {
				historyEvents = append(historyEvents, event)
			}
		}
		writtenHistoryCh <- historyEvents
		return tests.UpdateWorkflowExecutionResponse, nil
	}).Times(expectedUpdateWorkflowExecutionCalls)

	return writtenHistoryCh
}

func (s *WorkflowTaskHandlerCallbacksUpdateSuite) getWorkflowContext(tv *testvars.TestVars) workflow.Context {
	weContext, release, err := s.workflowCache.GetOrCreateWorkflowExecution(
		metrics.AddMetricsContext(context.Background()),
		s.workflowTaskHandlerCallback.shardContext,
		tv.NamespaceID(),
		tv.WorkflowExecution(),
		workflow.LockPriorityHigh,
	)
	if err != nil {
		return nil
	}
	defer release(nil)

	return weContext
}

func (s *WorkflowTaskHandlerCallbacksUpdateSuite) createStartedWorkflow(tv *testvars.TestVars) (*workflow.MutableStateImpl, []byte) {
	ms := workflow.TestLocalMutableState(s.workflowTaskHandlerCallback.shardContext, s.mockEventsCache, tv.Namespace(),
		tv.WorkflowID(), tv.RunID(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, tv.WorkflowExecution(), tv.WorkflowType().Name, tv.TaskQueue().Name, tv.Any().Payloads(),
		tv.InfiniteTimeout().AsDuration(), tv.InfiniteTimeout().AsDuration(), tv.InfiniteTimeout().AsDuration(), tv.Any().String())
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tv.TaskQueue().Name, tv.Any().String())

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
			return &persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(ms)}, nil
		}).AnyTimes()

	taskToken := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      tv.NamespaceID().String(),
		WorkflowId:       tv.WorkflowID(),
		RunId:            tv.RunID(),
		ScheduledEventId: wt.ScheduledEventID,
	}
	serializedTaskToken, err := taskToken.Marshal()
	s.NoError(err)

	return ms, serializedTaskToken
}

func (s *WorkflowTaskHandlerCallbacksUpdateSuite) createSentUpdate(tv *testvars.TestVars, updateID string) (*protocolpb.Message, *update.Update) {
	ctx := context.Background()

	weContext := s.getWorkflowContext(tv)
	ms, err := weContext.LoadMutableState(ctx, s.workflowTaskHandlerCallback.shardContext)
	s.NoError(err)

	upd, alreadyExisted, err := weContext.UpdateRegistry(ctx).FindOrCreate(ctx, tv.UpdateID(updateID))
	s.False(alreadyExisted)
	s.NoError(err)

	updReq := &updatepb.Request{
		Meta: &updatepb.Meta{UpdateId: tv.UpdateID(updateID)},
		Input: &updatepb.Input{
			Name: tv.HandlerName(),
			Args: payloads.EncodeString("args-value-of-" + tv.UpdateID(updateID)),
		}}

	eventStore := workflow.WithEffects(effect.Immediate(ctx), ms)

	err = upd.Request(ctx, updReq, eventStore)
	s.NoError(err)

	seqID := &protocolpb.Message_EventId{EventId: tv.Any().EventID()}
	msg := upd.Send(ctx, false, seqID, eventStore)
	s.NotNil(msg)

	updRequestMsg := &protocolpb.Message{
		Id:                 tv.Any().String(),
		ProtocolInstanceId: tv.UpdateID(updateID),
		SequencingId:       seqID,
		Body:               protoutils.MarshalAny(s.T(), updReq),
	}

	return updRequestMsg, upd
}
