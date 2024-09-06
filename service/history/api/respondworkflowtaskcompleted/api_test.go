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

package respondworkflowtaskcompleted

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/locks"
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
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/history/workflow/update"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	WorkflowTaskCompletedHandlerSuite struct {
		suite.Suite
		*require.Assertions
		protorequire.ProtoAssertions
		historyrequire.HistoryRequire
		updateutils.UpdateUtils

		controller         *gomock.Controller
		mockShard          *shard.ContextTest
		mockEventsCache    *events.MockCache
		mockExecutionMgr   *persistence.MockExecutionManager
		workflowCache      wcache.Cache
		mockNamespaceCache *namespace.MockRegistry

		logger log.Logger

		workflowTaskCompletedHandler *WorkflowTaskCompletedHandler
	}
)

func TestWorkflowTaskCompletedHandlerSuite(t *testing.T) {
	suite.Run(t, new(WorkflowTaskCompletedHandlerSuite))
}

func (s *WorkflowTaskCompletedHandlerSuite) SetupSubTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
	s.HistoryRequire = historyrequire.New(s.T())
	s.UpdateUtils = updateutils.New(s.T())

	s.controller = gomock.NewController(s.T())
	config := tests.NewDynamicConfig()
	s.mockShard = shard.NewTestContext(
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
	s.mockShard.SetStateMachineRegistry(reg)

	mockEngine := shard.NewMockEngine(s.controller)
	mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockShard.SetEngineForTesting(mockEngine)

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr

	s.mockShard.Resource.ShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	mockClusterMetadata := s.mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(nil).AnyTimes()

	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	s.logger = s.mockShard.GetLogger()

	s.workflowCache = wcache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	s.workflowTaskCompletedHandler = NewWorkflowTaskCompletedHandler(
		s.mockShard,
		common.NewProtoTaskTokenSerializer(),
		events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
		nil,
		nil,
		nil,
		api.NewWorkflowConsistencyChecker(s.mockShard, s.workflowCache))
}

func (s *WorkflowTaskCompletedHandlerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *WorkflowTaskCompletedHandlerSuite) TestUpdateWorkflow() {

	createWrittenHistoryCh := func(expectedUpdateWorkflowExecutionCalls int) <-chan []*historypb.HistoryEvent {
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

	s.Run("Accept Complete", func() {
		tv := testvars.New(s.T())
		s.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()
		wfContext := s.createStartedWorkflow(tv)
		writtenHistoryCh := createWrittenHistoryCh(1)

		updRequestMsg, upd, serializedTaskToken := s.createSentUpdate(tv, "1", wfContext)
		s.NotNil(upd)

		_, err := s.workflowTaskCompletedHandler.Invoke(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
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
  2 WorkflowTaskScheduled // Speculative WFT events are persisted on WFT completion.
  3 WorkflowTaskStarted // Speculative WFT events are persisted on WFT completion.
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted`, <-writtenHistoryCh)
	})

	s.Run("Reject", func() {
		tv := testvars.New(s.T())
		s.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()
		wfContext := s.createStartedWorkflow(tv)

		updRequestMsg, upd, serializedTaskToken := s.createSentUpdate(tv, "1", wfContext)
		s.NotNil(upd)

		_, err := s.workflowTaskCompletedHandler.Invoke(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
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
	})

	s.Run("Write failed on normal task queue", func() {
		tv := testvars.New(s.T())
		s.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()
		wfContext := s.createStartedWorkflow(tv)

		writeErr := errors.New("write failed")
		s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, writeErr)

		updRequestMsg, upd, serializedTaskToken := s.createSentUpdate(tv, "1", wfContext)
		s.NotNil(upd)

		_, err := s.workflowTaskCompletedHandler.Invoke(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tv.NamespaceID().String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: serializedTaskToken,
				Commands:  s.UpdateAcceptCompleteCommands(tv, "1"),
				Messages:  s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"),
				Identity:  tv.Any().String(),
			},
		})
		s.ErrorIs(err, writeErr)

		s.Nil(wfContext.(*workflow.ContextImpl).MutableState, "mutable state must be cleared")
	})

	s.Run("Write failed on sticky task queue", func() {
		tv := testvars.New(s.T())
		s.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()
		wfContext := s.createStartedWorkflow(tv)

		writeErr := serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT, "write failed")
		// First write of MS
		s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, writeErr)
		// Second write of MS to clear stickiness
		s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

		updRequestMsg, upd, serializedTaskToken := s.createSentUpdate(tv, "1", wfContext)
		s.NotNil(upd)

		_, err := s.workflowTaskCompletedHandler.Invoke(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tv.NamespaceID().String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: serializedTaskToken,
				Commands:  s.UpdateAcceptCompleteCommands(tv, "1"),
				Messages:  s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"),
				Identity:  tv.Any().String(),
				StickyAttributes: &taskqueuepb.StickyExecutionAttributes{
					WorkerTaskQueue:        tv.StickyTaskQueue(),
					ScheduleToStartTimeout: tv.InfiniteTimeout(),
				},
			},
		})
		s.ErrorIs(err, writeErr)

		s.Nil(wfContext.(*workflow.ContextImpl).MutableState, "mutable state must be cleared")
	})

	s.Run("GetHistory failed", func() {
		tv := testvars.New(s.T())
		s.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()
		wfContext := s.createStartedWorkflow(tv)
		writtenHistoryCh := createWrittenHistoryCh(1)

		updRequestMsg, upd, serializedTaskToken := s.createSentUpdate(tv, "1", wfContext)
		s.NotNil(upd)

		readHistoryErr := errors.New("get history failed")
		s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(nil, readHistoryErr)

		_, err := s.workflowTaskCompletedHandler.Invoke(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
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
  2 WorkflowTaskScheduled // Speculative WFT events are persisted on WFT completion.
  3 WorkflowTaskStarted // Speculative WFT events are persisted on WFT completion.
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted`, <-writtenHistoryCh)
	})
}

func (s *WorkflowTaskCompletedHandlerSuite) TestHandleBufferedQueries() {
	constructQueryResults := func(ids []string, resultSize int) map[string]*querypb.WorkflowQueryResult {
		results := make(map[string]*querypb.WorkflowQueryResult)
		for _, id := range ids {
			results[id] = &querypb.WorkflowQueryResult{
				ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
				Answer:     payloads.EncodeBytes(make([]byte, resultSize)),
			}
		}
		return results
	}

	constructQueryRegistry := func(numQueries int) workflow.QueryRegistry {
		queryRegistry := workflow.NewQueryRegistry()
		for i := 0; i < numQueries; i++ {
			queryRegistry.BufferQuery(&querypb.WorkflowQuery{})
		}
		return queryRegistry
	}

	assertQueryCounts := func(queryRegistry workflow.QueryRegistry, buffered, completed, unblocked, failed int) {
		s.Len(queryRegistry.GetBufferedIDs(), buffered)
		s.Len(queryRegistry.GetCompletedIDs(), completed)
		s.Len(queryRegistry.GetUnblockedIDs(), unblocked)
		s.Len(queryRegistry.GetFailedIDs(), failed)
	}

	setupBufferedQueriesMocks := func() (workflow.QueryRegistry, *workflow.MockMutableState) {
		queryRegistry := constructQueryRegistry(10)
		mockMutableState := workflow.NewMockMutableState(s.controller)
		mockMutableState.EXPECT().GetQueryRegistry().Return(queryRegistry)
		mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			WorkflowId: tests.WorkflowID,
		}).AnyTimes()
		mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
			RunId: tests.RunID,
		}).AnyTimes()
		return queryRegistry, mockMutableState
	}

	s.Run("New WorkflowTask", func() {
		queryRegistry, mockMutableState := setupBufferedQueriesMocks()
		assertQueryCounts(queryRegistry, 10, 0, 0, 0)
		queryResults := constructQueryResults(queryRegistry.GetBufferedIDs()[0:5], 10)
		s.workflowTaskCompletedHandler.handleBufferedQueries(mockMutableState, queryResults, true, tests.GlobalNamespaceEntry)
		assertQueryCounts(queryRegistry, 5, 5, 0, 0)
	})

	s.Run("No New WorkflowTask", func() {
		queryRegistry, mockMutableState := setupBufferedQueriesMocks()
		assertQueryCounts(queryRegistry, 10, 0, 0, 0)
		queryResults := constructQueryResults(queryRegistry.GetBufferedIDs()[0:5], 10)
		s.workflowTaskCompletedHandler.handleBufferedQueries(mockMutableState, queryResults, false, tests.GlobalNamespaceEntry)
		assertQueryCounts(queryRegistry, 0, 5, 5, 0)
	})

	s.Run("Query Too Large", func() {
		queryRegistry, mockMutableState := setupBufferedQueriesMocks()
		assertQueryCounts(queryRegistry, 10, 0, 0, 0)
		bufferedIDs := queryRegistry.GetBufferedIDs()
		queryResults := constructQueryResults(bufferedIDs[0:5], 10)
		largeQueryResults := constructQueryResults(bufferedIDs[5:10], 10*1024*1024)
		maps.Copy(queryResults, largeQueryResults)
		s.workflowTaskCompletedHandler.handleBufferedQueries(mockMutableState, queryResults, false, tests.GlobalNamespaceEntry)
		assertQueryCounts(queryRegistry, 0, 5, 0, 5)
	})
}

func (s *WorkflowTaskCompletedHandlerSuite) createStartedWorkflow(tv *testvars.TestVars) workflow.Context {
	ms := workflow.TestLocalMutableState(s.workflowTaskCompletedHandler.shardContext, s.mockEventsCache, tv.Namespace(),
		tv.WorkflowID(), tv.RunID(), log.NewTestLogger())

	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		Input:                    tv.Any().Payloads(),
		WorkflowExecutionTimeout: durationpb.New(tv.InfiniteTimeout().AsDuration()),
		WorkflowRunTimeout:       durationpb.New(tv.InfiniteTimeout().AsDuration()),
		WorkflowTaskTimeout:      durationpb.New(tv.InfiniteTimeout().AsDuration()),
		Identity:                 tv.Any().String(),
	}

	_, _ = ms.AddWorkflowExecutionStartedEvent(
		tv.WorkflowExecution(),
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:             1,
			NamespaceId:         tv.NamespaceID().String(),
			StartRequest:        startRequest,
			ParentExecutionInfo: nil,
		},
	)

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
			return &persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(ms)}, nil
		}).AnyTimes()

	// Create WF context in the cache and load MS for it.
	wfContext, release, err := s.workflowCache.GetOrCreateWorkflowExecution(
		metrics.AddMetricsContext(context.Background()),
		s.mockShard,
		tv.NamespaceID(),
		tv.WorkflowExecution(),
		locks.PriorityHigh,
	)
	s.NoError(err)
	s.NotNil(wfContext)

	loadedMS, err := wfContext.LoadMutableState(context.Background(), s.mockShard)
	s.NoError(err)
	s.NotNil(loadedMS)
	release(nil)

	return wfContext
}

func (s *WorkflowTaskCompletedHandlerSuite) createSentUpdate(tv *testvars.TestVars, updateID string, wfContext workflow.Context) (*protocolpb.Message, *update.Update, []byte) {
	ctx := context.Background()

	ms, err := wfContext.LoadMutableState(ctx, s.workflowTaskCompletedHandler.shardContext)
	s.NoError(err)

	// 1. Create speculative WFT for update.
	wt, _ := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE)
	_, _, _ = ms.AddWorkflowTaskStartedEvent(
		wt.ScheduledEventID,
		tv.RunID(),
		tv.StickyTaskQueue(),
		tv.Any().String(),
		nil,
		nil,
		false,
	)
	taskToken := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      tv.NamespaceID().String(),
		WorkflowId:       tv.WorkflowID(),
		RunId:            tv.RunID(),
		ScheduledEventId: wt.ScheduledEventID,
	}
	serializedTaskToken, err := taskToken.Marshal()
	s.NoError(err)

	// 2. Create update.
	upd, alreadyExisted, err := wfContext.UpdateRegistry(ctx, nil).FindOrCreate(ctx, tv.UpdateID(updateID))
	s.False(alreadyExisted)
	s.NoError(err)

	updReq := &updatepb.Request{
		Meta: &updatepb.Meta{UpdateId: tv.UpdateID(updateID)},
		Input: &updatepb.Input{
			Name: tv.HandlerName(),
			Args: payloads.EncodeString("args-value-of-" + tv.UpdateID(updateID)),
		}}

	eventStore := workflow.WithEffects(effect.Immediate(ctx), ms)

	err = upd.Admit(updReq, eventStore)
	s.NoError(err)

	seqID := &protocolpb.Message_EventId{EventId: tv.Any().EventID()}
	msg := upd.Send(false, seqID)
	s.NotNil(msg)

	updRequestMsg := &protocolpb.Message{
		Id:                 tv.Any().String(),
		ProtocolInstanceId: tv.UpdateID(updateID),
		SequencingId:       seqID,
		Body:               protoutils.MarshalAny(s.T(), updReq),
	}

	return updRequestMsg, upd, serializedTaskToken
}
