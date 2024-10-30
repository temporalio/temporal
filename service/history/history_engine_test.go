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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/getworkflowexecutionrawhistoryv2"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	esIndexName = ""
)

type (
	engineSuite struct {
		suite.Suite
		*require.Assertions
		protorequire.ProtoAssertions

		controller               *gomock.Controller
		mockShard                *shard.ContextTest
		mockTxProcessor          *queues.MockQueue
		mockTimerProcessor       *queues.MockQueue
		mockVisibilityProcessor  *queues.MockQueue
		mockArchivalProcessor    *queues.MockQueue
		mockMemoryScheduledQueue *queues.MockQueue
		mockOutboundProcessor    *queues.MockQueue
		mockNamespaceCache       *namespace.MockRegistry
		mockMatchingClient       *matchingservicemock.MockMatchingServiceClient
		mockHistoryClient        *historyservicemock.MockHistoryServiceClient
		mockClusterMetadata      *cluster.MockMetadata
		mockEventsReapplier      *ndc.MockEventsReapplier
		mockWorkflowResetter     *ndc.MockWorkflowResetter

		workflowCache     wcache.Cache
		mockHistoryEngine *historyEngineImpl
		mockExecutionMgr  *persistence.MockExecutionManager
		mockShardManager  *persistence.MockShardManager
		mockVisibilityMgr *manager.MockVisibilityManager

		mockSearchAttributesProvider       *searchattribute.MockProvider
		mockSearchAttributesMapperProvider *searchattribute.MockMapperProvider

		eventsCache events.Cache
		config      *configs.Config
	}
)

func TestEngineSuite(t *testing.T) {
	s := new(engineSuite)
	suite.Run(t, s)
}

func (s *engineSuite) SetupSuite() {

}

func (s *engineSuite) TearDownSuite() {
}

func (s *engineSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockEventsReapplier = ndc.NewMockEventsReapplier(s.controller)
	s.mockWorkflowResetter = ndc.NewMockWorkflowResetter(s.controller)
	s.mockTxProcessor = queues.NewMockQueue(s.controller)
	s.mockTimerProcessor = queues.NewMockQueue(s.controller)
	s.mockVisibilityProcessor = queues.NewMockQueue(s.controller)
	s.mockArchivalProcessor = queues.NewMockQueue(s.controller)
	s.mockMemoryScheduledQueue = queues.NewMockQueue(s.controller)
	s.mockOutboundProcessor = queues.NewMockQueue(s.controller)
	s.mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	s.mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	s.mockVisibilityProcessor.EXPECT().Category().Return(tasks.CategoryVisibility).AnyTimes()
	s.mockArchivalProcessor.EXPECT().Category().Return(tasks.CategoryArchival).AnyTimes()
	s.mockMemoryScheduledQueue.EXPECT().Category().Return(tasks.CategoryMemoryTimer).AnyTimes()
	s.mockOutboundProcessor.EXPECT().Category().Return(tasks.CategoryOutbound).AnyTimes()
	s.mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockVisibilityProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockArchivalProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockMemoryScheduledQueue.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockOutboundProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()

	s.config = tests.NewDynamicConfig()
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		s.config,
	)
	s.workflowCache = wcache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	s.eventsCache = events.NewHostLevelEventsCache(
		s.mockShard.GetExecutionManager(),
		s.mockShard.GetConfig(),
		s.mockShard.GetMetricsHandler(),
		s.mockShard.GetLogger(),
		false,
	)
	s.mockShard.SetEventsCacheForTesting(s.eventsCache)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	s.mockShard.SetStateMachineRegistry(reg)

	s.mockMatchingClient = s.mockShard.Resource.MatchingClient
	s.mockHistoryClient = s.mockShard.Resource.HistoryClient
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockShardManager = s.mockShard.Resource.ShardMgr
	s.mockVisibilityMgr = s.mockShard.Resource.VisibilityManager
	s.mockSearchAttributesProvider = s.mockShard.Resource.SearchAttributesProvider
	s.mockSearchAttributesMapperProvider = s.mockShard.Resource.SearchAttributesMapperProvider
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	eventNotifier := events.NewNotifier(
		clock.NewRealTimeSource(),
		s.mockShard.Resource.MetricsHandler,
		func(namespaceID namespace.ID, workflowID string) int32 {
			key := namespaceID.String() + "_" + workflowID
			return int32(len(key))
		},
	)

	h := &historyEngineImpl{
		currentClusterName: s.mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       s.mockShard,
		clusterMetadata:    s.mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		logger:             s.mockShard.GetLogger(),
		metricsHandler:     s.mockShard.GetMetricsHandler(),
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		eventNotifier:      eventNotifier,
		config:             s.config,
		queueProcessors: map[tasks.Category]queues.Queue{
			s.mockTxProcessor.Category():          s.mockTxProcessor,
			s.mockTimerProcessor.Category():       s.mockTimerProcessor,
			s.mockVisibilityProcessor.Category():  s.mockVisibilityProcessor,
			s.mockArchivalProcessor.Category():    s.mockArchivalProcessor,
			s.mockMemoryScheduledQueue.Category(): s.mockMemoryScheduledQueue,
			s.mockOutboundProcessor.Category():    s.mockOutboundProcessor,
		},
		eventsReapplier:            s.mockEventsReapplier,
		workflowResetter:           s.mockWorkflowResetter,
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(s.mockShard, s.workflowCache),
		throttledLogger:            log.NewNoopLogger(),
		persistenceVisibilityMgr:   s.mockVisibilityMgr,
		versionChecker:             headers.NewDefaultVersionChecker(),
	}
	s.mockShard.SetEngineForTesting(h)

	h.eventNotifier.Start()

	s.mockHistoryEngine = h
}

func (s *engineSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
	s.mockHistoryEngine.eventNotifier.Stop()
}

func (s *engineSuite) TestGetMutableStateSync() {
	ctx := context.Background()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache, tests.LocalNamespaceEntry,
		execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	// right now the next event ID is 4
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	// test get the next event ID instantly
	response, err := s.mockHistoryEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: tests.NamespaceID.String(),
		Execution:   &execution,
	})
	s.Nil(err)
	s.Equal(int64(4), response.GetNextEventId())
	s.Equal(tests.RunID, response.GetFirstExecutionRunId())
}

func (s *engineSuite) TestGetMutableState_IntestRunID() {
	ctx := context.Background()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      "run-id-not-valid-uuid",
	}

	_, err := s.mockHistoryEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: tests.NamespaceID.String(),
		Execution:   &execution,
	})
	s.Equal(errRunIDNotValid, err)
}

func (s *engineSuite) TestGetMutableState_EmptyRunID() {
	ctx := context.Background()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
	}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	_, err := s.mockHistoryEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: tests.NamespaceID.String(),
		Execution:   &execution,
	})
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestGetMutableStateLongPoll() {
	ctx := context.Background()

	namespaceID := tests.NamespaceID
	execution := commonpb.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache, tests.LocalNamespaceEntry,
		execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	// right now the next event ID is 4
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	// test long poll on next event ID change
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	asycWorkflowUpdate := func(delay time.Duration) {
		tt := &tokenspb.Task{
			Attempt:          1,
			NamespaceId:      namespaceID.String(),
			WorkflowId:       execution.WorkflowId,
			RunId:            execution.RunId,
			ScheduledEventId: 2,
		}
		taskToken, _ := tt.Marshal()
		s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

		timer := time.NewTimer(delay)

		<-timer.C
		_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tests.NamespaceID.String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: taskToken,
				Identity:  identity,
			},
		})
		s.Nil(err)
		waitGroup.Done()
		// right now the next event ID is 5
	}

	// return immediately, since the expected next event ID appears
	response, err := s.mockHistoryEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId:         tests.NamespaceID.String(),
		Execution:           &execution,
		ExpectedNextEventId: 3,
	})
	s.Nil(err)
	s.Equal(int64(4), response.NextEventId)

	// long poll, new event happen before long poll timeout
	go asycWorkflowUpdate(time.Second)
	start := time.Now().UTC()
	pollResponse, err := s.mockHistoryEngine.PollMutableState(ctx, &historyservice.PollMutableStateRequest{
		NamespaceId:         tests.NamespaceID.String(),
		Execution:           &execution,
		ExpectedNextEventId: 4,
	})
	s.True(time.Now().UTC().After(start.Add(time.Second)))
	s.Nil(err)
	s.Equal(int64(5), pollResponse.GetNextEventId())
	waitGroup.Wait()
}

func (s *engineSuite) TestGetMutableStateLongPoll_CurrentBranchChanged() {
	ctx := context.Background()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(
		s.mockHistoryEngine.shardContext,
		s.eventsCache,
		tests.LocalNamespaceEntry,
		execution.GetWorkflowId(),
		execution.GetRunId(),
		log.NewTestLogger(),
	)
	addWorkflowExecutionStartedEvent(ms, execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	// right now the next event ID is 4
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	// test long poll on next event ID change
	asyncBranchTokenUpdate := func(delay time.Duration) {
		timer := time.NewTimer(delay)
		<-timer.C
		s.mockHistoryEngine.eventNotifier.NotifyNewHistoryEvent(events.NewNotification(
			ms.GetWorkflowKey().NamespaceID,
			execution,
			int64(1),
			int64(0),
			int64(12),
			int64(1),
			enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			ms.GetExecutionInfo().GetVersionHistories()),
		)
	}

	// return immediately, since the expected next event ID appears
	response0, err := s.mockHistoryEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId:         tests.NamespaceID.String(),
		Execution:           execution,
		ExpectedNextEventId: 3,
	})
	s.Nil(err)
	s.Equal(int64(4), response0.GetNextEventId())

	// long poll, new event happen before long poll timeout
	go asyncBranchTokenUpdate(time.Second)
	start := time.Now().UTC()
	response1, err := s.mockHistoryEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId:         tests.NamespaceID.String(),
		Execution:           execution,
		ExpectedNextEventId: 10,
	})
	s.True(time.Now().UTC().After(start.Add(time.Second)))
	s.Nil(err)
	s.Equal(response0.GetCurrentBranchToken(), response1.GetCurrentBranchToken())
}

func (s *engineSuite) TestGetMutableStateLongPollTimeout() {
	ctx := context.Background()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	// right now the next event ID is 4
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	s.mockShard.GetConfig().LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Second)

	// long poll, no event happen after long poll timeout
	response, err := s.mockHistoryEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId:         tests.NamespaceID.String(),
		Execution:           &execution,
		ExpectedNextEventId: 4,
	})
	s.Nil(err)
	s.Equal(int64(4), response.GetNextEventId())
}

func (s *engineSuite) TestQueryWorkflow_RejectBasedOnCompleted() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_RejectBasedOnCompleted",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache, tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	event := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	addCompleteWorkflowEvent(ms, event.GetEventId(), nil)
	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	request := &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution:            &execution,
			Query:                &querypb.WorkflowQuery{},
			QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN,
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.NoError(err)
	s.Nil(resp.GetResponse().QueryResult)
	s.NotNil(resp.GetResponse().QueryRejected)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, resp.GetResponse().GetQueryRejected().GetStatus())
}

func (s *engineSuite) TestQueryWorkflow_RejectBasedOnFailed() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_RejectBasedOnFailed",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache, tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	event := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	addFailWorkflowEvent(ms, event.GetEventId(), failure.NewServerFailure("failure reason", true), enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE)
	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	request := &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution:            &execution,
			Query:                &querypb.WorkflowQuery{},
			QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN,
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.NoError(err)
	s.Nil(resp.GetResponse().QueryResult)
	s.NotNil(resp.GetResponse().QueryRejected)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, resp.GetResponse().GetQueryRejected().GetStatus())

	request = &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution:            &execution,
			Query:                &querypb.WorkflowQuery{},
			QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY,
		},
	}
	resp, err = s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.NoError(err)
	s.Nil(resp.GetResponse().QueryResult)
	s.NotNil(resp.GetResponse().QueryRejected)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, resp.GetResponse().GetQueryRejected().GetStatus())
}

func (s *engineSuite) TestQueryWorkflow_DirectlyThroughMatching() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_DirectlyThroughMatching",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache, tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	startedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, startedEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)
	s.mockMatchingClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(&matchingservice.QueryWorkflowResponse{QueryResult: payloads.EncodeBytes([]byte{1, 2, 3})}, nil)
	s.mockHistoryEngine.matchingClient = s.mockMatchingClient
	request := &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution: &execution,
			Query:     &querypb.WorkflowQuery{},
			// since workflow is open this filter does not reject query
			QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN,
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.NoError(err)
	s.NotNil(resp.GetResponse().QueryResult)
	s.Nil(resp.GetResponse().QueryRejected)

	var queryResult []byte
	err = payloads.Decode(resp.GetResponse().GetQueryResult(), &queryResult)
	s.NoError(err)
	s.Equal([]byte{1, 2, 3}, queryResult)
}

func (s *engineSuite) TestQueryWorkflow_WorkflowTaskDispatch_Timeout() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_WorkflowTaskDispatch_Timeout",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache, tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	startedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, startedEvent.EventId, identity)
	wt = addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)
	request := &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution: &execution,
			Query:     &querypb.WorkflowQuery{},
			// since workflow is open this filter does not reject query
			QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN,
		},
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()
		resp, err := s.mockHistoryEngine.QueryWorkflow(ctx, request)
		s.Error(err)
		s.Nil(resp)
		wg.Done()
	}()

	time.Sleep(time.Second)
	ms1 := s.getMutableState(tests.NamespaceID, &execution)
	s.NotNil(ms1)
	qr := ms1.GetQueryRegistry()
	s.True(qr.HasBufferedQuery())
	s.False(qr.HasCompletedQuery())
	s.False(qr.HasUnblockedQuery())
	s.False(qr.HasFailedQuery())
	wg.Wait()
	s.False(qr.HasBufferedQuery())
	s.False(qr.HasCompletedQuery())
	s.False(qr.HasUnblockedQuery())
	s.False(qr.HasFailedQuery())
}

func (s *engineSuite) TestQueryWorkflow_ConsistentQueryBufferFull() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_ConsistentQueryBufferFull",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache, tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	startedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, startedEvent.EventId, identity)
	wt = addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	// buffer query so that when history.QueryWorkflow is called buffer is already full
	ctx, release, err := s.workflowCache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		tests.NamespaceID,
		&execution,
		locks.PriorityHigh,
	)
	s.NoError(err)
	loadedMS, err := ctx.LoadMutableState(context.Background(), s.mockShard)
	s.NoError(err)
	qr := workflow.NewQueryRegistry()
	qr.BufferQuery(&querypb.WorkflowQuery{})
	loadedMS.(*workflow.MutableStateImpl).QueryRegistry = qr
	release(nil)

	request := &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution: &execution,
			Query:     &querypb.WorkflowQuery{},
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.Nil(resp)
	s.Equal(consts.ErrConsistentQueryBufferExceeded, err)
}

func (s *engineSuite) TestQueryWorkflow_WorkflowTaskDispatch_Complete() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_WorkflowTaskDispatch_Complete",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache, tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	startedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, startedEvent.EventId, identity)
	wt = addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	asyncQueryUpdate := func(delay time.Duration, answer []byte) {
		defer waitGroup.Done()
		time.Sleep(delay)
		ms1 := s.getMutableState(tests.NamespaceID, &execution)
		s.NotNil(ms1)
		qr := ms1.GetQueryRegistry()
		buffered := qr.GetBufferedIDs()
		for _, id := range buffered {
			resultType := enumspb.QUERY_RESULT_TYPE_ANSWERED
			succeededCompletionState := &workflow.QueryCompletionState{
				Type: workflow.QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType: resultType,
					Answer:     payloads.EncodeBytes(answer),
				},
			}
			err := qr.SetCompletionState(id, succeededCompletionState)
			s.NoError(err)
			state, err := qr.GetCompletionState(id)
			s.NoError(err)
			s.Equal(workflow.QueryCompletionTypeSucceeded, state.Type)
		}
	}

	request := &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution: &execution,
			Query:     &querypb.WorkflowQuery{},
		},
	}
	go asyncQueryUpdate(time.Second*2, []byte{1, 2, 3})
	start := time.Now().UTC()
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.True(time.Now().UTC().After(start.Add(time.Second)))
	s.NoError(err)

	var queryResult []byte
	err = payloads.Decode(resp.GetResponse().GetQueryResult(), &queryResult)
	s.NoError(err)
	s.Equal([]byte{1, 2, 3}, queryResult)

	ms1 := s.getMutableState(tests.NamespaceID, &execution)
	s.NotNil(ms1)
	qr := ms1.GetQueryRegistry()
	s.False(qr.HasBufferedQuery())
	s.False(qr.HasCompletedQuery())
	waitGroup.Wait()
}

func (s *engineSuite) TestQueryWorkflow_WorkflowTaskDispatch_Unblocked() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_WorkflowTaskDispatch_Unblocked",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache, tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	startedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, startedEvent.EventId, identity)
	wt = addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)
	s.mockMatchingClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(&matchingservice.QueryWorkflowResponse{QueryResult: payloads.EncodeBytes([]byte{1, 2, 3})}, nil)
	s.mockHistoryEngine.matchingClient = s.mockMatchingClient
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	asyncQueryUpdate := func(delay time.Duration, answer []byte) {
		defer waitGroup.Done()
		time.Sleep(delay)
		ms1 := s.getMutableState(tests.NamespaceID, &execution)
		s.NotNil(ms1)
		qr := ms1.GetQueryRegistry()
		buffered := qr.GetBufferedIDs()
		for _, id := range buffered {
			s.NoError(qr.SetCompletionState(id, &workflow.QueryCompletionState{Type: workflow.QueryCompletionTypeUnblocked}))
			state, err := qr.GetCompletionState(id)
			s.NoError(err)
			s.Equal(workflow.QueryCompletionTypeUnblocked, state.Type)
		}
	}

	request := &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution: &execution,
			Query:     &querypb.WorkflowQuery{},
		},
	}
	go asyncQueryUpdate(time.Second*2, []byte{1, 2, 3})
	start := time.Now().UTC()
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.True(time.Now().UTC().After(start.Add(time.Second)))
	s.NoError(err)

	var queryResult []byte
	err = payloads.Decode(resp.GetResponse().GetQueryResult(), &queryResult)
	s.NoError(err)
	s.Equal([]byte{1, 2, 3}, queryResult)

	ms1 := s.getMutableState(tests.NamespaceID, &execution)
	s.NotNil(ms1)
	qr := ms1.GetQueryRegistry()
	s.False(qr.HasBufferedQuery())
	s.False(qr.HasCompletedQuery())
	s.False(qr.HasUnblockedQuery())
	waitGroup.Wait()
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedInvalidToken() {

	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: invalidToken,
			Commands:  nil,
			Identity:  identity,
		},
	})

	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedIfNoExecution() {
	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedIfGetExecutionFailed() {
	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("FAILED"))

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedUpdateExecutionFailed() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tq := "testTaskQueue"

	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tq, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tq, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, errors.New("FAILED"))
	s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil).AnyTimes() // might be called in background goroutine

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedIfTaskCompleted() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tq := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tq, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	startedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tq, identity)
	addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, startedEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedIfTaskNotStarted() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tq := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tq, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedConflictOnUpdate() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tq := "testTaskQueue"
	identity := "testIdentity"
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := payloads.EncodeString("input1")
	activity1Result := payloads.EncodeString("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := payloads.EncodeString("input2")
	activity2Result := payloads.EncodeString("activity2_result")
	activity3ID := "activity3"
	activity3Type := "activity_type3"
	activity3Input := payloads.EncodeString("input3")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tq, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt1 := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent1 := addWorkflowTaskStartedEvent(ms, wt1.ScheduledEventID, tq, identity)
	workflowTaskCompletedEvent1 := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt1.ScheduledEventID, workflowTaskStartedEvent1.EventId, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent1.EventId, activity1ID, activity1Type, tq, activity1Input, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent1.EventId, activity2ID, activity2Type, tq, activity2Input, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activity1StartedEvent := addActivityTaskStartedEvent(ms, activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(ms, activity2ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(ms, activity1ScheduledEvent.EventId,
		activity1StartedEvent.EventId, activity1Result, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tq, identity)

	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: wt2.ScheduledEventID,
	}
	taskToken, _ := tt.Marshal()

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:             activity3ID,
			ActivityType:           &commonpb.ActivityType{Name: activity3Type},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tq},
			Input:                  activity3Input,
			ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
			ScheduleToStartTimeout: durationpb.New(10 * time.Second),
			StartToCloseTimeout:    durationpb.New(50 * time.Second),
			HeartbeatTimeout:       durationpb.New(5 * time.Second),
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	addActivityTaskCompletedEvent(ms, activity2ScheduledEvent.EventId,
		activity2StartedEvent.EventId, activity2Result, identity)

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, &persistence.ConditionFailedError{})

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Error(err)
	s.Equal(&persistence.ConditionFailedError{}, err)
}

func (s *engineSuite) TestValidateSignalRequest() {
	workflowType := "testType"
	input := payloads.EncodeString("input")
	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:               "ID",
		WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: "taskptr"},
		Input:                    input,
		WorkflowExecutionTimeout: durationpb.New(20 * time.Second),
		WorkflowRunTimeout:       durationpb.New(10 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
		Identity:                 "identity",
	}
	err := api.ValidateStartWorkflowExecutionRequest(
		context.Background(), startRequest, s.mockHistoryEngine.shardContext, tests.LocalNamespaceEntry, "SignalWithStartWorkflowExecution")
	s.Error(err, "startRequest doesn't have request id, it should error out")

	startRequest.RequestId = "request-id"
	startRequest.Memo = &commonpb.Memo{Fields: map[string]*commonpb.Payload{
		"data": payload.EncodeBytes(make([]byte, 4*1024*1024)),
	}}
	err = api.ValidateStartWorkflowExecutionRequest(
		context.Background(), startRequest, s.mockHistoryEngine.shardContext, tests.LocalNamespaceEntry, "SignalWithStartWorkflowExecution")
	s.Error(err, "memo should be too big")
}

func (s *engineSuite) TestRespondWorkflowTaskCompleted_StaleCache() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	tt.ScheduledEventId = 4 // Set it to 4 to emulate stale cache.

	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	input := payloads.EncodeString("input")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:             "activity1",
			ActivityType:           &commonpb.ActivityType{Name: "activity_type1"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
			Input:                  input,
			ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
			ScheduleToStartTimeout: durationpb.New(10 * time.Second),
			StartToCloseTimeout:    durationpb.New(50 * time.Second),
			HeartbeatTimeout:       durationpb.New(5 * time.Second),
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil).Times(2)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedCompleteWorkflowFailed() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	identity := "testIdentity"
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := payloads.EncodeString("input1")
	activity1Result := payloads.EncodeString("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := payloads.EncodeString("input2")
	activity2Result := payloads.EncodeString("activity2_result")
	workflowResult := payloads.EncodeString("workflow result")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, identity)
	wt1 := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent1 := addWorkflowTaskStartedEvent(ms, wt1.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent1 := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt1.ScheduledEventID, workflowTaskStartedEvent1.EventId, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent1.EventId, activity1ID, activity1Type, tl, activity1Input, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent1.EventId, activity2ID, activity2Type, tl, activity2Input, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activity1StartedEvent := addActivityTaskStartedEvent(ms, activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(ms, activity2ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(ms, activity1ScheduledEvent.EventId,
		activity1StartedEvent.EventId, activity1Result, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)
	addActivityTaskCompletedEvent(ms, activity2ScheduledEvent.EventId,
		activity2StartedEvent.EventId, activity2Result, identity)

	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: wt2.ScheduledEventID,
	}
	taskToken, _ := tt.Marshal()

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
			Result: workflowResult,
		}},
	}}

	ms1 := workflow.TestCloneToProto(ms)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)

	ms2 := common.CloneProto(ms1)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)

	var updatedWorkflowMutation persistence.WorkflowMutation
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		updatedWorkflowMutation = request.UpdateWorkflowMutation
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("UnhandledCommand", err.Error())

	s.NotNil(updatedWorkflowMutation)
	s.Equal(int64(15), updatedWorkflowMutation.NextEventID)
	s.Equal(workflowTaskStartedEvent1.EventId, updatedWorkflowMutation.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, updatedWorkflowMutation.ExecutionState.State)
	s.Equal(updatedWorkflowMutation.NextEventID-1, updatedWorkflowMutation.ExecutionInfo.WorkflowTaskScheduledEventId)
	s.Equal(int32(1), updatedWorkflowMutation.ExecutionInfo.Attempt)
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedFailWorkflowFailed() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	identity := "testIdentity"
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := payloads.EncodeString("input1")
	activity1Result := payloads.EncodeString("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := payloads.EncodeString("input2")
	activity2Result := payloads.EncodeString("activity2_result")
	reason := "workflow fail reason"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, identity)
	wt1 := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent1 := addWorkflowTaskStartedEvent(ms, wt1.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent1 := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt1.ScheduledEventID, workflowTaskStartedEvent1.EventId, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent1.EventId, activity1ID, activity1Type, tl, activity1Input, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent1.EventId, activity2ID, activity2Type, tl, activity2Input, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activity1StartedEvent := addActivityTaskStartedEvent(ms, activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(ms, activity2ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(ms, activity1ScheduledEvent.EventId,
		activity1StartedEvent.EventId, activity1Result, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)
	addActivityTaskCompletedEvent(ms, activity2ScheduledEvent.EventId,
		activity2StartedEvent.EventId, activity2Result, identity)

	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: wt2.ScheduledEventID,
	}
	taskToken, _ := tt.Marshal()

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
			Failure: failure.NewServerFailure(reason, false),
		}},
	}}

	ms1 := workflow.TestCloneToProto(ms)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)

	ms2 := common.CloneProto(ms1)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)

	var updatedWorkflowMutation persistence.WorkflowMutation
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		updatedWorkflowMutation = request.UpdateWorkflowMutation
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("UnhandledCommand", err.Error())

	s.NotNil(updatedWorkflowMutation)
	s.Equal(int64(15), updatedWorkflowMutation.NextEventID)
	s.Equal(workflowTaskStartedEvent1.EventId, updatedWorkflowMutation.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, updatedWorkflowMutation.ExecutionState.State)
	s.Equal(updatedWorkflowMutation.NextEventID-1, updatedWorkflowMutation.ExecutionInfo.WorkflowTaskScheduledEventId)
	s.Equal(int32(1), updatedWorkflowMutation.ExecutionInfo.Attempt)
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedBadCommandAttributes() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	identity := "testIdentity"
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := payloads.EncodeString("input1")
	activity1Result := payloads.EncodeString("activity1_result")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, identity)
	wt1 := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent1 := addWorkflowTaskStartedEvent(ms, wt1.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent1 := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt1.ScheduledEventID, workflowTaskStartedEvent1.EventId, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent1.EventId, activity1ID, activity1Type, tl, activity1Input, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activity1StartedEvent := addActivityTaskStartedEvent(ms, activity1ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(ms, activity1ScheduledEvent.EventId,
		activity1StartedEvent.EventId, activity1Result, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: wt2.ScheduledEventID,
	}
	taskToken, _ := tt.Marshal()

	// commands with nil attributes
	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
	}}

	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(ms)}
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(ms)}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("BadCompleteWorkflowExecutionAttributes: CompleteWorkflowExecutionCommandAttributes is not set on CompleteWorkflowExecutionCommand.", err.Error())
}

// This test unit tests the activity schedule timeout validation logic of HistoryEngine's RespondWorkflowTaskComplete function.
// A ScheduleActivityTask command and the corresponding ActivityTaskScheduledEvent have 3 timeouts: ScheduleToClose, ScheduleToStart and StartToClose.
// This test verifies that when either ScheduleToClose or ScheduleToStart and StartToClose are specified,
// HistoryEngine's validateActivityScheduleAttribute will deduce the missing timeout and fill it in
// instead of returning a BadRequest error and only when all three are missing should a BadRequest be returned.
func (s *engineSuite) TestRespondWorkflowTaskCompletedSingleActivityScheduledAttribute() {
	runTimeout := int32(100)
	testIterationVariables := []struct {
		scheduleToClose         int32
		scheduleToStart         int32
		startToClose            int32
		heartbeat               int32
		expectedScheduleToClose int32
		expectedScheduleToStart int32
		expectedStartToClose    int32
		expectWorkflowTaskFail  bool
	}{
		// No ScheduleToClose timeout, will use runTimeout
		{0, 3, 7, 0,
			runTimeout, 3, 7, false},
		// Has ScheduleToClose timeout but not ScheduleToStart or StartToClose,
		// will use ScheduleToClose for ScheduleToStart and StartToClose
		{7, 0, 0, 0,
			7, 7, 7, false},
		// Only StartToClose timeout
		{0, 0, 7, 0,
			runTimeout, runTimeout, 7, false},
		// No ScheduleToClose timeout, ScheduleToStart or StartToClose, expect error return
		{0, 0, 0, 0,
			0, 0, 0, true},
		// Negative ScheduleToClose, expect error return
		{-1, 0, 0, 0,
			0, 0, 0, true},
		// Negative ScheduleToStart, expect error return
		{0, -1, 0, 0,
			0, 0, 0, true},
		// Negative StartToClose, expect error return
		{0, 0, -1, 0,
			0, 0, 0, true},
		// Negative HeartBeat, expect error return
		{0, 0, 0, -1,
			0, 0, 0, true},
		// Use workflow timeout
		{runTimeout, 0, 0, 0,
			runTimeout, runTimeout, runTimeout, false},
		// Timeout larger than workflow timeout
		{runTimeout + 1, 0, 0, 0,
			runTimeout, runTimeout, runTimeout, false},
		{0, runTimeout + 1, 0, 0,
			0, 0, 0, true},
		{0, 0, runTimeout + 1, 0,
			runTimeout, runTimeout, runTimeout, false},
		{0, 0, 0, runTimeout + 1,
			0, 0, 0, true},
		// No ScheduleToClose timeout, will use ScheduleToStart + StartToClose, but exceed limit
		{0, runTimeout, 10, 0,
			runTimeout, runTimeout, 10, false},
	}

	for _, iVar := range testIterationVariables {
		namespaceID := tests.NamespaceID
		we := commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		}
		tl := "testTaskQueue"
		tt := &tokenspb.Task{
			Attempt:          1,
			NamespaceId:      namespaceID.String(),
			WorkflowId:       tests.WorkflowID,
			RunId:            we.GetRunId(),
			ScheduledEventId: 2,
		}
		taskToken, _ := tt.Marshal()
		identity := "testIdentity"
		input := payloads.EncodeString("input")

		ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
			tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
		addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), time.Duration(runTimeout*10)*time.Second, time.Duration(runTimeout)*time.Second, 200*time.Second, identity)
		wt := addWorkflowTaskScheduledEvent(ms)
		addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

		commands := []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             "activity1",
				ActivityType:           &commonpb.ActivityType{Name: "activity_type1"},
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
				Input:                  input,
				ScheduleToCloseTimeout: durationpb.New(time.Duration(iVar.scheduleToClose) * time.Second),
				ScheduleToStartTimeout: durationpb.New(time.Duration(iVar.scheduleToStart) * time.Second),
				StartToCloseTimeout:    durationpb.New(time.Duration(iVar.startToClose) * time.Second),
				HeartbeatTimeout:       durationpb.New(time.Duration(iVar.heartbeat) * time.Second),
			}},
		}}

		gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(ms)}
		s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)
		ms2 := workflow.TestCloneToProto(ms)
		if iVar.expectWorkflowTaskFail {
			gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}
			s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)
		}

		var updatedWorkflowMutation persistence.WorkflowMutation
		s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			updatedWorkflowMutation = request.UpdateWorkflowMutation
			return tests.UpdateWorkflowExecutionResponse, nil
		})

		_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tests.NamespaceID.String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: taskToken,
				Commands:  commands,
				Identity:  identity,
			},
		})

		if !iVar.expectWorkflowTaskFail {
			s.NoError(err)
			ms := s.getMutableState(tests.NamespaceID, &we)
			s.Equal(int64(6), ms.GetNextEventID())
			s.Equal(int64(3), ms.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
			s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms.GetExecutionState().State)
			s.False(ms.HasPendingWorkflowTask())

			activity1Attributes := s.getActivityScheduledEvent(ms, int64(5)).GetActivityTaskScheduledEventAttributes()
			s.Equal(time.Duration(iVar.expectedScheduleToClose)*time.Second, timestamp.DurationValue(activity1Attributes.GetScheduleToCloseTimeout()), iVar)
			s.Equal(time.Duration(iVar.expectedScheduleToStart)*time.Second, timestamp.DurationValue(activity1Attributes.GetScheduleToStartTimeout()), iVar)
			s.Equal(time.Duration(iVar.expectedStartToClose)*time.Second, timestamp.DurationValue(activity1Attributes.GetStartToCloseTimeout()), iVar)
		} else {
			s.Error(err)
			s.IsType(&serviceerror.InvalidArgument{}, err)
			s.True(strings.HasPrefix(err.Error(), "BadScheduleActivityAttributes"), err.Error())
			s.NotNil(updatedWorkflowMutation)
			s.Equal(int64(5), updatedWorkflowMutation.NextEventID, iVar)
			s.Equal(common.EmptyEventID, updatedWorkflowMutation.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId, iVar)
			s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, updatedWorkflowMutation.ExecutionState.State, iVar)
			s.True(updatedWorkflowMutation.ExecutionInfo.WorkflowTaskScheduledEventId != common.EmptyEventID, iVar)
		}
		s.TearDownTest()
		s.SetupTest()
	}
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedBadBinary() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ns := tests.LocalNamespaceEntry.Clone(
		namespace.WithID(uuid.New()),
		namespace.WithBadBinary("test-bad-binary"),
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(ns.ID()).Return(ns, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(ns.ID()).Return(ns, nil).AnyTimes()
	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		ns, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	var commands []*commandpb.Command

	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(ms)}
	ms2 := workflow.TestCloneToProto(ms)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)
	var updatedWorkflowMutation persistence.WorkflowMutation
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		updatedWorkflowMutation = request.UpdateWorkflowMutation
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: ns.ID().String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken:      taskToken,
			Commands:       commands,
			Identity:       identity,
			BinaryChecksum: "test-bad-binary",
		},
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("BadBinary: binary test-bad-binary is marked as bad deployment", err.Error())

	s.NotNil(updatedWorkflowMutation)
	s.Equal(int64(5), updatedWorkflowMutation.NextEventID)
	s.Equal(common.EmptyEventID, updatedWorkflowMutation.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, updatedWorkflowMutation.ExecutionState.State)
	s.True(updatedWorkflowMutation.ExecutionInfo.WorkflowTaskScheduledEventId != common.EmptyEventID)
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedSingleActivityScheduledWorkflowTask() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	input := payloads.EncodeString("input")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 90*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:             "activity1",
			ActivityType:           &commonpb.ActivityType{Name: "activity_type1"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
			Input:                  input,
			ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
			ScheduleToStartTimeout: durationpb.New(10 * time.Second),
			StartToCloseTimeout:    durationpb.New(50 * time.Second),
			HeartbeatTimeout:       durationpb.New(5 * time.Second),
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.NoError(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(6), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.False(ms2.HasPendingWorkflowTask())

	activity1Attributes := s.getActivityScheduledEvent(ms2, int64(5)).GetActivityTaskScheduledEventAttributes()
	s.Equal("activity1", activity1Attributes.ActivityId)
	s.Equal("activity_type1", activity1Attributes.ActivityType.Name)
	s.Equal(int64(4), activity1Attributes.WorkflowTaskCompletedEventId)
	s.Equal(tl, activity1Attributes.TaskQueue.Name)
	s.Equal(input, activity1Attributes.Input)
	s.Equal(90*time.Second, timestamp.DurationValue(activity1Attributes.ScheduleToCloseTimeout)) // runTimeout
	s.Equal(10*time.Second, timestamp.DurationValue(activity1Attributes.ScheduleToStartTimeout))
	s.Equal(50*time.Second, timestamp.DurationValue(activity1Attributes.StartToCloseTimeout))
	s.Equal(5*time.Second, timestamp.DurationValue(activity1Attributes.HeartbeatTimeout))
}

func (s *engineSuite) TestRespondWorkflowTaskCompleted_SignalTaskGeneration() {
	resp := s.testRespondWorkflowTaskCompletedSignalGeneration(false)
	s.NotNil(resp.GetStartedResponse())
}

func (s *engineSuite) TestRespondWorkflowTaskCompleted_SkipSignalTaskGeneration() {
	resp := s.testRespondWorkflowTaskCompletedSignalGeneration(true)
	s.Nil(resp.GetStartedResponse())
}

func (s *engineSuite) testRespondWorkflowTaskCompletedSignalGeneration(skipGenerateTask bool) *historyservice.RespondWorkflowTaskCompletedResponse {
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      tests.NamespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	signal := workflowservice.SignalWorkflowExecutionRequest{
		Namespace:                tests.NamespaceID.String(),
		WorkflowExecution:        &we,
		Identity:                 identity,
		SignalName:               "test signal name",
		Input:                    payloads.EncodeString("test input"),
		SkipGenerateWorkflowTask: skipGenerateTask,
		RequestId:                uuid.New(),
	}
	signalRequest := &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId:   tests.NamespaceID.String(),
		SignalRequest: &signal,
	}

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 90*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil).AnyTimes()

	_, err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.NoError(err)

	if !skipGenerateTask {
		s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap, nil)
		s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()
		s.mockNamespaceCache.EXPECT().GetNamespaceName(tests.NamespaceID).Return(tests.Namespace, nil)
		s.mockVisibilityMgr.EXPECT().GetIndexName().Return(esIndexName).AnyTimes()
		s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{HistoryEvents: []*historypb.HistoryEvent{}}, nil)
	}

	var commands []*commandpb.Command
	resp, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken:             taskToken,
			Commands:              commands,
			Identity:              identity,
			ReturnNewWorkflowTask: true,
		},
	})
	s.NoError(err)
	s.NotNil(resp)

	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms.GetExecutionState().State)

	return resp
}

func (s *engineSuite) TestRespondWorkflowTaskCompleted_ActivityEagerExecution_NotCancelled() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	input := payloads.EncodeString("input")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 90*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	scheduleToCloseTimeout := durationpb.New(90 * time.Second)
	scheduleToStartTimeout := durationpb.New(10 * time.Second)
	startToCloseTimeout := durationpb.New(50 * time.Second)
	heartbeatTimeout := durationpb.New(5 * time.Second)
	commands := []*commandpb.Command{
		{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             "activity1",
				ActivityType:           &commonpb.ActivityType{Name: "activity_type1"},
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
				Input:                  input,
				ScheduleToCloseTimeout: scheduleToCloseTimeout,
				ScheduleToStartTimeout: scheduleToStartTimeout,
				StartToCloseTimeout:    startToCloseTimeout,
				HeartbeatTimeout:       heartbeatTimeout,
				RequestEagerExecution:  false,
			}},
		},
		{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             "activity2",
				ActivityType:           &commonpb.ActivityType{Name: "activity_type2"},
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
				Input:                  input,
				ScheduleToCloseTimeout: scheduleToCloseTimeout,
				ScheduleToStartTimeout: scheduleToStartTimeout,
				StartToCloseTimeout:    startToCloseTimeout,
				HeartbeatTimeout:       heartbeatTimeout,
				RequestEagerExecution:  true,
			}},
		},
	}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	resp, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.NoError(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(7), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.False(ms2.HasPendingWorkflowTask())

	ai1, ok := ms2.GetActivityByActivityID("activity1")
	s.True(ok)
	s.Equal(common.EmptyEventID, ai1.StartedEventId)

	ai2, ok := ms2.GetActivityByActivityID("activity2")
	s.True(ok)
	s.Equal(common.TransientEventID, ai2.StartedEventId)
	s.NotZero(ai2.StartedTime)

	scheduledEvent := s.getActivityScheduledEvent(ms2, ai2.ScheduledEventId)

	s.Len(resp.ActivityTasks, 1)
	activityTask := resp.ActivityTasks[0]
	s.Equal("activity2", activityTask.ActivityId)
	s.Equal("activity_type2", activityTask.ActivityType.GetName())
	s.Equal(input, activityTask.Input)
	protorequire.ProtoEqual(s.T(), &we, activityTask.WorkflowExecution)
	s.Equal(scheduledEvent.EventTime, activityTask.CurrentAttemptScheduledTime)
	s.Equal(scheduledEvent.EventTime, activityTask.ScheduledTime)
	s.Equal(scheduleToCloseTimeout.AsDuration(), activityTask.ScheduleToCloseTimeout.AsDuration())
	s.ProtoEqual(startToCloseTimeout, activityTask.StartToCloseTimeout)
	s.ProtoEqual(heartbeatTimeout, activityTask.HeartbeatTimeout)
	s.Equal(int32(1), activityTask.Attempt)
	s.Nil(activityTask.HeartbeatDetails)
	s.Equal(tests.LocalNamespaceEntry.Name().String(), activityTask.WorkflowNamespace)
}

func (s *engineSuite) TestRespondWorkflowTaskCompleted_ActivityEagerExecution_Cancelled() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	input := payloads.EncodeString("input")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 90*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	scheduleToCloseTimeout := durationpb.New(90 * time.Second)
	scheduleToStartTimeout := durationpb.New(10 * time.Second)
	startToCloseTimeout := durationpb.New(50 * time.Second)
	heartbeatTimeout := durationpb.New(5 * time.Second)
	commands := []*commandpb.Command{
		{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             "activity1",
				ActivityType:           &commonpb.ActivityType{Name: "activity_type1"},
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
				Input:                  input,
				ScheduleToCloseTimeout: scheduleToCloseTimeout,
				ScheduleToStartTimeout: scheduleToStartTimeout,
				StartToCloseTimeout:    startToCloseTimeout,
				HeartbeatTimeout:       heartbeatTimeout,
				RequestEagerExecution:  true,
			}},
		},
		{
			CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
			Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
				ScheduledEventId: 5,
			}},
		},
	}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap, nil)
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(tests.NamespaceID).Return(tests.Namespace, nil)
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return(esIndexName).AnyTimes()
	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{HistoryEvents: []*historypb.HistoryEvent{}}, nil)

	resp, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken:             taskToken,
			Commands:              commands,
			Identity:              identity,
			ReturnNewWorkflowTask: true,
		},
	})
	s.NoError(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(10), ms2.GetNextEventID()) // activity scheduled, request cancel, cancelled, workflow task scheduled, started
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.True(ms2.HasPendingWorkflowTask())

	_, ok := ms2.GetActivityByActivityID("activity1")
	s.False(ok)

	s.Len(resp.ActivityTasks, 0)
	s.NotNil(resp.StartedResponse)
	s.Equal(int64(10), resp.StartedResponse.NextEventId)
	s.Equal(int64(3), resp.StartedResponse.PreviousStartedEventId)
}

func (s *engineSuite) TestRespondWorkflowTaskCompleted_ActivityEagerExecution_WorkflowClosed() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	input := payloads.EncodeString("input")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 90*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	scheduleToCloseTimeout := durationpb.New(90 * time.Second)
	scheduleToStartTimeout := durationpb.New(10 * time.Second)
	startToCloseTimeout := durationpb.New(50 * time.Second)
	heartbeatTimeout := durationpb.New(5 * time.Second)
	commands := []*commandpb.Command{
		{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             "activity1",
				ActivityType:           &commonpb.ActivityType{Name: "activity_type1"},
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
				Input:                  input,
				ScheduleToCloseTimeout: scheduleToCloseTimeout,
				ScheduleToStartTimeout: scheduleToStartTimeout,
				StartToCloseTimeout:    startToCloseTimeout,
				HeartbeatTimeout:       heartbeatTimeout,
				RequestEagerExecution:  true,
			}},
		},
		{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("complete"),
			}},
		},
	}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	resp, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken:             taskToken,
			Commands:              commands,
			Identity:              identity,
			ReturnNewWorkflowTask: true,
		},
	})
	s.NoError(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(7), ms2.GetNextEventID()) // activity scheduled, workflow completed
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, ms2.GetExecutionState().State)
	s.False(ms2.HasPendingWorkflowTask())

	activityInfo, ok := ms2.GetActivityByActivityID("activity1")
	s.True(ok)
	s.Equal(int64(5), activityInfo.ScheduledEventId)          // activity scheduled
	s.Equal(common.EmptyEventID, activityInfo.StartedEventId) // activity not started

	s.Len(resp.ActivityTasks, 0)
	s.Nil(resp.StartedResponse)
}

func (s *engineSuite) TestRespondWorkflowTaskCompleted_WorkflowTaskHeartbeatTimeout() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	ms.GetExecutionInfo().WorkflowTaskOriginalScheduledTime = timestamppb.New(time.Now().UTC().Add(-time.Hour))

	var commands []*commandpb.Command

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			ForceCreateNewWorkflowTask: true,
			TaskToken:                  taskToken,
			Commands:                   commands,
			Identity:                   identity,
		},
	})
	s.Error(err, "workflow task heartbeat timeout")
}

func (s *engineSuite) TestRespondWorkflowTaskCompleted_WorkflowTaskHeartbeatNotTimeout() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	ms.GetExecutionInfo().WorkflowTaskOriginalScheduledTime = timestamppb.New(time.Now().UTC().Add(-time.Minute))

	var commands []*commandpb.Command

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			ForceCreateNewWorkflowTask: true,
			TaskToken:                  taskToken,
			Commands:                   commands,
			Identity:                   identity,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRespondWorkflowTaskCompleted_WorkflowTaskHeartbeatNotTimeout_ZeroOrignalScheduledTime() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	ms.GetExecutionInfo().WorkflowTaskOriginalScheduledTime = nil

	var commands []*commandpb.Command

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			ForceCreateNewWorkflowTask: true,
			TaskToken:                  taskToken,
			Commands:                   commands,
			Identity:                   identity,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedCompleteWorkflowSuccess() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	workflowResult := payloads.EncodeString("success")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
			Result: workflowResult,
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.NoError(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(6), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, ms2.GetExecutionState().State)
	s.False(ms2.HasPendingWorkflowTask())
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedFailWorkflowSuccess() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	reason := "fail workflow reason"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
			Failure: failure.NewServerFailure(reason, false),
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.NoError(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(6), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, ms2.GetExecutionState().State)
	s.False(ms2.HasPendingWorkflowTask())
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedSignalExternalWorkflowSuccess() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
			Namespace: tests.Namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: "signal",
			Input:      payloads.EncodeString("test input"),
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.NoError(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(6), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedStartChildWorkflowWithAbandonPolicy() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	abandon := enumspb.PARENT_CLOSE_POLICY_ABANDON
	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
			Namespace:  tests.Namespace.String(),
			WorkflowId: "child-workflow-id",
			WorkflowType: &commonpb.WorkflowType{
				Name: "child-workflow-type",
			},
			ParentClosePolicy: abandon,
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().
		GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.NoError(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(6), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(1, len(ms2.GetPendingChildExecutionInfos()))
	var childID int64
	for c := range ms2.GetPendingChildExecutionInfos() {
		childID = c
		break
	}
	s.Equal("child-workflow-id", ms2.GetPendingChildExecutionInfos()[childID].StartedWorkflowId)
	s.Equal(enumspb.PARENT_CLOSE_POLICY_ABANDON, ms2.GetPendingChildExecutionInfos()[childID].ParentClosePolicy)
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedStartChildWorkflowWithTerminatePolicy() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	terminate := enumspb.PARENT_CLOSE_POLICY_TERMINATE
	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
			Namespace:  tests.Namespace.String(),
			WorkflowId: "child-workflow-id",
			WorkflowType: &commonpb.WorkflowType{
				Name: "child-workflow-type",
			},
			ParentClosePolicy: terminate,
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().
		GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.NoError(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(6), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(1, len(ms2.GetPendingChildExecutionInfos()))
	var childID int64
	for c := range ms2.GetPendingChildExecutionInfos() {
		childID = c
		break
	}
	s.Equal("child-workflow-id", ms2.GetPendingChildExecutionInfos()[childID].StartedWorkflowId)
	s.Equal(enumspb.PARENT_CLOSE_POLICY_TERMINATE, ms2.GetPendingChildExecutionInfos()[childID].ParentClosePolicy)
}

func (s *engineSuite) TestRespondWorkflowTaskCompletedSignalExternalWorkflowFailed_UnKnownNamespace() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	foreignNamespace := namespace.Name("unknown namespace")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
			Namespace: foreignNamespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: "signal",
			Input:      payloads.EncodeString("test input"),
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockNamespaceCache.EXPECT().GetNamespace(foreignNamespace).Return(
		nil, errors.New("get foreign namespace error"),
	)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})

	s.NotNil(err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedInvalidToken() {

	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	_, err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: invalidToken,
			Result:    nil,
			Identity:  identity,
		},
	})

	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoExecution() {
	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	_, err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoRunID() {
	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	_, err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfGetExecutionFailed() {
	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("FAILED"))

	_, err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoAIdProvided() {
	namespaceID := tests.NamespaceID
	execution := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: common.EmptyEventID,
	}
	taskToken, _ := tt.Marshal()

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.EqualError(err, "activityID cannot be empty")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNotFound() {
	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       "aid",
	}
	taskToken, _ := tt.Marshal()
	execution := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.Error(err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedUpdateExecutionFailed() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	activityResult := payloads.EncodeString("activity result")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, errors.New("FAILED"))
	s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil).AnyTimes() // might be called in background goroutine

	_, err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfTaskCompleted() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	activityResult := payloads.EncodeString("activity result")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activityStartedEvent := addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(ms, activityScheduledEvent.EventId, activityStartedEvent.EventId,
		activityResult, identity)
	addWorkflowTaskScheduledEvent(ms)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfTaskNotStarted() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	activityResult := payloads.EncodeString("activity result")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedConflictOnUpdate() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	activityResult := payloads.EncodeString("activity result")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, &persistence.ConditionFailedError{})

	_, err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	s.Equal(&persistence.ConditionFailedError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedSuccess() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	activityResult := payloads.EncodeString("activity result")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	s.NoError(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(9), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	s.True(ms2.HasPendingWorkflowTask())
	wt = ms2.GetWorkflowTaskByID(int64(8))
	s.NotNil(wt)
	s.EqualValues(int64(100), wt.WorkflowTaskTimeout.Seconds())
	s.Equal(int64(8), wt.ScheduledEventID)
	s.Equal(common.EmptyEventID, wt.StartedEventID)
}

func (s *engineSuite) TestRespondActivityTaskCompletedByIdSuccess() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"

	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	activityResult := payloads.EncodeString("activity result")
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       activityID,
	}
	taskToken, _ := tt.Marshal()

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	workflowTaskScheduledEvent := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, workflowTaskScheduledEvent.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, workflowTaskScheduledEvent.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: we.RunId}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	s.NoError(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(9), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	s.True(ms2.HasPendingWorkflowTask())
	wt := ms2.GetWorkflowTaskByID(int64(8))
	s.NotNil(wt)
	s.EqualValues(int64(100), wt.WorkflowTaskTimeout.Seconds())
	s.Equal(int64(8), wt.ScheduledEventID)
	s.Equal(common.EmptyEventID, wt.StartedEventID)
}

func (s *engineSuite) TestRespondActivityTaskFailedInvalidToken() {

	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	_, err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: invalidToken,
			Identity:  identity,
		},
	})

	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfNoExecution() {
	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil,
		serviceerror.NewNotFound(""))

	_, err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfNoRunID() {
	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil,
		serviceerror.NewNotFound(""))

	_, err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfGetExecutionFailed() {
	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil,
		errors.New("FAILED"))

	_, err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskFailededIfNoAIdProvided() {
	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: common.EmptyEventID,
	}
	taskToken, _ := tt.Marshal()
	execution := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.EqualError(err, "activityID cannot be empty")
}

func (s *engineSuite) TestRespondActivityTaskFailededIfNotFound() {
	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       "aid",
	}
	taskToken, _ := tt.Marshal()
	execution := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.Error(err)
}

func (s *engineSuite) TestRespondActivityTaskFailedUpdateExecutionFailed() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, errors.New("FAILED"))
	s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil).AnyTimes() // might be called in background goroutine

	_, err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskFailedIfTaskCompleted() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	failure := failure.NewServerFailure("fail reason", true)

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activityStartedEvent := addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	addActivityTaskFailedEvent(ms, activityScheduledEvent.EventId, activityStartedEvent.EventId, failure, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, identity)
	addWorkflowTaskScheduledEvent(ms)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Failure:   failure,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfTaskNotStarted() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedConflictOnUpdate() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, &persistence.ConditionFailedError{})

	_, err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.Equal(&persistence.ConditionFailedError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedSuccess() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	failure := failure.NewServerFailure("failed", false)

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Failure:   failure,
			Identity:  identity,
		},
	})
	s.Nil(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(9), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	s.True(ms2.HasPendingWorkflowTask())
	wt = ms2.GetWorkflowTaskByID(int64(8))
	s.NotNil(wt)
	s.EqualValues(int64(100), wt.WorkflowTaskTimeout.Seconds())
	s.Equal(int64(8), wt.ScheduledEventID)
	s.Equal(common.EmptyEventID, wt.StartedEventID)
}

func (s *engineSuite) TestRespondActivityTaskFailedWithHeartbeatSuccess() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	failure := failure.NewServerFailure("failed", false)

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, activityInfo := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ActivityInfos[activityInfo.ScheduledEventId] = activityInfo
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	details := payloads.EncodeString("details")

	s.Nil(activityInfo.GetLastHeartbeatDetails())

	_, err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken:            taskToken,
			Failure:              failure,
			Identity:             identity,
			LastHeartbeatDetails: details,
		},
	})
	s.Nil(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(9), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	s.True(ms2.HasPendingWorkflowTask())
	wt = ms2.GetWorkflowTaskByID(int64(8))
	s.NotNil(wt)
	s.EqualValues(int64(100), wt.WorkflowTaskTimeout.Seconds())
	s.Equal(int64(8), wt.ScheduledEventID)
	s.Equal(common.EmptyEventID, wt.StartedEventID)

	s.NotNil(activityInfo.GetLastHeartbeatDetails())
}

func (s *engineSuite) TestRespondActivityTaskFailedByIdSuccess() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"

	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	failure := failure.NewServerFailure("failed", false)
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       activityID,
	}
	taskToken, _ := tt.Marshal()

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	workflowTaskScheduledEvent := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, workflowTaskScheduledEvent.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, workflowTaskScheduledEvent.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: we.RunId}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Failure:   failure,
			Identity:  identity,
		},
	})
	s.Nil(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(9), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	s.True(ms2.HasPendingWorkflowTask())
	wt := ms2.GetWorkflowTaskByID(int64(8))
	s.NotNil(wt)
	s.EqualValues(int64(100), wt.WorkflowTaskTimeout.Seconds())
	s.Equal(int64(8), wt.ScheduledEventID)
	s.Equal(common.EmptyEventID, wt.StartedEventID)
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatSuccess_NoTimer() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 0*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	// No HeartBeat timer running.
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	detais := payloads.EncodeString("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId: tests.NamespaceID.String(),
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   detais,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatSuccess_TimerRunning() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 1*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	// HeartBeat timer running.
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	detais := payloads.EncodeString("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId: tests.NamespaceID.String(),
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   detais,
		},
	})
	s.Nil(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(7), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.False(ms2.HasPendingWorkflowTask())
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatByIDSuccess() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       activityID,
	}
	taskToken, _ := tt.Marshal()

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 0*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	// No HeartBeat timer running.
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	detais := payloads.EncodeString("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId: tests.NamespaceID.String(),
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   detais,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRespondActivityTaskCanceled_Scheduled() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 1*time.Second)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCanceled_Started() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 1*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	_, _, err := ms.AddActivityTaskCancelRequestedEvent(workflowTaskCompletedEvent.EventId, activityScheduledEvent.EventId, identity)
	s.Nil(err)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	s.Nil(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(10), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	s.True(ms2.HasPendingWorkflowTask())
	wt = ms2.GetWorkflowTaskByID(int64(9))
	s.NotNil(wt)
	s.EqualValues(int64(100), wt.WorkflowTaskTimeout.Seconds())
	s.Equal(int64(9), wt.ScheduledEventID)
	s.Equal(common.EmptyEventID, wt.StartedEventID)
}

func (s *engineSuite) TestRespondActivityTaskCanceledById_Started() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       activityID,
	}
	taskToken, _ := tt.Marshal()

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	workflowTaskScheduledEvent := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, workflowTaskScheduledEvent.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, workflowTaskScheduledEvent.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 1*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	_, _, err := ms.AddActivityTaskCancelRequestedEvent(workflowTaskCompletedEvent.EventId, activityScheduledEvent.EventId, identity)
	s.Nil(err)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: we.RunId}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	s.Nil(err)
	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(10), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	s.True(ms2.HasPendingWorkflowTask())
	wt := ms2.GetWorkflowTaskByID(int64(9))
	s.NotNil(wt)
	s.EqualValues(int64(100), wt.WorkflowTaskTimeout.Seconds())
	s.Equal(int64(9), wt.ScheduledEventID)
	s.Equal(common.EmptyEventID, wt.StartedEventID)
}

func (s *engineSuite) TestRespondActivityTaskCanceledIfNoRunID() {
	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	_, err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCanceledIfNoAIdProvided() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-respond-activity-task-canceled-if-no-activity-id-provided",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"

	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: common.EmptyEventID,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, workflowExecution.WorkflowId, workflowExecution.RunId, log.NewTestLogger())
	// Add dummy event
	addWorkflowExecutionStartedEvent(ms, &workflowExecution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.EqualError(err, "activityID cannot be empty")
}

func (s *engineSuite) TestRespondActivityTaskCanceledIfNotFound() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-respond-activity-task-canceled-if-not-found",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"

	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       "aid",
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, workflowExecution.WorkflowId, workflowExecution.RunId, log.NewTestLogger())
	// Add dummy event
	addWorkflowExecutionStartedEvent(ms, &workflowExecution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.Error(err)
}

func (s *engineSuite) TestRequestCancel_RespondWorkflowTaskCompleted_NotScheduled() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityScheduledEventID := int64(99)

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
			ScheduledEventId: activityScheduledEventID,
		}},
	}}

	ms1 := workflow.TestCloneToProto(ms)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}
	ms2 := workflow.TestCloneToProto(ms)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)
	var updatedWorkflowMutation persistence.WorkflowMutation
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		updatedWorkflowMutation = request.UpdateWorkflowMutation
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("BadRequestCancelActivityAttributes: invalid history builder state for action: add-activitytask-cancel-requested-event, ScheduledEventID: 99", err.Error())
	s.NotNil(updatedWorkflowMutation)
	s.Equal(int64(5), updatedWorkflowMutation.NextEventID)
	s.Equal(common.EmptyEventID, updatedWorkflowMutation.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, updatedWorkflowMutation.ExecutionState.State)
	s.True(updatedWorkflowMutation.ExecutionInfo.WorkflowTaskScheduledEventId != common.EmptyEventID)
}

func (s *engineSuite) TestRequestCancel_RespondWorkflowTaskCompleted_Scheduled() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 6,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	_, aInfo := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 1*time.Second)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
			ScheduledEventId: aInfo.ScheduledEventId,
		}},
	}}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Nil(err)

	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(12), ms2.GetNextEventID())
	s.Equal(int64(7), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.True(ms2.HasPendingWorkflowTask())
	wt2 = ms2.GetWorkflowTaskByID(ms2.GetNextEventID() - 1)
	s.NotNil(wt2)
	s.Equal(ms2.GetNextEventID()-1, wt2.ScheduledEventID)
	s.Equal(int32(1), wt2.Attempt)
}

func (s *engineSuite) TestRequestCancel_RespondWorkflowTaskCompleted_Started() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 7,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 0*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
			ScheduledEventId: activityScheduledEvent.GetEventId(),
		}},
	}}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Nil(err)

	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(11), ms2.GetNextEventID())
	s.Equal(int64(8), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.False(ms2.HasPendingWorkflowTask())
}

func (s *engineSuite) TestRequestCancel_RespondWorkflowTaskCompleted_Completed() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 6,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	workflowResult := payloads.EncodeString("workflow result")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	_, aInfo := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 0*time.Second)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{
		{
			CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
			Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
				ScheduledEventId: aInfo.ScheduledEventId,
			}},
		},
		{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: workflowResult,
			}},
		},
	}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Nil(err)

	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(11), ms2.GetNextEventID())
	s.Equal(int64(7), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, ms2.GetExecutionState().State)
	s.False(ms2.HasPendingWorkflowTask())
}

func (s *engineSuite) TestRequestCancel_RespondWorkflowTaskCompleted_NoHeartBeat() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 7,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 0*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
			ScheduledEventId: activityScheduledEvent.GetEventId(),
		}},
	}}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Nil(err)

	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(11), ms2.GetNextEventID())
	s.Equal(int64(8), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.False(ms2.HasPendingWorkflowTask())

	// Try recording activity heartbeat
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	att := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 5,
	}
	activityTaskToken, _ := att.Marshal()

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId: tests.NamespaceID.String(),
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	s.Nil(err)
	s.NotNil(hbResponse)
	s.True(hbResponse.CancelRequested)

	// Try cancelling the request.
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	s.Nil(err)

	ms2 = s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(13), ms2.GetNextEventID())
	s.Equal(int64(8), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.True(ms2.HasPendingWorkflowTask())
}

func (s *engineSuite) TestRequestCancel_RespondWorkflowTaskCompleted_Success() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 7,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 1*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
			ScheduledEventId: activityScheduledEvent.GetEventId(),
		}},
	}}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Nil(err)

	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(11), ms2.GetNextEventID())
	s.Equal(int64(8), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.False(ms2.HasPendingWorkflowTask())

	// Try recording activity heartbeat
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	att := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 5,
	}
	activityTaskToken, _ := att.Marshal()

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId: tests.NamespaceID.String(),
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	s.Nil(err)
	s.NotNil(hbResponse)
	s.True(hbResponse.CancelRequested)

	// Try cancelling the request.
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	s.Nil(err)

	ms2 = s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(13), ms2.GetNextEventID())
	s.Equal(int64(8), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.True(ms2.HasPendingWorkflowTask())
}

func (s *engineSuite) TestRequestCancel_RespondWorkflowTaskCompleted_SuccessWithQueries() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 7,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 1*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
			ScheduledEventId: activityScheduledEvent.GetEventId(),
		}},
	}}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	// load mutable state such that it already exists in memory when respond workflow task is called
	// this enables us to set query registry on it
	ctx, release, err := s.workflowCache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		tests.NamespaceID,
		&we,
		locks.PriorityHigh,
	)
	s.NoError(err)
	loadedMS, err := ctx.LoadMutableState(context.Background(), s.mockShard)
	s.NoError(err)
	qr := workflow.NewQueryRegistry()
	id1, _ := qr.BufferQuery(&querypb.WorkflowQuery{})
	id2, _ := qr.BufferQuery(&querypb.WorkflowQuery{})
	id3, _ := qr.BufferQuery(&querypb.WorkflowQuery{})
	loadedMS.(*workflow.MutableStateImpl).QueryRegistry = qr
	release(nil)
	result1 := &querypb.WorkflowQueryResult{
		ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
		Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
	}
	result2 := &querypb.WorkflowQueryResult{
		ResultType:   enumspb.QUERY_RESULT_TYPE_FAILED,
		ErrorMessage: "error reason",
	}
	queryResults := map[string]*querypb.WorkflowQueryResult{
		id1: result1,
		id2: result2,
	}
	_, err = s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken:    taskToken,
			Commands:     commands,
			Identity:     identity,
			QueryResults: queryResults,
		},
	})
	s.Nil(err)

	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(11), ms2.GetNextEventID())
	s.Equal(int64(8), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.False(ms2.HasPendingWorkflowTask())
	s.Len(qr.GetCompletedIDs(), 2)
	succeeded1, err := qr.GetCompletionState(id1)
	s.NoError(err)
	s.EqualValues(succeeded1.Result, result1)
	s.Equal(workflow.QueryCompletionTypeSucceeded, succeeded1.Type)
	succeeded2, err := qr.GetCompletionState(id2)
	s.NoError(err)
	s.EqualValues(succeeded2.Result, result2)
	s.Equal(workflow.QueryCompletionTypeSucceeded, succeeded2.Type)
	s.Len(qr.GetBufferedIDs(), 0)
	s.Len(qr.GetFailedIDs(), 0)
	s.Len(qr.GetUnblockedIDs(), 1)
	unblocked1, err := qr.GetCompletionState(id3)
	s.NoError(err)
	s.Nil(unblocked1.Result)
	s.Equal(workflow.QueryCompletionTypeUnblocked, unblocked1.Type)

	// Try recording activity heartbeat
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	att := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 5,
	}
	activityTaskToken, _ := att.Marshal()

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId: tests.NamespaceID.String(),
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	s.Nil(err)
	s.NotNil(hbResponse)
	s.True(hbResponse.CancelRequested)

	// Try cancelling the request.
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	s.Nil(err)

	ms2 = s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(13), ms2.GetNextEventID())
	s.Equal(int64(8), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.True(ms2.HasPendingWorkflowTask())
}

func (s *engineSuite) TestStarTimer_DuplicateTimerID() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	timerID := "t1"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())

	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_START_TIMER,
		Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
			TimerId:            timerID,
			StartToFireTimeout: durationpb.New(1 * time.Second),
		}},
	}}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Nil(err)

	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	// Try to add the same timer ID again.
	wt2 := addWorkflowTaskScheduledEvent(ms2)
	addWorkflowTaskStartedEvent(ms2, wt2.ScheduledEventID, tl, identity)
	tt2 := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: wt2.ScheduledEventID,
	}
	taskToken2, _ := tt2.Marshal()

	wfMs2 := workflow.TestCloneToProto(ms2)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: wfMs2}

	workflowTaskFailedEvent := false
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)
	var updatedWorkflowMutation persistence.WorkflowMutation
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		for _, newEvents := range request.UpdateWorkflowEvents {
			decTaskIndex := len(newEvents.Events) - 1
			if decTaskIndex >= 0 && newEvents.Events[decTaskIndex].EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
				workflowTaskFailedEvent = true
			}
		}
		updatedWorkflowMutation = request.UpdateWorkflowMutation
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	_, err = s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken2,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("StartTimerDuplicateId: invalid history builder state for action: add-timer-started-event, TimerID: t1", err.Error())

	s.True(workflowTaskFailedEvent)

	s.NotNil(updatedWorkflowMutation)
	s.Equal(int64(9), updatedWorkflowMutation.NextEventID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, updatedWorkflowMutation.ExecutionState.State)
	s.Equal(updatedWorkflowMutation.NextEventID, updatedWorkflowMutation.ExecutionInfo.WorkflowTaskScheduledEventId)
	s.Equal(int32(2), updatedWorkflowMutation.ExecutionInfo.WorkflowTaskAttempt)
}

func (s *engineSuite) TestUserTimer_RespondWorkflowTaskCompleted() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 6,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	timerID := "t1"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	addTimerStartedEvent(ms, workflowTaskCompletedEvent.EventId, timerID, 10*time.Second)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER,
		Attributes: &commandpb.Command_CancelTimerCommandAttributes{CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
			TimerId: timerID,
		}},
	}}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Nil(err)

	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(10), ms2.GetNextEventID())
	s.Equal(int64(7), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.False(ms2.HasPendingWorkflowTask())
}

func (s *engineSuite) TestCancelTimer_RespondWorkflowTaskCompleted_NoStartTimer() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	timerID := "t1"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	ms2 := workflow.TestCloneToProto(ms)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER,
		Attributes: &commandpb.Command_CancelTimerCommandAttributes{CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
			TimerId: timerID,
		}},
	}}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)
	var updatedWorkflowMutation persistence.WorkflowMutation
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		updatedWorkflowMutation = request.UpdateWorkflowMutation
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	_, err := s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("BadCancelTimerAttributes: invalid history builder state for action: add-timer-canceled-event, TimerID: t1", err.Error())

	s.NotNil(updatedWorkflowMutation)
	s.Equal(int64(5), updatedWorkflowMutation.NextEventID)
	s.Equal(common.EmptyEventID, updatedWorkflowMutation.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, updatedWorkflowMutation.ExecutionState.State)
	s.True(updatedWorkflowMutation.ExecutionInfo.WorkflowTaskScheduledEventId != common.EmptyEventID)
}

func (s *engineSuite) TestCancelTimer_RespondWorkflowTaskCompleted_TimerFired() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 6,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	timerID := "t1"

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	addTimerStartedEvent(ms, workflowTaskCompletedEvent.EventId, timerID, 10*time.Second)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)
	addTimerFiredEvent(ms, timerID)
	_, _, err := ms.CloseTransactionAsMutation(workflow.TransactionPolicyActive)
	s.Nil(err)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.True(len(gwmsResponse.State.BufferedEvents) > 0)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER,
		Attributes: &commandpb.Command_CancelTimerCommandAttributes{CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
			TimerId: timerID,
		}},
	}}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			s.True(request.UpdateWorkflowMutation.ClearBufferedEvents)
			return tests.UpdateWorkflowExecutionResponse, nil
		})

	_, err = s.mockHistoryEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Nil(err)

	ms2 := s.getMutableState(tests.NamespaceID, &we)
	s.Equal(int64(10), ms2.GetNextEventID())
	s.Equal(int64(7), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.False(ms2.HasPendingWorkflowTask())
	s.False(ms2.HasBufferedEvents())
}

func (s *engineSuite) TestSignalWorkflowExecution() {
	signalRequest := &historyservice.SignalWorkflowExecutionRequest{}
	_, err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "Missing namespace UUID.")

	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	signalRequest = &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         tests.NamespaceID.String(),
			WorkflowExecution: &we,
			Identity:          identity,
			SignalName:        signalName,
			Input:             input,
		},
	}

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionInfo.NamespaceId = tests.NamespaceID.String()
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.Nil(err)
}

// Test signal workflow task by adding request ID
func (s *engineSuite) TestSignalWorkflowExecution_DuplicateRequest() {
	signalRequest := &historyservice.SignalWorkflowExecutionRequest{}
	_, err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "Missing namespace UUID.")

	we := commonpb.WorkflowExecution{
		WorkflowId: "wId2",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name 2"
	input := payloads.EncodeString("test input 2")
	requestID := uuid.New()
	signalRequest = &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         tests.NamespaceID.String(),
			WorkflowExecution: &we,
			Identity:          identity,
			SignalName:        signalName,
			Input:             input,
			RequestId:         requestID,
		},
	}

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	// assume duplicate request id
	wfMs.SignalRequestedIds = []string{requestID}
	wfMs.ExecutionInfo.NamespaceId = tests.NamespaceID.String()
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err = s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.Nil(err)
}

// Test signal workflow task by dedup request ID & workflow finished
func (s *engineSuite) TestSignalWorkflowExecution_DuplicateRequest_Completed() {
	signalRequest := &historyservice.SignalWorkflowExecutionRequest{}
	_, err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "Missing namespace UUID.")

	we := commonpb.WorkflowExecution{
		WorkflowId: "wId2",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name 2"
	input := payloads.EncodeString("test input 2")
	requestID := uuid.New()
	signalRequest = &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         tests.NamespaceID.String(),
			WorkflowExecution: &we,
			Identity:          identity,
			SignalName:        signalName,
			Input:             input,
			RequestId:         requestID,
		},
	}

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	// assume duplicate request id
	wfMs.SignalRequestedIds = []string{requestID}
	wfMs.ExecutionInfo.NamespaceId = tests.NamespaceID.String()
	wfMs.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err = s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.Nil(err)
}

func (s *engineSuite) TestSignalWorkflowExecution_Failed() {
	signalRequest := &historyservice.SignalWorkflowExecutionRequest{}
	_, err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "Missing namespace UUID.")

	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	signalRequest = &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         tests.NamespaceID.String(),
			WorkflowExecution: we,
			Identity:          identity,
			SignalName:        signalName,
			Input:             input,
		},
	}

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, we, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err = s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "workflow execution already completed")
}

func (s *engineSuite) TestSignalWorkflowExecution_WorkflowTaskBackoff() {
	signalRequest := &historyservice.SignalWorkflowExecutionRequest{}
	_, err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "Missing namespace UUID.")

	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	signalInput := payloads.EncodeString("test input")
	signalRequest = &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         tests.NamespaceID.String(),
			WorkflowExecution: &we,
			Identity:          identity,
			SignalName:        signalName,
			Input:             signalInput,
		},
	}

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:               we.WorkflowId,
		WorkflowType:             &commonpb.WorkflowType{Name: "wType"},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: taskqueue},
		Input:                    payloads.EncodeString("input"),
		WorkflowExecutionTimeout: durationpb.New(100 * time.Second),
		WorkflowRunTimeout:       durationpb.New(50 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(200 * time.Second),
		Identity:                 identity,
	}

	_, err = ms.AddWorkflowExecutionStartedEvent(
		&we,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:                  1,
			NamespaceId:              tests.NamespaceID.String(),
			StartRequest:             startRequest,
			ContinueAsNewInitiator:   enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY,
			FirstWorkflowTaskBackoff: durationpb.New(time.Second * 10),
		},
	)
	s.NoError(err)

	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionInfo.NamespaceId = tests.NamespaceID.String()
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		s.Len(request.UpdateWorkflowEvents[0].Events, 1) // no workflow task scheduled event
		// s.Empty(request.UpdateWorkflowMutation.Tasks[tasks.CategoryTransfer]) // no workflow transfer task
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	_, err = s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.Nil(err)
}

func (s *engineSuite) TestRemoveSignalMutableState() {
	removeRequest := &historyservice.RemoveSignalMutableStateRequest{}
	_, err := s.mockHistoryEngine.RemoveSignalMutableState(context.Background(), removeRequest)
	s.EqualError(err, "Missing namespace UUID.")

	execution := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	requestID := uuid.New()
	removeRequest = &historyservice.RemoveSignalMutableStateRequest{
		NamespaceId:       tests.NamespaceID.String(),
		WorkflowExecution: &execution,
		RequestId:         requestID,
	}

	ms := workflow.TestLocalMutableState(s.mockHistoryEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionInfo.NamespaceId = tests.NamespaceID.String()
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = s.mockHistoryEngine.RemoveSignalMutableState(context.Background(), removeRequest)
	s.Nil(err)
}

func (s *engineSuite) TestReapplyEvents_ReturnSuccess() {
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-reapply",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	eventVersion := int64(100)
	history := []*historypb.HistoryEvent{
		{
			EventId:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			Version:   eventVersion,
		},
	}
	globalNamespaceID := uuid.New()
	globalNamespaceName := "global-namespace-name"
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: globalNamespaceID, Name: globalNamespaceName},
		&persistencespb.NamespaceConfig{},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		tests.Version,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(namespaceEntry.Name()).Return(namespaceEntry, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, eventVersion).Return(cluster.TestAlternativeClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()

	ms := workflow.TestGlobalMutableState(
		s.mockHistoryEngine.shardContext,
		s.eventsCache,
		log.NewTestLogger(),
		namespaceEntry.FailoverVersion(),
		workflowExecution.GetWorkflowId(),
		workflowExecution.GetRunId(),
	)
	// Add dummy event
	addWorkflowExecutionStartedEvent(ms, &workflowExecution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}
	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockEventsReapplier.EXPECT().ReapplyEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any())

	err := s.mockHistoryEngine.ReapplyEvents(
		context.Background(),
		namespaceEntry.ID(),
		workflowExecution.GetWorkflowId(),
		workflowExecution.GetRunId(),
		history,
	)
	s.NoError(err)
}

func (s *engineSuite) TestReapplyEvents_IgnoreSameClusterEvents() {
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-reapply-same-cluster",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	// TODO: Figure out why version is empty?
	history := []*historypb.HistoryEvent{
		{
			EventId:   1,
			EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
			Version:   common.EmptyVersion,
		},
	}
	ms := workflow.TestLocalMutableState(
		s.mockHistoryEngine.shardContext,
		s.eventsCache,
		tests.LocalNamespaceEntry,
		workflowExecution.GetWorkflowId(),
		workflowExecution.GetRunId(),
		log.NewTestLogger(),
	)
	// Add dummy event
	addWorkflowExecutionStartedEvent(ms, &workflowExecution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}
	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockEventsReapplier.EXPECT().ReapplyEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	err := s.mockHistoryEngine.ReapplyEvents(
		context.Background(),
		tests.NamespaceID,
		workflowExecution.GetWorkflowId(),
		workflowExecution.GetRunId(),
		history,
	)
	s.NoError(err)
}

func (s *engineSuite) TestReapplyEvents_ResetWorkflow() {
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-reapply-reset-workflow",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	eventVersion := int64(100)
	history := []*historypb.HistoryEvent{
		{
			EventId:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			Version:   eventVersion,
		},
	}
	globalNamespaceID := uuid.New()
	globalNamespaceName := "global-namespace-name"
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: globalNamespaceID, Name: globalNamespaceName},
		&persistencespb.NamespaceConfig{},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		tests.Version,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(namespaceEntry.Name()).Return(namespaceEntry, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, eventVersion).Return(cluster.TestAlternativeClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()

	ms := workflow.TestGlobalMutableState(
		s.mockHistoryEngine.shardContext,
		s.eventsCache,
		log.NewTestLogger(),
		namespaceEntry.FailoverVersion(),
		workflowExecution.GetWorkflowId(),
		workflowExecution.GetRunId(),
	)
	// Add dummy event
	addWorkflowExecutionStartedEvent(ms, &workflowExecution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)

	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	wfMs.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId = 1
	token, err := ms.GetCurrentBranchToken()
	s.NoError(err)
	item := versionhistory.NewVersionHistoryItem(1, 1)
	versionHistory := versionhistory.NewVersionHistory(token, []*historyspb.VersionHistoryItem{item})
	wfMs.ExecutionInfo.VersionHistories = versionhistory.NewVersionHistories(versionHistory)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}
	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockEventsReapplier.EXPECT().ReapplyEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	s.mockWorkflowResetter.EXPECT().ResetWorkflow(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil)

	err = s.mockHistoryEngine.ReapplyEvents(
		context.Background(),
		namespaceEntry.ID(),
		workflowExecution.GetWorkflowId(),
		workflowExecution.GetRunId(),
		history,
	)
	s.NoError(err)
}

func (s *engineSuite) TestEagerWorkflowStart_DoesNotCreateTransferTask() {
	var recordedTasks []tasks.Task

	s.mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil)
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("mock")
	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes("mock", false).Return(searchattribute.NameTypeMap{}, nil)
	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
		recordedTasks = request.NewWorkflowSnapshot.Tasks[tasks.CategoryTransfer]
		persistenceResponse := persistence.CreateWorkflowExecutionResponse{NewMutableStateStats: tests.CreateWorkflowExecutionResponse.NewMutableStateStats}
		return &persistenceResponse, nil
	})

	i := interceptor.NewTelemetryInterceptor(s.mockShard.GetNamespaceRegistry(),
		s.mockShard.GetMetricsHandler(),
		s.mockShard.Resource.Logger,
		s.config.LogAllReqErrors)
	response, err := i.UnaryIntercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "StartWorkflowExecution"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		response, err := s.mockHistoryEngine.StartWorkflowExecution(ctx, &historyservice.StartWorkflowExecutionRequest{
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
		})
		return response, err
	})
	s.NoError(err)
	s.Equal(len(response.(*historyservice.StartWorkflowExecutionResponse).EagerWorkflowTask.History.Events), 3)
	s.Equal(response.(*historyservice.StartWorkflowExecutionResponse).EagerWorkflowTask.History.Events[0].EventType, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
	s.Equal(response.(*historyservice.StartWorkflowExecutionResponse).EagerWorkflowTask.History.Events[1].EventType, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)
	s.Equal(response.(*historyservice.StartWorkflowExecutionResponse).EagerWorkflowTask.History.Events[2].EventType, enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED)
	s.Equal(len(recordedTasks), 0)
}

func (s *engineSuite) TestEagerWorkflowStart_FromCron_SkipsEager() {
	var recordedTasks []tasks.Task

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
		recordedTasks = request.NewWorkflowSnapshot.Tasks[tasks.CategoryTransfer]
		persistenceResponse := persistence.CreateWorkflowExecutionResponse{NewMutableStateStats: tests.CreateWorkflowExecutionResponse.NewMutableStateStats}
		return &persistenceResponse, nil
	})

	i := interceptor.NewTelemetryInterceptor(s.mockShard.GetNamespaceRegistry(),
		s.mockShard.GetMetricsHandler(),
		s.mockShard.Resource.Logger,
		s.config.LogAllReqErrors)
	response, err := i.UnaryIntercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "StartWorkflowExecution"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		firstWorkflowTaskBackoff := time.Second
		response, err := s.mockHistoryEngine.StartWorkflowExecution(ctx, &historyservice.StartWorkflowExecutionRequest{
			NamespaceId:              tests.NamespaceID.String(),
			Attempt:                  1,
			ContinueAsNewInitiator:   enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
			FirstWorkflowTaskBackoff: durationpb.New(firstWorkflowTaskBackoff),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId:            "test",
				Namespace:             tests.Namespace.String(),
				WorkflowType:          &commonpb.WorkflowType{Name: "test"},
				TaskQueue:             &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "test"},
				Identity:              "test",
				RequestId:             "test",
				CronSchedule:          "* * * * *",
				RequestEagerExecution: true,
			},
		})
		return response, err
	})
	s.NoError(err)
	s.Nil(response.(*historyservice.StartWorkflowExecutionResponse).EagerWorkflowTask)
	s.Equal(len(recordedTasks), 0)
}

func (s *engineSuite) TestGetHistory() {
	firstEventID := int64(100)
	nextEventID := int64(102)
	branchToken := []byte{1}
	we := commonpb.WorkflowExecution{
		WorkflowId: "wid",
		RunId:      "rid",
	}
	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      2,
		NextPageToken: []byte{},
		ShardID:       1,
	}
	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), req).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{
			{
				EventId:   int64(100),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			},
			{
				EventId:   int64(101),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
					WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
						SearchAttributes: &commonpb.SearchAttributes{
							IndexedFields: map[string]*commonpb.Payload{
								"CustomKeywordField":    payload.EncodeString("random-keyword"),
								"TemporalChangeVersion": payload.EncodeString("random-data"),
							},
						},
					},
				},
			},
		},
		NextPageToken: []byte{},
		Size:          1,
	}, nil)

	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap, nil)
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(tests.NamespaceID).Return(tests.Namespace, nil)
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return(esIndexName).AnyTimes()

	history, token, err := api.GetHistory(
		context.Background(),
		s.mockShard,
		tests.NamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: we.WorkflowId,
			RunId:      we.RunId,
		},
		firstEventID,
		nextEventID,
		2,
		[]byte{},
		nil,
		branchToken,
		s.mockVisibilityMgr,
	)
	s.NoError(err)
	s.NotNil(history)
	s.Equal([]byte{}, token)

	s.EqualValues("Keyword", history.Events[1].GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes().GetIndexedFields()["AliasForCustomKeywordField"].GetMetadata()["type"])
	s.EqualValues(`"random-data"`, history.Events[1].GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes().GetIndexedFields()["TemporalChangeVersion"].GetData())
}

func (s *engineSuite) TestGetWorkflowExecutionHistory() {
	we := commonpb.WorkflowExecution{WorkflowId: "wid1", RunId: uuid.New()}
	newRunID := uuid.New()

	req := &historyservice.GetWorkflowExecutionHistoryRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.GetWorkflowExecutionHistoryRequest{
			Execution:              &we,
			MaximumPageSize:        10,
			NextPageToken:          nil,
			WaitNewEvent:           true,
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
			SkipArchival:           true,
		},
	}

	// set up mocks to simulate a failed workflow with a retry policy. the failure event is id 5.
	branchToken := []byte{1, 2, 3}

	s.mockNamespaceCache.EXPECT().GetNamespaceName(tests.NamespaceID).Return(tests.Namespace, nil).AnyTimes()
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &persistencespb.WorkflowMutableState{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  we.RunId,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		},
		NextEventId: 6,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:         tests.NamespaceID.String(),
			WorkflowId:          we.WorkflowId,
			VersionHistories:    versionHistories,
			WorkflowTypeName:    "mytype",
			LastFirstEventId:    5,
			LastFirstEventTxnId: 100,
		},
	}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  we.WorkflowId,
		RunID:       we.RunId,
	}).Return(&persistence.GetWorkflowExecutionResponse{State: mState}, nil).AnyTimes()
	// GetWorkflowExecutionHistory will request the last event
	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    5,
		MaxEventID:    6,
		PageSize:      10,
		NextPageToken: nil,
		ShardID:       1,
	}).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{
			{
				EventId:   int64(5),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
					WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
						Failure:                      &failurepb.Failure{Message: "this workflow failed"},
						RetryState:                   enumspb.RETRY_STATE_IN_PROGRESS,
						WorkflowTaskCompletedEventId: 4,
						NewExecutionRunId:            newRunID,
					},
				},
			},
		},
		NextPageToken: []byte{},
		Size:          1,
	}, nil).Times(2)

	s.mockExecutionMgr.EXPECT().TrimHistoryBranch(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return(esIndexName).AnyTimes()

	engine, err := s.mockHistoryEngine.shardContext.GetEngine(context.Background())
	s.NoError(err)

	oldGoSDKVersion := "1.9.1"
	newGoSDKVersion := "1.10.1"

	// new sdk: should see failed event
	ctx := headers.SetVersionsForTests(context.Background(), newGoSDKVersion, headers.ClientNameGoSDK, headers.SupportedServerVersions, headers.AllFeatures)
	resp, err := engine.GetWorkflowExecutionHistory(ctx, req)
	s.NoError(err)
	s.False(resp.Response.Archived)
	event := resp.Response.History.Events[0]
	s.Equal(int64(5), event.EventId)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, event.EventType)
	attrs := event.GetWorkflowExecutionFailedEventAttributes()
	s.Equal("this workflow failed", attrs.Failure.Message)
	s.Equal(newRunID, attrs.NewExecutionRunId)
	s.Equal(enumspb.RETRY_STATE_IN_PROGRESS, attrs.RetryState)

	// old sdk: should see continued-as-new event
	// TODO: We can remove this once we no longer support SDK versions prior to around September 2021.
	// See comment in workflowHandler.go:GetWorkflowExecutionHistory
	ctx = headers.SetVersionsForTests(context.Background(), oldGoSDKVersion, headers.ClientNameGoSDK, headers.SupportedServerVersions, "")
	resp, err = engine.GetWorkflowExecutionHistory(ctx, req)
	s.NoError(err)
	s.False(resp.Response.Archived)
	event = resp.Response.History.Events[0]
	s.Equal(int64(5), event.EventId)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, event.EventType)
	attrs2 := event.GetWorkflowExecutionContinuedAsNewEventAttributes()
	s.Equal(newRunID, attrs2.NewExecutionRunId)
	s.Equal("this workflow failed", attrs2.Failure.Message)
}

func (s *engineSuite) TestGetWorkflowExecutionHistory_RawHistoryWithTransientDecision() {
	we := commonpb.WorkflowExecution{WorkflowId: "wid1", RunId: uuid.New()}

	engine, err := s.mockHistoryEngine.shardContext.GetEngine(context.Background())
	s.NoError(err)

	branchToken := []byte{1, 2, 3}
	persistenceToken := []byte("some random persistence token")
	nextPageToken, err := api.SerializeHistoryToken(&tokenspb.HistoryContinuation{
		RunId:            we.GetRunId(),
		FirstEventId:     common.FirstEventID,
		NextEventId:      5,
		PersistenceToken: persistenceToken,
		TransientWorkflowTask: &historyspb.TransientWorkflowTaskInfo{
			HistorySuffix: []*historypb.HistoryEvent{
				{
					EventId:   5,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				},
				{
					EventId:   6,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				},
			},
		},
		BranchToken: branchToken,
	})
	s.NoError(err)
	s.config.SendRawWorkflowHistory = func(string) bool { return true }
	req := &historyservice.GetWorkflowExecutionHistoryRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.GetWorkflowExecutionHistoryRequest{
			Execution:              &we,
			MaximumPageSize:        10,
			NextPageToken:          nextPageToken,
			WaitNewEvent:           false,
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
			SkipArchival:           true,
		},
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceID(tests.Namespace).Return(tests.NamespaceID, nil).AnyTimes()
	historyBlob1, err := s.mockShard.GetPayloadSerializer().SerializeEvent(
		&historypb.HistoryEvent{
			EventId:   int64(3),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		},
		enumspb.ENCODING_TYPE_PROTO3,
	)
	s.NoError(err)
	historyBlob2, err := s.mockShard.GetPayloadSerializer().SerializeEvent(
		&historypb.HistoryEvent{
			EventId:   int64(4),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
		},
		enumspb.ENCODING_TYPE_PROTO3,
	)
	s.NoError(err)
	s.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    1,
		MaxEventID:    5,
		PageSize:      10,
		NextPageToken: persistenceToken,
		ShardID:       1,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{historyBlob1, historyBlob2},
		NextPageToken:     []byte{},
		Size:              1,
	}, nil).Times(1)

	ctx := headers.SetVersionsForTests(context.Background(), "1.10.1", headers.ClientNameGoSDK, headers.SupportedServerVersions, headers.AllFeatures)
	resp, err := engine.GetWorkflowExecutionHistory(ctx, req)
	s.NoError(err)
	s.False(resp.Response.Archived)
	s.Empty(resp.Response.History.Events)
	s.Len(resp.Response.RawHistory, 4)
	event, err := s.mockShard.GetPayloadSerializer().DeserializeEvent(resp.Response.RawHistory[2])
	s.NoError(err)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, event.EventType)
	event, err = s.mockShard.GetPayloadSerializer().DeserializeEvent(resp.Response.RawHistory[3])
	s.NoError(err)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, event.EventType)
}

func (s *engineSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnInvalidWorkflowID() {
	engine, err := s.mockHistoryEngine.shardContext.GetEngine(context.Background())
	s.NoError(err)

	ctx := context.Background()
	namespaceID := namespace.ID(uuid.New())
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id: namespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(namespaceEntry, nil).AnyTimes()
	_, err = engine.GetWorkflowExecutionRawHistoryV2(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: namespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId: namespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "",
					RunId:      uuid.New(),
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	s.Error(err)
}

func (s *engineSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnInvalidRunID() {
	engine, err := s.mockHistoryEngine.shardContext.GetEngine(context.Background())
	s.NoError(err)

	ctx := context.Background()
	namespaceID := namespace.ID(uuid.New())
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id: namespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(namespaceEntry, nil).AnyTimes()
	_, err = engine.GetWorkflowExecutionRawHistoryV2(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: namespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId: namespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      "runID",
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	s.Error(err)
}

func (s *engineSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnNamespaceCache() {
	engine, err := s.mockHistoryEngine.shardContext.GetEngine(context.Background())
	s.NoError(err)

	ctx := context.Background()
	namespaceID := namespace.ID(uuid.New())
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(nil, fmt.Errorf("test"))
	_, err = engine.GetWorkflowExecutionRawHistoryV2(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: namespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId: namespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      uuid.New(),
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	s.Error(err)
}

func (s *engineSuite) Test_GetWorkflowExecutionRawHistoryV2() {
	engine, err := s.mockHistoryEngine.shardContext.GetEngine(context.Background())
	s.NoError(err)

	ctx := context.Background()
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Name: tests.Namespace.String(),
			Id:   tests.NamespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
	branchToken := []byte{1}
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &persistencespb.WorkflowMutableState{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: 11,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:      tests.NamespaceID.String(),
			VersionHistories: versionHistories,
		},
	}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mState}, nil).AnyTimes()

	s.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{},
		NextPageToken:     []byte{},
		Size:              0,
	}, nil)
	_, err = engine.GetWorkflowExecutionRawHistoryV2(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: tests.NamespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId: tests.NamespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      uuid.New(),
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   10,
				NextPageToken:     nil,
			},
		})
	s.NoError(err)
}

func (s *engineSuite) Test_GetWorkflowExecutionRawHistoryV2_SameStartIDAndEndID() {
	engine, err := s.mockHistoryEngine.shardContext.GetEngine(context.Background())
	s.NoError(err)

	ctx := context.Background()
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Name: tests.Namespace.String(),
			Id:   tests.NamespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
	branchToken := []byte{1}
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &persistencespb.WorkflowMutableState{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: 11,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:      tests.NamespaceID.String(),
			VersionHistories: versionHistories,
		},
	}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mState}, nil).AnyTimes()

	resp, err := engine.GetWorkflowExecutionRawHistoryV2(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: tests.NamespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId: tests.NamespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      uuid.New(),
				},
				StartEventId:      10,
				StartEventVersion: 100,
				EndEventId:        common.EmptyEventID,
				EndEventVersion:   common.EmptyVersion,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	s.Nil(resp.Response.NextPageToken)
	s.NoError(err)
}

func (s *engineSuite) Test_GetWorkflowExecutionRawHistory_FailedOnInvalidWorkflowID() {
	engine, err := s.mockHistoryEngine.shardContext.GetEngine(context.Background())
	s.NoError(err)

	ctx := context.Background()
	namespaceID := namespace.ID(uuid.New())
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id: namespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(namespaceEntry, nil).AnyTimes()
	_, err = engine.GetWorkflowExecutionRawHistory(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryRequest{
			NamespaceId: namespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryRequest{
				NamespaceId: namespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "",
					RunId:      uuid.New(),
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	s.Error(err)
}

func (s *engineSuite) Test_GetWorkflowExecutionRawHistory_FailedOnInvalidRunID() {
	engine, err := s.mockHistoryEngine.shardContext.GetEngine(context.Background())
	s.NoError(err)

	ctx := context.Background()
	namespaceID := namespace.ID(uuid.New())
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id: namespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(namespaceEntry, nil).AnyTimes()
	_, err = engine.GetWorkflowExecutionRawHistory(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryRequest{
			NamespaceId: namespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryRequest{
				NamespaceId: namespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      "runID",
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	s.Error(err)
}

func (s *engineSuite) Test_GetWorkflowExecutionRawHistory_FailedOnNamespaceCache() {
	engine, err := s.mockHistoryEngine.shardContext.GetEngine(context.Background())
	s.NoError(err)

	ctx := context.Background()
	namespaceID := namespace.ID(uuid.New())
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(nil, fmt.Errorf("test"))
	_, err = engine.GetWorkflowExecutionRawHistory(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryRequest{
			NamespaceId: namespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryRequest{
				NamespaceId: namespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      uuid.New(),
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	s.Error(err)
}

func (s *engineSuite) Test_GetWorkflowExecutionRawHistory() {
	engine, err := s.mockHistoryEngine.shardContext.GetEngine(context.Background())
	s.NoError(err)

	ctx := context.Background()
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Name: tests.Namespace.String(),
			Id:   tests.NamespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
	branchToken := []byte{1}
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &persistencespb.WorkflowMutableState{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: 11,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:      tests.NamespaceID.String(),
			VersionHistories: versionHistories,
		},
	}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mState}, nil).AnyTimes()

	s.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{},
		NextPageToken:     []byte{},
		Size:              0,
	}, nil)
	_, err = engine.GetWorkflowExecutionRawHistory(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryRequest{
			NamespaceId: tests.NamespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryRequest{
				NamespaceId: tests.NamespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      uuid.New(),
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   10,
				NextPageToken:     nil,
			},
		})
	s.NoError(err)
}

func (s *engineSuite) Test_GetWorkflowExecutionRawHistory_SameStartIDAndEndID() {
	engine, err := s.mockHistoryEngine.shardContext.GetEngine(context.Background())
	s.NoError(err)

	ctx := context.Background()
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Name: tests.Namespace.String(),
			Id:   tests.NamespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
	branchToken := []byte{1}
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &persistencespb.WorkflowMutableState{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: 11,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:      tests.NamespaceID.String(),
			VersionHistories: versionHistories,
		},
	}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mState}, nil).AnyTimes()
	s.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{},
		NextPageToken:     []byte{},
		Size:              0,
	}, nil)
	resp, err := engine.GetWorkflowExecutionRawHistory(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryRequest{
			NamespaceId: tests.NamespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryRequest{
				NamespaceId: tests.NamespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      uuid.New(),
				},
				StartEventId:      10,
				StartEventVersion: 100,
				EndEventId:        common.EmptyEventID,
				EndEventVersion:   common.EmptyVersion,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	s.Nil(resp.Response.NextPageToken)
	s.NoError(err)
}

func (s *engineSuite) Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedStartAndEnd() {
	inputStartEventID := int64(1)
	inputStartVersion := int64(10)
	inputEndEventID := int64(100)
	inputEndVersion := int64(11)
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	endItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, endItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	namespaceID := namespace.ID(uuid.New())
	request := &historyservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: namespaceID.String(),
		Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      inputStartEventID,
			StartEventVersion: inputStartVersion,
			EndEventId:        inputEndEventID,
			EndEventVersion:   inputEndVersion,
			MaximumPageSize:   10,
			NextPageToken:     nil,
		},
	}

	targetVersionHistory, err := getworkflowexecutionrawhistoryv2.SetRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	s.Equal(request.GetRequest().GetStartEventId(), inputStartEventID)
	s.Equal(request.GetRequest().GetEndEventId(), inputEndEventID)
	s.Equal(targetVersionHistory, versionHistory)
	s.NoError(err)
}

func (s *engineSuite) Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedEndEvent() {
	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(11)
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	targetItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, targetItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	namespaceID := namespace.ID(uuid.New())
	request := &historyservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: namespaceID.String(),
		Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      common.EmptyEventID,
			StartEventVersion: common.EmptyVersion,
			EndEventId:        inputEndEventID,
			EndEventVersion:   inputEndVersion,
			MaximumPageSize:   10,
			NextPageToken:     nil,
		},
	}

	targetVersionHistory, err := getworkflowexecutionrawhistoryv2.SetRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	s.Equal(request.GetRequest().GetStartEventId(), inputStartEventID-1)
	s.Equal(request.GetRequest().GetEndEventId(), inputEndEventID)
	s.Equal(targetVersionHistory, versionHistory)
	s.NoError(err)
}

func (s *engineSuite) Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedStartEvent() {
	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(11)
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	targetItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, targetItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	namespaceID := namespace.ID(uuid.New())
	request := &historyservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: namespaceID.String(),
		Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      inputStartEventID,
			StartEventVersion: inputStartVersion,
			EndEventId:        common.EmptyEventID,
			EndEventVersion:   common.EmptyVersion,
			MaximumPageSize:   10,
			NextPageToken:     nil,
		},
	}

	targetVersionHistory, err := getworkflowexecutionrawhistoryv2.SetRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	s.Equal(request.GetRequest().GetStartEventId(), inputStartEventID)
	s.Equal(request.GetRequest().GetEndEventId(), inputEndEventID+1)
	s.Equal(targetVersionHistory, versionHistory)
	s.NoError(err)
}

func (s *engineSuite) Test_SetRequestDefaultValueAndGetTargetVersionHistory_NonCurrentBranch() {
	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(101)
	item1 := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	item2 := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory1 := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{item1, item2})
	item3 := versionhistory.NewVersionHistoryItem(int64(10), int64(20))
	item4 := versionhistory.NewVersionHistoryItem(int64(20), int64(51))
	versionHistory2 := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{item1, item3, item4})
	versionHistories := versionhistory.NewVersionHistories(versionHistory1)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, versionHistory2)
	s.NoError(err)
	namespaceID := namespace.ID(uuid.New())
	request := &historyservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: namespaceID.String(),
		Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      9,
			StartEventVersion: 20,
			EndEventId:        inputEndEventID,
			EndEventVersion:   inputEndVersion,
			MaximumPageSize:   10,
			NextPageToken:     nil,
		},
	}

	targetVersionHistory, err := getworkflowexecutionrawhistoryv2.SetRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	s.Equal(request.GetRequest().GetStartEventId(), inputStartEventID)
	s.Equal(request.GetRequest().GetEndEventId(), inputEndEventID)
	s.Equal(targetVersionHistory, versionHistory1)
	s.NoError(err)
}

func (s *engineSuite) getMutableState(testNamespaceID namespace.ID, we *commonpb.WorkflowExecution) workflow.MutableState {
	context, release, err := s.workflowCache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		tests.NamespaceID,
		we,
		locks.PriorityHigh,
	)
	if err != nil {
		return nil
	}
	defer release(nil)

	return context.(*workflow.ContextImpl).MutableState
}

func (s *engineSuite) getActivityScheduledEvent(
	ms workflow.MutableState,
	scheduledEventID int64,
) *historypb.HistoryEvent {
	event, _ := ms.GetActivityScheduledEvent(context.Background(), scheduledEventID)
	return event
}

func addWorkflowExecutionStartedEventWithParent(ms workflow.MutableState, workflowExecution *commonpb.WorkflowExecution,
	workflowType, taskQueue string, input *commonpb.Payloads, executionTimeout, runTimeout, taskTimeout time.Duration,
	parentInfo *workflowspb.ParentExecutionInfo, identity string) *historypb.HistoryEvent {

	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:               workflowExecution.WorkflowId,
		WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
		Input:                    input,
		WorkflowExecutionTimeout: durationpb.New(executionTimeout),
		WorkflowRunTimeout:       durationpb.New(runTimeout),
		WorkflowTaskTimeout:      durationpb.New(taskTimeout),
		Identity:                 identity,
	}

	event, _ := ms.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:             1,
			NamespaceId:         tests.NamespaceID.String(),
			StartRequest:        startRequest,
			ParentExecutionInfo: parentInfo,
		},
	)

	return event
}

func addWorkflowExecutionStartedEvent(ms workflow.MutableState, workflowExecution *commonpb.WorkflowExecution,
	workflowType, taskQueue string, input *commonpb.Payloads, executionTimeout, runTimeout, taskTimeout time.Duration,
	identity string) *historypb.HistoryEvent {
	return addWorkflowExecutionStartedEventWithParent(ms, workflowExecution, workflowType, taskQueue, input,
		executionTimeout, runTimeout, taskTimeout, nil, identity)
}

func addWorkflowTaskScheduledEvent(ms workflow.MutableState) *workflow.WorkflowTaskInfo {
	workflowTask, _ := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	return workflowTask
}

func addWorkflowTaskStartedEvent(ms workflow.MutableState, scheduledEventID int64, taskQueue,
	identity string) *historypb.HistoryEvent {
	return addWorkflowTaskStartedEventWithRequestID(ms, scheduledEventID, tests.RunID, taskQueue, identity)
}

func addWorkflowTaskStartedEventWithRequestID(ms workflow.MutableState, scheduledEventID int64, requestID string,
	taskQueue, identity string) *historypb.HistoryEvent {
	event, _, _ := ms.AddWorkflowTaskStartedEvent(
		scheduledEventID,
		requestID,
		&taskqueuepb.TaskQueue{Name: taskQueue},
		identity,
		nil,
		nil,
		false,
	)

	return event
}

func addWorkflowTaskCompletedEvent(s *suite.Suite, ms workflow.MutableState, scheduledEventID, startedEventID int64, identity string) *historypb.HistoryEvent {
	workflowTask := ms.GetWorkflowTaskByID(scheduledEventID)
	s.NotNil(workflowTask)
	s.Equal(startedEventID, workflowTask.StartedEventID)

	event, _ := ms.AddWorkflowTaskCompletedEvent(workflowTask, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: identity,
	}, defaultWorkflowTaskCompletionLimits)

	ms.FlushBufferedEvents()

	return event
}

func addActivityTaskScheduledEvent(
	ms workflow.MutableState,
	workflowTaskCompletedID int64,
	activityID, activityType,
	taskQueue string,
	input *commonpb.Payloads,
	scheduleToCloseTimeout time.Duration,
	scheduleToStartTimeout time.Duration,
	startToCloseTimeout time.Duration,
	heartbeatTimeout time.Duration,
) (*historypb.HistoryEvent,
	*persistencespb.ActivityInfo) {

	event, ai, _ := ms.AddActivityTaskScheduledEvent(workflowTaskCompletedID, &commandpb.ScheduleActivityTaskCommandAttributes{
		ActivityId:             activityID,
		ActivityType:           &commonpb.ActivityType{Name: activityType},
		TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
		Input:                  input,
		ScheduleToCloseTimeout: durationpb.New(scheduleToCloseTimeout),
		ScheduleToStartTimeout: durationpb.New(scheduleToStartTimeout),
		StartToCloseTimeout:    durationpb.New(startToCloseTimeout),
		HeartbeatTimeout:       durationpb.New(heartbeatTimeout),
	}, false)

	return event, ai
}

func addActivityTaskScheduledEventWithRetry(
	ms workflow.MutableState,
	workflowTaskCompletedID int64,
	activityID, activityType,
	taskQueue string,
	input *commonpb.Payloads,
	scheduleToCloseTimeout time.Duration,
	scheduleToStartTimeout time.Duration,
	startToCloseTimeout time.Duration,
	heartbeatTimeout time.Duration,
	retryPolicy *commonpb.RetryPolicy,
) (*historypb.HistoryEvent, *persistencespb.ActivityInfo) {

	event, ai, _ := ms.AddActivityTaskScheduledEvent(workflowTaskCompletedID, &commandpb.ScheduleActivityTaskCommandAttributes{
		ActivityId:             activityID,
		ActivityType:           &commonpb.ActivityType{Name: activityType},
		TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
		Input:                  input,
		ScheduleToCloseTimeout: durationpb.New(scheduleToCloseTimeout),
		ScheduleToStartTimeout: durationpb.New(scheduleToStartTimeout),
		StartToCloseTimeout:    durationpb.New(startToCloseTimeout),
		HeartbeatTimeout:       durationpb.New(heartbeatTimeout),
		RetryPolicy:            retryPolicy,
	}, false)

	return event, ai
}

func addActivityTaskStartedEvent(ms workflow.MutableState, scheduledEventID int64, identity string) *historypb.HistoryEvent {
	ai, _ := ms.GetActivityInfo(scheduledEventID)
	event, _ := ms.AddActivityTaskStartedEvent(ai, scheduledEventID, tests.RunID, identity, nil, nil)
	return event
}

func addActivityTaskCompletedEvent(ms workflow.MutableState, scheduledEventID, startedEventID int64, result *commonpb.Payloads,
	identity string) *historypb.HistoryEvent {
	event, _ := ms.AddActivityTaskCompletedEvent(scheduledEventID, startedEventID, &workflowservice.RespondActivityTaskCompletedRequest{
		Result:   result,
		Identity: identity,
	})

	return event
}

func addActivityTaskFailedEvent(ms workflow.MutableState, scheduledEventID, startedEventID int64, failure *failurepb.Failure, retryState enumspb.RetryState, identity string) *historypb.HistoryEvent {
	event, _ := ms.AddActivityTaskFailedEvent(scheduledEventID, startedEventID, failure, retryState, identity, nil)
	return event
}

func addTimerStartedEvent(ms workflow.MutableState, workflowTaskCompletedEventID int64, timerID string,
	timeout time.Duration) (*historypb.HistoryEvent, *persistencespb.TimerInfo) {
	event, ti, _ := ms.AddTimerStartedEvent(workflowTaskCompletedEventID,
		&commandpb.StartTimerCommandAttributes{
			TimerId:            timerID,
			StartToFireTimeout: durationpb.New(timeout),
		})
	return event, ti
}

func addTimerFiredEvent(ms workflow.MutableState, timerID string) *historypb.HistoryEvent {
	event, _ := ms.AddTimerFiredEvent(timerID)
	return event
}

func addRequestCancelInitiatedEvent(ms workflow.MutableState, workflowTaskCompletedEventID int64,
	cancelRequestID string, namespace namespace.Name, namespaceID namespace.ID, workflowID, runID string) (*historypb.HistoryEvent, *persistencespb.RequestCancelInfo) {
	event, rci, _ := ms.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID,
		cancelRequestID, &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
			Namespace:  namespace.String(),
			WorkflowId: workflowID,
			RunId:      runID,
			Reason:     "cancellation reason",
		},
		namespaceID)

	return event, rci
}

func addCancelRequestedEvent(ms workflow.MutableState, initiatedID int64, namespace namespace.Name, namespaceID namespace.ID, workflowID, runID string) *historypb.HistoryEvent {
	event, _ := ms.AddExternalWorkflowExecutionCancelRequested(initiatedID, namespace, namespaceID, workflowID, runID)
	return event
}

func addRequestSignalInitiatedEvent(ms workflow.MutableState, workflowTaskCompletedEventID int64,
	signalRequestID string, namespace namespace.Name, namespaceID namespace.ID, workflowID, runID, signalName string, input *commonpb.Payloads,
	control string, header *commonpb.Header) (*historypb.HistoryEvent, *persistencespb.SignalInfo) {
	event, si, _ := ms.AddSignalExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, signalRequestID,
		&commandpb.SignalExternalWorkflowExecutionCommandAttributes{
			Namespace: namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			SignalName: signalName,
			Input:      input,
			Control:    control,
			Header:     header,
		}, namespaceID)

	return event, si
}

func addSignaledEvent(ms workflow.MutableState, initiatedID int64, namespace namespace.Name, namespaceID namespace.ID, workflowID, runID string, control string) *historypb.HistoryEvent {
	event, _ := ms.AddExternalWorkflowExecutionSignaled(initiatedID, namespace, namespaceID, workflowID, runID, control)
	return event
}

func addStartChildWorkflowExecutionInitiatedEvent(
	ms workflow.MutableState,
	workflowTaskCompletedID int64,
	createRequestID string,
	namespace namespace.Name,
	namespaceID namespace.ID,
	workflowID, workflowType, taskQueue string,
	input *commonpb.Payloads,
	executionTimeout, runTimeout, taskTimeout time.Duration,
	parentClosePolicy enumspb.ParentClosePolicy,
) (*historypb.HistoryEvent, *persistencespb.ChildExecutionInfo) {

	event, cei, _ := ms.AddStartChildWorkflowExecutionInitiatedEvent(workflowTaskCompletedID, createRequestID,
		&commandpb.StartChildWorkflowExecutionCommandAttributes{
			Namespace:                namespace.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			Input:                    input,
			WorkflowExecutionTimeout: durationpb.New(executionTimeout),
			WorkflowRunTimeout:       durationpb.New(runTimeout),
			WorkflowTaskTimeout:      durationpb.New(taskTimeout),
			Control:                  "",
			ParentClosePolicy:        parentClosePolicy,
		}, namespaceID)
	return event, cei
}

func addChildWorkflowExecutionStartedEvent(ms workflow.MutableState, initiatedID int64, workflowID, runID string,
	workflowType string, clock *clockspb.VectorClock) *historypb.HistoryEvent {
	event, _ := ms.AddChildWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		&commonpb.WorkflowType{Name: workflowType},
		initiatedID,
		&commonpb.Header{},
		clock,
	)
	return event
}

func addChildWorkflowExecutionCompletedEvent(ms workflow.MutableState, initiatedID int64, childExecution *commonpb.WorkflowExecution,
	attributes *historypb.WorkflowExecutionCompletedEventAttributes) *historypb.HistoryEvent {
	event, _ := ms.AddChildWorkflowExecutionCompletedEvent(initiatedID, childExecution, attributes)
	return event
}

func addCompleteWorkflowEvent(ms workflow.MutableState, workflowTaskCompletedEventID int64,
	result *commonpb.Payloads) *historypb.HistoryEvent {
	event, _ := ms.AddCompletedWorkflowEvent(
		workflowTaskCompletedEventID,
		&commandpb.CompleteWorkflowExecutionCommandAttributes{
			Result: result,
		},
		"")
	return event
}

func addFailWorkflowEvent(
	ms workflow.MutableState,
	workflowTaskCompletedEventID int64,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event, _ := ms.AddFailWorkflowEvent(
		workflowTaskCompletedEventID,
		retryState,
		&commandpb.FailWorkflowExecutionCommandAttributes{
			Failure: failure,
		},
		"",
	)
	return event
}
