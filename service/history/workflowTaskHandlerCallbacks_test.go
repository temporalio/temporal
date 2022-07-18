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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	"golang.org/x/exp/maps"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

type (
	WorkflowTaskHandlerCallbackSuite struct {
		*require.Assertions
		suite.Suite

		controller       *gomock.Controller
		mockEventsCache  *events.MockCache
		mockExecutionMgr *persistence.MockExecutionManager

		logger log.Logger

		workflowTaskHandlerCallback *workflowTaskHandlerCallbacksImpl
	}
)

func TestWorkflowTaskHandlerCallbackSuite(t *testing.T) {
	suite.Run(t, new(WorkflowTaskHandlerCallbackSuite))
}

func (s *WorkflowTaskHandlerCallbackSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	config := tests.NewDynamicConfig()
	mockShard := shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 1,
				RangeId: 1,
			}},
		config,
	)
	mockShard.Resource.ShardMgr.EXPECT().AssertShardOwnership(gomock.Any(), gomock.Any()).AnyTimes()

	mockNamespaceCache := mockShard.Resource.NamespaceCache
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	s.mockExecutionMgr = mockShard.Resource.ExecutionMgr
	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockEventsCache = mockShard.MockEventsCache
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	s.logger = mockShard.GetLogger()

	historyCache := workflow.NewCache(mockShard)
	h := &historyEngineImpl{
		currentClusterName: mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shard:              mockShard,
		clusterMetadata:    mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		logger:             s.logger,
		throttledLogger:    s.logger,
		metricsClient:      metrics.NoopClient,
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		config:             config,
		timeSource:         mockShard.GetTimeSource(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopClient, func(namespace.ID, string) int32 { return 1 }),
		searchAttributesValidator: searchattribute.NewValidator(
			searchattribute.NewTestProvider(),
			mockShard.Resource.SearchAttributesMapper,
			config.SearchAttributesNumberOfKeysLimit,
			config.SearchAttributesSizeOfValueLimit,
			config.SearchAttributesTotalSizeLimit,
		),
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(mockShard, historyCache),
	}

	s.workflowTaskHandlerCallback = newWorkflowTaskHandlerCallback(h)
}

func (s *WorkflowTaskHandlerCallbackSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *WorkflowTaskHandlerCallbackSuite) TestVerifyFirstWorkflowTaskScheduled_WorkflowNotFound() {
	request := &historyservice.VerifyFirstWorkflowTaskScheduledRequest{
		NamespaceId: tests.NamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
	}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NotFound{})

	err := s.workflowTaskHandlerCallback.verifyFirstWorkflowTaskScheduled(context.Background(), request)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *WorkflowTaskHandlerCallbackSuite) TestVerifyFirstWorkflowTaskScheduled_WorkflowCompleted() {
	request := &historyservice.VerifyFirstWorkflowTaskScheduledRequest{
		NamespaceId: tests.NamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
	}

	msBuilder := workflow.TestGlobalMutableState(s.workflowTaskHandlerCallback.shard, s.mockEventsCache, s.logger, tests.Version, tests.RunID)
	addWorkflowExecutionStartedEvent(msBuilder, commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")

	_, err := msBuilder.AddTimeoutWorkflowEvent(
		msBuilder.GetNextEventID(),
		enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
		uuid.New(),
	)
	s.NoError(err)

	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	err = s.workflowTaskHandlerCallback.verifyFirstWorkflowTaskScheduled(context.Background(), request)
	s.NoError(err)
}

func (s *WorkflowTaskHandlerCallbackSuite) TestVerifyFirstWorkflowTaskScheduled_WorkflowZombie() {
	request := &historyservice.VerifyFirstWorkflowTaskScheduledRequest{
		NamespaceId: tests.NamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
	}

	msBuilder := workflow.TestGlobalMutableState(s.workflowTaskHandlerCallback.shard, s.mockEventsCache, s.logger, tests.Version, tests.RunID)
	addWorkflowExecutionStartedEvent(msBuilder, commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")

	// zombie state should be treated as open
	msBuilder.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	)
	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	err := s.workflowTaskHandlerCallback.verifyFirstWorkflowTaskScheduled(context.Background(), request)
	s.IsType(&serviceerror.WorkflowNotReady{}, err)
}

func (s *WorkflowTaskHandlerCallbackSuite) TestVerifyFirstWorkflowTaskScheduled_WorkflowRunning_TaskPending() {
	request := &historyservice.VerifyFirstWorkflowTaskScheduledRequest{
		NamespaceId: tests.NamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
	}

	msBuilder := workflow.TestGlobalMutableState(s.workflowTaskHandlerCallback.shard, s.mockEventsCache, s.logger, tests.Version, tests.RunID)
	addWorkflowExecutionStartedEvent(msBuilder, commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	addWorkflowTaskScheduledEvent(msBuilder)

	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	err := s.workflowTaskHandlerCallback.verifyFirstWorkflowTaskScheduled(context.Background(), request)
	s.NoError(err)
}

func (s *WorkflowTaskHandlerCallbackSuite) TestVerifyFirstWorkflowTaskScheduled_WorkflowRunning_TaskProcessed() {
	request := &historyservice.VerifyFirstWorkflowTaskScheduledRequest{
		NamespaceId: tests.NamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
	}

	msBuilder := workflow.TestGlobalMutableState(s.workflowTaskHandlerCallback.shard, s.mockEventsCache, s.logger, tests.Version, tests.RunID)
	addWorkflowExecutionStartedEvent(msBuilder, commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	wt := addWorkflowTaskScheduledEvent(msBuilder)
	workflowTasksStartEvent := addWorkflowTaskStartedEvent(msBuilder, wt.ScheduledEventID, "testTaskQueue", uuid.New())
	wt.StartedEventID = workflowTasksStartEvent.GetEventId()
	addWorkflowTaskCompletedEvent(msBuilder, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	err := s.workflowTaskHandlerCallback.verifyFirstWorkflowTaskScheduled(context.Background(), request)
	s.NoError(err)
}

func (s *WorkflowTaskHandlerCallbackSuite) TestHandleBufferedQueries_HeartbeatWorkflowTask() {
	queryRegistry, mockMutableState := s.setupBufferedQueriesMocks()
	s.assertQueryCounts(queryRegistry, 10, 0, 0, 0)
	queryResults := s.constructQueryResults(queryRegistry.GetBufferedIDs()[0:5], 10)
	s.workflowTaskHandlerCallback.handleBufferedQueries(mockMutableState, queryResults, false, tests.GlobalNamespaceEntry, true)
	s.assertQueryCounts(queryRegistry, 10, 0, 0, 0)
}

func (s *WorkflowTaskHandlerCallbackSuite) TestHandleBufferedQueries_NewWorkflowTask() {
	queryRegistry, mockMutableState := s.setupBufferedQueriesMocks()
	s.assertQueryCounts(queryRegistry, 10, 0, 0, 0)
	queryResults := s.constructQueryResults(queryRegistry.GetBufferedIDs()[0:5], 10)
	s.workflowTaskHandlerCallback.handleBufferedQueries(mockMutableState, queryResults, true, tests.GlobalNamespaceEntry, false)
	s.assertQueryCounts(queryRegistry, 5, 5, 0, 0)
}

func (s *WorkflowTaskHandlerCallbackSuite) TestHandleBufferedQueries_NoNewWorkflowTask() {
	queryRegistry, mockMutableState := s.setupBufferedQueriesMocks()
	s.assertQueryCounts(queryRegistry, 10, 0, 0, 0)
	queryResults := s.constructQueryResults(queryRegistry.GetBufferedIDs()[0:5], 10)
	s.workflowTaskHandlerCallback.handleBufferedQueries(mockMutableState, queryResults, false, tests.GlobalNamespaceEntry, false)
	s.assertQueryCounts(queryRegistry, 0, 5, 5, 0)
}

func (s *WorkflowTaskHandlerCallbackSuite) TestHandleBufferedQueries_QueryTooLarge() {
	queryRegistry, mockMutableState := s.setupBufferedQueriesMocks()
	s.assertQueryCounts(queryRegistry, 10, 0, 0, 0)
	bufferedIDs := queryRegistry.GetBufferedIDs()
	queryResults := s.constructQueryResults(bufferedIDs[0:5], 10)
	largeQueryResults := s.constructQueryResults(bufferedIDs[5:10], 10*1024*1024)
	maps.Copy(queryResults, largeQueryResults)
	s.workflowTaskHandlerCallback.handleBufferedQueries(mockMutableState, queryResults, false, tests.GlobalNamespaceEntry, false)
	s.assertQueryCounts(queryRegistry, 0, 5, 0, 5)
}

func (s *WorkflowTaskHandlerCallbackSuite) setupBufferedQueriesMocks() (workflow.QueryRegistry, *workflow.MockMutableState) {
	queryRegistry := s.constructQueryRegistry(10)
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

func (s *WorkflowTaskHandlerCallbackSuite) constructQueryResults(ids []string, resultSize int) map[string]*querypb.WorkflowQueryResult {
	results := make(map[string]*querypb.WorkflowQueryResult)
	for _, id := range ids {
		results[id] = &querypb.WorkflowQueryResult{
			ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
			Answer:     payloads.EncodeBytes(make([]byte, resultSize)),
		}
	}
	return results
}

func (s *WorkflowTaskHandlerCallbackSuite) constructQueryRegistry(numQueries int) workflow.QueryRegistry {
	queryRegistry := workflow.NewQueryRegistry()
	for i := 0; i < numQueries; i++ {
		queryRegistry.BufferQuery(&querypb.WorkflowQuery{})
	}
	return queryRegistry
}

func (s *WorkflowTaskHandlerCallbackSuite) assertQueryCounts(queryRegistry workflow.QueryRegistry, buffered, completed, unblocked, failed int) {
	s.Len(queryRegistry.GetBufferedIDs(), buffered)
	s.Len(queryRegistry.GetCompletedIDs(), completed)
	s.Len(queryRegistry.GetUnblockedIDs(), unblocked)
	s.Len(queryRegistry.GetFailedIDs(), failed)
}
