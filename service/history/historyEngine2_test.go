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
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"

	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
)

type (
	engine2Suite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		mockShard               *shard.ContextTest
		mockTxProcessor         *queues.MockQueue
		mockTimerProcessor      *queues.MockQueue
		mockVisibilityProcessor *queues.MockQueue
		mockEventsCache         *events.MockCache
		mockNamespaceCache      *namespace.MockRegistry
		mockClusterMetadata     *cluster.MockMetadata

		workflowCache    workflow.Cache
		historyEngine    *historyEngineImpl
		mockExecutionMgr *persistence.MockExecutionManager

		config *configs.Config
		logger log.Logger
	}
)

func TestEngine2Suite(t *testing.T) {
	s := new(engine2Suite)
	suite.Run(t, s)
}

func (s *engine2Suite) SetupSuite() {

}

func (s *engine2Suite) TearDownSuite() {
}

func (s *engine2Suite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.mockTxProcessor = queues.NewMockQueue(s.controller)
	s.mockTimerProcessor = queues.NewMockQueue(s.controller)
	s.mockVisibilityProcessor = queues.NewMockQueue(s.controller)
	s.mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	s.mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	s.mockVisibilityProcessor.EXPECT().Category().Return(tasks.CategoryVisibility).AnyTimes()
	s.mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockVisibilityProcessor.EXPECT().NotifyNewTasks(gomock.Any(), gomock.Any()).AnyTimes()

	s.config = tests.NewDynamicConfig()
	mockShard := shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 1,
				RangeId: 1,
			}},
		s.config,
	)
	s.mockShard = mockShard
	s.mockShard.Resource.ShardMgr.EXPECT().AssertShardOwnership(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata

	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ParentNamespaceID).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ChildNamespace).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.workflowCache = workflow.NewCache(s.mockShard)
	s.logger = s.mockShard.GetLogger()

	h := &historyEngineImpl{
		currentClusterName: s.mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shard:              s.mockShard,
		clusterMetadata:    s.mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		logger:             s.logger,
		throttledLogger:    s.logger,
		metricsClient:      metrics.NoopClient,
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		config:             s.config,
		timeSource:         s.mockShard.GetTimeSource(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopClient, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			s.mockTxProcessor.Category():         s.mockTxProcessor,
			s.mockTimerProcessor.Category():      s.mockTimerProcessor,
			s.mockVisibilityProcessor.Category(): s.mockVisibilityProcessor,
		},
		searchAttributesValidator: searchattribute.NewValidator(
			searchattribute.NewTestProvider(),
			s.mockShard.Resource.SearchAttributesMapper,
			s.config.SearchAttributesNumberOfKeysLimit,
			s.config.SearchAttributesSizeOfValueLimit,
			s.config.SearchAttributesTotalSizeLimit,
		),
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(mockShard, s.workflowCache),
	}
	s.mockShard.SetEngineForTesting(h)
	h.workflowTaskHandler = newWorkflowTaskHandlerCallback(h)

	s.historyEngine = h
}

func (s *engine2Suite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *engine2Suite) TestRecordWorkflowTaskStartedSuccessStickyEnabled() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	stickyTl := "stickyTaskQueue"
	identity := "testIdentity"

	msBuilder := workflow.TestLocalMutableState(s.historyEngine.shard, s.mockEventsCache, tests.LocalNamespaceEntry,
		log.NewTestLogger(), we.GetRunId())
	executionInfo := msBuilder.GetExecutionInfo()
	executionInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	executionInfo.StickyTaskQueue = stickyTl

	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(msBuilder)

	ms := workflow.TestCloneToProto(msBuilder)

	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	request := historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &we,
		ScheduledEventId:  2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: stickyTl,
			},
			Identity: identity,
		},
	}

	expectedResponse := historyservice.RecordWorkflowTaskStartedResponse{}
	expectedResponse.WorkflowType = msBuilder.GetWorkflowType()
	executionInfo = msBuilder.GetExecutionInfo()
	if executionInfo.LastWorkflowTaskStartedEventId != common.EmptyEventID {
		expectedResponse.PreviousStartedEventId = executionInfo.LastWorkflowTaskStartedEventId
	}
	expectedResponse.ScheduledEventId = wt.ScheduledEventID
	expectedResponse.ScheduledTime = wt.ScheduledTime
	expectedResponse.StartedEventId = wt.ScheduledEventID + 1
	expectedResponse.StickyExecutionEnabled = true
	expectedResponse.NextEventId = msBuilder.GetNextEventID() + 1
	expectedResponse.Attempt = wt.Attempt
	expectedResponse.WorkflowExecutionTaskQueue = &taskqueuepb.TaskQueue{
		Name: executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	currentBranchTokken, err := msBuilder.GetCurrentBranchToken()
	s.NoError(err)
	expectedResponse.BranchToken = currentBranchTokken

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &request)
	s.Nil(err)
	s.NotNil(response)
	s.True(response.StartedTime.After(*expectedResponse.ScheduledTime))
	expectedResponse.StartedTime = response.StartedTime
	expectedResponse.Queries = make(map[string]*querypb.WorkflowQuery)
	s.Equal(&expectedResponse, response)
}

func (s *engine2Suite) TestRecordWorkflowTaskStartedIfNoExecution() {
	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engine2Suite) TestRecordWorkflowTaskStartedIfGetExecutionFailed() {
	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("FAILED"))

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.EqualError(err, "FAILED")
}

func (s *engine2Suite) TestRecordWorkflowTaskStartedIfTaskAlreadyStarted() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, true)
	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &workflowExecution,
		ScheduledEventId:  2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&serviceerrors.TaskAlreadyStarted{}, err)
	s.logger.Error("RecordWorkflowTaskStarted failed with", tag.Error(err))
}

func (s *engine2Suite) TestRecordWorkflowTaskStartedIfTaskAlreadyCompleted() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, true)
	addWorkflowTaskCompletedEvent(msBuilder, int64(2), int64(3), identity)

	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &workflowExecution,
		ScheduledEventId:  2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
	s.logger.Error("RecordWorkflowTaskStarted failed with", tag.Error(err))
}

func (s *engine2Suite) TestRecordWorkflowTaskStartedConflictOnUpdate() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)

	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &persistence.ConditionFailedError{})

	ms2 := workflow.TestCloneToProto(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &workflowExecution,
		ScheduledEventId:  2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})
	s.Nil(err)
	s.NotNil(response)
	s.Equal("wType", response.WorkflowType.Name)
	s.True(response.PreviousStartedEventId == 0)
	s.Equal(int64(3), response.StartedEventId)
}

func (s *engine2Suite) TestRecordWorkflowTaskRetrySameRequest() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	tl := "testTaskQueue"
	identity := "testIdentity"
	requestID := "testRecordWorkflowTaskRetrySameRequestID"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &persistence.ConditionFailedError{})

	startedEventID := addWorkflowTaskStartedEventWithRequestID(msBuilder, int64(2), requestID, tl, identity)
	ms2 := workflow.TestCloneToProto(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &workflowExecution,
		ScheduledEventId:  2,
		TaskId:            100,
		RequestId:         requestID,
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})

	s.Nil(err)
	s.NotNil(response)
	s.Equal("wType", response.WorkflowType.Name)
	s.True(response.PreviousStartedEventId == 0)
	s.Equal(startedEventID.EventId, response.StartedEventId)
}

func (s *engine2Suite) TestRecordWorkflowTaskRetryDifferentRequest() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	tl := "testTaskQueue"
	identity := "testIdentity"
	requestID := "testRecordWorkflowTaskRetrySameRequestID"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &persistence.ConditionFailedError{})

	// Add event.
	addWorkflowTaskStartedEventWithRequestID(msBuilder, int64(2), "some_other_req", tl, identity)
	ms2 := workflow.TestCloneToProto(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &workflowExecution,
		ScheduledEventId:  2,
		TaskId:            100,
		RequestId:         requestID,
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})

	s.Nil(response)
	s.NotNil(err)
	s.IsType(&serviceerrors.TaskAlreadyStarted{}, err)
	s.logger.Info("Failed with error", tag.Error(err))
}

func (s *engine2Suite) TestRecordWorkflowTaskStartedMaxAttemptsExceeded() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	tl := "testTaskQueue"
	identity := "testIdentity"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	for i := 0; i < conditionalRetryCount; i++ {
		ms := workflow.TestCloneToProto(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	}

	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil,
		&persistence.ConditionFailedError{}).Times(conditionalRetryCount)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &workflowExecution,
		ScheduledEventId:  2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})

	s.NotNil(err)
	s.Nil(response)
	s.Equal(consts.ErrMaxAttemptsExceeded, err)
}

func (s *engine2Suite) TestRecordWorkflowTaskSuccess() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	tl := "testTaskQueue"
	identity := "testIdentity"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	// load mutable state such that it already exists in memory when respond workflow task is called
	// this enables us to set query registry on it
	ctx, release, err := s.workflowCache.GetOrCreateWorkflowExecution(
		metrics.AddMetricsContext(context.Background()),
		tests.NamespaceID,
		workflowExecution,
		workflow.CallerTypeAPI,
	)
	s.NoError(err)
	loadedMS, err := ctx.LoadWorkflowExecution(context.Background())
	s.NoError(err)
	qr := workflow.NewQueryRegistry()
	id1, _ := qr.BufferQuery(&querypb.WorkflowQuery{})
	id2, _ := qr.BufferQuery(&querypb.WorkflowQuery{})
	id3, _ := qr.BufferQuery(&querypb.WorkflowQuery{})
	loadedMS.(*workflow.MutableStateImpl).QueryRegistry = qr
	release(nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &workflowExecution,
		ScheduledEventId:  2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})

	s.Nil(err)
	s.NotNil(response)
	s.Equal("wType", response.WorkflowType.Name)
	s.True(response.PreviousStartedEventId == 0)
	s.Equal(int64(3), response.StartedEventId)
	expectedQueryMap := map[string]*querypb.WorkflowQuery{
		id1: {},
		id2: {},
		id3: {},
	}
	s.Equal(expectedQueryMap, response.Queries)
}

func (s *engine2Suite) TestRecordActivityTaskStartedIfNoExecution() {
	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	response, err := s.historyEngine.RecordActivityTaskStarted(
		metrics.AddMetricsContext(context.Background()),
		&historyservice.RecordActivityTaskStartedRequest{
			NamespaceId:       namespaceID.String(),
			WorkflowExecution: workflowExecution,
			ScheduledEventId:  5,
			TaskId:            100,
			RequestId:         "reqId",
			PollRequest: &workflowservice.PollActivityTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: tl,
				},
				Identity: identity,
			},
		},
	)
	if err != nil {
		s.logger.Error("Unexpected Error", tag.Error(err))
	}
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engine2Suite) TestRecordActivityTaskStartedSuccess() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, true)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(msBuilder, int64(2), int64(3), identity)
	scheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)

	ms1 := workflow.TestCloneToProto(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	s.mockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		events.EventKey{
			NamespaceID: namespaceID,
			WorkflowID:  workflowExecution.GetWorkflowId(),
			RunID:       workflowExecution.GetRunId(),
			EventID:     scheduledEvent.GetEventId(),
			Version:     0,
		},
		workflowTaskCompletedEvent.GetEventId(),
		gomock.Any(),
	).Return(scheduledEvent, nil)
	response, err := s.historyEngine.RecordActivityTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordActivityTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &workflowExecution,
		ScheduledEventId:  5,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollActivityTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})
	s.Nil(err)
	s.NotNil(response)
	s.Equal(scheduledEvent, response.ScheduledEvent)
}

func (s *engine2Suite) TestRequestCancelWorkflowExecution_Running() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	ms1 := workflow.TestCloneToProto(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	err := s.historyEngine.RequestCancelWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowExecution.WorkflowId,
				RunId:      workflowExecution.RunId,
			},
			Identity: "identity",
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(namespaceID, workflowExecution)
	s.Equal(int64(4), executionBuilder.GetNextEventID())
}

func (s *engine2Suite) TestRequestCancelWorkflowExecution_Finished() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	msBuilder.GetExecutionState().State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	ms1 := workflow.TestCloneToProto(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)

	err := s.historyEngine.RequestCancelWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowExecution.WorkflowId,
				RunId:      workflowExecution.RunId,
			},
			Identity: "identity",
		},
	})
	s.Nil(err)
}

func (s *engine2Suite) TestRequestCancelWorkflowExecution_NotFound() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	err := s.historyEngine.RequestCancelWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowExecution.WorkflowId,
				RunId:      workflowExecution.RunId,
			},
			Identity: "identity",
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engine2Suite) TestRequestCancelWorkflowExecution_ParentMismatch() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	parentInfo := &workflowspb.ParentExecutionInfo{
		NamespaceId: tests.ParentNamespaceID.String(),
		Namespace:   tests.ParentNamespace.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "parent wId",
			RunId:      "parent rId",
		},
		InitiatedId:      123,
		InitiatedVersion: 456,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	msBuilder := s.createExecutionStartedStateWithParent(workflowExecution, tl, parentInfo, identity, false)
	ms1 := workflow.TestCloneToProto(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)

	err := s.historyEngine.RequestCancelWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowExecution.WorkflowId,
				RunId:      workflowExecution.RunId,
			},
			Identity: "identity",
		},
		ExternalWorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "unknown wId",
			RunId:      "unknown rId",
		},
		ChildWorkflowOnly: true,
	})
	s.Equal(consts.ErrWorkflowParent, err)
}

func (s *engine2Suite) TestTerminateWorkflowExecution_ParentMismatch() {
	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	parentInfo := &workflowspb.ParentExecutionInfo{
		NamespaceId: tests.ParentNamespaceID.String(),
		Namespace:   tests.ParentNamespace.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "parent wId",
			RunId:      "parent rId",
		},
		InitiatedId:      123,
		InitiatedVersion: 456,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	msBuilder := s.createExecutionStartedStateWithParent(workflowExecution, tl, parentInfo, identity, false)
	ms1 := workflow.TestCloneToProto(msBuilder)
	currentExecutionResp := &persistence.GetCurrentExecutionResponse{
		RunID: tests.RunID,
	}
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(currentExecutionResp, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)

	err := s.historyEngine.TerminateWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.TerminateWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowExecution.WorkflowId,
			},
			Identity:            "identity",
			FirstExecutionRunId: workflowExecution.RunId,
		},
		ExternalWorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "unknown wId",
			RunId:      "unknown rId",
		},
		ChildWorkflowOnly: true,
	})
	s.Equal(consts.ErrWorkflowParent, err)
}

func (s *engine2Suite) createExecutionStartedState(
	we commonpb.WorkflowExecution, tl string,
	identity string,
	startWorkflowTask bool,
) workflow.MutableState {
	return s.createExecutionStartedStateWithParent(we, tl, nil, identity, startWorkflowTask)
}

func (s *engine2Suite) createExecutionStartedStateWithParent(
	we commonpb.WorkflowExecution, tl string,
	parentInfo *workflowspb.ParentExecutionInfo,
	identity string,
	startWorkflowTask bool,
) workflow.MutableState {
	msBuilder := workflow.TestLocalMutableState(s.historyEngine.shard, s.mockEventsCache, tests.LocalNamespaceEntry,
		s.logger, we.GetRunId())
	addWorkflowExecutionStartedEventWithParent(msBuilder, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, parentInfo, identity)
	wt := addWorkflowTaskScheduledEvent(msBuilder)
	if startWorkflowTask {
		addWorkflowTaskStartedEvent(msBuilder, wt.ScheduledEventID, tl, identity)
	}
	_ = msBuilder.SetHistoryTree(we.GetRunId())
	versionHistory, _ := versionhistory.GetCurrentVersionHistory(
		msBuilder.GetExecutionInfo().VersionHistories,
	)
	_ = versionhistory.AddOrUpdateVersionHistoryItem(
		versionHistory,
		versionhistory.NewVersionHistoryItem(0, 0),
	)

	return msBuilder
}

func (s *engine2Suite) TestRespondWorkflowTaskCompletedRecordMarkerCommand() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	taskToken := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       "wId",
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	serializedTaskToken, _ := taskToken.Marshal()
	identity := "testIdentity"
	markerDetails := payloads.EncodeString("marker details")
	markerName := "marker name"

	msBuilder := workflow.TestLocalMutableState(s.historyEngine.shard, s.mockEventsCache, tests.LocalNamespaceEntry,
		log.NewTestLogger(), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(msBuilder)
	addWorkflowTaskStartedEvent(msBuilder, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
		Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
			MarkerName: markerName,
			Details: map[string]*commonpb.Payloads{
				"data": markerDetails,
			},
		}},
	}}

	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.historyEngine.RespondWorkflowTaskCompleted(metrics.AddMetricsContext(context.Background()), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: namespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: serializedTaskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(namespaceID, we)
	s.Equal(int64(6), executionBuilder.GetNextEventID())
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, executionBuilder.GetExecutionState().State)
	s.False(executionBuilder.HasPendingWorkflowTask())
}

func (s *engine2Suite) TestRespondWorkflowTaskCompleted_StartChildWithSearchAttributes() {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	taskToken := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       "wId",
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	serializedTaskToken, _ := taskToken.Marshal()
	identity := "testIdentity"

	msBuilder := workflow.TestLocalMutableState(s.historyEngine.shard, s.mockEventsCache, tests.LocalNamespaceEntry,
		log.NewTestLogger(), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, nil, 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(msBuilder)
	addWorkflowTaskStartedEvent(msBuilder, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
			Namespace:    tests.Namespace.String(),
			WorkflowId:   tests.WorkflowID,
			WorkflowType: &commonpb.WorkflowType{Name: "wType"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: tl},
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				"AliasForCustomTextField": payload.EncodeString("search attribute value")},
			},
		}},
	}}

	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		eventsToSave := request.UpdateWorkflowEvents[0].Events
		s.Len(eventsToSave, 2)
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, eventsToSave[0].GetEventType())
		s.Equal(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, eventsToSave[1].GetEventType())
		startChildEventAttributes := eventsToSave[1].GetStartChildWorkflowExecutionInitiatedEventAttributes()
		// Search attribute name was mapped and saved under field name.
		s.Equal(
			payload.EncodeString("search attribute value"),
			startChildEventAttributes.GetSearchAttributes().GetIndexedFields()["CustomTextField"])
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	s.mockShard.Resource.SearchAttributesMapper.EXPECT().
		GetFieldName("AliasForCustomTextField", tests.Namespace.String()).Return("CustomTextField", nil).
		Times(2) // One for validator, one for actual mapper

	_, err := s.historyEngine.RespondWorkflowTaskCompleted(metrics.AddMetricsContext(context.Background()), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: serializedTaskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	s.Nil(err)
}

func (s *engine2Suite) TestStartWorkflowExecution_BrandNew() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	requestID := uuid.New()
	resp, err := s.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: namespaceID.String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			WorkflowExecutionTimeout: timestamp.DurationPtr(20 * time.Second),
			WorkflowRunTimeout:       timestamp.DurationPtr(1 * time.Second),
			WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
		},
	})
	s.Nil(err)
	s.NotNil(resp.RunId)
}

func (s *engine2Suite) TestStartWorkflowExecution_BrandNew_SearchAttributes() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
		eventsToSave := request.NewWorkflowEvents[0].Events
		s.Len(eventsToSave, 2)
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, eventsToSave[0].GetEventType())
		startEventAttributes := eventsToSave[0].GetWorkflowExecutionStartedEventAttributes()
		// Search attribute name was mapped and saved under field name.
		s.Equal(
			payload.EncodeString("test"),
			startEventAttributes.GetSearchAttributes().GetIndexedFields()["CustomKeywordField"])
		return tests.CreateWorkflowExecutionResponse, nil
	})

	requestID := uuid.New()
	resp, err := s.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: namespaceID.String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			WorkflowExecutionTimeout: timestamp.DurationPtr(20 * time.Second),
			WorkflowRunTimeout:       timestamp.DurationPtr(1 * time.Second),
			WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": payload.EncodeString("test"),
			}}},
	})
	s.Nil(err)
	s.NotNil(resp.RunId)
}

func (s *engine2Suite) TestStartWorkflowExecution_StillRunning_Dedup() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	runID := "runID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	requestID := "requestID"
	lastWriteVersion := common.EmptyVersion

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &persistence.CurrentWorkflowConditionFailedError{
		Msg:              "random message",
		RequestID:        requestID,
		RunID:            runID,
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		LastWriteVersion: lastWriteVersion,
	})

	resp, err := s.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: namespaceID.String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
			WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
		},
	})
	s.Nil(err)
	s.Equal(runID, resp.GetRunId())
}

func (s *engine2Suite) TestStartWorkflowExecution_StillRunning_NonDeDup() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	runID := "runID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	lastWriteVersion := common.EmptyVersion

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &persistence.CurrentWorkflowConditionFailedError{
		Msg:              "random message",
		RequestID:        "oldRequestID",
		RunID:            runID,
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		LastWriteVersion: lastWriteVersion,
	})

	resp, err := s.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: namespaceID.String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
			WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
			Identity:                 identity,
			RequestId:                "newRequestID",
		},
	})
	if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); !ok {
		s.Fail("return err is not *serviceerror.WorkflowExecutionAlreadyStarted")
	}
	s.Nil(resp)
}

func (s *engine2Suite) TestStartWorkflowExecution_NotRunning_PrevSuccess() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	runID := "runID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	lastWriteVersion := common.EmptyVersion

	options := []enumspb.WorkflowIdReusePolicy{
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}

	expecedErrs := []bool{true, false, true}

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		newCreateWorkflowExecutionRequestMatcher(func(request *persistence.CreateWorkflowExecutionRequest) bool {
			return request.Mode == persistence.CreateWorkflowModeBrandNew
		}),
	).Return(nil, &persistence.CurrentWorkflowConditionFailedError{
		Msg:              "random message",
		RequestID:        "oldRequestID",
		RunID:            runID,
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		LastWriteVersion: lastWriteVersion,
	}).Times(len(expecedErrs))

	for index, option := range options {
		if !expecedErrs[index] {
			s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(
				gomock.Any(),
				newCreateWorkflowExecutionRequestMatcher(func(request *persistence.CreateWorkflowExecutionRequest) bool {
					return request.Mode == persistence.CreateWorkflowModeUpdateCurrent &&
						request.PreviousRunID == runID &&
						request.PreviousLastWriteVersion == lastWriteVersion
				}),
			).Return(tests.CreateWorkflowExecutionResponse, nil)
		}

		resp, err := s.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				Namespace:                namespaceID.String(),
				WorkflowId:               workflowID,
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
				WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
				Identity:                 identity,
				RequestId:                "newRequestID",
				WorkflowIdReusePolicy:    option,
			},
		})

		if expecedErrs[index] {
			if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); !ok {
				s.Fail("return err is not *serviceerror.WorkflowExecutionAlreadyStarted")
			}
			s.Nil(resp)
		} else {
			s.Nil(err)
			s.NotNil(resp)
		}
	}
}

func (s *engine2Suite) TestStartWorkflowExecution_NotRunning_PrevFail() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	lastWriteVersion := common.EmptyVersion

	options := []enumspb.WorkflowIdReusePolicy{
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}

	expecedErrs := []bool{false, false, true}

	statuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}
	runIDs := []string{"1", "2", "3", "4"}

	for i, status := range statuses {

		s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(
			gomock.Any(),
			newCreateWorkflowExecutionRequestMatcher(func(request *persistence.CreateWorkflowExecutionRequest) bool {
				return request.Mode == persistence.CreateWorkflowModeBrandNew
			}),
		).Return(nil, &persistence.CurrentWorkflowConditionFailedError{
			Msg:              "random message",
			RequestID:        "oldRequestID",
			RunID:            runIDs[i],
			State:            enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			Status:           status,
			LastWriteVersion: lastWriteVersion,
		}).Times(len(expecedErrs))

		for j, option := range options {

			if !expecedErrs[j] {
				s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(
					gomock.Any(),
					newCreateWorkflowExecutionRequestMatcher(func(request *persistence.CreateWorkflowExecutionRequest) bool {
						return request.Mode == persistence.CreateWorkflowModeUpdateCurrent &&
							request.PreviousRunID == runIDs[i] &&
							request.PreviousLastWriteVersion == lastWriteVersion
					}),
				).Return(tests.CreateWorkflowExecutionResponse, nil)
			}

			resp, err := s.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.StartWorkflowExecutionRequest{
				Attempt:     1,
				NamespaceId: namespaceID.String(),
				StartRequest: &workflowservice.StartWorkflowExecutionRequest{
					Namespace:                namespaceID.String(),
					WorkflowId:               workflowID,
					WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
					TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
					WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
					WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
					Identity:                 identity,
					RequestId:                "newRequestID",
					WorkflowIdReusePolicy:    option,
				},
			})

			if expecedErrs[j] {
				if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); !ok {
					s.Fail("return err is not *serviceerror.WorkflowExecutionAlreadyStarted")
				}
				s.Nil(resp)
			} else {
				s.Nil(err)
				s.NotNil(resp)
			}
		}
	}
}

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_JustSignal() {
	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
	s.EqualError(err, "Missing namespace UUID.")

	namespaceID := tests.NamespaceID
	workflowID := "wId"
	workflowType := "workflowType"
	runID := tests.RunID
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	requestID := uuid.New()
	sRequest = &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:    namespaceID.String(),
			WorkflowId:   workflowID,
			WorkflowType: &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
			Identity:     identity,
			SignalName:   signalName,
			Input:        input,
			RequestId:    requestID,
		},
	}

	msBuilder := workflow.TestLocalMutableState(s.historyEngine.shard, s.mockEventsCache, tests.LocalNamespaceEntry,
		log.NewTestLogger(), runID)
	addWorkflowExecutionStartedEvent(msBuilder, commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, identity)
	_ = addWorkflowTaskScheduledEvent(msBuilder)
	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: runID}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
	s.Nil(err)
	s.Equal(runID, resp.GetRunId())
}

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_WorkflowNotExist() {
	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
	s.EqualError(err, "Missing namespace UUID.")

	namespaceID := tests.NamespaceID
	workflowID := "wId"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	requestID := uuid.New()

	sRequest = &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
			WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
			Identity:                 identity,
			SignalName:               signalName,
			Input:                    input,
			RequestId:                requestID,
		},
	}

	notExistErr := serviceerror.NewNotFound("Workflow not exist")

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, notExistErr)
	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
	s.Nil(err)
	s.NotNil(resp.GetRunId())
}

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_WorkflowNotRunning() {
	we := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"

	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
	s.EqualError(err, "Missing namespace UUID.")

	namespaceID := tests.NamespaceID
	workflowID := "wId"
	runID := tests.RunID
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	requestID := uuid.New()
	sRequest = &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			Input:                    input,
			WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
			WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
			WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			SignalName:               signalName,
			SignalInput:              nil,
			Control:                  "",
			RetryPolicy:              nil,
			CronSchedule:             "",
			Memo:                     nil,
			SearchAttributes:         nil,
			Header:                   nil,
		},
	}

	msBuilder := workflow.TestLocalMutableState(s.historyEngine.shard, s.mockEventsCache, tests.LocalNamespaceEntry,
		log.NewTestLogger(), runID)
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	ms := workflow.TestCloneToProto(msBuilder)
	ms.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: runID}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
	s.Nil(err)
	s.NotNil(resp.GetRunId())
	s.NotEqual(runID, resp.GetRunId())
}

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_Start_DuplicateRequests() {
	namespaceID := tests.NamespaceID
	workflowID := "wId"
	runID := tests.RunID
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	requestID := "testRequestID"
	we := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			Input:                    input,
			WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
			WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
			WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			SignalName:               signalName,
			SignalInput:              nil,
			Control:                  "",
			RetryPolicy:              nil,
			CronSchedule:             "",
			Memo:                     nil,
			SearchAttributes:         nil,
			Header:                   nil,
		},
	}

	msBuilder := workflow.TestLocalMutableState(s.historyEngine.shard, s.mockEventsCache, tests.LocalNamespaceEntry,
		log.NewTestLogger(), runID)
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	ms := workflow.TestCloneToProto(msBuilder)
	ms.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: runID}
	workflowAlreadyStartedErr := &persistence.CurrentWorkflowConditionFailedError{
		Msg:              "random message",
		RequestID:        requestID, // use same requestID
		RunID:            runID,
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		LastWriteVersion: common.EmptyVersion,
	}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, workflowAlreadyStartedErr)

	ctx := metrics.AddMetricsContext(context.Background())
	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(ctx, sRequest)
	if err != nil {
		println("================================================================================================")
		println("================================================================================================")
		println("================================================================================================")
		println(err)
		println("================================================================================================")
		println("================================================================================================")
		println("================================================================================================")
	}
	s.Nil(err)
	s.NotNil(resp.GetRunId())
	s.Equal(runID, resp.GetRunId())
}

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_Start_WorkflowAlreadyStarted() {
	namespaceID := tests.NamespaceID
	workflowID := "wId"
	runID := tests.RunID
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	requestID := "testRequestID"
	we := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			Input:                    input,
			WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
			WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
			WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			SignalName:               signalName,
			SignalInput:              nil,
			Control:                  "",
			RetryPolicy:              nil,
			CronSchedule:             "",
			Memo:                     nil,
			SearchAttributes:         nil,
			Header:                   nil,
		},
	}

	msBuilder := workflow.TestLocalMutableState(s.historyEngine.shard, s.mockEventsCache, tests.LocalNamespaceEntry,
		log.NewTestLogger(), runID)
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	ms := workflow.TestCloneToProto(msBuilder)
	ms.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: runID}
	workflowAlreadyStartedErr := &persistence.CurrentWorkflowConditionFailedError{
		Msg:              "random message",
		RequestID:        "new request ID",
		RunID:            runID,
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		LastWriteVersion: common.EmptyVersion,
	}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, workflowAlreadyStartedErr)

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
	s.Nil(resp)
	s.NotNil(err)
}

func (s *engine2Suite) TestRecordChildExecutionCompleted() {
	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	request := &historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		CompletedExecution: &commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		CompletionEvent: &historypb.HistoryEvent{
			EventId:   456,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
				WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{},
			},
		},
		ParentInitiatedId:      123,
		ParentInitiatedVersion: 100,
	}

	msBuilder := workflow.TestGlobalMutableState(s.historyEngine.shard, s.mockEventsCache, log.NewTestLogger(), tests.Version, tests.RunID)
	addWorkflowExecutionStartedEvent(msBuilder, commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	// reload mutable state due to potential stale mutable state (initiated event not found)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil).Times(2)
	err := s.historyEngine.RecordChildExecutionCompleted(metrics.AddMetricsContext(context.Background()), request)
	s.IsType(&serviceerror.NotFound{}, err)

	// add child init event
	wt := addWorkflowTaskScheduledEvent(msBuilder)
	workflowTasksStartEvent := addWorkflowTaskStartedEvent(msBuilder, wt.ScheduledEventID, "testTaskQueue", uuid.New())
	wt.StartedEventID = workflowTasksStartEvent.GetEventId()
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(msBuilder, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	initiatedEvent, _ := addStartChildWorkflowExecutionInitiatedEvent(msBuilder, workflowTaskCompletedEvent.GetEventId(), uuid.New(),
		tests.ChildNamespace, tests.ChildNamespaceID, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second, enumspb.PARENT_CLOSE_POLICY_TERMINATE)
	request.ParentInitiatedId = initiatedEvent.GetEventId()
	request.ParentInitiatedVersion = initiatedEvent.GetVersion()

	// reload mutable state due to potential stale mutable state (started event not found)
	ms = workflow.TestCloneToProto(msBuilder)
	gwmsResponse = &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil).Times(2)
	err = s.historyEngine.RecordChildExecutionCompleted(metrics.AddMetricsContext(context.Background()), request)
	s.IsType(&serviceerror.NotFound{}, err)

	// add child started event
	addChildWorkflowExecutionStartedEvent(msBuilder, initiatedEvent.GetEventId(), childWorkflowID, childRunID, childWorkflowType, nil)

	ms = workflow.TestCloneToProto(msBuilder)
	gwmsResponse = &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	err = s.historyEngine.RecordChildExecutionCompleted(metrics.AddMetricsContext(context.Background()), request)
	s.NoError(err)
}

func (s *engine2Suite) TestVerifyChildExecutionCompletionRecorded_WorkflowNotExist() {

	request := &historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId: tests.ParentNamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
			WorkflowId: "child workflowId",
			RunId:      "child runId",
		},
		ParentInitiatedId:      123,
		ParentInitiatedVersion: 100,
	}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NotFound{})

	err := s.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engine2Suite) TestVerifyChildExecutionCompletionRecorded_WorkflowClosed() {

	request := &historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId: tests.ParentNamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
			WorkflowId: "child workflowId",
			RunId:      "child runId",
		},
		ParentInitiatedId:      123,
		ParentInitiatedVersion: 100,
	}

	msBuilder := workflow.TestGlobalMutableState(s.historyEngine.shard, s.mockEventsCache, log.NewTestLogger(), tests.Version, tests.RunID)
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

	err = s.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	s.NoError(err)
}

func (s *engine2Suite) TestVerifyChildExecutionCompletionRecorded_InitiatedEventNotFound() {

	request := &historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId: tests.NamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
			WorkflowId: "child workflowId",
			RunId:      "child runId",
		},
		ParentInitiatedId:      123,
		ParentInitiatedVersion: 100,
	}

	msBuilder := workflow.TestGlobalMutableState(s.historyEngine.shard, s.mockEventsCache, log.NewTestLogger(), tests.Version, tests.RunID)
	addWorkflowExecutionStartedEvent(msBuilder, commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	err := s.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	s.IsType(&serviceerror.WorkflowNotReady{}, err)
}

func (s *engine2Suite) TestVerifyChildExecutionCompletionRecorded_InitiatedEventFoundOnNonCurrentBranch() {

	inititatedVersion := tests.Version - 100
	request := &historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId: tests.NamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
			WorkflowId: "child workflowId",
			RunId:      "child runId",
		},
		ParentInitiatedId:      123,
		ParentInitiatedVersion: inititatedVersion,
	}

	msBuilder := workflow.TestGlobalMutableState(s.historyEngine.shard, s.mockEventsCache, log.NewTestLogger(), tests.Version, tests.RunID)
	addWorkflowExecutionStartedEvent(msBuilder, commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	msBuilder.GetExecutionInfo().VersionHistories = &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte{1, 2, 3},
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 100, Version: inititatedVersion},
					{EventId: 456, Version: tests.Version},
				},
			},
			{
				BranchToken: []byte{4, 5, 6},
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 456, Version: inititatedVersion},
				},
			},
		},
	}

	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	err := s.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engine2Suite) TestVerifyChildExecutionCompletionRecorded_InitiatedEventFoundOnCurrentBranch() {

	taskQueueName := "testTaskQueue"

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	msBuilder := workflow.TestGlobalMutableState(s.historyEngine.shard, s.mockEventsCache, log.NewTestLogger(), tests.Version, tests.RunID)
	addWorkflowExecutionStartedEvent(msBuilder, commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", taskQueueName, payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	wt := addWorkflowTaskScheduledEvent(msBuilder)
	workflowTasksStartEvent := addWorkflowTaskStartedEvent(msBuilder, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = workflowTasksStartEvent.GetEventId()
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(msBuilder, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	initiatedEvent, ci := addStartChildWorkflowExecutionInitiatedEvent(msBuilder, workflowTaskCompletedEvent.GetEventId(), uuid.New(),
		tests.ChildNamespace, tests.ChildNamespaceID, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second, enumspb.PARENT_CLOSE_POLICY_TERMINATE)

	request := &historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId: tests.NamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		ParentInitiatedId:      initiatedEvent.GetEventId(),
		ParentInitiatedVersion: initiatedEvent.GetVersion(),
	}

	// child workflow not started in mutable state
	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	err := s.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	s.IsType(&serviceerror.WorkflowNotReady{}, err)

	// child workflow started but not completed
	addChildWorkflowExecutionStartedEvent(msBuilder, initiatedEvent.GetEventId(), childWorkflowID, childRunID, childWorkflowType, nil)

	ms = workflow.TestCloneToProto(msBuilder)
	gwmsResponse = &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	err = s.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	s.IsType(&serviceerror.WorkflowNotReady{}, err)

	// child completion recorded
	addChildWorkflowExecutionCompletedEvent(
		msBuilder,
		ci.InitiatedEventId,
		&commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		&historypb.WorkflowExecutionCompletedEventAttributes{
			Result:                       payloads.EncodeString("some random child workflow execution result"),
			WorkflowTaskCompletedEventId: workflowTaskCompletedEvent.GetEventId(),
		},
	)

	ms = workflow.TestCloneToProto(msBuilder)
	gwmsResponse = &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	err = s.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	s.NoError(err)
}

func (s *engine2Suite) TestRefreshWorkflowTasks() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	msBuilder := workflow.TestGlobalMutableState(s.historyEngine.shard, s.mockEventsCache, log.NewTestLogger(), tests.Version, tests.RunID)
	startEvent := addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	startVersion := startEvent.GetVersion()
	_, err := msBuilder.AddTimeoutWorkflowEvent(
		msBuilder.GetNextEventID(),
		enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
		uuid.New(),
	)
	s.NoError(err)

	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).Return(nil)
	s.mockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		events.EventKey{
			NamespaceID: tests.NamespaceID,
			WorkflowID:  execution.GetWorkflowId(),
			RunID:       execution.GetRunId(),
			EventID:     common.FirstEventID,
			Version:     startVersion,
		},
		common.FirstEventID,
		gomock.Any(),
	).Return(startEvent, nil).AnyTimes()

	err = s.historyEngine.RefreshWorkflowTasks(metrics.AddMetricsContext(context.Background()), tests.NamespaceID, execution)
	s.NoError(err)
}

func (s *engine2Suite) getBuilder(namespaceID namespace.ID, we commonpb.WorkflowExecution) workflow.MutableState {
	weContext, release, err := s.workflowCache.GetOrCreateWorkflowExecution(
		metrics.AddMetricsContext(context.Background()),
		namespaceID,
		we,
		workflow.CallerTypeAPI,
	)
	if err != nil {
		return nil
	}
	defer release(nil)

	return weContext.(*workflow.ContextImpl).MutableState
}

type createWorkflowExecutionRequestMatcher struct {
	f func(request *persistence.CreateWorkflowExecutionRequest) bool
}

func newCreateWorkflowExecutionRequestMatcher(f func(request *persistence.CreateWorkflowExecutionRequest) bool) gomock.Matcher {
	return &createWorkflowExecutionRequestMatcher{
		f: f,
	}
}

func (m *createWorkflowExecutionRequestMatcher) Matches(x interface{}) bool {
	request, ok := x.(*persistence.CreateWorkflowExecutionRequest)
	if !ok {
		return false
	}
	return m.f(request)
}

func (m *createWorkflowExecutionRequestMatcher) String() string {
	return "CreateWorkflowExecutionRequest match condition"
}
