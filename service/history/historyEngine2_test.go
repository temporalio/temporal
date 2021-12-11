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
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
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

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockTxProcessor     *MocktransferQueueProcessor
		mockTimerProcessor  *MocktimerQueueProcessor
		mockEventsCache     *events.MockCache
		mockNamespaceCache  *namespace.MockRegistry
		mockClusterMetadata *cluster.MockMetadata

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

	s.mockTxProcessor = NewMocktransferQueueProcessor(s.controller)
	s.mockTimerProcessor = NewMocktimerQueueProcessor(s.controller)
	s.mockTxProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTimers(gomock.Any(), gomock.Any()).AnyTimes()

	s.config = tests.NewDynamicConfig()
	mockShard := shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:          1,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		s.config,
	)
	s.mockShard = mockShard
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata

	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String()}, &persistencespb.NamespaceConfig{}, "",
	), nil).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	historyCache := workflow.NewCache(s.mockShard)
	h := &historyEngineImpl{
		currentClusterName:     s.mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shard:                  s.mockShard,
		clusterMetadata:        s.mockClusterMetadata,
		executionManager:       s.mockExecutionMgr,
		historyCache:           historyCache,
		logger:                 s.logger,
		throttledLogger:        s.logger,
		metricsClient:          metrics.NewNoopMetricsClient(),
		tokenSerializer:        common.NewProtoTaskTokenSerializer(),
		config:                 s.config,
		timeSource:             s.mockShard.GetTimeSource(),
		eventNotifier:          events.NewNotifier(clock.NewRealTimeSource(), metrics.NewNoopMetricsClient(), func(namespace.ID, string) int32 { return 1 }),
		txProcessor:            s.mockTxProcessor,
		timerProcessor:         s.mockTimerProcessor,
		searchAttributesMapper: s.mockShard.Resource.SearchAttributesMapper,
		searchAttributesValidator: searchattribute.NewValidator(
			searchattribute.NewTestProvider(),
			s.mockShard.Resource.SearchAttributesMapper,
			s.config.SearchAttributesNumberOfKeysLimit,
			s.config.SearchAttributesSizeOfValueLimit,
			s.config.SearchAttributesTotalSizeLimit,
		),
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
	di := addWorkflowTaskScheduledEvent(msBuilder)

	ms := workflow.TestCloneToProto(msBuilder)

	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	request := historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &we,
		ScheduleId:        2,
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
	if executionInfo.LastWorkflowTaskStartId != common.EmptyEventID {
		expectedResponse.PreviousStartedEventId = executionInfo.LastWorkflowTaskStartId
	}
	expectedResponse.ScheduledEventId = di.ScheduleID
	expectedResponse.ScheduledTime = di.ScheduledTime
	expectedResponse.StartedEventId = di.ScheduleID + 1
	expectedResponse.StickyExecutionEnabled = true
	expectedResponse.NextEventId = msBuilder.GetNextEventID() + 1
	expectedResponse.Attempt = di.Attempt
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

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduleId:        2,
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

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(nil, errors.New("FAILED"))

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduleId:        2,
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
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &workflowExecution,
		ScheduleId:        2,
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

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &workflowExecution,
		ScheduleId:        2,
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

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(nil, &persistence.ConditionFailedError{})

	ms2 := workflow.TestCloneToProto(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse2, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &workflowExecution,
		ScheduleId:        2,
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

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(nil, &persistence.ConditionFailedError{})

	startedEventID := addWorkflowTaskStartedEventWithRequestID(msBuilder, int64(2), requestID, tl, identity)
	ms2 := workflow.TestCloneToProto(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse2, nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &workflowExecution,
		ScheduleId:        2,
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
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(nil, &persistence.ConditionFailedError{})

	// Add event.
	addWorkflowTaskStartedEventWithRequestID(msBuilder, int64(2), "some_other_req", tl, identity)
	ms2 := workflow.TestCloneToProto(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse2, nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &workflowExecution,
		ScheduleId:        2,
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

		s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)
	}

	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(nil,
		&persistence.ConditionFailedError{}).Times(conditionalRetryCount)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: &workflowExecution,
		ScheduleId:        2,
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
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	// load mutable state such that it already exists in memory when respond workflow task is called
	// this enables us to set query registry on it
	ctx, release, err := s.historyEngine.historyCache.GetOrCreateWorkflowExecution(
		metrics.AddMetricsContext(context.Background()),
		tests.NamespaceID,
		workflowExecution,
		workflow.CallerTypeAPI,
	)
	s.NoError(err)
	loadedMS, err := ctx.LoadWorkflowExecution()
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
		ScheduleId:        2,
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

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	response, err := s.historyEngine.RecordActivityTaskStarted(
		metrics.AddMetricsContext(context.Background()),
		&historyservice.RecordActivityTaskStartedRequest{
			NamespaceId:       namespaceID.String(),
			WorkflowExecution: workflowExecution,
			ScheduleId:        5,
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

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse1, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	s.mockEventsCache.EXPECT().GetEvent(
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
		ScheduleId:        5,
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

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse1, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

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

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse1, nil)

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

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(nil, &serviceerror.NotFound{})

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

func (s *engine2Suite) createExecutionStartedState(
	we commonpb.WorkflowExecution, tl string,
	identity string,
	startWorkflowTask bool,
) workflow.MutableState {
	msBuilder := workflow.TestLocalMutableState(s.historyEngine.shard, s.mockEventsCache, tests.LocalNamespaceEntry,
		s.logger, we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	di := addWorkflowTaskScheduledEvent(msBuilder)
	if startWorkflowTask {
		addWorkflowTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
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
		ScheduleAttempt: 1,
		WorkflowId:      "wId",
		RunId:           we.GetRunId(),
		ScheduleId:      2,
	}
	serializedTaskToken, _ := taskToken.Marshal()
	identity := "testIdentity"
	markerDetails := payloads.EncodeString("marker details")
	markerName := "marker name"

	msBuilder := workflow.TestLocalMutableState(s.historyEngine.shard, s.mockEventsCache, tests.LocalNamespaceEntry,
		log.NewTestLogger(), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	di := addWorkflowTaskScheduledEvent(msBuilder)
	addWorkflowTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

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

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

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
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastWorkflowTaskStartId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, executionBuilder.GetExecutionState().State)
	s.False(executionBuilder.HasPendingWorkflowTask())
}

func (s *engine2Suite) TestStartWorkflowExecution_BrandNew() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

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

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)
	s.mockShard.Resource.SearchAttributesMapper.EXPECT().GetFieldName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(alias string, namespace string) (string, error) {
			return strings.TrimPrefix(alias, "AliasFor"), nil
		}).Times(2)

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
				"AliasForCustomKeywordField": payload.EncodeString("test"),
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

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any()).Return(nil, &persistence.CurrentWorkflowConditionFailedError{
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

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any()).Return(nil, &persistence.CurrentWorkflowConditionFailedError{
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
				newCreateWorkflowExecutionRequestMatcher(func(request *persistence.CreateWorkflowExecutionRequest) bool {
					return request.Mode == persistence.CreateWorkflowModeWorkflowIDReuse &&
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
					newCreateWorkflowExecutionRequestMatcher(func(request *persistence.CreateWorkflowExecutionRequest) bool {
						return request.Mode == persistence.CreateWorkflowModeWorkflowIDReuse &&
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
	runID := tests.RunID
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	sRequest = &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:  namespaceID.String(),
			WorkflowId: workflowID,
			Identity:   identity,
			SignalName: signalName,
			Input:      input,
		},
	}

	msBuilder := workflow.TestLocalMutableState(s.historyEngine.shard, s.mockEventsCache, tests.LocalNamespaceEntry,
		log.NewTestLogger(), runID)
	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: runID}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

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

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any()).Return(nil, notExistErr)
	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

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

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

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

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any()).Return(nil, workflowAlreadyStartedErr)

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

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any()).Return(nil, workflowAlreadyStartedErr)

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
	s.Nil(resp)
	s.NotNil(err)
}

func (s *engine2Suite) getBuilder(namespaceID namespace.ID, we commonpb.WorkflowExecution) workflow.MutableState {
	weContext, release, err := s.historyEngine.historyCache.GetOrCreateWorkflowExecution(
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
