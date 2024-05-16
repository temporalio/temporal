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
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/protorequire"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"

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
		protorequire.ProtoAssertions

		controller               *gomock.Controller
		mockShard                *shard.ContextTest
		mockTxProcessor          *queues.MockQueue
		mockTimerProcessor       *queues.MockQueue
		mockVisibilityProcessor  *queues.MockQueue
		mockArchivalProcessor    *queues.MockQueue
		mockMemoryScheduledQueue *queues.MockQueue
		mockEventsCache          *events.MockCache
		mockNamespaceCache       *namespace.MockRegistry
		mockClusterMetadata      *cluster.MockMetadata
		mockVisibilityManager    *manager.MockVisibilityManager

		workflowCache    wcache.Cache
		historyEngine    *historyEngineImpl
		mockExecutionMgr *persistence.MockExecutionManager

		config        *configs.Config
		logger        *log.MockLogger
		errorMessages []string
	}
)

func TestEngine2Suite(t *testing.T) {
	s := new(engine2Suite)
	suite.Run(t, s)
}

func (s *engine2Suite) SetupSuite() {}

func (s *engine2Suite) TearDownSuite() {}

func (s *engine2Suite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.mockTxProcessor = queues.NewMockQueue(s.controller)
	s.mockTimerProcessor = queues.NewMockQueue(s.controller)
	s.mockVisibilityProcessor = queues.NewMockQueue(s.controller)
	s.mockArchivalProcessor = queues.NewMockQueue(s.controller)
	s.mockMemoryScheduledQueue = queues.NewMockQueue(s.controller)
	s.mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	s.mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	s.mockVisibilityProcessor.EXPECT().Category().Return(tasks.CategoryVisibility).AnyTimes()
	s.mockArchivalProcessor.EXPECT().Category().Return(tasks.CategoryArchival).AnyTimes()
	s.mockMemoryScheduledQueue.EXPECT().Category().Return(tasks.CategoryMemoryTimer).AnyTimes()
	s.mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockVisibilityProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockArchivalProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockMemoryScheduledQueue.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()

	s.config = tests.NewDynamicConfig()
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		s.config,
	)
	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	mockShard.SetStateMachineRegistry(reg)

	s.mockShard = mockShard
	s.mockShard.Resource.ShardMgr.EXPECT().AssertShardOwnership(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockVisibilityManager = s.mockShard.Resource.VisibilityManager

	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ParentNamespaceID).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ChildNamespace).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(tests.Version, tests.Version).Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockVisibilityManager.EXPECT().GetIndexName().Return("").AnyTimes()
	s.mockVisibilityManager.EXPECT().
		ValidateCustomSearchAttributes(gomock.Any()).
		DoAndReturn(
			func(searchAttributes map[string]any) (map[string]any, error) {
				return searchAttributes, nil
			},
		).
		AnyTimes()
	s.workflowCache = wcache.NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler)
	s.logger = log.NewMockLogger(s.controller)
	s.logger.EXPECT().Debug(gomock.Any(), gomock.Any()).AnyTimes()
	s.logger.EXPECT().Info(gomock.Any(), gomock.Any()).AnyTimes()
	s.logger.EXPECT().Warn(gomock.Any(), gomock.Any()).AnyTimes()
	s.errorMessages = make([]string, 0)
	s.logger.EXPECT().Error(gomock.Any(), gomock.Any()).AnyTimes().Do(func(msg string, tags ...tag.Tag) {
		s.errorMessages = append(s.errorMessages, msg)
	})

	h := &historyEngineImpl{
		currentClusterName: s.mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       s.mockShard,
		clusterMetadata:    s.mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		logger:             s.logger,
		throttledLogger:    s.logger,
		metricsHandler:     metrics.NoopMetricsHandler,
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		config:             s.config,
		timeSource:         s.mockShard.GetTimeSource(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			s.mockArchivalProcessor.Category():    s.mockArchivalProcessor,
			s.mockTxProcessor.Category():          s.mockTxProcessor,
			s.mockTimerProcessor.Category():       s.mockTimerProcessor,
			s.mockVisibilityProcessor.Category():  s.mockVisibilityProcessor,
			s.mockMemoryScheduledQueue.Category(): s.mockMemoryScheduledQueue,
		},
		searchAttributesValidator: searchattribute.NewValidator(
			searchattribute.NewTestProvider(),
			s.mockShard.Resource.SearchAttributesMapperProvider,
			s.config.SearchAttributesNumberOfKeysLimit,
			s.config.SearchAttributesSizeOfValueLimit,
			s.config.SearchAttributesTotalSizeLimit,
			s.mockVisibilityManager,
			dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
			dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		),
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(mockShard, s.workflowCache),
		persistenceVisibilityMgr:   s.mockVisibilityManager,
	}
	s.mockShard.SetEngineForTesting(h)

	s.historyEngine = h
}

func (s *engine2Suite) SetupSubTest() {
	s.SetupTest()
}

func (s *engine2Suite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *engine2Suite) TearDownSubTest() {
	s.TearDownTest()
}

func (s *engine2Suite) TestRecordWorkflowTaskStartedSuccessStickyEnabled() {
	fakeHistory := []*historypb.HistoryEvent{
		{
			EventId:   int64(1),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		},
		{
			EventId:   int64(2),
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
		{
			EventId:   int64(3),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		},
	}

	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: fakeHistory,
		NextPageToken: []byte{},
		Size:          1,
	}, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceName(tests.NamespaceID).Return(tests.Namespace, nil)
	s.mockShard.Resource.SearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap, nil)
	s.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()

	namespaceID := tests.NamespaceID
	we := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	stickyTl := "stickyTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
		we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	executionInfo := ms.GetExecutionInfo()
	executionInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	executionInfo.StickyTaskQueue = stickyTl

	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)

	wfMs := workflow.TestCloneToProto(ms)

	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	request := historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: we,
		ScheduledEventId:  2,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: stickyTl,
			},
			Identity: identity,
		},
	}

	expectedResponse := historyservice.RecordWorkflowTaskStartedResponse{}
	expectedResponse.WorkflowType = ms.GetWorkflowType()
	executionInfo = ms.GetExecutionInfo()
	if executionInfo.LastWorkflowTaskStartedEventId != common.EmptyEventID {
		expectedResponse.PreviousStartedEventId = executionInfo.LastWorkflowTaskStartedEventId
	}
	expectedResponse.Version = tests.GlobalNamespaceEntry.FailoverVersion()
	expectedResponse.ScheduledEventId = wt.ScheduledEventID
	expectedResponse.ScheduledTime = timestamppb.New(wt.ScheduledTime)
	expectedResponse.StartedEventId = wt.ScheduledEventID + 1
	expectedResponse.StickyExecutionEnabled = true
	expectedResponse.NextEventId = ms.GetNextEventID() + 1
	expectedResponse.Attempt = wt.Attempt
	expectedResponse.WorkflowExecutionTaskQueue = &taskqueuepb.TaskQueue{
		Name: executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	currentBranchTokken, err := ms.GetCurrentBranchToken()
	s.NoError(err)
	expectedResponse.BranchToken = currentBranchTokken
	expectedResponse.History = &historypb.History{Events: fakeHistory}
	expectedResponse.NextPageToken = nil

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &request)
	s.Nil(err)
	s.NotNil(response)
	s.True(response.StartedTime.AsTime().After(expectedResponse.ScheduledTime.AsTime()))
	expectedResponse.StartedTime = response.StartedTime
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

func (s *engine2Suite) TestRecordWorkflowTaskStarted_NoMessages() {
	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	tl := "testTaskQueue"
	identity := "testIdentity"

	ms := s.createExecutionStartedState(workflowExecution, tl, identity, false, false)
	// Use UpdateCurrentVersion explicitly here,
	// because there is no call to CloseTransactionAsSnapshot,
	// because it converts speculative WT to normal, but WT needs to be speculative for this test.
	err := ms.UpdateCurrentVersion(tests.GlobalNamespaceEntry.FailoverVersion(), true)
	s.NoError(err)

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
			wfMs := ms.CloneToProto()
			gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
			return gwmsResponse, nil
		},
	)

	wt, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE)
	s.NoError(err)
	s.NotNil(wt)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  wt.ScheduledEventID,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})

	s.Nil(response)
	s.Error(err)
	s.IsType(&serviceerror.NotFound{}, err, err.Error())
	s.EqualError(err, "No messages for speculative workflow task.")
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
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	ms := s.createExecutionStartedState(workflowExecution, tl, identity, true, true)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  2,
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
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	ms := s.createExecutionStartedState(workflowExecution, tl, identity, true, true)
	addWorkflowTaskCompletedEvent(&s.Suite, ms, int64(2), int64(3), identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  2,
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
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	tl := "testTaskQueue"
	identity := "testIdentity"

	ms := s.createExecutionStartedState(workflowExecution, tl, identity, true, false)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &persistence.ConditionFailedError{})

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  2,
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
	s.Equal(&persistence.ConditionFailedError{}, err)
}

func (s *engine2Suite) TestRecordWorkflowTaskStartedSuccess() {
	fakeHistory := []*historypb.HistoryEvent{
		{
			EventId:   int64(1),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		},
		{
			EventId:   int64(2),
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
		{
			EventId:   int64(3),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		},
	}

	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: fakeHistory,
		NextPageToken: []byte{},
		Size:          1,
	}, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceName(tests.NamespaceID).Return(tests.Namespace, nil)
	s.mockShard.Resource.SearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap, nil)
	s.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	tl := "testTaskQueue"
	identity := "testIdentity"

	ms := s.createExecutionStartedState(workflowExecution, tl, identity, true, false)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	// load mutable state such that it already exists in memory when respond workflow task is called
	// this enables us to set query registry on it
	ctx, release, err := s.workflowCache.GetOrCreateWorkflowExecution(
		metrics.AddMetricsContext(context.Background()),
		s.mockShard,
		tests.NamespaceID,
		workflowExecution,
		workflow.LockPriorityHigh,
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

	response, err := s.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  2,
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
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := s.createExecutionStartedState(workflowExecution, tl, identity, true, true)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, int64(2), int64(3), identity)
	scheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)

	ms1 := workflow.TestCloneToProto(ms)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	s.mockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
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
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  5,
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
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	ms := s.createExecutionStartedState(workflowExecution, tl, identity, true, false)
	ms1 := workflow.TestCloneToProto(ms)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := s.historyEngine.RequestCancelWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.RequestCancelWorkflowExecutionRequest{
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

	ms2 := s.getMutableState(namespaceID, workflowExecution)
	s.Equal(int64(4), ms2.GetNextEventID())
}

func (s *engine2Suite) TestRequestCancelWorkflowExecution_Finished() {
	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	ms := s.createExecutionStartedState(workflowExecution, tl, identity, true, false)
	ms.GetExecutionState().State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	ms1 := workflow.TestCloneToProto(ms)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)

	_, err := s.historyEngine.RequestCancelWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.RequestCancelWorkflowExecutionRequest{
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
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	_, err := s.historyEngine.RequestCancelWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.RequestCancelWorkflowExecutionRequest{
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
	workflowExecution := &commonpb.WorkflowExecution{
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

	ms := s.createExecutionStartedStateWithParent(workflowExecution, tl, parentInfo, identity, true, false)
	ms1 := workflow.TestCloneToProto(ms)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)

	_, err := s.historyEngine.RequestCancelWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.RequestCancelWorkflowExecutionRequest{
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
	workflowExecution := &commonpb.WorkflowExecution{
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

	ms := s.createExecutionStartedStateWithParent(workflowExecution, tl, parentInfo, identity, true, false)
	ms1 := workflow.TestCloneToProto(ms)
	currentExecutionResp := &persistence.GetCurrentExecutionResponse{
		RunID: tests.RunID,
	}
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(currentExecutionResp, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)

	_, err := s.historyEngine.TerminateWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.TerminateWorkflowExecutionRequest{
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

func (s *engine2Suite) createExecutionStartedState(we *commonpb.WorkflowExecution, tl string, identity string, scheduleWorkflowTask bool, startWorkflowTask bool) workflow.MutableState {
	return s.createExecutionStartedStateWithParent(we, tl, nil, identity, scheduleWorkflowTask, startWorkflowTask)
}

func (s *engine2Suite) createExecutionStartedStateWithParent(we *commonpb.WorkflowExecution, tl string, parentInfo *workflowspb.ParentExecutionInfo, identity string, scheduleWorkflowTask bool, startWorkflowTask bool) workflow.MutableState {
	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
		we.GetWorkflowId(), we.GetRunId(), s.logger)
	addWorkflowExecutionStartedEventWithParent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, parentInfo, identity)
	var wt *workflow.WorkflowTaskInfo
	if scheduleWorkflowTask {
		wt = addWorkflowTaskScheduledEvent(ms)
	}
	if wt != nil && startWorkflowTask {
		addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	}
	_ = ms.SetHistoryTree(nil, nil, we.GetRunId())
	versionHistory, _ := versionhistory.GetCurrentVersionHistory(
		ms.GetExecutionInfo().VersionHistories,
	)
	_ = versionhistory.AddOrUpdateVersionHistoryItem(
		versionHistory,
		versionhistory.NewVersionHistoryItem(0, 0),
	)

	return ms
}

func (s *engine2Suite) TestRespondWorkflowTaskCompletedRecordMarkerCommand() {
	namespaceID := tests.NamespaceID
	we := &commonpb.WorkflowExecution{
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

	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
		we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
		Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
			MarkerName: markerName,
			Details: map[string]*commonpb.Payloads{
				"data": markerDetails,
			},
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

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
	ms2 := s.getMutableState(namespaceID, we)
	s.Equal(int64(6), ms2.GetNextEventID())
	s.Equal(int64(3), ms2.GetExecutionInfo().LastWorkflowTaskStartedEventId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	s.False(ms2.HasPendingWorkflowTask())
}

func (s *engine2Suite) TestRespondWorkflowTaskCompleted_StartChildWithSearchAttributes() {
	namespaceID := tests.NamespaceID
	we := &commonpb.WorkflowExecution{
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

	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
		we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, nil, 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

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

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		eventsToSave := request.UpdateWorkflowEvents[0].Events
		s.Len(eventsToSave, 2)
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, eventsToSave[0].GetEventType())
		s.Equal(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, eventsToSave[1].GetEventType())
		startChildEventAttributes := eventsToSave[1].GetStartChildWorkflowExecutionInitiatedEventAttributes()
		// Search attribute name was mapped and saved under field name.
		s.ProtoEqual(
			payload.EncodeString("search attribute value"),
			startChildEventAttributes.GetSearchAttributes().GetIndexedFields()["CustomTextField"])
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	s.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().
		GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil)

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

func (s *engine2Suite) TestRespondWorkflowTaskCompleted_StartChildWorkflow_ExceedsLimit() {
	namespaceID := tests.NamespaceID
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	workflowType := "testWorkflowType"

	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	ms := workflow.TestLocalMutableState(
		s.historyEngine.shardContext,
		s.mockEventsCache,
		tests.LocalNamespaceEntry,
		we.GetWorkflowId(),
		we.GetRunId(),
		log.NewTestLogger(),
	)

	addWorkflowExecutionStartedEvent(
		ms,
		we,
		workflowType,
		taskQueue,
		nil,
		time.Minute,
		time.Minute,
		time.Minute,
		identity,
	)

	s.mockShard.SetLoggers(s.logger)
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	var commands []*commandpb.Command
	for i := 0; i < 6; i++ {
		commands = append(
			commands,
			&commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						Namespace:    tests.Namespace.String(),
						WorkflowId:   tests.WorkflowID,
						WorkflowType: &commonpb.WorkflowType{Name: workflowType},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
					}},
			},
		)
	}

	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(
		ms,
		wt.ScheduledEventID,
		taskQueue,
		identity,
	)
	taskToken := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	taskTokenBytes, _ := taskToken.Marshal()
	response := &persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(ms)}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(response, nil).AnyTimes()
	s.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().
		GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).
		AnyTimes()
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	s.historyEngine.shardContext.GetConfig().NumPendingChildExecutionsLimit = func(namespace string) int {
		return 5
	}
	_, err := s.historyEngine.RespondWorkflowTaskCompleted(metrics.AddMetricsContext(context.Background()), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskTokenBytes,
			Commands:  commands,
			Identity:  identity,
		},
	})

	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Len(s.errorMessages, 1)
	s.Equal("the number of pending child workflow executions, 5, has reached the per-workflow limit of 5", s.errorMessages[0])
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
			WorkflowExecutionTimeout: durationpb.New(20 * time.Second),
			WorkflowRunTimeout:       durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
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
		s.ProtoEqual(
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
			WorkflowExecutionTimeout: durationpb.New(20 * time.Second),
			WorkflowRunTimeout:       durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": payload.EncodeString("test"),
			}}},
	})
	s.Nil(err)
	s.NotNil(resp.RunId)
}

func (s *engine2Suite) TestStartWorkflowExecution_Dedup() {
	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	prevRunID := "prevRunID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	requestID := "requestID"
	prevRequestID := "oldRequestID"
	lastWriteVersion := common.EmptyVersion

	brandNewExecutionRequest := mock.MatchedBy(func(request *persistence.CreateWorkflowExecutionRequest) bool {
		return request.Mode == persistence.CreateWorkflowModeBrandNew
	})

	makeStartRequest := func(
		wfReusePolicy enumspb.WorkflowIdReusePolicy,
		wfConflictPolicy enumspb.WorkflowIdConflictPolicy,
	) *historyservice.StartWorkflowExecutionRequest {
		return &historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				Namespace:                namespaceID.String(),
				WorkflowId:               workflowID,
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
				WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
				WorkflowIdReusePolicy:    wfReusePolicy,
				WorkflowIdConflictPolicy: wfConflictPolicy,
				Identity:                 identity,
				RequestId:                requestID,
			},
		}
	}

	s.Run("when workflow is running", func() {
		makeCurrentWorkflowConditionFailedError := func(
			requestID string,
		) *persistence.CurrentWorkflowConditionFailedError {
			return &persistence.CurrentWorkflowConditionFailedError{
				Msg:              "random message",
				RequestID:        requestID,
				RunID:            prevRunID,
				State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				LastWriteVersion: lastWriteVersion,
			}
		}

		s.Run("ignore error when request ID is the same", func() {
			s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
				Return(nil, makeCurrentWorkflowConditionFailedError(requestID)) // *same* request ID!

			resp, err := s.historyEngine.StartWorkflowExecution(
				metrics.AddMetricsContext(context.Background()),
				makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

			s.NoError(err)
			s.True(resp.Started)
			s.Equal(prevRunID, resp.GetRunId())
		})

		s.Run("return error when id conflict policy is POLICY_FAIL", func() {
			s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
				Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))

			resp, err := s.historyEngine.StartWorkflowExecution(
				metrics.AddMetricsContext(context.Background()),
				makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

			var expectedErr *serviceerror.WorkflowExecutionAlreadyStarted
			s.ErrorAs(err, &expectedErr)
			s.Nil(resp)
		})

		s.Run("ignore error when id conflict policy is USE_EXISTING", func() {
			s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
				Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))

			resp, err := s.historyEngine.StartWorkflowExecution(
				metrics.AddMetricsContext(context.Background()),
				makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING))

			s.NoError(err)
			s.False(resp.Started)
			s.Equal(prevRunID, resp.GetRunId())
		})

		s.Run("terminate workflow when", func() {
			expectWorkflowTerminate := func() {
				failedError := makeCurrentWorkflowConditionFailedError(prevRequestID)
				failedError.RunID = uuid.New()
				s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
					Return(nil, failedError)

				ms := workflow.TestGlobalMutableState(s.historyEngine.shardContext, s.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
				ms.GetExecutionInfo().VersionHistories.Histories[0].Items = []*historyspb.VersionHistoryItem{{Version: 0, EventId: 0}}

				s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
					Return(&persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(ms)}, nil)
				s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(
					gomock.Any(),
					mock.MatchedBy(func(req *persistence.UpdateWorkflowExecutionRequest) bool {
						return req.UpdateWorkflowMutation.ExecutionState.Status == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
					}),
				).Return(&persistence.UpdateWorkflowExecutionResponse{
					UpdateMutableStateStats: persistence.MutableStateStatistics{
						HistoryStatistics: &persistence.HistoryStatistics{SizeDiff: 1},
					},
				}, nil)
			}

			s.Run("id conflict policy is TERMINATE_EXISTING", func() {
				expectWorkflowTerminate()

				resp, err := s.historyEngine.StartWorkflowExecution(
					metrics.AddMetricsContext(context.Background()),
					makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED, enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING))

				s.NoError(err)
				s.True(resp.Started)
				s.NotEqual(prevRunID, resp.GetRunId())
			})

			s.Run("id reuse policy is TERMINATE_IF_RUNNING", func() {
				expectWorkflowTerminate()

				resp, err := s.historyEngine.StartWorkflowExecution(
					metrics.AddMetricsContext(context.Background()),
					makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING, enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED))

				s.NoError(err)
				s.True(resp.Started)
				s.NotEqual(prevRunID, resp.GetRunId())
			})
		})
	})

	s.Run("when workflow completed", func() {
		makeCurrentWorkflowConditionFailedError := func(
			requestID string,
		) *persistence.CurrentWorkflowConditionFailedError {
			return &persistence.CurrentWorkflowConditionFailedError{
				Msg:              "random message",
				RequestID:        requestID,
				RunID:            prevRunID,
				State:            enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				LastWriteVersion: lastWriteVersion,
			}
		}

		updateExecutionRequest := mock.MatchedBy(func(request *persistence.CreateWorkflowExecutionRequest) bool {
			return request.Mode == persistence.CreateWorkflowModeUpdateCurrent &&
				request.PreviousRunID == prevRunID &&
				request.PreviousLastWriteVersion == lastWriteVersion
		})

		s.Run("ignore error when request ID is the same", func() {
			s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
				Return(nil, makeCurrentWorkflowConditionFailedError(requestID)) // *same* request ID!

			resp, err := s.historyEngine.StartWorkflowExecution(
				metrics.AddMetricsContext(context.Background()),
				makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

			s.NoError(err)
			s.Equal(prevRunID, resp.GetRunId())
		})

		s.Run("with success", func() {
			s.Run("and id reuse policy is ALLOW_DUPLICATE", func() {
				s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
					Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))
				s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), updateExecutionRequest).
					Return(tests.CreateWorkflowExecutionResponse, nil)

				resp, err := s.historyEngine.StartWorkflowExecution(
					metrics.AddMetricsContext(context.Background()),
					makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

				s.NoError(err)
				s.True(resp.Started)
				s.NotEqual(prevRunID, resp.GetRunId())
			})

			s.Run("and id reuse policy is TERMINATE_IF_RUNNING", func() {
				s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
					Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))
				s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), updateExecutionRequest).
					Return(tests.CreateWorkflowExecutionResponse, nil)

				resp, err := s.historyEngine.StartWorkflowExecution(
					metrics.AddMetricsContext(context.Background()),
					makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

				s.NoError(err)
				s.True(resp.Started)
				s.NotEqual(prevRunID, resp.GetRunId())
			})

			s.Run("and id reuse policy ALLOW_DUPLICATE_FAILED_ONLY", func() {
				s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
					Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))

				resp, err := s.historyEngine.StartWorkflowExecution(
					metrics.AddMetricsContext(context.Background()),
					makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

				var expectedErr *serviceerror.WorkflowExecutionAlreadyStarted
				s.ErrorAs(err, &expectedErr)
				s.Nil(resp)
			})

			s.Run("and id reuse policy REJECT_DUPLICATE", func() {
				s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
					Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))

				resp, err := s.historyEngine.StartWorkflowExecution(
					metrics.AddMetricsContext(context.Background()),
					makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

				var expectedErr *serviceerror.WorkflowExecutionAlreadyStarted
				s.ErrorAs(err, &expectedErr)
				s.Nil(resp)
			})
		})

		s.Run("with failure", func() {
			failureStatuses := []enumspb.WorkflowExecutionStatus{
				enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
				enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
				enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
				enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
			}

			for _, status := range failureStatuses {
				makeCurrentWorkflowConditionFailedError := func(
					requestID string,
				) *persistence.CurrentWorkflowConditionFailedError {
					return &persistence.CurrentWorkflowConditionFailedError{
						Msg:              "random message",
						RequestID:        requestID,
						RunID:            prevRunID,
						State:            enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
						Status:           status,
						LastWriteVersion: lastWriteVersion,
					}
				}

				s.Run(fmt.Sprintf("status %v", status), func() {
					s.Run("and id reuse policy ALLOW_DUPLICATE", func() {
						s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
							Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))

						resp, err := s.historyEngine.StartWorkflowExecution(
							metrics.AddMetricsContext(context.Background()),
							makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

						var expectedErr *serviceerror.WorkflowExecutionAlreadyStarted
						s.ErrorAs(err, &expectedErr)
						s.Nil(resp)
					})

					s.Run("and id reuse policy ALLOW_DUPLICATE_FAILED_ONLY", func() {
						s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
							Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))
						s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), updateExecutionRequest).
							Return(tests.CreateWorkflowExecutionResponse, nil)

						resp, err := s.historyEngine.StartWorkflowExecution(
							metrics.AddMetricsContext(context.Background()),
							makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

						s.NoError(err)
						s.True(resp.Started)
						s.NotEqual(prevRunID, resp.GetRunId())
					})

					s.Run("and id reuse policy REJECT_DUPLICATE", func() {
						s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
							Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))

						resp, err := s.historyEngine.StartWorkflowExecution(
							metrics.AddMetricsContext(context.Background()),
							makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

						var expectedErr *serviceerror.WorkflowExecutionAlreadyStarted
						s.ErrorAs(err, &expectedErr)
						s.Nil(resp)
					})
				})
			}
		})
	})
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

	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
		workflowID, runID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, identity)
	_ = addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
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
			WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
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
	we := &commonpb.WorkflowExecution{
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
			WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
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

	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
		workflowID, runID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
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
	we := &commonpb.WorkflowExecution{
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
			WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
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

	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
		workflowID, runID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
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
	s.NoError(err)
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
	we := &commonpb.WorkflowExecution{
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
			WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
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

	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
		workflowID, runID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
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
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
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

	ms := workflow.TestGlobalMutableState(s.historyEngine.shardContext, s.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	// reload mutable state due to potential stale mutable state (initiated event not found)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil).Times(2)
	_, err := s.historyEngine.RecordChildExecutionCompleted(metrics.AddMetricsContext(context.Background()), request)
	s.IsType(&serviceerror.NotFound{}, err)

	// add child init event
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTasksStartEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, "testTaskQueue", uuid.New())
	wt.StartedEventID = workflowTasksStartEvent.GetEventId()
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	initiatedEvent, _ := addStartChildWorkflowExecutionInitiatedEvent(ms, workflowTaskCompletedEvent.GetEventId(), uuid.New(),
		tests.ChildNamespace, tests.ChildNamespaceID, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second, enumspb.PARENT_CLOSE_POLICY_TERMINATE)
	request.ParentInitiatedId = initiatedEvent.GetEventId()
	request.ParentInitiatedVersion = initiatedEvent.GetVersion()

	// add child started event
	addChildWorkflowExecutionStartedEvent(ms, initiatedEvent.GetEventId(), childWorkflowID, childRunID, childWorkflowType, nil)

	wfMs = workflow.TestCloneToProto(ms)
	gwmsResponse = &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	_, err = s.historyEngine.RecordChildExecutionCompleted(metrics.AddMetricsContext(context.Background()), request)
	s.NoError(err)
}

func (s *engine2Suite) TestRecordChildExecutionCompleted_MissingChildStartedEvent() {
	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	request := &historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
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

	ms := workflow.TestGlobalMutableState(s.historyEngine.shardContext, s.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	// add child init event
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTasksStartEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, "testTaskQueue", uuid.New())
	wt.StartedEventID = workflowTasksStartEvent.GetEventId()
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	initiatedEvent, _ := addStartChildWorkflowExecutionInitiatedEvent(ms, workflowTaskCompletedEvent.GetEventId(), uuid.New(),
		tests.ChildNamespace, tests.ChildNamespaceID, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second, enumspb.PARENT_CLOSE_POLICY_TERMINATE)
	request.ParentInitiatedId = initiatedEvent.GetEventId()
	request.ParentInitiatedVersion = initiatedEvent.GetVersion()

	// started event not found, should automatically be added
	wfMs = workflow.TestCloneToProto(ms)
	gwmsResponse = &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockEventsCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(initiatedEvent, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	_, err := s.historyEngine.RecordChildExecutionCompleted(metrics.AddMetricsContext(context.Background()), request)
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

	_, err := s.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
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

	ms := workflow.TestGlobalMutableState(s.historyEngine.shardContext, s.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	_, err := ms.AddTimeoutWorkflowEvent(
		ms.GetNextEventID(),
		enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
		uuid.New(),
	)
	s.NoError(err)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err = s.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
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

	ms := workflow.TestGlobalMutableState(s.historyEngine.shardContext, s.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
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

	ms := workflow.TestGlobalMutableState(s.historyEngine.shardContext, s.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	ms.GetExecutionInfo().VersionHistories = &historyspb.VersionHistories{
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

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engine2Suite) TestVerifyChildExecutionCompletionRecorded_InitiatedEventFoundOnCurrentBranch() {

	taskQueueName := "testTaskQueue"

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	ms := workflow.TestGlobalMutableState(s.historyEngine.shardContext, s.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", taskQueueName, payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTasksStartEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = workflowTasksStartEvent.GetEventId()
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&s.Suite, ms, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	initiatedEvent, ci := addStartChildWorkflowExecutionInitiatedEvent(ms, workflowTaskCompletedEvent.GetEventId(), uuid.New(),
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
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := s.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	s.IsType(&serviceerror.WorkflowNotReady{}, err)

	// child workflow started but not completed
	addChildWorkflowExecutionStartedEvent(ms, initiatedEvent.GetEventId(), childWorkflowID, childRunID, childWorkflowType, nil)

	wfMs = workflow.TestCloneToProto(ms)
	gwmsResponse = &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err = s.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	s.IsType(&serviceerror.WorkflowNotReady{}, err)

	// child completion recorded
	addChildWorkflowExecutionCompletedEvent(
		ms,
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

	wfMs = workflow.TestCloneToProto(ms)
	gwmsResponse = &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err = s.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	s.NoError(err)
}

func (s *engine2Suite) TestRefreshWorkflowTasks() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	ms := workflow.TestGlobalMutableState(s.historyEngine.shardContext, s.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	startEvent := addWorkflowExecutionStartedEvent(ms, execution, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	startVersion := startEvent.GetVersion()
	timeoutEvent, err := ms.AddTimeoutWorkflowEvent(
		ms.GetNextEventID(),
		enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
		uuid.New(),
	)
	s.NoError(err)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).Return(nil)
	s.mockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
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
	s.mockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		gomock.Any(),
		events.EventKey{
			NamespaceID: tests.NamespaceID,
			WorkflowID:  execution.GetWorkflowId(),
			RunID:       execution.GetRunId(),
			EventID:     timeoutEvent.GetEventId(),
			Version:     startVersion,
		},
		timeoutEvent.GetEventId(),
		gomock.Any(),
	).Return(startEvent, nil).AnyTimes()

	err = s.historyEngine.RefreshWorkflowTasks(metrics.AddMetricsContext(context.Background()), tests.NamespaceID, execution)
	s.NoError(err)
}

func (s *engine2Suite) getMutableState(namespaceID namespace.ID, we *commonpb.WorkflowExecution) workflow.MutableState {
	weContext, release, err := s.workflowCache.GetOrCreateWorkflowExecution(
		metrics.AddMetricsContext(context.Background()),
		s.mockShard,
		namespaceID,
		we,
		workflow.LockPriorityHigh,
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
