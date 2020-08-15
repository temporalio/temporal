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
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

const (
	testShardID = 1
)

type (
	historyReplicatorSuite struct {
		suite.Suite
		*require.Assertions

		controller               *gomock.Controller
		mockShard                *shardContextTest
		mockWorkflowResetor      *MockworkflowResetor
		mockTxProcessor          *MocktransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MocktimerQueueProcessor
		mockStateBuilder         *MockstateBuilder
		mockNamespaceCache       *cache.MockNamespaceCache
		mockClusterMetadata      *cluster.MockMetadata

		logger           log.Logger
		mockExecutionMgr *mocks.ExecutionManager
		mockHistoryV2Mgr *mocks.HistoryV2Manager
		mockShardManager *mocks.ShardManager

		historyReplicator *historyReplicator
	}
)

func TestHistoryReplicatorSuite(t *testing.T) {
	s := new(historyReplicatorSuite)
	suite.Run(t, s)
}

func (s *historyReplicatorSuite) SetupSuite() {

}

func (s *historyReplicatorSuite) TearDownSuite() {

}

func (s *historyReplicatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockWorkflowResetor = NewMockworkflowResetor(s.controller)
	s.mockTxProcessor = NewMocktransferQueueProcessor(s.controller)
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockTimerProcessor = NewMocktimerQueueProcessor(s.controller)
	s.mockStateBuilder = NewMockstateBuilder(s.controller)
	s.mockTxProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTimers(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardId:          testShardID,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)

	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockShardManager = s.mockShard.resource.ShardMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockNamespaceCache = s.mockShard.resource.NamespaceCache
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	historyCache := newHistoryCache(s.mockShard)
	engine := &historyEngineImpl{
		currentClusterName:   s.mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shard:                s.mockShard,
		clusterMetadata:      s.mockClusterMetadata,
		executionManager:     s.mockExecutionMgr,
		historyCache:         historyCache,
		logger:               s.logger,
		tokenSerializer:      common.NewProtoTaskTokenSerializer(),
		metricsClient:        s.mockShard.GetMetricsClient(),
		timeSource:           s.mockShard.GetTimeSource(),
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string, string) int { return testShardID }),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
	}
	s.mockShard.SetEngine(engine)

	s.historyReplicator = newHistoryReplicator(s.mockShard, clock.NewEventTimeSource(), engine, historyCache, s.mockNamespaceCache, s.mockHistoryV2Mgr, s.logger)
	s.historyReplicator.resetor = s.mockWorkflowResetor
}

func (s *historyReplicatorSuite) TearDownTest() {
	s.historyReplicator = nil
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *historyReplicatorSuite) TestApplyStartEvent() {

}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_MissingCurrent() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UTC()
	req := &historyservice.ReplicateEventsRequest{
		History: &historypb.History{
			Events: []*historypb.HistoryEvent{
				{
					Version:   version,
					EventTime: &now,
				},
			},
		},
	}

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(nil, serviceerror.NewNotFound(""))

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(context.Background(), namespaceID, workflowID, runID, req, s.logger)
	s.Equal(serviceerrors.NewRetryTask(ErrWorkflowNotFoundMsg, namespaceID, workflowID, runID, common.FirstEventID), err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingLessThanCurrent_NoEventsReapplication() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UTC()
	currentRunID := uuid.New()
	currentVersion := version + 1
	currentNextEventID := int64(2333)
	req := &historyservice.ReplicateEventsRequest{
		History: &historypb.History{
			Events: []*historypb.HistoryEvent{
				{
					Version:   version,
					EventTime: &now,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
				},
			},
		},
	}

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      currentRunID,
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(namespaceID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		RunID:                  currentRunID,
		NextEventID:            currentNextEventID,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
	}).AnyTimes()
	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(context.Background(), namespaceID, workflowID, runID, req, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingLessThanCurrent_EventsReapplication_PendingWorkflowTask() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UTC()
	currentRunID := uuid.New()
	currentVersion := version + 1
	currentNextEventID := int64(2333)

	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalIdentity := "some random signal identity"

	req := &historyservice.ReplicateEventsRequest{
		History: &historypb.History{
			Events: []*historypb.HistoryEvent{
				{
					Version:   version,
					EventTime: &now,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
					Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
						SignalName: signalName,
						Input:      signalInput,
						Identity:   signalIdentity,
					}},
				},
			},
		},
	}

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      currentRunID,
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(namespaceID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		RunID:                  currentRunID,
		NextEventID:            currentNextEventID,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
	}).AnyTimes()
	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentVersion, true).Return(nil).Times(1)
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		EventTime: timestamp.TimePtr(time.Now().UTC()),
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: signalName,
			Input:      signalInput,
			Identity:   signalIdentity,
		}},
	}, nil).Times(1)
	msBuilderCurrent.EXPECT().HasPendingWorkflowTask().Return(true).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(context.Background(), namespaceID, workflowID, runID, req, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingLessThanCurrent_EventsReapplication_NoPendingWorkflowTask() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UTC()

	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalIdentity := "some random signal identity"

	currentRunID := uuid.New()
	currentVersion := version + 1
	currentNextEventID := int64(2333)
	currentworkflowTaskTimeout := int64(100)
	currentWorkflowTaskStickyTimeout := int64(10)
	currentWorkflowTaskQueue := "some random workflow task queue"
	currentStickyWorkflowTaskQueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_STICKY, Name: "some random sticky workflow task queue"}

	req := &historyservice.ReplicateEventsRequest{
		History: &historypb.History{
			Events: []*historypb.HistoryEvent{
				{
					Version:   version,
					EventTime: &now,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
					Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
						SignalName: signalName,
						Input:      signalInput,
						Identity:   signalIdentity,
					}},
				},
			},
		},
	}

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      currentRunID,
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(namespaceID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID:                  namespaceID,
		RunID:                        currentRunID,
		NextEventID:                  currentNextEventID,
		TaskQueue:                    currentWorkflowTaskQueue,
		StickyTaskQueue:              currentStickyWorkflowTaskQueue.GetName(),
		WorkflowTaskTimeout:          currentworkflowTaskTimeout,
		StickyScheduleToStartTimeout: currentWorkflowTaskStickyTimeout,
		WorkflowTaskVersion:          common.EmptyVersion,
		WorkflowTaskScheduleID:       common.EmptyEventID,
		WorkflowTaskStartedID:        common.EmptyEventID,
	}).AnyTimes()
	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentVersion, true).Return(nil).Times(1)
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		EventTime: timestamp.TimePtr(time.Now().UTC()),
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: signalName,
			Input:      signalInput,
			Identity:   signalIdentity,
		}},
	}, nil).Times(1)
	msBuilderCurrent.EXPECT().HasPendingWorkflowTask().Return(false).Times(1)
	newWorkflowTask := &workflowTaskInfo{
		Version:    currentVersion,
		ScheduleID: 1234,
		StartedID:  common.EmptyEventID,
		TaskQueue:  currentStickyWorkflowTaskQueue,
		Attempt:    1,
	}
	msBuilderCurrent.EXPECT().AddWorkflowTaskScheduledEvent(false).Return(newWorkflowTask, nil).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(context.Background(), namespaceID, workflowID, runID, req, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingEqualToCurrent_CurrentRunning() {
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UTC()
	currentRunID := uuid.New()
	currentVersion := version
	currentNextEventID := int64(2333)
	req := &historyservice.ReplicateEventsRequest{
		History: &historypb.History{
			Events: []*historypb.HistoryEvent{
				{
					Version:   version,
					EventTime: &now,
				},
			},
		},
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			currentVersion,
			nil,
		), nil,
	).AnyTimes()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      currentRunID,
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				RunID:                  currentRunID,
				NextEventID:            currentNextEventID,
				State:                  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				WorkflowTaskVersion:    common.EmptyVersion,
				WorkflowTaskScheduleID: common.EmptyEventID,
				WorkflowTaskStartedID:  common.EmptyEventID,
			},
			ExecutionStats:   &persistenceblobs.ExecutionStats{},
			ReplicationState: &persistenceblobs.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(context.Background(), namespaceID, workflowID, runID, req, s.logger)
	s.Equal(serviceerrors.NewRetryTask(ErrWorkflowNotFoundMsg, namespaceID, workflowID, currentRunID, currentNextEventID), err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingEqualToCurrent_CurrentRunning_OutOfOrder() {
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	lastEventTaskID := int64(5667)
	now := time.Now().UTC()
	currentRunID := uuid.New()
	currentVersion := version
	currentNextEventID := int64(2333)
	currentLastEventTaskID := lastEventTaskID + 10
	req := &historyservice.ReplicateEventsRequest{
		History: &historypb.History{
			Events: []*historypb.HistoryEvent{
				{
					Version:   version,
					TaskId:    lastEventTaskID,
					EventTime: &now,
				},
			},
		},
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			currentVersion,
			nil,
		), nil,
	).AnyTimes()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      currentRunID,
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				RunID:                  currentRunID,
				NextEventID:            currentNextEventID,
				LastEventTaskID:        currentLastEventTaskID,
				State:                  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				WorkflowTaskVersion:    common.EmptyVersion,
				WorkflowTaskScheduleID: common.EmptyEventID,
				WorkflowTaskStartedID:  common.EmptyEventID,
			},
			ExecutionStats:   &persistenceblobs.ExecutionStats{},
			ReplicationState: &persistenceblobs.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(context.Background(), namespaceID, workflowID, runID, req, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingLargerThanCurrent_CurrentRunning() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UTC()
	currentRunID := uuid.New()
	currentVersion := version - 100
	currentNextEventID := int64(2333)
	req := &historyservice.ReplicateEventsRequest{
		History: &historypb.History{
			Events: []*historypb.HistoryEvent{
				{
					Version:   version,
					EventTime: &now,
				},
			},
		},
	}

	sourceClusterName := cluster.TestAlternativeClusterName
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(sourceClusterName).AnyTimes()
	currentClusterName := cluster.TestCurrentClusterName
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(currentClusterName).AnyTimes()

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(2)
	contextCurrent.EXPECT().unlock().Times(2)
	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(2)
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      currentRunID,
	}
	contextCurrent.EXPECT().getExecution().Return(currentExecution).AnyTimes()
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(namespaceID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:  currentRunID,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}, nil)

	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		RunID:                  currentRunID,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
	}).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes() // this is used to update the version on mutable state
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentVersion, true).Return(nil).Times(1)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:  currentRunID,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}, nil)

	msBuilderCurrent.EXPECT().AddWorkflowExecutionTerminatedEvent(
		currentNextEventID, workflowTerminationReason, gomock.Any(), workflowTerminationIdentity,
	).Return(&historypb.HistoryEvent{}, nil).Times(1)
	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(context.Background(), namespaceID, workflowID, runID, req, s.logger)
	s.Equal(serviceerrors.NewRetryTask(ErrWorkflowNotFoundMsg, namespaceID, workflowID, runID, common.FirstEventID), err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingNotLessThanCurrent_CurrentFinished() {
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UTC()
	currentRunID := uuid.New()
	currentVersion := version - 100
	currentNextEventID := int64(2333)
	req := &historyservice.ReplicateEventsRequest{
		History: &historypb.History{
			Events: []*historypb.HistoryEvent{
				{
					Version:   version,
					EventTime: &now,
				},
			},
		},
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			currentVersion,
			nil,
		), nil,
	).AnyTimes()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      currentRunID,
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				RunID:                  currentRunID,
				NextEventID:            currentNextEventID,
				State:                  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				WorkflowTaskVersion:    common.EmptyVersion,
				WorkflowTaskScheduleID: common.EmptyEventID,
				WorkflowTaskStartedID:  common.EmptyEventID,
			},
			ExecutionStats:   &persistenceblobs.ExecutionStats{},
			ReplicationState: &persistenceblobs.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(context.Background(), namespaceID, workflowID, runID, req, s.logger)
	s.Equal(serviceerrors.NewRetryTask(ErrWorkflowNotFoundMsg, namespaceID, workflowID, runID, common.FirstEventID), err)
}

func (s *historyReplicatorSuite) TestWorkflowReset() {
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UTC()
	currentRunID := uuid.New()
	currentVersion := version - 100
	currentNextEventID := int64(2333)
	req := &historyservice.ReplicateEventsRequest{
		History: &historypb.History{
			Events: []*historypb.HistoryEvent{
				{
					Version:   version,
					EventTime: &now,
				},
			},
		},
		ResetWorkflow: true,
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			currentVersion,
			nil,
		), nil,
	).AnyTimes()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      currentRunID,
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				RunID:                  currentRunID,
				NextEventID:            currentNextEventID,
				State:                  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				WorkflowTaskVersion:    common.EmptyVersion,
				WorkflowTaskScheduleID: common.EmptyEventID,
				WorkflowTaskStartedID:  common.EmptyEventID,
			},
			ExecutionStats:   &persistenceblobs.ExecutionStats{},
			ReplicationState: &persistenceblobs.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	reqCtx := context.Background()

	s.mockWorkflowResetor.EXPECT().ApplyResetEvent(
		reqCtx, req, namespaceID, workflowID, currentRunID,
	).Return(nil).Times(1)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(reqCtx, namespaceID, workflowID, runID, req, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingLessThanCurrent() {
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UTC()
	currentRunID := uuid.New()
	currentVersion := version + 100
	req := &historyservice.ReplicateEventsRequest{
		History: &historypb.History{
			Events: []*historypb.HistoryEvent{
				{
					Version:   version,
					EventTime: &now,
				},
			},
		},
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			currentVersion,
			nil,
		), nil,
	).AnyTimes()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      currentRunID,
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				RunID:                  currentRunID,
				WorkflowTaskVersion:    common.EmptyVersion,
				WorkflowTaskScheduleID: common.EmptyEventID,
				WorkflowTaskStartedID:  common.EmptyEventID,
				State:                  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			},
			ExecutionStats:   &persistenceblobs.ExecutionStats{},
			ReplicationState: &persistenceblobs.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(context.Background(), namespaceID, workflowID, runID, req, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowClosed_WorkflowIsCurrent_NoEventsReapplication() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	incomingVersion := int64(110)
	currentLastWriteVersion := int64(123)

	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().getNamespaceID().Return(namespaceID).AnyTimes()
	weContext.EXPECT().getExecution().Return(&commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}).AnyTimes()

	msBuilderIn := NewMockmutableState(s.controller)
	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
				EventTime: timestamp.TimePtr(time.Now().UTC()),
			},
		}},
	}
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{LastWriteVersion: currentLastWriteVersion}).AnyTimes()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: runID,
		// other attributes are not used
	}, nil)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowClosed_WorkflowIsNotCurrent_NoEventsReapplication() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	currentRunID := uuid.New()
	incomingVersion := int64(110)
	lastWriteVersion := int64(123)

	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().getNamespaceID().Return(namespaceID).AnyTimes()
	weContext.EXPECT().getExecution().Return(&commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}).AnyTimes()

	msBuilderIn := NewMockmutableState(s.controller)
	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
				EventTime: timestamp.TimePtr(time.Now().UTC()),
			},
		}},
	}
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{LastWriteVersion: lastWriteVersion}).AnyTimes()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      currentRunID,
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(namespaceID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(lastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowClosed_WorkflowIsNotCurrent_EventsReapplication_PendingWorkflowTask() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	currentRunID := uuid.New()
	incomingVersion := int64(110)
	lastWriteVersion := int64(123)
	currentLastWriteVersion := lastWriteVersion

	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalIdentity := "some random signal identity"

	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().getNamespaceID().Return(namespaceID).AnyTimes()
	weContext.EXPECT().getExecution().Return(&commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}).AnyTimes()

	msBuilderIn := NewMockmutableState(s.controller)
	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				EventTime: timestamp.TimePtr(time.Now().UTC()),
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: signalName,
					Input:      signalInput,
					Identity:   signalIdentity,
				}},
			},
		}},
	}
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{LastWriteVersion: lastWriteVersion}).AnyTimes()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      currentRunID,
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(namespaceID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentLastWriteVersion, true).Return(nil).Times(1)
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		EventTime: timestamp.TimePtr(time.Now().UTC()),
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: signalName,
			Input:      signalInput,
			Identity:   signalIdentity,
		}},
	}, nil).Times(1)
	msBuilderCurrent.EXPECT().HasPendingWorkflowTask().Return(true).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowClosed_WorkflowIsNotCurrent_EventsReapplication_NoPendingWorkflowTask() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	currentRunID := uuid.New()
	incomingVersion := int64(110)
	lastWriteVersion := int64(123)
	currentLastWriteVersion := lastWriteVersion

	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalIdentity := "some random signal identity"

	stickyWorkflowTaskQueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_STICKY, Name: "some random sticky workflow task queue"}

	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().getNamespaceID().Return(namespaceID).AnyTimes()
	weContext.EXPECT().getExecution().Return(&commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}).AnyTimes()

	msBuilderIn := NewMockmutableState(s.controller)
	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				EventTime: timestamp.TimePtr(time.Now().UTC()),
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: signalName,
					Input:      signalInput,
					Identity:   signalIdentity,
				}},
			},
		}},
	}
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{LastWriteVersion: lastWriteVersion}).AnyTimes()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      currentRunID,
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(namespaceID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentLastWriteVersion, true).Return(nil).Times(1)
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		EventTime: timestamp.TimePtr(time.Now().UTC()),
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: signalName,
			Input:      signalInput,
			Identity:   signalIdentity,
		}},
	}, nil).Times(1)
	msBuilderCurrent.EXPECT().HasPendingWorkflowTask().Return(false).Times(1)

	newWorkflowTask := &workflowTaskInfo{
		Version:    currentLastWriteVersion,
		ScheduleID: 1234,
		StartedID:  common.EmptyEventID,
		TaskQueue:  stickyWorkflowTaskQueue,
		Attempt:    1,
	}
	msBuilderCurrent.EXPECT().AddWorkflowTaskScheduledEvent(false).Return(newWorkflowTask, nil).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowRunning_NoEventsReapplication() {
	incomingVersion := int64(110)
	currentLastWriteVersion := int64(123)

	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)
	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
				EventTime: timestamp.TimePtr(time.Now().UTC()),
			},
		}},
	}
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(1)
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{LastWriteVersion: currentLastWriteVersion}).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowRunning_EventsReapplication_PendingWorkflowTask() {
	incomingVersion := int64(110)
	currentLastWriteVersion := int64(123)

	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalIdentity := "some random signal identity"

	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)
	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				EventTime: timestamp.TimePtr(time.Now().UTC()),
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: signalName,
					Input:      signalInput,
					Identity:   signalIdentity,
				}},
			},
		}},
	}
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{LastWriteVersion: currentLastWriteVersion}).AnyTimes()
	msBuilderIn.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderIn.EXPECT().UpdateCurrentVersion(currentLastWriteVersion, true).Return(nil).Times(1)
	msBuilderIn.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		EventTime: timestamp.TimePtr(time.Now().UTC()),
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: signalName,
			Input:      signalInput,
			Identity:   signalIdentity,
		}},
	}, nil).Times(1)
	msBuilderIn.EXPECT().HasPendingWorkflowTask().Return(true).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	weContext.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowRunning_EventsReapplication_NoPendingWorkflowTask() {
	incomingVersion := int64(110)
	currentLastWriteVersion := int64(123)

	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalIdentity := "some random signal identity"

	stickyWorkflowTaskQueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_STICKY, Name: "some random sticky workflow task queue"}

	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)
	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				EventTime: timestamp.TimePtr(time.Now().UTC()),
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: signalName,
					Input:      signalInput,
					Identity:   signalIdentity,
				}},
			},
		}},
	}
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{LastWriteVersion: currentLastWriteVersion}).AnyTimes()
	msBuilderIn.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderIn.EXPECT().UpdateCurrentVersion(currentLastWriteVersion, true).Return(nil).Times(1)
	msBuilderIn.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		EventTime: timestamp.TimePtr(time.Now().UTC()),
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: signalName,
			Input:      signalInput,
			Identity:   signalIdentity,
		}},
	}, nil).Times(1)
	msBuilderIn.EXPECT().HasPendingWorkflowTask().Return(false).Times(1)

	newWorkflowTask := &workflowTaskInfo{
		Version:    currentLastWriteVersion,
		ScheduleID: 1234,
		StartedID:  common.EmptyEventID,
		TaskQueue:  stickyWorkflowTaskQueue,
		Attempt:    1,
	}
	msBuilderIn.EXPECT().AddWorkflowTaskScheduledEvent(false).Return(newWorkflowTask, nil).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	weContext.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingEqualToCurrent() {
	incomingVersion := int64(110)
	currentLastWriteVersion := incomingVersion

	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)
	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{EventTime: timestamp.TimePtr(time.Now().UTC())},
		}},
	}
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{LastWriteVersion: currentLastWriteVersion}).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn, request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasNotActive_SameCluster() {
	currentLastWriteVersion := int64(10)
	incomingVersion := currentLastWriteVersion + 10

	prevActiveCluster := cluster.TestAlternativeClusterName
	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{EventTime: timestamp.TimePtr(time.Now().UTC())},
		}},
	}
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
	}).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(incomingVersion, currentLastWriteVersion).Return(true).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn,
		request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasNotActive_DiffCluster() {
	currentLastWriteVersion := int64(10)
	incomingVersion := currentLastWriteVersion + 10

	prevActiveCluster := cluster.TestAlternativeClusterName
	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{EventTime: timestamp.TimePtr(time.Now().UTC())},
		}},
	}
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
	}).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(incomingVersion, currentLastWriteVersion).Return(false).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Equal(ErrMoreThan2DC, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_MissingReplicationInfo() {
	runID := uuid.New()

	currentLastWriteVersion := int64(11)
	currentLastEventID := int64(99)
	currentReplicationInfoLastWriteVersion := currentLastWriteVersion - 10
	currentReplicationInfoLastEventID := currentLastEventID - 11
	incomingVersion := currentLastWriteVersion + 10

	updateCondition := int64(1394)

	incomingActiveCluster := cluster.TestAlternativeClusterName
	prevActiveCluster := cluster.TestCurrentClusterName
	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &historyservice.ReplicateEventsRequest{
		Version:         incomingVersion,
		ReplicationInfo: map[string]*replicationspb.ReplicationInfo{},
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{EventTime: timestamp.TimePtr(time.Now().UTC())},
		}},
	}
	startTimeStamp := time.Now().UTC()
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventId: currentLastEventID,
		LastReplicationInfo: map[string]*replicationspb.ReplicationInfo{
			incomingActiveCluster: {
				Version:     currentReplicationInfoLastWriteVersion,
				LastEventId: currentReplicationInfoLastEventID,
			},
		},
	}).AnyTimes()
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	exeInfo := &persistence.WorkflowExecutionInfo{
		StartTimestamp:         startTimeStamp,
		RunID:                  runID,
		State:                  currentState,
		Status:                 enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
	}
	msBuilderIn.EXPECT().GetExecutionInfo().Return(exeInfo).AnyTimes()
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(currentState != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED).AnyTimes()
	msBuilderIn.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()
	msBuilderIn.EXPECT().GetUpdateCondition().Return(updateCondition).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()

	mockConflictResolver := NewMockconflictResolver(s.controller)
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger log.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := NewMockmutableState(s.controller)
	msBuilderMid.EXPECT().GetNextEventID().Return(int64(12345)).AnyTimes() // this is used by log
	mockConflictResolver.EXPECT().reset(
		runID, currentLastWriteVersion, currentState, gomock.Any(), currentReplicationInfoLastEventID, exeInfo, updateCondition,
	).Return(msBuilderMid, nil).Times(1)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionLocalLarger() {
	runID := uuid.New()

	currentLastWriteVersion := int64(120)
	currentLastEventID := int64(980)
	currentReplicationInfoLastWriteVersion := currentLastWriteVersion - 20
	currentReplicationInfoLastEventID := currentLastEventID - 15
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentReplicationInfoLastWriteVersion - 10
	incomingReplicationInfoLastEventID := currentReplicationInfoLastEventID - 20

	updateCondition := int64(1394)

	incomingActiveCluster := cluster.TestAlternativeClusterName
	prevActiveCluster := cluster.TestCurrentClusterName
	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		ReplicationInfo: map[string]*replicationspb.ReplicationInfo{
			prevActiveCluster: {
				Version:     incomingReplicationInfoLastWriteVersion,
				LastEventId: incomingReplicationInfoLastEventID,
			},
		},
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{EventTime: timestamp.TimePtr(time.Now().UTC())},
		}},
	}
	startTimeStamp := time.Now().UTC()
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventId: currentLastEventID,
		LastReplicationInfo: map[string]*replicationspb.ReplicationInfo{
			incomingActiveCluster: {
				Version:     currentReplicationInfoLastWriteVersion,
				LastEventId: currentReplicationInfoLastEventID,
			},
		},
	}).AnyTimes()
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	exeInfo := &persistence.WorkflowExecutionInfo{
		StartTimestamp:         startTimeStamp,
		RunID:                  runID,
		State:                  currentState,
		Status:                 enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
	}
	msBuilderIn.EXPECT().GetExecutionInfo().Return(exeInfo).AnyTimes()
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(currentState != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED)
	msBuilderIn.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()
	msBuilderIn.EXPECT().GetUpdateCondition().Return(updateCondition).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()

	mockConflictResolver := NewMockconflictResolver(s.controller)
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger log.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := NewMockmutableState(s.controller)
	msBuilderMid.EXPECT().GetNextEventID().Return(int64(12345)).AnyTimes() // this is used by log
	mockConflictResolver.EXPECT().reset(
		runID, currentLastWriteVersion, currentState, gomock.Any(), currentReplicationInfoLastEventID, exeInfo, updateCondition,
	).Return(msBuilderMid, nil).Times(1)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionLocalSmaller() {
	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastEventID := currentLastEventID

	prevActiveCluster := cluster.TestCurrentClusterName
	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		ReplicationInfo: map[string]*replicationspb.ReplicationInfo{
			prevActiveCluster: {
				Version:     incomingReplicationInfoLastWriteVersion,
				LastEventId: incomingReplicationInfoLastEventID,
			},
		},
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{EventTime: timestamp.TimePtr(time.Now().UTC())},
		}},
	}
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventId: currentLastEventID,
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn, request, s.logger)
	s.Nil(msBuilderOut)
	s.Equal(ErrImpossibleRemoteClaimSeenHigherVersion, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_ResolveConflict() {
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID - 10

	updateCondition := int64(1394)

	prevActiveCluster := cluster.TestCurrentClusterName
	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		ReplicationInfo: map[string]*replicationspb.ReplicationInfo{
			prevActiveCluster: {
				Version:     incomingReplicationInfoLastWriteVersion,
				LastEventId: incomingReplicationInfoLastEventID,
			},
		},
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{EventTime: timestamp.TimePtr(time.Now().UTC())},
		}},
	}
	startTimeStamp := time.Now().UTC()
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventId: currentLastEventID,
	}).AnyTimes()
	msBuilderIn.EXPECT().HasBufferedEvents().Return(false).Times(1)
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_CREATED
	exeInfo := &persistence.WorkflowExecutionInfo{
		StartTimestamp:         startTimeStamp,
		RunID:                  runID,
		State:                  currentState,
		Status:                 enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
	}
	msBuilderIn.EXPECT().GetExecutionInfo().Return(exeInfo).AnyTimes()
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(currentState != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED).AnyTimes()
	msBuilderIn.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()
	msBuilderIn.EXPECT().GetUpdateCondition().Return(updateCondition).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()

	mockConflictResolver := NewMockconflictResolver(s.controller)
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger log.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := NewMockmutableState(s.controller)
	msBuilderMid.EXPECT().GetNextEventID().Return(int64(12345)).AnyTimes() // this is used by log
	mockConflictResolver.EXPECT().reset(
		runID, currentLastWriteVersion, currentState, gomock.Any(), incomingReplicationInfoLastEventID, exeInfo, updateCondition,
	).Return(msBuilderMid, nil).Times(1)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_Corrputed() {
	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID + 10

	prevActiveCluster := cluster.TestCurrentClusterName
	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		ReplicationInfo: map[string]*replicationspb.ReplicationInfo{
			prevActiveCluster: {
				Version:     incomingReplicationInfoLastWriteVersion,
				LastEventId: incomingReplicationInfoLastEventID,
			},
		},
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{EventTime: timestamp.TimePtr(time.Now().UTC())},
		}},
	}
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventId: currentLastEventID,
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn, request, s.logger)
	s.Nil(msBuilderOut)
	s.Equal(ErrCorruptedReplicationInfo, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_ResolveConflict_OtherCase() {
	// other cases will be tested in TestConflictResolutionTerminateContinueAsNew
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_NoBufferedEvent_NoOp() {
	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID

	prevActiveCluster := cluster.TestCurrentClusterName
	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		ReplicationInfo: map[string]*replicationspb.ReplicationInfo{
			prevActiveCluster: {
				Version:     incomingReplicationInfoLastWriteVersion,
				LastEventId: incomingReplicationInfoLastEventID,
			},
		},
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{EventTime: timestamp.TimePtr(time.Now().UTC())},
		}},
	}

	msBuilderIn.EXPECT().HasBufferedEvents().Return(false).Times(1)
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventId: currentLastEventID,
	}).AnyTimes()
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(1)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn,
		request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_BufferedEvent_ResolveConflict() {
	namespaceID := uuid.New()
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID
	workflowTaskTimeout := int64(100)
	workflowTaskStickyTimeout := int64(10)
	workflowTaskqueue := "some random workflow taskqueue"
	stickyWorkflowTaskQueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_STICKY, Name: "some random sticky workflow task queue"}

	updateCondition := int64(1394)

	prevActiveCluster := cluster.TestCurrentClusterName
	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &historyservice.ReplicateEventsRequest{
		Version: incomingVersion,
		ReplicationInfo: map[string]*replicationspb.ReplicationInfo{
			prevActiveCluster: {
				Version:     incomingReplicationInfoLastWriteVersion,
				LastEventId: incomingReplicationInfoLastEventID,
			},
		},
		History: &historypb.History{Events: []*historypb.HistoryEvent{
			{EventTime: timestamp.TimePtr(time.Now().UTC())},
		}},
	}
	startTimeStamp := time.Now().UTC()
	pendingWorkflowTaskInfo := &workflowTaskInfo{
		Version:    currentLastWriteVersion,
		ScheduleID: 56,
		StartedID:  57,
	}
	msBuilderIn.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).Times(1)
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventId: currentLastEventID,
	}).Times(1)
	msBuilderIn.EXPECT().HasBufferedEvents().Return(true).Times(1)
	msBuilderIn.EXPECT().GetInFlightWorkflowTask().Return(pendingWorkflowTaskInfo, true).Times(1)
	msBuilderIn.EXPECT().UpdateCurrentVersion(currentLastWriteVersion, true).Return(nil).Times(1)
	msBuilderIn.EXPECT().AddWorkflowTaskFailedEvent(pendingWorkflowTaskInfo.ScheduleID, pendingWorkflowTaskInfo.StartedID,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND, nil, identityHistoryService, "", "", "", int64(0),
	).Return(&historypb.HistoryEvent{}, nil).Times(1)
	msBuilderIn.EXPECT().HasPendingWorkflowTask().Return(false).Times(1)
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	exeInfo := &persistence.WorkflowExecutionInfo{
		StartTimestamp:               startTimeStamp,
		NamespaceID:                  namespaceID,
		RunID:                        runID,
		TaskQueue:                    workflowTaskqueue,
		StickyTaskQueue:              stickyWorkflowTaskQueue.GetName(),
		WorkflowTaskTimeout:          workflowTaskTimeout,
		StickyScheduleToStartTimeout: workflowTaskStickyTimeout,
		State:                        currentState,
		Status:                       enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		WorkflowTaskVersion:          common.EmptyVersion,
		WorkflowTaskScheduleID:       common.EmptyEventID,
		WorkflowTaskStartedID:        common.EmptyEventID,
	}
	msBuilderIn.EXPECT().GetExecutionInfo().Return(exeInfo).AnyTimes()
	newWorkflowTask := &workflowTaskInfo{
		Version:    currentLastWriteVersion,
		ScheduleID: currentLastEventID + 2,
		StartedID:  common.EmptyEventID,
		TaskQueue:  stickyWorkflowTaskQueue,
		Attempt:    1,
	}
	msBuilderIn.EXPECT().AddWorkflowTaskScheduledEvent(false).Return(newWorkflowTask, nil).Times(1)

	weContext.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	// after the flush, the pending buffered events are gone, however, the last event ID should increase
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventId: currentLastEventID + 2,
	}).Times(1)
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(currentState != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED).AnyTimes()
	msBuilderIn.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).Times(1)
	msBuilderIn.EXPECT().GetUpdateCondition().Return(updateCondition).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()

	mockConflictResolver := NewMockconflictResolver(s.controller)
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger log.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := NewMockmutableState(s.controller)
	msBuilderMid.EXPECT().GetNextEventID().Return(int64(12345)).AnyTimes() // this is used by log
	mockConflictResolver.EXPECT().reset(
		runID, currentLastWriteVersion, currentState, gomock.Any(), incomingReplicationInfoLastEventID, exeInfo, updateCondition,
	).Return(msBuilderMid, nil).Times(1)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context.Background(), weContext, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingLessThanCurrent() {
	currentNextEventID := int64(10)
	incomingFirstEventID := currentNextEventID - 4

	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilder := NewMockmutableState(s.controller)

	request := &historyservice.ReplicateEventsRequest{
		FirstEventId: incomingFirstEventID,
		History:      &historypb.History{},
	}
	msBuilder.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilder.EXPECT().GetReplicationState().Return(&persistenceblobs.ReplicationState{}).AnyTimes() // logger will use this

	err := s.historyReplicator.ApplyOtherEvents(context.Background(), weContext, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingEqualToCurrent() {
	// TODO
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingGreaterThanCurrent() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().getNamespaceID().Return(namespaceID).AnyTimes()
	weContext.EXPECT().getExecution().Return(&commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}).AnyTimes()

	msBuilder := NewMockmutableState(s.controller)

	request := &historyservice.ReplicateEventsRequest{
		SourceCluster: incomingSourceCluster,
		Version:       incomingVersion,
		FirstEventId:  incomingFirstEventID,
		NextEventId:   incomingNextEventID,
		History:       &historypb.History{},
	}

	msBuilder.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilder.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

	err := s.historyReplicator.ApplyOtherEvents(context.Background(), weContext, msBuilder, request, s.logger)
	s.Equal(serviceerrors.NewRetryTask(ErrRetryBufferEventsMsg, namespaceID, workflowID, runID, currentNextEventID), err)
}

func (s *historyReplicatorSuite) TestApplyReplicationTask() {
	// TODO
}

func (s *historyReplicatorSuite) TestApplyReplicationTask_WorkflowClosed() {
	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	weContext := NewMockworkflowExecutionContext(s.controller)
	msBuilder := NewMockmutableState(s.controller)

	request := &historyservice.ReplicateEventsRequest{
		SourceCluster:     incomingSourceCluster,
		Version:           incomingVersion,
		FirstEventId:      incomingFirstEventID,
		NextEventId:       incomingNextEventID,
		ForceBufferEvents: true,
		History:           &historypb.History{Events: []*historypb.HistoryEvent{{}}},
	}

	msBuilder.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()

	err := s.historyReplicator.ApplyReplicationTask(context.Background(), weContext, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_BrandNew() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	workflowType := "some random workflow type"
	workflowTimeout := int64(3721)
	runTimeout := int64(3333)
	workflowTaskTimeout := int64(4411)

	initiatedID := int64(4810)
	parentNamespaceID := testNamespaceID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	weContext := newWorkflowExecutionContext(namespaceID, commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &workflowTaskInfo{
		Version:             version,
		ScheduleID:          common.FirstEventID + 1,
		StartedID:           common.EmptyEventID,
		WorkflowTaskTimeout: workflowTaskTimeout,
		TaskQueue:           taskqueue,
	}

	requestID := uuid.New()
	// Round(0) to clean monotonic clock part.
	now := time.Now().UTC().Round(0)
	history := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{Version: version, EventId: 1, EventTime: &now},
			{Version: version, EventId: 2, EventTime: &now},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistenceblobs.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventId: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:            requestID,
		NamespaceID:                namespaceID,
		WorkflowID:                 workflowID,
		RunID:                      runID,
		ParentNamespaceID:          parentNamespaceID,
		ParentWorkflowID:           parentWorkflowID,
		ParentRunID:                parentRunID,
		InitiatedID:                initiatedID,
		TaskQueue:                  taskqueue.GetName(),
		WorkflowTypeName:           workflowType,
		WorkflowExecutionTimeout:   workflowTimeout,
		WorkflowRunTimeout:         runTimeout,
		DefaultWorkflowTaskTimeout: workflowTaskTimeout,
		NextEventID:                nextEventID,
		LastProcessedEvent:         common.EmptyEventID,
		BranchToken:                []byte("some random branch token"),
		WorkflowTaskVersion:        di.Version,
		WorkflowTaskScheduleID:     di.ScheduleID,
		WorkflowTaskStartedID:      di.StartedID,
		WorkflowTaskTimeout:        di.WorkflowTaskTimeout,
		State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: historySize}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		s.Equal(&persistence.CreateWorkflowExecutionRequest{
			Mode:                persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:       "",
			NewWorkflowSnapshot: *newWorkflowSnapshot,
		}, input)
		return true
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(context.Background(), weContext, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_ISE() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	workflowType := "some random workflow type"
	workflowTimeout := int64(3721)
	runTimeout := int64(3333)
	workflowTaskTimeout := int64(4411)

	initiatedID := int64(4810)
	parentNamespaceID := testNamespaceID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	weContext := newWorkflowExecutionContext(namespaceID, commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &workflowTaskInfo{
		Version:             version,
		ScheduleID:          common.FirstEventID + 1,
		StartedID:           common.EmptyEventID,
		WorkflowTaskTimeout: workflowTaskTimeout,
		TaskQueue:           taskqueue,
	}

	requestID := uuid.New()
	now := time.Now().UTC().Round(0)
	history := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{Version: version, EventId: 1, EventTime: &now},
			{Version: version, EventId: 2, EventTime: &now},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistenceblobs.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventId: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:            requestID,
		NamespaceID:                namespaceID,
		WorkflowID:                 workflowID,
		RunID:                      runID,
		ParentNamespaceID:          parentNamespaceID,
		ParentWorkflowID:           parentWorkflowID,
		ParentRunID:                parentRunID,
		InitiatedID:                initiatedID,
		TaskQueue:                  taskqueue.GetName(),
		WorkflowTypeName:           workflowType,
		WorkflowExecutionTimeout:   workflowTimeout,
		WorkflowRunTimeout:         runTimeout,
		DefaultWorkflowTaskTimeout: workflowTaskTimeout,
		NextEventID:                nextEventID,
		LastProcessedEvent:         common.EmptyEventID,
		BranchToken:                []byte("some random branch token"),
		WorkflowTaskVersion:        di.Version,
		WorkflowTaskScheduleID:     di.ScheduleID,
		WorkflowTaskStartedID:      di.StartedID,
		WorkflowTaskTimeout:        di.WorkflowTaskTimeout,
		State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: historySize}, nil).Once()
	errRet := serviceerror.NewInternal("")
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet)
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil) // this is called when err is returned, and shard will try to update

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(context.Background(), weContext, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Equal(errRet, err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_SameRunID() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	workflowType := "some random workflow type"
	workflowTimeout := int64(3721)
	runTimeout := int64(3333)
	workflowTaskTimeout := int64(4411)

	initiatedID := int64(4810)
	parentNamespaceID := testNamespaceID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	weContext := newWorkflowExecutionContext(namespaceID, commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &workflowTaskInfo{
		Version:             version,
		ScheduleID:          common.FirstEventID + 1,
		StartedID:           common.EmptyEventID,
		WorkflowTaskTimeout: workflowTaskTimeout,
		TaskQueue:           taskqueue,
	}

	requestID := uuid.New()
	now := time.Now().UTC().Round(0)
	history := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{Version: version, EventId: 1, EventTime: &now},
			{Version: version, EventId: 2, EventTime: &now},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistenceblobs.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventId: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:            requestID,
		NamespaceID:                namespaceID,
		WorkflowID:                 workflowID,
		RunID:                      runID,
		ParentNamespaceID:          parentNamespaceID,
		ParentWorkflowID:           parentWorkflowID,
		ParentRunID:                parentRunID,
		InitiatedID:                initiatedID,
		TaskQueue:                  taskqueue.GetName(),
		WorkflowTypeName:           workflowType,
		WorkflowExecutionTimeout:   workflowTimeout,
		WorkflowRunTimeout:         runTimeout,
		DefaultWorkflowTaskTimeout: workflowTaskTimeout,
		NextEventID:                nextEventID,
		LastProcessedEvent:         common.EmptyEventID,
		BranchToken:                []byte("some random branch token"),
		WorkflowTaskVersion:        di.Version,
		WorkflowTaskScheduleID:     di.ScheduleID,
		WorkflowTaskStartedID:      di.StartedID,
		WorkflowTaskTimeout:        di.WorkflowTaskTimeout,
		State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version
	currentRunID := runID
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anything
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(context.Background(), weContext, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingLessThanCurrent() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	workflowType := "some random workflow type"
	workflowTimeout := int64(3721)
	workflowTaskTimeout := int64(4411)
	cronSchedule := "some random cron scredule"
	retryPolicy := &commonpb.RetryPolicy{
		InitialInterval:        timestamp.DurationPtr(1 * time.Second),
		MaximumAttempts:        3,
		MaximumInterval:        timestamp.DurationPtr(1 * time.Second),
		NonRetryableErrorTypes: []string{"bad-bug"},
		BackoffCoefficient:     1,
	}

	initiatedID := int64(4810)
	parentNamespaceID := testNamespaceID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	weContext := newWorkflowExecutionContext(namespaceID, commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &workflowTaskInfo{
		Version:             version,
		ScheduleID:          common.FirstEventID + 1,
		StartedID:           common.EmptyEventID,
		WorkflowTaskTimeout: workflowTaskTimeout,
		TaskQueue:           taskqueue,
	}

	requestID := uuid.New()
	now := time.Now().UTC().Round(0)
	history := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{Version: version, EventId: 1, EventTime: &now},
			{Version: version, EventId: 2, EventTime: &now},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistenceblobs.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventId: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:            requestID,
		NamespaceID:                namespaceID,
		WorkflowID:                 workflowID,
		RunID:                      runID,
		ParentNamespaceID:          parentNamespaceID,
		ParentWorkflowID:           parentWorkflowID,
		ParentRunID:                parentRunID,
		InitiatedID:                initiatedID,
		TaskQueue:                  taskqueue.GetName(),
		WorkflowTypeName:           workflowType,
		WorkflowExecutionTimeout:   workflowTimeout,
		WorkflowRunTimeout:         workflowTimeout,
		DefaultWorkflowTaskTimeout: workflowTaskTimeout,
		NextEventID:                nextEventID,
		LastProcessedEvent:         common.EmptyEventID,
		BranchToken:                []byte("some random branch token"),
		WorkflowTaskVersion:        di.Version,
		WorkflowTaskScheduleID:     di.ScheduleID,
		WorkflowTaskStartedID:      di.StartedID,
		WorkflowTaskTimeout:        di.WorkflowTaskTimeout,
		State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		CronSchedule:               cronSchedule,
		HasRetryPolicy:             true,
		InitialInterval:            int64(timestamp.DurationValue(retryPolicy.GetInitialInterval()).Seconds()),
		BackoffCoefficient:         retryPolicy.GetBackoffCoefficient(),
		MaximumAttempts:            retryPolicy.GetMaximumAttempts(),
		MaximumInterval:            int64(timestamp.DurationValue(retryPolicy.GetMaximumInterval()).Seconds()),
		NonRetryableErrorTypes:     retryPolicy.GetNonRetryableErrorTypes(),
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version + 1
	currentRunID := uuid.New()
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:       "",
			NewWorkflowSnapshot: *newWorkflowSnapshot,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                     persistence.CreateWorkflowModeWorkflowIDReuse,
			PreviousRunID:            currentRunID,
			PreviousLastWriteVersion: currentVersion,
			NewWorkflowSnapshot:      *newWorkflowSnapshot,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(context.Background(), weContext, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingEqualToThanCurrent() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	workflowType := "some random workflow type"
	workflowTimeout := int64(3721)
	workflowTaskTimeout := int64(4411)

	initiatedID := int64(4810)
	parentNamespaceID := testNamespaceID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	weContext := newWorkflowExecutionContext(namespaceID, commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	wti := &workflowTaskInfo{
		Version:             version,
		ScheduleID:          common.FirstEventID + 1,
		StartedID:           common.EmptyEventID,
		WorkflowTaskTimeout: workflowTaskTimeout,
		TaskQueue:           taskqueue,
	}

	requestID := uuid.New()
	now := time.Now().UTC().Round(0)
	history := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{Version: version, EventId: 1, EventTime: &now},
			{Version: version, EventId: 2, EventTime: &now},
		},
	}
	nextEventID := wti.ScheduleID + 1
	replicationState := &persistenceblobs.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventId: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:            requestID,
		NamespaceID:                namespaceID,
		WorkflowID:                 workflowID,
		RunID:                      runID,
		ParentNamespaceID:          parentNamespaceID,
		ParentWorkflowID:           parentWorkflowID,
		ParentRunID:                parentRunID,
		InitiatedID:                initiatedID,
		TaskQueue:                  taskqueue.GetName(),
		WorkflowTypeName:           workflowType,
		WorkflowExecutionTimeout:   workflowTimeout,
		WorkflowRunTimeout:         workflowTimeout,
		DefaultWorkflowTaskTimeout: workflowTaskTimeout,
		NextEventID:                nextEventID,
		LastProcessedEvent:         common.EmptyEventID,
		BranchToken:                []byte("some random branch token"),
		WorkflowTaskVersion:        wti.Version,
		WorkflowTaskScheduleID:     wti.ScheduleID,
		WorkflowTaskStartedID:      wti.StartedID,
		WorkflowTaskTimeout:        wti.WorkflowTaskTimeout,
		State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version
	currentRunID := uuid.New()
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:       "",
			NewWorkflowSnapshot: *newWorkflowSnapshot,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                     persistence.CreateWorkflowModeWorkflowIDReuse,
			PreviousRunID:            currentRunID,
			PreviousLastWriteVersion: currentVersion,
			NewWorkflowSnapshot:      *newWorkflowSnapshot,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(context.Background(), weContext, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingNotLessThanCurrent() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	workflowType := "some random workflow type"
	workflowTimeout := int64(3721)
	workflowTaskTimeout := int64(4411)

	initiatedID := int64(4810)
	parentNamespaceID := testNamespaceID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	weContext := newWorkflowExecutionContext(namespaceID, commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &workflowTaskInfo{
		Version:             version,
		ScheduleID:          common.FirstEventID + 1,
		StartedID:           common.EmptyEventID,
		WorkflowTaskTimeout: workflowTaskTimeout,
		TaskQueue:           taskqueue,
	}

	requestID := uuid.New()
	now := time.Now().UTC().Round(0)
	history := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{Version: version, EventId: 1, EventTime: &now},
			{Version: version, EventId: 2, EventTime: &now},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistenceblobs.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventId: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:            requestID,
		NamespaceID:                namespaceID,
		WorkflowID:                 workflowID,
		RunID:                      runID,
		ParentNamespaceID:          parentNamespaceID,
		ParentWorkflowID:           parentWorkflowID,
		ParentRunID:                parentRunID,
		InitiatedID:                initiatedID,
		TaskQueue:                  taskqueue.GetName(),
		WorkflowTypeName:           workflowType,
		WorkflowExecutionTimeout:   workflowTimeout,
		WorkflowRunTimeout:         workflowTimeout,
		DefaultWorkflowTaskTimeout: workflowTaskTimeout,
		NextEventID:                nextEventID,
		LastProcessedEvent:         common.EmptyEventID,
		BranchToken:                []byte("some random branch token"),
		WorkflowTaskVersion:        di.Version,
		WorkflowTaskScheduleID:     di.ScheduleID,
		WorkflowTaskStartedID:      di.StartedID,
		WorkflowTaskTimeout:        di.WorkflowTaskTimeout,
		State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version - 1
	currentRunID := uuid.New()
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:       "",
			NewWorkflowSnapshot: *newWorkflowSnapshot,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                     persistence.CreateWorkflowModeWorkflowIDReuse,
			PreviousRunID:            currentRunID,
			PreviousLastWriteVersion: currentVersion,
			NewWorkflowSnapshot:      *newWorkflowSnapshot,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(context.Background(), weContext, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingLessThanCurrent_NoEventsReapplication() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	workflowType := "some random workflow type"
	workflowTimeout := int64(3721)
	workflowTaskTimeout := int64(4411)

	initiatedID := int64(4810)
	parentNamespaceID := testNamespaceID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	weContext := newWorkflowExecutionContext(namespaceID, commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &workflowTaskInfo{
		Version:             version,
		ScheduleID:          common.FirstEventID + 1,
		StartedID:           common.EmptyEventID,
		WorkflowTaskTimeout: workflowTaskTimeout,
		TaskQueue:           taskqueue,
	}

	requestID := uuid.New()
	now := time.Now().UTC().Round(0)
	history := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{Version: version, EventId: 1, EventTime: &now},
			{Version: version, EventId: 2, EventTime: &now},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistenceblobs.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventId: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:            requestID,
		NamespaceID:                namespaceID,
		WorkflowID:                 workflowID,
		RunID:                      runID,
		ParentNamespaceID:          parentNamespaceID,
		ParentWorkflowID:           parentWorkflowID,
		ParentRunID:                parentRunID,
		InitiatedID:                initiatedID,
		TaskQueue:                  taskqueue.GetName(),
		WorkflowTypeName:           workflowType,
		WorkflowExecutionTimeout:   workflowTimeout,
		WorkflowRunTimeout:         workflowTimeout,
		DefaultWorkflowTaskTimeout: workflowTaskTimeout,
		NextEventID:                nextEventID,
		LastProcessedEvent:         common.EmptyEventID,
		BranchToken:                []byte("some random branch token"),
		WorkflowTaskVersion:        di.Version,
		WorkflowTaskScheduleID:     di.ScheduleID,
		WorkflowTaskStartedID:      di.StartedID,
		WorkflowTaskTimeout:        di.WorkflowTaskTimeout,
		State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	msBuilder.EXPECT().GetCurrentBranchToken().Return(executionInfo.BranchToken, nil).AnyTimes()
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version + 1
	currentRunID := uuid.New()
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	shardId := testShardID
	delReq := &persistence.DeleteHistoryBranchRequest{
		BranchToken: executionInfo.BranchToken,
		ShardID:     &shardId,
	}
	// the test above already assert the create workflow request, so here just use anything
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockHistoryV2Mgr.On("DeleteHistoryBranch", delReq).Return(nil).Once()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      currentRunID,
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(namespaceID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	err := s.historyReplicator.replicateWorkflowStarted(context.Background(), weContext, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingLessThanCurrent_EventsReapplication_PendingWorkflowTask() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	workflowType := "some random workflow type"
	workflowTimeout := int64(3721)
	workflowTaskTimeout := int64(4411)

	initiatedID := int64(4810)
	parentNamespaceID := testNamespaceID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalIdentity := "some random signal identity"

	weContext := newWorkflowExecutionContext(namespaceID, commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &workflowTaskInfo{
		Version:             version,
		ScheduleID:          common.FirstEventID + 1,
		StartedID:           common.EmptyEventID,
		WorkflowTaskTimeout: workflowTaskTimeout,
		TaskQueue:           taskqueue,
	}

	requestID := uuid.New()
	now := time.Now().UTC().Round(0)
	history := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{Version: version, EventId: 1, EventTime: &now},
			{
				Version:   version,
				EventId:   2,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				EventTime: &now,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: signalName,
					Input:      signalInput,
					Identity:   signalIdentity,
				}},
			},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistenceblobs.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventId: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:            requestID,
		NamespaceID:                namespaceID,
		WorkflowID:                 workflowID,
		RunID:                      runID,
		ParentNamespaceID:          parentNamespaceID,
		ParentWorkflowID:           parentWorkflowID,
		ParentRunID:                parentRunID,
		InitiatedID:                initiatedID,
		TaskQueue:                  taskqueue.GetName(),
		WorkflowTypeName:           workflowType,
		WorkflowExecutionTimeout:   workflowTimeout,
		WorkflowRunTimeout:         workflowTimeout,
		DefaultWorkflowTaskTimeout: workflowTaskTimeout,
		NextEventID:                nextEventID,
		LastProcessedEvent:         common.EmptyEventID,
		BranchToken:                []byte("some random branch token"),
		WorkflowTaskVersion:        di.Version,
		WorkflowTaskScheduleID:     di.ScheduleID,
		WorkflowTaskStartedID:      di.StartedID,
		WorkflowTaskTimeout:        di.WorkflowTaskTimeout,
		State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	msBuilder.EXPECT().GetCurrentBranchToken().Return(executionInfo.BranchToken, nil).AnyTimes()
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version + 1
	currentRunID := uuid.New()
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anything
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockHistoryV2Mgr.On("DeleteHistoryBranch", mock.Anything).Return(nil).Once()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      currentRunID,
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(namespaceID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		EventTime: timestamp.TimePtr(time.Now().UTC()),
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: signalName,
			Input:      signalInput,
			Identity:   signalIdentity,
		}},
	}, nil).Times(1)
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentVersion, true).Return(nil).Times(1)
	msBuilderCurrent.EXPECT().HasPendingWorkflowTask().Return(true).Times(1)
	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(context.Background(), weContext, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingLessThanCurrent_EventsReapplication_NoPendingWorkflowTask() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	workflowType := "some random workflow type"
	workflowTimeout := int64(3721)
	workflowTaskTimeout := int64(4411)

	initiatedID := int64(4810)
	parentNamespaceID := testNamespaceID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalIdentity := "some random signal identity"

	weContext := newWorkflowExecutionContext(namespaceID, commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &workflowTaskInfo{
		Version:             version,
		ScheduleID:          common.FirstEventID + 1,
		StartedID:           common.EmptyEventID,
		WorkflowTaskTimeout: workflowTaskTimeout,
		TaskQueue:           taskqueue,
	}

	requestID := uuid.New()
	now := time.Now().UTC().Round(0)
	history := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{Version: version, EventId: 1, EventTime: &now},
			{
				Version:   version,
				EventId:   2,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				EventTime: &now,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: signalName,
					Input:      signalInput,
					Identity:   signalIdentity,
				}},
			},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistenceblobs.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventId: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:            requestID,
		NamespaceID:                namespaceID,
		WorkflowID:                 workflowID,
		RunID:                      runID,
		ParentNamespaceID:          parentNamespaceID,
		ParentWorkflowID:           parentWorkflowID,
		ParentRunID:                parentRunID,
		InitiatedID:                initiatedID,
		TaskQueue:                  taskqueue.GetName(),
		WorkflowTypeName:           workflowType,
		WorkflowExecutionTimeout:   workflowTimeout,
		WorkflowRunTimeout:         workflowTimeout,
		DefaultWorkflowTaskTimeout: workflowTaskTimeout,
		NextEventID:                nextEventID,
		LastProcessedEvent:         common.EmptyEventID,
		BranchToken:                []byte("some random branch token"),
		WorkflowTaskVersion:        di.Version,
		WorkflowTaskScheduleID:     di.ScheduleID,
		WorkflowTaskStartedID:      di.StartedID,
		WorkflowTaskTimeout:        di.WorkflowTaskTimeout,
		State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	msBuilder.EXPECT().GetCurrentBranchToken().Return(executionInfo.BranchToken, nil).AnyTimes()
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version + 1
	currentRunID := uuid.New()
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	currentStickyWorkflowTaskQueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_STICKY, Name: "some random sticky workflow task queue"}

	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anything
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockHistoryV2Mgr.On("DeleteHistoryBranch", mock.Anything).Return(nil).Once()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      currentRunID,
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(namespaceID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		EventTime: timestamp.TimePtr(time.Now().UTC()),
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: signalName,
			Input:      signalInput,
			Identity:   signalIdentity,
		}},
	}, nil).Times(1)
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentVersion, true).Return(nil).Times(1)
	msBuilderCurrent.EXPECT().HasPendingWorkflowTask().Return(false).Times(1)

	newWorkflowTask := &workflowTaskInfo{
		Version:    currentVersion,
		ScheduleID: 1234,
		StartedID:  common.EmptyEventID,
		TaskQueue:  currentStickyWorkflowTaskQueue,
		Attempt:    1,
	}
	msBuilderCurrent.EXPECT().AddWorkflowTaskScheduledEvent(false).Return(newWorkflowTask, nil).Times(1)

	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(context.Background(), weContext, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingEqualToCurrent() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	workflowType := "some random workflow type"
	workflowTimeout := int64(3721)
	workflowTaskTimeout := int64(4411)

	initiatedID := int64(4810)
	parentNamespaceID := testNamespaceID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	weContext := newWorkflowExecutionContext(namespaceID, commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &workflowTaskInfo{
		Version:             version,
		ScheduleID:          common.FirstEventID + 1,
		StartedID:           common.EmptyEventID,
		WorkflowTaskTimeout: workflowTaskTimeout,
		TaskQueue:           taskqueue,
	}

	requestID := uuid.New()
	now := time.Now().UTC().Round(0)
	history := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{Version: version, EventId: 1, EventTime: &now},
			{Version: version, EventId: 2, EventTime: &now},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistenceblobs.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventId: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:            requestID,
		NamespaceID:                namespaceID,
		WorkflowID:                 workflowID,
		RunID:                      runID,
		ParentNamespaceID:          parentNamespaceID,
		ParentWorkflowID:           parentWorkflowID,
		ParentRunID:                parentRunID,
		InitiatedID:                initiatedID,
		TaskQueue:                  taskqueue.GetName(),
		WorkflowTypeName:           workflowType,
		WorkflowExecutionTimeout:   workflowTimeout,
		WorkflowRunTimeout:         workflowTimeout,
		DefaultWorkflowTaskTimeout: workflowTaskTimeout,
		NextEventID:                nextEventID,
		LastProcessedEvent:         common.EmptyEventID,
		BranchToken:                []byte("some random branch token"),
		WorkflowTaskVersion:        di.Version,
		WorkflowTaskScheduleID:     di.ScheduleID,
		WorkflowTaskStartedID:      di.StartedID,
		WorkflowTaskTimeout:        di.WorkflowTaskTimeout,
		State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version
	currentRunID := uuid.New()
	currentNextEventID := int64(3456)
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      currentRunID,
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(namespaceID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID:            namespaceID,
		WorkflowID:             workflowID,
		RunID:                  currentRunID,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
	}).AnyTimes()
	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(context.Background(), weContext, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Equal(serviceerrors.NewRetryTask(ErrRetryExistingWorkflowMsg, namespaceID, workflowID, currentRunID, currentNextEventID), err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingEqualToCurrent_OutOfOrder() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	workflowType := "some random workflow type"
	workflowTimeout := int64(3721)
	workflowTaskTimeout := int64(4411)

	initiatedID := int64(4810)
	parentNamespaceID := testNamespaceID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	lastEventTaskID := int64(2333)

	weContext := newWorkflowExecutionContext(namespaceID, commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &workflowTaskInfo{
		Version:             version,
		ScheduleID:          common.FirstEventID + 1,
		StartedID:           common.EmptyEventID,
		WorkflowTaskTimeout: workflowTaskTimeout,
		TaskQueue:           taskqueue,
	}

	requestID := uuid.New()
	now := time.Now().UTC().Round(0)
	history := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{Version: version, EventId: 1, EventTime: &now},
			{Version: version, EventId: 2, EventTime: &now},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistenceblobs.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventId: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:            requestID,
		NamespaceID:                namespaceID,
		WorkflowID:                 workflowID,
		RunID:                      runID,
		ParentNamespaceID:          parentNamespaceID,
		ParentWorkflowID:           parentWorkflowID,
		ParentRunID:                parentRunID,
		InitiatedID:                initiatedID,
		TaskQueue:                  taskqueue.GetName(),
		WorkflowTypeName:           workflowType,
		WorkflowExecutionTimeout:   workflowTimeout,
		WorkflowRunTimeout:         workflowTimeout,
		DefaultWorkflowTaskTimeout: workflowTaskTimeout,
		NextEventID:                nextEventID,
		LastProcessedEvent:         common.EmptyEventID,
		BranchToken:                []byte("some random branch token"),
		WorkflowTaskVersion:        di.Version,
		WorkflowTaskScheduleID:     di.ScheduleID,
		WorkflowTaskStartedID:      di.StartedID,
		WorkflowTaskTimeout:        di.WorkflowTaskTimeout,
		State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version
	currentRunID := uuid.New()
	currentNextEventID := int64(3456)
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anything
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      currentRunID,
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(namespaceID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID:            namespaceID,
		WorkflowID:             workflowID,
		RunID:                  currentRunID,
		LastEventTaskID:        lastEventTaskID + 10,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
	}).AnyTimes()
	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(context.Background(), weContext, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingLargerThanCurrent() {
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	workflowType := "some random workflow type"
	workflowTimeout := int64(3721)
	workflowTaskTimeout := int64(4411)
	cronSchedule := "some random cron scredule"
	retryPolicy := &commonpb.RetryPolicy{
		InitialInterval:        timestamp.DurationPtr(1 * time.Second),
		MaximumAttempts:        3,
		MaximumInterval:        timestamp.DurationPtr(1 * time.Second),
		NonRetryableErrorTypes: []string{"bad-bug"},
		BackoffCoefficient:     1,
	}

	initiatedID := int64(4810)
	parentNamespaceID := testNamespaceID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	weContext := newWorkflowExecutionContext(namespaceID, commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &workflowTaskInfo{
		Version:             version,
		ScheduleID:          common.FirstEventID + 1,
		StartedID:           common.EmptyEventID,
		WorkflowTaskTimeout: workflowTaskTimeout,
		TaskQueue:           taskqueue,
	}

	requestID := uuid.New()
	now := time.Now().UTC().Round(0)
	history := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{Version: version, EventId: 1, EventTime: &now},
			{Version: version, EventId: 2, EventTime: &now},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistenceblobs.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventId: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:            requestID,
		NamespaceID:                namespaceID,
		WorkflowID:                 workflowID,
		RunID:                      runID,
		ParentNamespaceID:          parentNamespaceID,
		ParentWorkflowID:           parentWorkflowID,
		ParentRunID:                parentRunID,
		InitiatedID:                initiatedID,
		TaskQueue:                  taskqueue.GetName(),
		WorkflowTypeName:           workflowType,
		WorkflowExecutionTimeout:   workflowTimeout,
		WorkflowRunTimeout:         workflowTimeout,
		DefaultWorkflowTaskTimeout: workflowTaskTimeout,
		NextEventID:                nextEventID,
		LastProcessedEvent:         common.EmptyEventID,
		BranchToken:                []byte("some random branch token"),
		WorkflowTaskVersion:        di.Version,
		WorkflowTaskScheduleID:     di.ScheduleID,
		WorkflowTaskStartedID:      di.StartedID,
		WorkflowTaskTimeout:        di.WorkflowTaskTimeout,
		State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		CronSchedule:               cronSchedule,
		HasRetryPolicy:             true,
		InitialInterval:            int64(timestamp.DurationValue(retryPolicy.GetInitialInterval()).Seconds()),
		BackoffCoefficient:         retryPolicy.GetBackoffCoefficient(),
		MaximumInterval:            int64(timestamp.DurationValue(retryPolicy.GetMaximumInterval()).Seconds()),
		MaximumAttempts:            retryPolicy.GetMaximumAttempts(),
		NonRetryableErrorTypes:     retryPolicy.GetNonRetryableErrorTypes(),
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentNextEventID := int64(2333)
	currentVersion := version - 1
	currentRunID := uuid.New()
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:       "",
			NewWorkflowSnapshot: *newWorkflowSnapshot,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                     persistence.CreateWorkflowModeWorkflowIDReuse,
			PreviousRunID:            currentRunID,
			PreviousLastWriteVersion: currentVersion,
			NewWorkflowSnapshot:      *newWorkflowSnapshot,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()

	// this mocks are for the terminate current workflow operation
	namespaceVersion := int64(4081)
	namespace := "some random namespace name"
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			namespaceVersion,
			nil,
		), nil,
	).AnyTimes()

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)
	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      currentRunID,
	}
	contextCurrent.EXPECT().getExecution().Return(currentExecution).AnyTimes()
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(namespaceID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes() // this is used to update the version on mutable state
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentVersion, true).Return(nil).Times(1)

	currentClusterName := cluster.TestCurrentClusterName
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(currentClusterName).AnyTimes()

	msBuilderCurrent.EXPECT().AddWorkflowExecutionTerminatedEvent(
		currentNextEventID, workflowTerminationReason, gomock.Any(), workflowTerminationIdentity,
	).Return(&historypb.HistoryEvent{}, nil).Times(1)
	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	err := s.historyReplicator.replicateWorkflowStarted(context.Background(), weContext, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetRunning() {
	runID := uuid.New()
	lastWriteVersion := int64(1394)
	state := enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	incomingVersion := int64(4096)
	incomingTime := time.Now().UTC()

	msBuilderTarget := NewMockmutableState(s.controller)

	msBuilderTarget.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderTarget.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		RunID:                  runID,
		State:                  state,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
	}).AnyTimes()
	msBuilderTarget.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil).AnyTimes()
	prevRunID, prevLastWriteVersion, prevState, err := s.historyReplicator.conflictResolutionTerminateCurrentRunningIfNotSelf(
		context.Background(), msBuilderTarget, incomingVersion, incomingTime, s.logger,
	)
	s.Nil(err)
	s.Equal(runID, prevRunID)
	s.Equal(lastWriteVersion, prevLastWriteVersion)
	s.Equal(state, prevState)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetClosed_CurrentClosed() {
	incomingVersion := int64(4096)
	incomingTime := time.Now().UTC()

	namespaceID := testNamespaceID
	workflowID := "some random target workflow ID"
	targetRunID := uuid.New()

	msBuilderTarget := NewMockmutableState(s.controller)

	msBuilderTarget.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	msBuilderTarget.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID:            namespaceID,
		WorkflowID:             workflowID,
		RunID:                  targetRunID,
		State:                  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:                 enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
	}).AnyTimes()

	currentRunID := uuid.New()
	currentLastWriteVersion := int64(1394)
	currentState := enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:            currentRunID,
		State:            currentState,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		LastWriteVersion: currentLastWriteVersion,
	}, nil)

	prevRunID, prevLastWriteVersion, prevState, err := s.historyReplicator.conflictResolutionTerminateCurrentRunningIfNotSelf(
		context.Background(), msBuilderTarget, incomingVersion, incomingTime, s.logger,
	)
	s.Nil(err)
	s.Equal(currentRunID, prevRunID)
	s.Equal(currentLastWriteVersion, prevLastWriteVersion)
	s.Equal(currentState, prevState)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetClosed_CurrentRunning_LowerVersion() {
	incomingVersion := int64(4096)
	incomingTime := time.Now().UTC()
	incomingCluster := cluster.TestAlternativeClusterName
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(incomingVersion).Return(incomingCluster).AnyTimes()

	namespaceID := testNamespaceID
	workflowID := "some random target workflow ID"
	targetRunID := uuid.New()

	msBuilderTarget := NewMockmutableState(s.controller)
	msBuilderTarget.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	msBuilderTarget.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID:            namespaceID,
		WorkflowID:             workflowID,
		RunID:                  targetRunID,
		State:                  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:                 enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
	}).AnyTimes()

	currentRunID := uuid.New()
	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)
	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      currentRunID,
	}
	contextCurrent.EXPECT().getExecution().Return(currentExecution).AnyTimes()
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(namespaceID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	currentNextEventID := int64(2333)
	currentVersion := incomingVersion - 10
	currentCluster := cluster.TestCurrentClusterName
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(currentCluster).AnyTimes()

	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes() // this is used to update the version on mutable state
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentVersion, true).Return(nil).Times(1)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:            currentRunID,
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		LastWriteVersion: currentVersion,
	}, nil)

	msBuilderCurrent.EXPECT().AddWorkflowExecutionTerminatedEvent(
		currentNextEventID, workflowTerminationReason, gomock.Any(), workflowTerminationIdentity,
	).Return(&historypb.HistoryEvent{}, nil).Times(1)
	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	prevRunID, prevLastWriteVersion, prevState, err := s.historyReplicator.conflictResolutionTerminateCurrentRunningIfNotSelf(context.Background(), msBuilderTarget, incomingVersion, incomingTime, s.logger)
	s.Nil(err)
	s.Equal(currentRunID, prevRunID)
	s.Equal(currentVersion, prevLastWriteVersion)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, prevState)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetClosed_CurrentRunning_NotLowerVersion() {
	incomingVersion := int64(4096)
	incomingTime := time.Now().UTC()
	incomingCluster := cluster.TestAlternativeClusterName
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(incomingVersion).Return(incomingCluster).AnyTimes()

	namespaceID := testNamespaceID
	workflowID := "some random target workflow ID"
	targetRunID := uuid.New()

	msBuilderTarget := NewMockmutableState(s.controller)
	msBuilderTarget.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	msBuilderTarget.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID:            namespaceID,
		WorkflowID:             workflowID,
		RunID:                  targetRunID,
		Status:                 enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
	}).AnyTimes()

	currentRunID := uuid.New()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:            currentRunID,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		LastWriteVersion: incomingVersion,
	}, nil)

	prevRunID, _, _, err := s.historyReplicator.conflictResolutionTerminateCurrentRunningIfNotSelf(context.Background(), msBuilderTarget, incomingVersion, incomingTime, s.logger)
	s.Nil(err)
	s.Equal("", prevRunID)
}
