// Copyright (c) 2017 Uber Technologies, Inc.
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
	ctx "context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	historyReplicatorSuite struct {
		suite.Suite
		logger              bark.Logger
		mockExecutionMgr    *mocks.ExecutionManager
		mockHistoryMgr      *mocks.HistoryManager
		mockShardManager    *mocks.ShardManager
		mockClusterMetadata *mocks.ClusterMetadata
		mockProducer        *mocks.KafkaProducer
		mockMetadataMgr     *mocks.MetadataManager
		mockMessagingClient messaging.Client
		mockService         service.Service
		mockShard           *shardContextImpl
		mockMutableState    *mockMutableState

		historyReplicator *historyReplicator
	}
)

func TestHistoryReplicatorSuite(t *testing.T) {
	s := new(historyReplicatorSuite)
	suite.Run(t, s)
}

func (s *historyReplicatorSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

}

func (s *historyReplicatorSuite) TearDownSuite() {

}

func (s *historyReplicatorSuite) SetupTest() {
	log2 := log.New()
	log2.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(log2)
	s.mockHistoryMgr = &mocks.HistoryManager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockShardManager = &mocks.ShardManager{}
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockMetadataMgr = &mocks.MetadataManager{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, metricsClient, s.logger)

	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: 0, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              s.mockShardManager,
		historyMgr:                s.mockHistoryMgr,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewConfig(dynamicconfig.NewNopCollection(), 1),
		logger:                    s.logger,
		domainCache:               cache.NewDomainCache(s.mockMetadataMgr, s.mockClusterMetadata, metricsClient, s.logger),
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
		standbyClusterCurrentTime: make(map[string]time.Time),
	}
	s.mockMutableState = &mockMutableState{}
	historyCache := newHistoryCache(s.mockShard, s.logger)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	h := &historyEngineImpl{
		currentClusterName: s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:              s.mockShard,
		historyMgr:         s.mockHistoryMgr,
		executionManager:   s.mockExecutionMgr,
		historyCache:       historyCache,
		logger:             s.logger,
		tokenSerializer:    common.NewJSONTaskTokenSerializer(),
		hSerializerFactory: persistence.NewHistorySerializerFactory(),
		metricsClient:      s.mockShard.GetMetricsClient(),
	}
	s.historyReplicator = newHistoryReplicator(s.mockShard, h, historyCache, s.mockShard.domainCache, s.mockHistoryMgr, s.logger)
}

func (s *historyReplicatorSuite) TearDownTest() {
	s.historyReplicator = nil
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockMetadataMgr.AssertExpectations(s.T())
}

func (s *historyReplicatorSuite) TestApplyStartEvent() {

}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingNotLessThanCurrent() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	version := int64(123)
	currentRunID := uuid.New()
	currentVersion := version - 100

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			FailoverVersion: currentVersion,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(currentRunID),
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo:    &persistence.WorkflowExecutionInfo{RunID: currentRunID},
			ReplicationState: &persistence.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, version,
		s.logger)
	s.Equal(ErrRetryEntityNotExists, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingLessThanCurrent() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	version := int64(123)
	currentRunID := uuid.New()
	currentVersion := version + 100

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			FailoverVersion: currentVersion,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(currentRunID),
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo:    &persistence.WorkflowExecutionInfo{RunID: currentRunID},
			ReplicationState: &persistence.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, version,
		s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	incomingVersion := int64(110)
	currentLastWriteVersion := int64(123)

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{[]*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion})

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingEqualToCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	incomingVersion := int64(110)
	currentLastWriteVersion := incomingVersion

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{[]*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion})

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasNotActive_SameCluster() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	incomingVersion := currentLastWriteVersion + 10

	prevActiveCluster := cluster.TestAlternativeClusterName
	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{[]*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
	})
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)
	s.mockClusterMetadata.On("IsVersionFromSameCluster", incomingVersion, currentLastWriteVersion).Return(true)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasNotActive_DiffCluster() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	incomingVersion := currentLastWriteVersion + 10

	prevActiveCluster := cluster.TestAlternativeClusterName
	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{[]*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
	})
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)
	s.mockClusterMetadata.On("IsVersionFromSameCluster", incomingVersion, currentLastWriteVersion).Return(false)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Equal(ErrMoreThan2DC, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_MissingReplicationInfo() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	currentReplicationInfoLastWriteVersion := currentLastWriteVersion - 10
	currentReplicationInfoLastEventID := currentLastEventID - 11
	incomingVersion := currentLastWriteVersion + 10

	incomingActiveCluster := cluster.TestAlternativeClusterName
	prevActiveCluster := cluster.TestCurrentClusterName
	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn

	request := &h.ReplicateEventsRequest{
		Version:         common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*h.ReplicationInfo{},
		History: &shared.History{[]*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
		LastReplicationInfo: map[string]*persistence.ReplicationInfo{
			incomingActiveCluster: &persistence.ReplicationInfo{
				Version:     currentReplicationInfoLastWriteVersion,
				LastEventID: currentReplicationInfoLastEventID,
			},
		},
	})
	msBuilderIn.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		StartTimestamp: startTimeStamp,
		RunID:          runID,
	})
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	mockConflictResolver := &mockConflictResolver{}
	s.historyReplicator.getNewConflictResolver = func(context *workflowExecutionContext, logger bark.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := &mockMutableState{}
	msBuilderMid.On("GetNextEventID").Return(int64(12345)) // this is used by log
	mockConflictResolver.On("reset", runID, mock.Anything, currentReplicationInfoLastEventID, startTimeStamp).Return(msBuilderMid, nil)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionLocalLarger() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentLastWriteVersion := int64(120)
	currentLastEventID := int64(980)
	currentReplicationInfoLastWriteVersion := currentLastWriteVersion - 20
	currentReplicationInfoLastEventID := currentLastEventID - 15
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentReplicationInfoLastWriteVersion - 10
	incomingReplicationInfoLastEventID := currentReplicationInfoLastEventID - 20

	incomingActiveCluster := cluster.TestAlternativeClusterName
	prevActiveCluster := cluster.TestCurrentClusterName
	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*h.ReplicationInfo{
			prevActiveCluster: &h.ReplicationInfo{
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{[]*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
		LastReplicationInfo: map[string]*persistence.ReplicationInfo{
			incomingActiveCluster: &persistence.ReplicationInfo{
				Version:     currentReplicationInfoLastWriteVersion,
				LastEventID: currentReplicationInfoLastEventID,
			},
		},
	})
	msBuilderIn.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		StartTimestamp: startTimeStamp,
		RunID:          runID,
	})
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	mockConflictResolver := &mockConflictResolver{}
	s.historyReplicator.getNewConflictResolver = func(context *workflowExecutionContext, logger bark.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := &mockMutableState{}
	msBuilderMid.On("GetNextEventID").Return(int64(12345)) // this is used by log
	mockConflictResolver.On("reset", runID, mock.Anything, currentReplicationInfoLastEventID, startTimeStamp).Return(msBuilderMid, nil)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionLocalSmaller() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastEventID := currentLastEventID

	prevActiveCluster := cluster.TestCurrentClusterName
	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*h.ReplicationInfo{
			prevActiveCluster: &h.ReplicationInfo{
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{[]*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	msBuilderIn.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{StartTimestamp: startTimeStamp})
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Nil(msBuilderOut)
	s.Equal(ErrImpossibleRemoteClaimSeenHigherVersion, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_ResolveConflict() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID - 10

	prevActiveCluster := cluster.TestCurrentClusterName
	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*h.ReplicationInfo{
			prevActiveCluster: &h.ReplicationInfo{
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{[]*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	msBuilderIn.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		StartTimestamp: startTimeStamp,
		RunID:          runID,
	})
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	mockConflictResolver := &mockConflictResolver{}
	s.historyReplicator.getNewConflictResolver = func(context *workflowExecutionContext, logger bark.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := &mockMutableState{}
	msBuilderMid.On("GetNextEventID").Return(int64(12345)) // this is used by log
	mockConflictResolver.On("reset", runID, mock.Anything, incomingReplicationInfoLastEventID, startTimeStamp).Return(msBuilderMid, nil)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_Corrputed() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID + 10

	prevActiveCluster := cluster.TestCurrentClusterName
	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*h.ReplicationInfo{
			prevActiveCluster: &h.ReplicationInfo{
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{[]*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	msBuilderIn.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{StartTimestamp: startTimeStamp})
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Nil(msBuilderOut)
	s.Equal(ErrCorruptedReplicationInfo, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_ResolveConflict_OtherCase() {
	// other cases will be tested in TestConflictResolutionTerminateContinueAsNew
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_NoBufferedEvent_NoOp() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID

	prevActiveCluster := cluster.TestCurrentClusterName
	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*h.ReplicationInfo{
			prevActiveCluster: &h.ReplicationInfo{
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{[]*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.On("HasBufferedEvents").Return(false)
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	msBuilderIn.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{StartTimestamp: startTimeStamp})
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_HasBufferedEvent_ResolveConflict() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID

	prevActiveCluster := cluster.TestCurrentClusterName
	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*h.ReplicationInfo{
			prevActiveCluster: &h.ReplicationInfo{
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{[]*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.On("HasBufferedEvents").Return(true)
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	msBuilderIn.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		StartTimestamp: startTimeStamp,
		RunID:          runID,
	})
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	mockConflictResolver := &mockConflictResolver{}
	s.historyReplicator.getNewConflictResolver = func(context *workflowExecutionContext, logger bark.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := &mockMutableState{}
	msBuilderMid.On("GetNextEventID").Return(int64(12345)) // this is used by log
	mockConflictResolver.On("reset", runID, mock.Anything, incomingReplicationInfoLastEventID, startTimeStamp).Return(msBuilderMid, nil)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingLessThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentNextEventID := int64(10)
	incomingFirstEventID := currentNextEventID - 4

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder

	request := &h.ReplicateEventsRequest{
		FirstEventId: common.Int64Ptr(incomingFirstEventID),
		History:      &shared.History{},
	}
	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{}) // logger will use this
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)

	err := s.historyReplicator.ApplyOtherEvents(ctx.Background(), context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingEqualToCurrent() {
	// TODO
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingGreaterThanCurrent_NoForceBuffer() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	context.updateCondition = currentNextEventID
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder

	request := &h.ReplicateEventsRequest{
		SourceCluster: common.StringPtr(incomingSourceCluster),
		Version:       common.Int64Ptr(incomingVersion),
		FirstEventId:  common.Int64Ptr(incomingFirstEventID),
		NextEventId:   common.Int64Ptr(incomingNextEventID),
		History:       &shared.History{},
	}

	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)

	err := s.historyReplicator.ApplyOtherEvents(ctx.Background(), context, msBuilder, request, s.logger)
	s.Equal(ErrRetryBufferEvents, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingGreaterThanCurrent_ForceBuffer_NoExistingBuffer() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentSourceCluster := "some random current source cluster"
	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	context.updateCondition = currentNextEventID
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder

	request := &h.ReplicateEventsRequest{
		SourceCluster:     common.StringPtr(incomingSourceCluster),
		Version:           common.Int64Ptr(incomingVersion),
		FirstEventId:      common.Int64Ptr(incomingFirstEventID),
		NextEventId:       common.Int64Ptr(incomingNextEventID),
		ForceBufferEvents: common.BoolPtr(true),
		History:           &shared.History{Events: []*shared.HistoryEvent{&shared.HistoryEvent{}}},
	}

	serializedHistoryBatch := &persistence.SerializedHistoryEventBatch{
		EncodingType: common.EncodingTypeJSON,
		Version:      144,
		Data:         []byte("some random history"),
	}

	bufferedReplicationTask := &persistence.BufferedReplicationTask{
		FirstEventID: request.GetFirstEventId(),
		NextEventID:  request.GetNextEventId(),
		Version:      request.GetVersion(),
		History:      serializedHistoryBatch,
	}

	executionInfo := &persistence.WorkflowExecutionInfo{
		State: persistence.WorkflowStateRunning,
	}
	replicationState := &persistence.ReplicationState{
		CurrentVersion:   currentVersion,
		StartVersion:     currentVersion,
		LastWriteVersion: currentVersion,
		LastWriteEventID: currentNextEventID - 1,
	}

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentVersion).Return(currentSourceCluster)
	msBuilder.On("GetBufferedReplicationTask", incomingFirstEventID).Return(nil, false).Once()
	msBuilder.On("GetCurrentVersion").Return(currentVersion)
	msBuilder.On("GetLastWriteVersion").Return(currentVersion)
	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("BufferReplicationTask", request).Return(nil).Once()
	msBuilder.On("CloseUpdateSession").Return(&mutableStateSessionUpdates{
		newEventsBuilder:                 newHistoryBuilder(msBuilder, s.logger),
		newBufferedReplicationEventsInfo: bufferedReplicationTask,
		deleteBufferedReplicationEvent:   nil,
	}, nil).Once()
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
	msBuilder.On("UpdateReplicationStateLastEventID", currentSourceCluster, currentVersion, currentNextEventID-1).Once()

	// these does not matter, but will be used by ms builder change notification
	msBuilder.On("GetLastFirstEventID").Return(currentNextEventID - 4)
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)

	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.MatchedBy(func(input *persistence.UpdateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		s.Equal(&persistence.UpdateWorkflowExecutionRequest{
			ExecutionInfo:                 executionInfo,
			ReplicationState:              replicationState,
			TransferTasks:                 nil,
			ReplicationTasks:              nil,
			TimerTasks:                    nil,
			Condition:                     currentNextEventID,
			DeleteTimerTask:               nil,
			UpsertActivityInfos:           nil,
			DeleteActivityInfos:           nil,
			UpserTimerInfos:               nil,
			DeleteTimerInfos:              nil,
			UpsertChildExecutionInfos:     nil,
			DeleteChildExecutionInfo:      nil,
			UpsertRequestCancelInfos:      nil,
			DeleteRequestCancelInfo:       nil,
			UpsertSignalInfos:             nil,
			DeleteSignalInfo:              nil,
			UpsertSignalRequestedIDs:      nil,
			DeleteSignalRequestedID:       "",
			NewBufferedEvents:             nil,
			ClearBufferedEvents:           false,
			NewBufferedReplicationTask:    bufferedReplicationTask,
			DeleteBufferedReplicationTask: nil,
			ContinueAsNew:                 nil,
			FinishExecution:               false,
			FinishedExecutionTTL:          0,
		}, input)
		return true
	})).Return(nil).Once()

	err := s.historyReplicator.ApplyOtherEvents(ctx.Background(), context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingGreaterThanCurrent_ForceBuffer_NoExistingBuffer_WorkflowClosed() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentSourceCluster := "some random current source cluster"
	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	context.updateCondition = currentNextEventID
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder

	request := &h.ReplicateEventsRequest{
		SourceCluster:     common.StringPtr(incomingSourceCluster),
		Version:           common.Int64Ptr(incomingVersion),
		FirstEventId:      common.Int64Ptr(incomingFirstEventID),
		NextEventId:       common.Int64Ptr(incomingNextEventID),
		ForceBufferEvents: common.BoolPtr(true),
		History:           &shared.History{Events: []*shared.HistoryEvent{&shared.HistoryEvent{}}},
	}

	serializedHistoryBatch := &persistence.SerializedHistoryEventBatch{
		EncodingType: common.EncodingTypeJSON,
		Version:      144,
		Data:         []byte("some random history"),
	}

	bufferedReplicationTask := &persistence.BufferedReplicationTask{
		FirstEventID: request.GetFirstEventId(),
		NextEventID:  request.GetNextEventId(),
		Version:      request.GetVersion(),
		History:      serializedHistoryBatch,
	}

	executionInfo := &persistence.WorkflowExecutionInfo{
		State: persistence.WorkflowStateRunning,
	}
	replicationState := &persistence.ReplicationState{
		CurrentVersion:   currentVersion,
		StartVersion:     currentVersion,
		LastWriteVersion: currentVersion,
		LastWriteEventID: currentNextEventID - 1,
	}

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentVersion).Return(currentSourceCluster)
	msBuilder.On("GetBufferedReplicationTask", incomingFirstEventID).Return(nil, false).Once()
	msBuilder.On("GetCurrentVersion").Return(currentVersion)
	msBuilder.On("GetLastWriteVersion").Return(currentVersion)
	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("BufferReplicationTask", request).Return(nil).Once()
	msBuilder.On("CloseUpdateSession").Return(&mutableStateSessionUpdates{
		newEventsBuilder:                 newHistoryBuilder(msBuilder, s.logger),
		newBufferedReplicationEventsInfo: bufferedReplicationTask,
		deleteBufferedReplicationEvent:   nil,
	}, nil).Once()
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
	msBuilder.On("UpdateReplicationStateLastEventID", currentSourceCluster, currentVersion, currentNextEventID-1).Once()

	// these does not matter, but will be used by ms builder change notification
	msBuilder.On("GetLastFirstEventID").Return(currentNextEventID - 4)
	msBuilder.On("IsWorkflowExecutionRunning").Return(false)

	err := s.historyReplicator.ApplyOtherEvents(ctx.Background(), context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingGreaterThanCurrent_ForceBuffer_StaleExistingBuffer() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentSourceCluster := "some random current source cluster"
	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	context.updateCondition = currentNextEventID
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder

	request := &h.ReplicateEventsRequest{
		SourceCluster:     common.StringPtr(incomingSourceCluster),
		Version:           common.Int64Ptr(incomingVersion),
		FirstEventId:      common.Int64Ptr(incomingFirstEventID),
		NextEventId:       common.Int64Ptr(incomingNextEventID),
		ForceBufferEvents: common.BoolPtr(true),
		History:           &shared.History{Events: []*shared.HistoryEvent{&shared.HistoryEvent{}}},
	}

	serializedHistoryBatch := &persistence.SerializedHistoryEventBatch{
		EncodingType: common.EncodingTypeJSON,
		Version:      144,
		Data:         []byte("some random history"),
	}

	bufferedReplicationTask := &persistence.BufferedReplicationTask{
		FirstEventID: request.GetFirstEventId(),
		NextEventID:  request.GetNextEventId(),
		Version:      request.GetVersion(),
		History:      serializedHistoryBatch,
	}

	executionInfo := &persistence.WorkflowExecutionInfo{
		State: persistence.WorkflowStateRunning,
	}
	replicationState := &persistence.ReplicationState{
		CurrentVersion:   currentVersion,
		StartVersion:     currentVersion,
		LastWriteVersion: currentVersion,
		LastWriteEventID: currentNextEventID - 1,
	}
	staleBufferReplicationTask := &persistence.BufferedReplicationTask{
		FirstEventID: request.GetFirstEventId(),
		NextEventID:  request.GetNextEventId(),
		Version:      request.GetVersion() - 1,
	}

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentVersion).Return(currentSourceCluster)
	msBuilder.On("GetBufferedReplicationTask", incomingFirstEventID).Return(staleBufferReplicationTask, true).Once()
	msBuilder.On("GetCurrentVersion").Return(currentVersion)
	msBuilder.On("GetLastWriteVersion").Return(currentVersion)
	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("BufferReplicationTask", request).Return(nil).Once()
	msBuilder.On("CloseUpdateSession").Return(&mutableStateSessionUpdates{
		newEventsBuilder:                 newHistoryBuilder(msBuilder, s.logger),
		newBufferedReplicationEventsInfo: bufferedReplicationTask,
		deleteBufferedReplicationEvent:   nil,
	}, nil).Once()
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
	msBuilder.On("UpdateReplicationStateLastEventID", currentSourceCluster, currentVersion, currentNextEventID-1).Once()

	// these does not matter, but will be used by ms builder change notification
	msBuilder.On("GetLastFirstEventID").Return(currentNextEventID - 4)
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)

	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.MatchedBy(func(input *persistence.UpdateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		s.Equal(&persistence.UpdateWorkflowExecutionRequest{
			ExecutionInfo:                 executionInfo,
			ReplicationState:              replicationState,
			TransferTasks:                 nil,
			ReplicationTasks:              nil,
			TimerTasks:                    nil,
			Condition:                     currentNextEventID,
			DeleteTimerTask:               nil,
			UpsertActivityInfos:           nil,
			DeleteActivityInfos:           nil,
			UpserTimerInfos:               nil,
			DeleteTimerInfos:              nil,
			UpsertChildExecutionInfos:     nil,
			DeleteChildExecutionInfo:      nil,
			UpsertRequestCancelInfos:      nil,
			DeleteRequestCancelInfo:       nil,
			UpsertSignalInfos:             nil,
			DeleteSignalInfo:              nil,
			UpsertSignalRequestedIDs:      nil,
			DeleteSignalRequestedID:       "",
			NewBufferedEvents:             nil,
			ClearBufferedEvents:           false,
			NewBufferedReplicationTask:    bufferedReplicationTask,
			DeleteBufferedReplicationTask: nil,
			ContinueAsNew:                 nil,
			FinishExecution:               false,
			FinishedExecutionTTL:          0,
		}, input)
		return true
	})).Return(nil).Once()

	err := s.historyReplicator.ApplyOtherEvents(ctx.Background(), context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingGreaterThanCurrent_ForceBuffer_StaleIncoming() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	context.updateCondition = currentNextEventID
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder

	request := &h.ReplicateEventsRequest{
		SourceCluster:     common.StringPtr(incomingSourceCluster),
		Version:           common.Int64Ptr(incomingVersion),
		FirstEventId:      common.Int64Ptr(incomingFirstEventID),
		NextEventId:       common.Int64Ptr(incomingNextEventID),
		ForceBufferEvents: common.BoolPtr(true),
		History:           &shared.History{Events: []*shared.HistoryEvent{&shared.HistoryEvent{}}},
	}

	bufferReplicationTask := &persistence.BufferedReplicationTask{
		FirstEventID: request.GetFirstEventId(),
		NextEventID:  request.GetNextEventId(),
		Version:      request.GetVersion() + 1,
	}
	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("GetBufferedReplicationTask", incomingFirstEventID).Return(bufferReplicationTask, true).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)

	err := s.historyReplicator.ApplyOtherEvents(ctx.Background(), context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) ApplyReplicationTask() {
	// TODO
}

func (s *historyReplicatorSuite) TestApplyReplicationTask_WorkflowClosed() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	context.updateCondition = currentNextEventID
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder

	request := &h.ReplicateEventsRequest{
		SourceCluster:     common.StringPtr(incomingSourceCluster),
		Version:           common.Int64Ptr(incomingVersion),
		FirstEventId:      common.Int64Ptr(incomingFirstEventID),
		NextEventId:       common.Int64Ptr(incomingNextEventID),
		ForceBufferEvents: common.BoolPtr(true),
		History:           &shared.History{Events: []*shared.HistoryEvent{&shared.HistoryEvent{}}},
	}

	msBuilder.On("IsWorkflowExecutionRunning").Return(false)

	err := s.historyReplicator.ApplyReplicationTask(ctx.Background(), context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestFlushBuffer() {
	// TODO
}

func (s *historyReplicatorSuite) TestFlushBuffer_AlreadyFinished() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	msBuilder.On("IsWorkflowExecutionRunning").Return(false).Once()

	err := s.historyReplicator.FlushBuffer(ctx.Background(), context, msBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_BrandNew() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		s.Equal(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               false,
			PreviousRunID:               "",
			ReplicationState:            replicationState,
		}, input)
		return true
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history, sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.True(now.Equal(transferTasks[0].GetVisibilityTimestamp()))
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_ISE() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once() // this is called when err is returned, and shard will try to update

	errRet := &shared.InternalServiceError{}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Equal(errRet, err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_SameRunID() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version
	currentRunID := runID
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingLessThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version + 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateCompleted
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockHistoryMgr.On("DeleteWorkflowExecutionHistory", mock.Anything).Return(nil).Once()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingEqualToThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateCompleted
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               false,
			PreviousRunID:               "",
			ReplicationState:            replicationState,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               true,
			PreviousRunID:               currentRunID,
			ReplicationState:            replicationState,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.True(now.Equal(transferTasks[0].GetVisibilityTimestamp()))
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingNotLessThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version - 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateCompleted
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               false,
			PreviousRunID:               "",
			ReplicationState:            replicationState,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               true,
			PreviousRunID:               currentRunID,
			ReplicationState:            replicationState,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.True(now.Equal(transferTasks[0].GetVisibilityTimestamp()))
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingLessThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version + 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockHistoryMgr.On("DeleteWorkflowExecutionHistory", mock.Anything).Return(nil).Once()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingEqualToCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()

	currentContext := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	currentMsBuilder := &mockMutableState{}
	currentMsBuilder.On("IsWorkflowExecutionRunning").Return(true)
	currentMsBuilder.On("HasBufferedReplicationTasks").Return(false)
	// return empty since not actually used
	currentMsBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})
	// return nil to bypass updating the version, since this test does not test that
	currentMsBuilder.On("GetReplicationState").Return(nil)
	currentContext.msBuilder = currentMsBuilder
	s.historyReplicator.historyCache.PutIfNotExist(currentRunID, currentContext)
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Equal(ErrRetryExistingWorkflow, err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingLargerThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version - 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               false,
			PreviousRunID:               "",
			ReplicationState:            replicationState,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               true,
			PreviousRunID:               currentRunID,
			ReplicationState:            replicationState,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()

	currentContext, currentRelease, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecution(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	})
	s.Nil(err)
	currentMsBuilder := &mockMutableState{}
	// 2 mocks below are just to by pass unnecessary mocks
	currentMsBuilder.On("GetReplicationState").Return(nil)
	currentMsBuilder.On("IsWorkflowExecutionRunning").Return(false)
	currentContext.msBuilder = currentMsBuilder
	currentRelease(nil)

	err = s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.True(now.Equal(transferTasks[0].GetVisibilityTimestamp()))
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetRunning() {
	runID := uuid.New()
	incomingVersion := int64(4096)
	incomingTimestamp := int64(11238)

	msBuilderTarget := &mockMutableState{}
	msBuilderTarget.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderTarget.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{RunID: runID})
	prevRunID, err := s.historyReplicator.conflictResolutionTerminateCurrentRunningIfNotSelf(ctx.Background(), msBuilderTarget, incomingVersion, incomingTimestamp, s.logger)
	s.Nil(err)
	s.Equal(runID, prevRunID)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetClosed_CurrentClosed() {
	incomingVersion := int64(4096)
	incomingTimestamp := int64(11238)

	domainID := validDomainID
	workflowID := "some random target workflow ID"
	targetRunID := uuid.New()

	msBuilderTarget := &mockMutableState{}
	msBuilderTarget.On("IsWorkflowExecutionRunning").Return(false)
	msBuilderTarget.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
		CloseStatus: persistence.WorkflowCloseStatusContinuedAsNew,
	})

	currentRunID := uuid.New()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:       currentRunID,
		CloseStatus: persistence.WorkflowCloseStatusCompleted,
	}, nil)

	prevRunID, err := s.historyReplicator.conflictResolutionTerminateCurrentRunningIfNotSelf(ctx.Background(), msBuilderTarget, incomingVersion, incomingTimestamp, s.logger)
	s.Nil(err)
	s.Equal(currentRunID, prevRunID)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetClosed_CurrentRunning() {
	incomingVersion := int64(4096)
	incomingTimestamp := int64(11238)
	incomingCluster := cluster.TestAlternativeClusterName
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", incomingVersion).Return(incomingCluster)

	domainVersion := int64(4081)
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random target workflow ID"
	targetRunID := uuid.New()

	msBuilderTarget := &mockMutableState{}
	msBuilderTarget.On("IsWorkflowExecutionRunning").Return(false)
	msBuilderTarget.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
		CloseStatus: persistence.WorkflowCloseStatusContinuedAsNew,
	})

	// this mocks are for the terminate current workflow operation
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			FailoverVersion: domainVersion, // this does not matter
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()

	currentRunID := uuid.New()
	contextCurrent, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecution(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	})
	s.Nil(err)
	msBuilderCurrent := &mockMutableState{}
	currentNextEventID := int64(999)
	msBuilderCurrent.On("GetNextEventID").Return(currentNextEventID)
	msBuilderCurrent.On("GetLastWriteVersion").Return(incomingVersion - 1)             // this is not actually used, but will be called
	msBuilderCurrent.On("GetReplicationState").Return(&persistence.ReplicationState{}) // this is used to update the version on mutable state
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true)                     // this is used to update the version on mutable state
	msBuilderCurrent.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{RunID: currentRunID, CloseStatus: persistence.WorkflowCloseStatusNone})
	msBuilderCurrent.On("UpdateReplicationStateVersion", domainVersion, false).Twice()
	msBuilderCurrent.On("UpdateReplicationStateVersion", incomingVersion, true).Once()
	contextCurrent.msBuilder = msBuilderCurrent
	release(nil)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:       currentRunID,
		CloseStatus: persistence.WorkflowCloseStatusNone,
	}, nil)

	// return nil, to trigger the history engine to return err, so we can assert on it
	// this is to save a lot of meaningless mock, since we are not testing functionality of history engine
	msBuilderCurrent.On("ReplicateWorkflowExecutionTerminatedEvent", mock.MatchedBy(func(input *shared.HistoryEvent) bool {
		return reflect.DeepEqual(&shared.HistoryEvent{
			EventId:   common.Int64Ptr(currentNextEventID),
			Timestamp: common.Int64Ptr(incomingTimestamp),
			Version:   common.Int64Ptr(incomingVersion),
			EventType: shared.EventTypeWorkflowExecutionTerminated.Ptr(),
			WorkflowExecutionTerminatedEventAttributes: &shared.WorkflowExecutionTerminatedEventAttributes{
				Reason:   common.StringPtr(workflowTerminationReason),
				Identity: common.StringPtr(workflowTerminationIdentity),
				Details:  nil,
			},
		}, input)
	})).Return(nil)

	expectedError := &shared.ServiceBusyError{} // return an error to by pass unnecessary mocks
	msBuilderCurrent.On("CloseUpdateSession").Return(nil, expectedError).Once()

	prevRunID, err := s.historyReplicator.conflictResolutionTerminateCurrentRunningIfNotSelf(ctx.Background(), msBuilderTarget, incomingVersion, incomingTimestamp, s.logger)
	s.Equal(expectedError, err)
	s.Equal(currentRunID, prevRunID)
}
