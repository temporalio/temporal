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

package replication

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
)

type (
	historyEventHandlerSuite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		clusterMetadata         *cluster.MockMetadata
		clientBean              *client.MockBean
		shardController         *shard.MockController
		namespaceCache          *namespace.MockRegistry
		ndcHistoryResender      *xdc.MockNDCHistoryResender
		remoteHistoryFetcher    *MockHistoryPaginatedFetcher
		metricsHandler          metrics.Handler
		logger                  log.Logger
		sourceCluster           string
		eagerNamespaceRefresher *MockEagerNamespaceRefresher
		config                  *configs.Config
		eventSerializer         serialization.Serializer

		historyEventHandler HistoryEventsHandler
	}
)

type ImportWorkflowExecutionRequestMatcher struct {
	ExpectedRequest *historyservice.ImportWorkflowExecutionRequest
}

func (m *ImportWorkflowExecutionRequestMatcher) Matches(x interface{}) bool {
	return m.ExpectedRequest.Equal(x)
}

func (m *ImportWorkflowExecutionRequestMatcher) String() string {
	return m.ExpectedRequest.String()
}

func TestHistoryEventHandlerSuite(t *testing.T) {
	s := new(historyEventHandlerSuite)
	suite.Run(t, s)
}

func (s *historyEventHandlerSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *historyEventHandlerSuite) TearDownSuite() {

}

func (s *historyEventHandlerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.ndcHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.sourceCluster = "some cluster"
	s.eagerNamespaceRefresher = NewMockEagerNamespaceRefresher(s.controller)
	s.config = tests.NewDynamicConfig()
	s.remoteHistoryFetcher = NewMockHistoryPaginatedFetcher(s.controller)
	s.eventSerializer = serialization.NewSerializer()

	s.historyEventHandler = NewHistoryEventsHandler(
		ProcessToolBox{
			Config:                  s.config,
			ClusterMetadata:         s.clusterMetadata,
			ClientBean:              s.clientBean,
			ShardController:         s.shardController,
			NamespaceCache:          s.namespaceCache,
			NDCHistoryResender:      s.ndcHistoryResender,
			MetricsHandler:          s.metricsHandler,
			Logger:                  s.logger,
			EagerNamespaceRefresher: s.eagerNamespaceRefresher,
			EventSerializer:         s.eventSerializer,

			DLQWriter:               NoopDLQWriter{},
			HistoryPaginatedFetcher: s.remoteHistoryFetcher,
		},
	)
}

func (s *historyEventHandlerSuite) TestHandleHistoryEvents_FutureOnly() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{1, 0, 1},
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 1},
			{EventId: 15, Version: 2},
		},
	}
	historyEvents := []*historypb.HistoryEvent{
		{
			EventId: 11,
		},
		{
			EventId: 12,
		},
	}
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}
	shardContext := shard.NewMockContext(s.controller)
	engine := shard.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(1)
	engine.EXPECT().ReplicateHistoryEvents(
		gomock.Any(),
		workflowKey,
		nil,
		versionHistory.Items,
		[][]*historypb.HistoryEvent{historyEvents},
		nil,
	).Times(1)

	metricsTag := "some random metrics tag"
	err := s.historyEventHandler.HandleHistoryEvents(
		context.Background(),
		workflowKey,
		remoteCluster,
		metricsTag,
		nil,
		versionHistory.Items,
		historyEvents,
		nil,
	)
	s.Nil(err)
}

func (s *historyEventHandlerSuite) TestHandleHistoryEvents_PastOnly_AlreadyExist() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 1},
			{EventId: 15, Version: 2},
		},
	}
	historyEvents := []*historypb.HistoryEvent{
		{
			EventId: 7,
		},
		{
			EventId: 8,
		},
	}
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}
	shardContext := shard.NewMockContext(s.controller)
	engine := shard.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(1)
	batch, _ := s.eventSerializer.SerializeEvents(historyEvents, enumspb.ENCODING_TYPE_PROTO3)

	request := &historyservice.ImportWorkflowExecutionRequest{
		NamespaceId: namespaceId,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runId,
		},
		VersionHistory: versionHistory,
		HistoryBatches: []*commonpb.DataBlob{batch},
		Token:          nil,
	}
	engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{ExpectedRequest: request}).Return(&historyservice.ImportWorkflowExecutionResponse{
		Token:         nil,
		EventsApplied: false,
	}, nil).Times(1)

	metricsTag := "some random metrics tag"
	err := s.historyEventHandler.HandleHistoryEvents(
		context.Background(),
		workflowKey,
		remoteCluster,
		metricsTag,
		nil,
		versionHistory.Items,
		historyEvents,
		nil,
	)
	s.Nil(err)
}

func (s *historyEventHandlerSuite) TestHandleHistoryEvents_PastOnly_AppliedAndDirectCommit() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 1},
			{EventId: 15, Version: 2},
		},
	}
	historyEvents := []*historypb.HistoryEvent{
		{
			EventId: 7,
		},
		{
			EventId: 8,
		},
		{
			EventId: 9,
		},
		{
			EventId: 10,
		},
	}
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}

	shardContext := shard.NewMockContext(s.controller)
	engine := shard.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(1)
	batch, _ := s.eventSerializer.SerializeEvents(historyEvents, enumspb.ENCODING_TYPE_PROTO3)
	returnToken1 := []byte{1, 0, 0, 1, 1, 1, 1, 0}
	gomock.InOrder(
		engine.EXPECT().ImportWorkflowExecution(gomock.Any(),
			&historyservice.ImportWorkflowExecutionRequest{
				NamespaceId: namespaceId,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      runId,
				},
				HistoryBatches: []*commonpb.DataBlob{batch},
				VersionHistory: versionHistory,
				Token:          nil,
			},
		).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token:         returnToken1,
			EventsApplied: true,
		}, nil).Times(1),

		engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{
			ExpectedRequest: &historyservice.ImportWorkflowExecutionRequest{
				NamespaceId: namespaceId,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      runId,
				},
				HistoryBatches: []*commonpb.DataBlob{},
				VersionHistory: versionHistory,
				Token:          returnToken1,
			},
		}).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token: nil,
		}, nil).Times(1),
	)

	metricsTag := "some random metrics tag"
	err := s.historyEventHandler.HandleHistoryEvents(
		context.Background(),
		workflowKey,
		remoteCluster,
		metricsTag,
		nil,
		versionHistory.Items,
		historyEvents,
		nil,
	)
	s.Nil(err)
}

func (s *historyEventHandlerSuite) TestHandleHistoryEvents_PastOnly_ImportAllPastAndCommit() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 5, Version: 3},
			{EventId: 20, Version: 1},
			{EventId: 25, Version: 2},
		},
	}
	initialHistoryEvents := []*historypb.HistoryEvent{
		{
			EventId: 7,
			Version: 1,
		},
		{
			EventId: 8,
			Version: 1,
		},
	}
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}

	shardContext := shard.NewMockContext(s.controller)
	engine := shard.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(1)

	dataBlob1 := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte{1, 0, 1},
	}
	dataBlob2 := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte{1, 1, 0},
	}
	historyBatch1 := historyBatch{
		RawEventBatch:  dataBlob1,
		VersionHistory: versionHistory,
	}
	historyBatch2 := historyBatch{
		RawEventBatch:  dataBlob2,
		VersionHistory: versionHistory,
	}

	times := 0
	fetcher := collection.NewPagingIterator(func(paginationToken []byte) ([]historyBatch, []byte, error) {
		if times < historyImportBlobSize {
			times++
			return []historyBatch{historyBatch1}, []byte{1, 1, 0}, nil
		}
		return []historyBatch{historyBatch2}, nil, nil
	})

	batch, _ := s.eventSerializer.SerializeEvents(initialHistoryEvents, enumspb.ENCODING_TYPE_PROTO3)
	returnToken1 := []byte{1, 0, 0, 1, 1, 1, 1, 0}
	returnToken2 := []byte{1, 0, 0, 1, 1, 1, 1, 1}
	returnToken3 := []byte{1, 1, 0, 1, 1, 1, 1, 1}

	fetchedBlob1 := []*commonpb.DataBlob{}
	for i := 0; i < historyImportBlobSize; i++ {
		fetchedBlob1 = append(fetchedBlob1, dataBlob1)
	}
	fetchedBlob2 := []*commonpb.DataBlob{dataBlob2}

	gomock.InOrder(
		engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{
			ExpectedRequest: &historyservice.ImportWorkflowExecutionRequest{
				NamespaceId: namespaceId,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      runId,
				},
				HistoryBatches: []*commonpb.DataBlob{batch},
				VersionHistory: versionHistory,
				Token:          nil,
			},
		}).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token:         returnToken1,
			EventsApplied: true,
		}, nil).Times(1),

		s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIterator(
			gomock.Any(),
			remoteCluster,
			namespace.ID(namespaceId),
			workflowId,
			runId,
			int64(8),
			int64(1),
			int64(20),
			int64(1),
		).Return(fetcher).Times(1),

		engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{
			ExpectedRequest: &historyservice.ImportWorkflowExecutionRequest{
				NamespaceId: namespaceId,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      runId,
				},
				HistoryBatches: fetchedBlob1,
				VersionHistory: versionHistory,
				Token:          returnToken1,
			},
		}).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token:         returnToken2,
			EventsApplied: true,
		}, nil).Times(1),

		engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{
			ExpectedRequest: &historyservice.ImportWorkflowExecutionRequest{
				NamespaceId: namespaceId,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      runId,
				},
				HistoryBatches: fetchedBlob2,
				VersionHistory: versionHistory,
				Token:          returnToken2,
			},
		}).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token:         returnToken3,
			EventsApplied: true,
		}, nil).Times(1),

		engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{
			ExpectedRequest: &historyservice.ImportWorkflowExecutionRequest{
				NamespaceId: namespaceId,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      runId,
				},
				HistoryBatches: []*commonpb.DataBlob{},
				VersionHistory: versionHistory,
				Token:          returnToken3,
			},
		}).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token: nil,
		}, nil).Times(1),
	)

	metricsTag := "some random metrics tag"
	err := s.historyEventHandler.HandleHistoryEvents(
		context.Background(),
		workflowKey,
		remoteCluster,
		metricsTag,
		nil,
		versionHistory.Items,
		initialHistoryEvents,
		nil,
	)
	s.Nil(err)
}

func (s *historyEventHandlerSuite) TestHandleHistoryEvents_PastAndFuture_ImportAndCommitPastThenApplyFuture() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 5, Version: 3},
			{EventId: 10, Version: 1},
			{EventId: 20, Version: 2},
		},
	}
	pastHistoryEvents := []*historypb.HistoryEvent{
		{
			EventId: 7,
			Version: 1,
		},
		{
			EventId: 8,
			Version: 1,
		},
		{
			EventId: 9,
			Version: 1,
		},
		{
			EventId: 10,
			Version: 1,
		},
	}
	futureHistoryEvents := []*historypb.HistoryEvent{
		{
			EventId: 11,
			Version: 2,
		},
		{
			EventId: 12,
			Version: 2,
		},
	}
	initialHistoryEvents := append(pastHistoryEvents, futureHistoryEvents...)
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}

	shardContext := shard.NewMockContext(s.controller)
	engine := shard.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(1)

	batch, _ := s.eventSerializer.SerializeEvents(pastHistoryEvents, enumspb.ENCODING_TYPE_PROTO3)
	returnToken1 := []byte{1, 0, 0, 1, 1, 1, 1, 0}

	gomock.InOrder(
		engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{
			ExpectedRequest: &historyservice.ImportWorkflowExecutionRequest{
				NamespaceId: namespaceId,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      runId,
				},
				HistoryBatches: []*commonpb.DataBlob{batch},
				VersionHistory: versionHistory,
				Token:          nil,
			},
		}).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token:         returnToken1,
			EventsApplied: true,
		}, nil).Times(1),

		engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{
			ExpectedRequest: &historyservice.ImportWorkflowExecutionRequest{
				NamespaceId: namespaceId,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      runId,
				},
				HistoryBatches: []*commonpb.DataBlob{},
				VersionHistory: versionHistory,
				Token:          returnToken1,
			},
		}).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token: nil,
		}, nil).Times(1),

		engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory.Items,
			[][]*historypb.HistoryEvent{futureHistoryEvents},
			nil,
		).Times(1),
	)

	metricsTag := "some random metrics tag"
	err := s.historyEventHandler.HandleHistoryEvents(
		context.Background(),
		workflowKey,
		remoteCluster,
		metricsTag,
		nil,
		versionHistory.Items,
		initialHistoryEvents,
		nil,
	)
	s.Nil(err)
}
