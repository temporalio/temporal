package eventhandler

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
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
)

type (
	pastEventsHandlerSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller
		testProcessToolBox
		replication.ProcessToolBox

		pastEventsHandler PastEventsHandler
	}
)

func TestPastEventsHandlerSuite(t *testing.T) {
	s := new(pastEventsHandlerSuite)
	suite.Run(t, s)
}

func (s *pastEventsHandlerSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *pastEventsHandlerSuite) TearDownSuite() {

}

func (s *pastEventsHandlerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.testProcessToolBox, s.ProcessToolBox = initializeToolBox(s.controller)
	s.pastEventsHandler = NewPastEventsHandler(
		s.ProcessToolBox,
	)
}

type ImportWorkflowExecutionRequestMatcher struct {
	ExpectedRequest *historyservice.ImportWorkflowExecutionRequest
}

func (m *ImportWorkflowExecutionRequestMatcher) Matches(x interface{}) bool {
	return m.ExpectedRequest.Equal(x)
}

func (m *ImportWorkflowExecutionRequestMatcher) String() string {
	return m.ExpectedRequest.String()
}

func (s *pastEventsHandlerSuite) TestHandlePastHistoryEvents_AlreadyExist() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 1},
			{EventId: 15, Version: 2},
		},
	}
	historyEvents := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 7,
			},
			{
				EventId: 8,
			},
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
	batch := serializeEvents(s.EventSerializer, historyEvents)

	request := &historyservice.ImportWorkflowExecutionRequest{
		NamespaceId: namespaceId,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runId,
		},
		HistoryBatches: batch,
		VersionHistory: versionHistory,
		Token:          nil,
	}
	engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{request}).Return(
		&historyservice.ImportWorkflowExecutionResponse{
			Token:         nil,
			EventsApplied: false,
		}, nil).Times(1)

	err := s.pastEventsHandler.HandlePastEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		versionHistory.Items,
		historyEvents,
	)
	s.Nil(err)
}

func (s *pastEventsHandlerSuite) TestHandlePastHistoryEvents_IncludeLastEvent_AppliedAndDirectCommit() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 1}, // Last past event is 10
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
		engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{
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
	)

	err := s.pastEventsHandler.HandlePastEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		versionHistory.Items,
		[][]*historypb.HistoryEvent{historyEvents},
	)
	s.Nil(err)
}

func (s *pastEventsHandlerSuite) TestHandleHistoryEvents_PastOnly_ImportAllPastAndCommit() {
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
	initialHistoryEvents := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 7,
				Version: 1,
			},
			{
				EventId: 8,
				Version: 1,
			},
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
	historyBatch1 := replication.HistoryBatch{
		RawEventBatch:  dataBlob1,
		VersionHistory: versionHistory,
	}
	historyBatch2 := replication.HistoryBatch{
		RawEventBatch:  dataBlob2,
		VersionHistory: versionHistory,
	}

	times := 0
	fetcher := collection.NewPagingIterator(func(paginationToken []byte) ([]replication.HistoryBatch, []byte, error) {
		if times < historyImportBlobSize {
			times++
			return []replication.HistoryBatch{historyBatch1}, []byte{1, 1, 0}, nil
		}
		return []replication.HistoryBatch{historyBatch2}, nil, nil
	})

	batch := serializeEvents(s.EventSerializer, initialHistoryEvents)
	returnToken1 := []byte{1, 0, 0, 1, 1, 1, 1, 0}
	returnToken2 := []byte{1, 0, 0, 1, 1, 1, 1, 1}
	returnToken3 := []byte{1, 1, 0, 1, 1, 1, 1, 1}

	fetchedBlob1 := []*commonpb.DataBlob{}
	for i := 0; i < historyImportBlobSize; i++ {
		fetchedBlob1 = append(fetchedBlob1, dataBlob1)
	}
	fetchedBlob2 := []*commonpb.DataBlob{dataBlob2}

	gomock.InOrder(
		// import the initial events
		engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{
			ExpectedRequest: &historyservice.ImportWorkflowExecutionRequest{
				NamespaceId: namespaceId,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      runId,
				},
				HistoryBatches: batch,
				VersionHistory: versionHistory,
				Token:          nil,
			},
		}).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token:         returnToken1,
			EventsApplied: true,
		}, nil).Times(1),

		// fetch more events
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

		// import the fetched events inside the fetch loop
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

		// import the fetched events outside the loop
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

		// commit the import
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

	err := s.pastEventsHandler.HandlePastEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		versionHistory.Items,
		initialHistoryEvents,
	)
	s.Nil(err)
}

func serializeEvents(serializer serialization.Serializer, events [][]*historypb.HistoryEvent) []*commonpb.DataBlob {
	blobs := []*commonpb.DataBlob{}
	for _, batch := range events {
		blob, err := serializer.SerializeEvents(batch, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			panic(err)
		}
		blobs = append(blobs, blob)
	}
	return blobs
}
