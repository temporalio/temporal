package eventhandler

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

type (
	eventImporterSuite struct {
		suite.Suite
		*require.Assertions
		controller           *gomock.Controller
		logger               log.Logger
		eventSerializer      serialization.Serializer
		remoteHistoryFetcher *MockHistoryPaginatedFetcher
		engineProvider       historyEngineProvider
		eventImporter        EventImporter
		engine               *historyi.MockEngine
	}
)

func TestEventImporterSuite(t *testing.T) {
	s := new(eventImporterSuite)
	suite.Run(t, s)
}

func (s *eventImporterSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *eventImporterSuite) TearDownSuite() {

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

func (s *eventImporterSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.logger = log.NewNoopLogger()
	s.eventSerializer = serialization.NewSerializer()
	s.remoteHistoryFetcher = NewMockHistoryPaginatedFetcher(s.controller)
	s.engine = historyi.NewMockEngine(s.controller)
	s.engineProvider = func(ctx context.Context, namespaceId namespace.ID, workflowId string) (historyi.Engine, error) {
		return s.engine, nil
	}
	s.eventImporter = NewEventImporter(
		s.remoteHistoryFetcher,
		s.engineProvider,
		s.eventSerializer,
		s.logger,
	)
}

func (s *eventImporterSuite) TestImportHistoryEvents_ImportAllLocalAndCommit() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 2, Version: 3},
			{EventId: 5, Version: 5},
			{EventId: 7, Version: 1001},
		},
	}
	historyBatch0 := []*historypb.HistoryEvent{
		{EventId: 1, Version: 3},
		{EventId: 2, Version: 3},
	}
	historyBatch1 := []*historypb.HistoryEvent{
		{EventId: 3, Version: 5},
		{EventId: 4, Version: 5},
		{EventId: 5, Version: 5},
	}
	historyBatch2 := []*historypb.HistoryEvent{
		{EventId: 6, Version: 1001},
		{EventId: 7, Version: 1001},
	}

	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}

	rawBatches := serializeEvents(s.eventSerializer, [][]*historypb.HistoryEvent{historyBatch0, historyBatch1, historyBatch2})
	fetcher := collection.NewPagingIterator(func(paginationToken []byte) ([]*HistoryBatch, []byte, error) {
		return []*HistoryBatch{
			{RawEventBatch: rawBatches[0], VersionHistory: versionHistory},
			{RawEventBatch: rawBatches[1], VersionHistory: versionHistory},
			{RawEventBatch: rawBatches[2], VersionHistory: versionHistory},
		}, nil, nil
	})

	returnToken1 := []byte{0}
	returnToken2 := []byte{1}
	returnToken3 := []byte{1, 0}

	gomock.InOrder(
		// fetch more events
		s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorInclusive(
			gomock.Any(),
			remoteCluster,
			namespace.ID(namespaceId),
			workflowId,
			runId,
			common.EmptyEventID,
			common.EmptyVersion,
			int64(7),
			int64(1001),
		).Return(fetcher).Times(1),

		// import the fetched events inside the fetch loop
		s.engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{
			ExpectedRequest: &historyservice.ImportWorkflowExecutionRequest{
				NamespaceId: namespaceId,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      runId,
				},
				HistoryBatches: []*commonpb.DataBlob{rawBatches[0]},
				VersionHistory: versionHistory,
				Token:          nil,
			},
		}).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token:         returnToken1,
			EventsApplied: true,
		}, nil).Times(1),

		s.engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{
			ExpectedRequest: &historyservice.ImportWorkflowExecutionRequest{
				NamespaceId: namespaceId,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      runId,
				},
				HistoryBatches: []*commonpb.DataBlob{rawBatches[1]},
				VersionHistory: versionHistory,
				Token:          returnToken1,
			},
		}).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token:         returnToken2,
			EventsApplied: true,
		}, nil).Times(1),

		// import the fetched events outside the loop
		s.engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{
			ExpectedRequest: &historyservice.ImportWorkflowExecutionRequest{
				NamespaceId: namespaceId,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      runId,
				},
				HistoryBatches: []*commonpb.DataBlob{rawBatches[2]},
				VersionHistory: versionHistory,
				Token:          returnToken2,
			},
		}).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token:         returnToken3,
			EventsApplied: true,
		}, nil).Times(1),

		// commit the import
		s.engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{
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

	err := s.eventImporter.ImportHistoryEventsFromBeginning(
		context.Background(),
		remoteCluster,
		workflowKey,
		7,
		1001,
	)
	s.Nil(err)
}

func serializeEvents(serializer serialization.Serializer, events [][]*historypb.HistoryEvent) []*commonpb.DataBlob {
	blobs := []*commonpb.DataBlob{}
	for _, batch := range events {
		blob, err := serializer.SerializeEvents(batch)
		if err != nil {
			panic(err)
		}
		blobs = append(blobs, blob)
	}
	return blobs
}
