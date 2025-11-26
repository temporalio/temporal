package eventhandler

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	resendHandlerSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockClusterMetadata *cluster.MockMetadata
		mockNamespaceCache  *namespace.MockRegistry
		mockClientBean      *client.MockBean
		mockAdminClient     *adminservicemock.MockAdminServiceClient
		mockHistoryClient   *historyservicemock.MockHistoryServiceClient

		namespaceID namespace.ID
		namespace   namespace.Name

		serializer     serialization.Serializer
		logger         log.Logger
		config         *configs.Config
		resendHandler  ResendHandler
		engine         *historyi.MockEngine
		historyFetcher *MockHistoryPaginatedFetcher
		importer       *MockEventImporter
	}
)

func TestResendHandlerSuite(t *testing.T) {
	s := new(resendHandlerSuite)
	suite.Run(t, s)
}

func (s *resendHandlerSuite) SetupSuite() {
}

func (s *resendHandlerSuite) TearDownSuite() {

}

func (s *resendHandlerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockClusterMetadata = cluster.NewMockMetadata(s.controller)
	s.mockClientBean = client.NewMockBean(s.controller)
	s.mockAdminClient = adminservicemock.NewMockAdminServiceClient(s.controller)
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockNamespaceCache = namespace.NewMockRegistry(s.controller)

	s.mockClientBean.EXPECT().GetRemoteAdminClient(gomock.Any()).Return(s.mockAdminClient, nil).AnyTimes()

	s.logger = log.NewTestLogger()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	s.namespaceID = namespace.ID(uuid.NewString())
	s.namespace = "some random namespace name"
	s.config = tests.NewDynamicConfig()
	s.historyFetcher = NewMockHistoryPaginatedFetcher(s.controller)
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: s.namespaceID.String(), Name: s.namespace.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(s.namespace).Return(namespaceEntry, nil).AnyTimes()
	s.engine = historyi.NewMockEngine(s.controller)
	s.serializer = serialization.NewSerializer()
	s.importer = NewMockEventImporter(s.controller)
	s.resendHandler = NewResendHandler(
		s.mockNamespaceCache,
		s.mockClientBean,
		s.serializer,
		s.mockClusterMetadata,
		func(ctx context.Context, namespaceId namespace.ID, workflowId string) (historyi.Engine, error) {
			return s.engine, nil
		},
		s.historyFetcher,
		s.importer,
		s.logger,
		s.config,
	)
}

func (s *resendHandlerSuite) TearDownTest() {
	s.controller.Finish()
}

type historyEventMatrixMatcher struct {
	expected [][]*historypb.HistoryEvent
}

func (m *historyEventMatrixMatcher) Matches(x interface{}) bool {
	actual, ok := x.([][]*historypb.HistoryEvent)
	if !ok {
		return false
	}
	if len(m.expected) != len(actual) {
		return false
	}
	for i := range m.expected {
		if len(m.expected[i]) != len(actual[i]) {
			return false
		}
		for j := range m.expected[i] {
			if m.expected[i][j].EventId != actual[i][j].EventId || m.expected[i][j].Version != actual[i][j].Version {
				return false
			}
		}
	}
	return true
}

func (m *historyEventMatrixMatcher) String() string {
	return "matches history event matrix"
}

// NewHistoryEventMatrixMatcher creates a gomock Matcher for [][]*historypb.HistoryEvent
func NewHistoryEventMatrixMatcher(expected [][]*historypb.HistoryEvent) gomock.Matcher {
	return &historyEventMatrixMatcher{expected: expected}
}

func (s *resendHandlerSuite) TestResendHistoryEvents_NoRemoteEvents() {
	workflowID := "some random workflow ID"
	runID := uuid.NewString()
	endEventID := int64(12)
	endEventVersion := int64(123)
	s.config.ReplicationResendMaxBatchCount = dynamicconfig.GetIntPropertyFn(2)

	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(123))
	s.mockClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))
	eventBatch := []*historypb.HistoryEvent{
		{EventId: 1, Version: 123},
		{EventId: 2, Version: 123},
	}
	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 123},
		},
	}
	fetcher := collection.NewPagingIterator(func(paginationToken []byte) ([]*HistoryBatch, []byte, error) {
		return []*HistoryBatch{
			{RawEventBatch: s.serializeEvents(eventBatch), VersionHistory: versionHistory},
		}, nil, nil
	})
	s.historyFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(),
		cluster.TestAlternativeClusterName,
		s.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		int64(12),
		int64(123),
	).Return(fetcher)
	err := s.resendHandler.ResendHistoryEvents(
		context.Background(),
		cluster.TestAlternativeClusterName,
		s.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		endEventID,
		endEventVersion,
	)
	s.Error(err)
}

func (s *resendHandlerSuite) TestSendSingleWorkflowHistory_AllRemoteEvents() {
	workflowID := "some random workflow ID"
	runID := uuid.NewString()
	endEventID := int64(13)
	endEventVersion := int64(123)
	s.config.ReplicationResendMaxBatchCount = dynamicconfig.GetIntPropertyFn(2)

	eventBatch0 := []*historypb.HistoryEvent{
		{EventId: 1, Version: 123},
		{EventId: 2, Version: 123},
	}
	eventBatch1 := []*historypb.HistoryEvent{
		{EventId: 3, Version: 123},
		{EventId: 4, Version: 123},
	}
	eventBatch2 := []*historypb.HistoryEvent{
		{EventId: 5, Version: 123},
		{EventId: 6, Version: 123},
	}
	eventBatch3 := []*historypb.HistoryEvent{
		{EventId: 7, Version: 123},
		{EventId: 8, Version: 123},
	}
	eventBatch4 := []*historypb.HistoryEvent{
		{EventId: 9, Version: 123},
		{EventId: 10, Version: 123},
	}
	eventBatch5 := []*historypb.HistoryEvent{
		{EventId: 11, Version: 123},
		{EventId: 12, Version: 123},
	}
	versionHistory0 := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 123},
		},
	}
	versionHistory1 := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 15, Version: 123},
		},
	}
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.mockClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	fetcher := collection.NewPagingIterator(func(paginationToken []byte) ([]*HistoryBatch, []byte, error) {
		return []*HistoryBatch{
			{RawEventBatch: s.serializeEvents(eventBatch0), VersionHistory: versionHistory0},
			{RawEventBatch: s.serializeEvents(eventBatch1), VersionHistory: versionHistory0},
			{RawEventBatch: s.serializeEvents(eventBatch2), VersionHistory: versionHistory0},
			{RawEventBatch: s.serializeEvents(eventBatch3), VersionHistory: versionHistory1},
			{RawEventBatch: s.serializeEvents(eventBatch4), VersionHistory: versionHistory1},
			{RawEventBatch: s.serializeEvents(eventBatch5), VersionHistory: versionHistory1},
		}, nil, nil
	})

	s.historyFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(),
		cluster.TestAlternativeClusterName,
		s.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		int64(13),
		int64(123),
	).Return(fetcher)

	workflowKey := definition.WorkflowKey{
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}

	gomock.InOrder(
		s.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory0.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch0, eventBatch1}),
			nil,
			"",
		).Times(1),
		s.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory0.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch2}),
			nil,
			"",
		).Times(1),
		s.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory1.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch3, eventBatch4}),
			nil,
			"",
		).Times(1),
		s.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory1.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch5}),
			nil,
			"",
		).Times(1),
	)

	err := s.resendHandler.ResendHistoryEvents(
		context.Background(),
		cluster.TestAlternativeClusterName,
		s.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		endEventID,
		endEventVersion,
	)
	s.Nil(err)
}

func (s *resendHandlerSuite) TestSendSingleWorkflowHistory_LocalAndRemoteEvents() {
	workflowID := "some random workflow ID"
	runID := uuid.NewString()
	endEventID := int64(9)
	endEventVersion := int64(124)
	s.config.ReplicationResendMaxBatchCount = dynamicconfig.GetIntPropertyFn(2)

	eventBatch0 := []*historypb.HistoryEvent{
		{EventId: 1, Version: 123},
		{EventId: 2, Version: 123},
	}
	eventBatch1 := []*historypb.HistoryEvent{
		{EventId: 3, Version: 123},
		{EventId: 4, Version: 123},
	}
	eventBatch2 := []*historypb.HistoryEvent{
		{EventId: 5, Version: 123},
		{EventId: 6, Version: 123},
	}
	eventBatch3 := []*historypb.HistoryEvent{
		{EventId: 7, Version: 124},
		{EventId: 8, Version: 124},
	}
	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 6, Version: 123},
			{EventId: 10, Version: 124},
		},
	}
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(123))
	s.mockClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	fetcher0 := collection.NewPagingIterator(func(paginationToken []byte) ([]*HistoryBatch, []byte, error) {
		return []*HistoryBatch{
			{RawEventBatch: s.serializeEvents(eventBatch0), VersionHistory: versionHistory},
			{RawEventBatch: s.serializeEvents(eventBatch1), VersionHistory: versionHistory},
			{RawEventBatch: s.serializeEvents(eventBatch2), VersionHistory: versionHistory},
			{RawEventBatch: s.serializeEvents(eventBatch3), VersionHistory: versionHistory},
		}, nil, nil
	})

	fetcher1 := collection.NewPagingIterator(func(paginationToken []byte) ([]*HistoryBatch, []byte, error) {
		return []*HistoryBatch{
			{RawEventBatch: s.serializeEvents(eventBatch3), VersionHistory: versionHistory},
		}, nil, nil
	})

	s.historyFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(),
		cluster.TestAlternativeClusterName,
		s.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		int64(9),
		int64(124),
	).Return(fetcher0).Times(1)

	s.historyFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(),
		cluster.TestAlternativeClusterName,
		s.namespaceID,
		workflowID,
		runID,
		int64(6),
		int64(123),
		int64(9),
		int64(124),
	).Return(fetcher1).Times(1)

	workflowKey := definition.WorkflowKey{
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}

	s.importer.EXPECT().ImportHistoryEventsFromBeginning(
		gomock.Any(),
		cluster.TestAlternativeClusterName,
		workflowKey,
		int64(6),
		int64(123),
	).Return(nil)
	s.engine.EXPECT().ReplicateHistoryEvents(
		gomock.Any(),
		workflowKey,
		nil,
		versionHistory.Items,
		NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch3}),
		nil,
		"",
	).Times(1)

	err := s.resendHandler.ResendHistoryEvents(
		context.Background(),
		cluster.TestAlternativeClusterName,
		s.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		endEventID,
		endEventVersion,
	)
	s.Nil(err)
}

func (s *resendHandlerSuite) TestSendSingleWorkflowHistory_MixedVersionHistory_RemoteEventsOnly() {
	workflowID := "some random workflow ID"
	runID := uuid.NewString()
	endEventID := int64(9)
	endEventVersion := int64(124)
	s.config.ReplicationResendMaxBatchCount = dynamicconfig.GetIntPropertyFn(2)

	eventBatch3 := []*historypb.HistoryEvent{
		{EventId: 7, Version: 124},
		{EventId: 8, Version: 124},
	}
	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 6, Version: 123},
			{EventId: 10, Version: 124},
		},
	}
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(123))
	s.mockClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	fetcher0 := collection.NewPagingIterator(func(paginationToken []byte) ([]*HistoryBatch, []byte, error) {
		return []*HistoryBatch{
			{RawEventBatch: s.serializeEvents(eventBatch3), VersionHistory: versionHistory},
		}, nil, nil
	})

	s.historyFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(),
		cluster.TestAlternativeClusterName,
		s.namespaceID,
		workflowID,
		runID,
		int64(6),
		int64(123),
		int64(9),
		int64(124),
	).Return(fetcher0).Times(1)

	workflowKey := definition.WorkflowKey{
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}

	s.engine.EXPECT().ReplicateHistoryEvents(
		gomock.Any(),
		workflowKey,
		nil,
		versionHistory.Items,
		NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch3}),
		nil,
		"",
	).Times(1)

	err := s.resendHandler.ResendHistoryEvents(
		context.Background(),
		cluster.TestAlternativeClusterName,
		s.namespaceID,
		workflowID,
		runID,
		int64(6),
		int64(123),
		endEventID,
		endEventVersion,
	)
	s.Nil(err)
}

func (s *resendHandlerSuite) TestSendSingleWorkflowHistory_AllRemoteEvents_BatchTest() {
	workflowID := "some random workflow ID"
	runID := uuid.NewString()
	endEventID := int64(13)
	endEventVersion := int64(123)
	s.config.ReplicationResendMaxBatchCount = dynamicconfig.GetIntPropertyFn(10)

	eventBatch0 := []*historypb.HistoryEvent{
		{EventId: 1},
		{EventId: 2},
	}
	eventBatch1 := []*historypb.HistoryEvent{
		{EventId: 3, Version: 123},
		{EventId: 4, Version: 123},
	}
	eventBatch2 := []*historypb.HistoryEvent{
		{EventId: 5, Version: 123},
		{EventId: 6, Version: 123},
	}
	eventBatch3 := []*historypb.HistoryEvent{
		{EventId: 7, Version: 124},
		{EventId: 8, Version: 124},
	}
	eventBatch4 := []*historypb.HistoryEvent{
		{EventId: 9, Version: 124},
		{EventId: 10, Version: 124},
	}
	eventBatch5 := []*historypb.HistoryEvent{
		{EventId: 11, Version: 127},
		{EventId: 12, Version: 127},
	}
	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 2},
			{EventId: 6, Version: 123},
			{EventId: 10, Version: 124},
			{EventId: 12, Version: 127},
		},
	}
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.mockClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	fetcher := collection.NewPagingIterator(func(paginationToken []byte) ([]*HistoryBatch, []byte, error) {
		return []*HistoryBatch{
			{RawEventBatch: s.serializeEvents(eventBatch0), VersionHistory: versionHistory},
			{RawEventBatch: s.serializeEvents(eventBatch1), VersionHistory: versionHistory},
			{RawEventBatch: s.serializeEvents(eventBatch2), VersionHistory: versionHistory},
			{RawEventBatch: s.serializeEvents(eventBatch3), VersionHistory: versionHistory},
			{RawEventBatch: s.serializeEvents(eventBatch4), VersionHistory: versionHistory},
			{RawEventBatch: s.serializeEvents(eventBatch5), VersionHistory: versionHistory},
		}, nil, nil
	})

	s.historyFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(),
		cluster.TestAlternativeClusterName,
		s.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		int64(13),
		int64(123),
	).Return(fetcher)

	workflowKey := definition.WorkflowKey{
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}

	gomock.InOrder(
		s.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch0}),
			nil,
			"",
		).Times(1),
		s.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch1, eventBatch2}),
			nil,
			"",
		).Times(1),
		s.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch3, eventBatch4}),
			nil,
			"",
		).Times(1),
		s.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch5}),
			nil,
			"",
		).Times(1),
	)

	err := s.resendHandler.ResendHistoryEvents(
		context.Background(),
		cluster.TestAlternativeClusterName,
		s.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		endEventID,
		endEventVersion,
	)
	s.Nil(err)
}

func (s *resendHandlerSuite) serializeEvents(events []*historypb.HistoryEvent) *commonpb.DataBlob {
	blob, err := s.serializer.SerializeEvents(events)
	s.Nil(err)
	return &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         blob.Data,
	}
}
