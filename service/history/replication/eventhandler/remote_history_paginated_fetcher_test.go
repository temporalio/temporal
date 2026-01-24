package eventhandler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	historyPaginatedFetcherSuite struct {
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

		serializer serialization.Serializer
		logger     log.Logger

		fetcher *HistoryPaginatedFetcherImpl
	}
)

func TestNDCHistoryResenderSuite(t *testing.T) {
	s := new(historyPaginatedFetcherSuite)
	suite.Run(t, s)
}

func (s *historyPaginatedFetcherSuite) SetupSuite() {
}

func (s *historyPaginatedFetcherSuite) TearDownSuite() {

}

func (s *historyPaginatedFetcherSuite) SetupTest() {
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
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Id: s.namespaceID.String(), Name: s.namespace.String()}.Build(),
		persistencespb.NamespaceConfig_builder{Retention: timestamp.DurationFromDays(1)}.Build(),
		persistencespb.NamespaceReplicationConfig_builder{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		}.Build(),
		1234,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(s.namespace).Return(namespaceEntry, nil).AnyTimes()
	s.serializer = serialization.NewSerializer()
	s.fetcher = &HistoryPaginatedFetcherImpl{
		NamespaceRegistry: s.mockNamespaceCache,
		ClientBean:        s.mockClientBean,
		Serializer:        serialization.NewSerializer(),
		Logger:            s.logger,
	}
}

func (s *historyPaginatedFetcherSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *historyPaginatedFetcherSuite) TestGetSingleWorkflowHistoryIterator() {
	workflowID := "some random workflow ID"
	runID := uuid.NewString()
	startEventID := int64(123)
	startEventVersion := int64(100)
	token := []byte{1}
	pageSize := defaultPageSize
	eventBatch := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId:   2,
			Version:   123,
			EventTime: timestamppb.New(time.Now().UTC()),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId:   3,
			Version:   123,
			EventTime: timestamppb.New(time.Now().UTC()),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		}.Build(),
	}
	blob := s.serializeEvents(eventBatch)
	versionHistoryItems := []*historyspb.VersionHistoryItem{
		historyspb.VersionHistoryItem_builder{
			EventId: 1,
			Version: 1,
		}.Build(),
	}

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(
		gomock.Any(),
		adminservice.GetWorkflowExecutionRawHistoryV2Request_builder{
			NamespaceId: s.namespaceID.String(),
			Execution: commonpb.WorkflowExecution_builder{
				WorkflowId: workflowID,
				RunId:      runID,
			}.Build(),
			StartEventId:      startEventID,
			StartEventVersion: startEventVersion,
			EndEventId:        common.EmptyEventID,
			EndEventVersion:   common.EmptyVersion,
			MaximumPageSize:   pageSize,
			NextPageToken:     nil,
		}.Build()).Return(adminservice.GetWorkflowExecutionRawHistoryV2Response_builder{
		HistoryBatches: []*commonpb.DataBlob{blob},
		NextPageToken:  token,
		VersionHistory: historyspb.VersionHistory_builder{
			Items: versionHistoryItems,
		}.Build(),
	}.Build(), nil)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(
		gomock.Any(),
		adminservice.GetWorkflowExecutionRawHistoryV2Request_builder{
			NamespaceId: s.namespaceID.String(),
			Execution: commonpb.WorkflowExecution_builder{
				WorkflowId: workflowID,
				RunId:      runID,
			}.Build(),
			StartEventId:      startEventID,
			StartEventVersion: startEventVersion,
			EndEventId:        common.EmptyEventID,
			EndEventVersion:   common.EmptyVersion,
			MaximumPageSize:   pageSize,
			NextPageToken:     token,
		}.Build()).Return(adminservice.GetWorkflowExecutionRawHistoryV2Response_builder{
		HistoryBatches: []*commonpb.DataBlob{blob},
		NextPageToken:  nil,
		VersionHistory: historyspb.VersionHistory_builder{
			Items: versionHistoryItems,
		}.Build(),
	}.Build(), nil)

	fetcher := s.fetcher.GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		context.Background(),
		cluster.TestCurrentClusterName,
		s.namespaceID,
		workflowID,
		runID,
		startEventID,
		startEventVersion,
		common.EmptyEventID,
		common.EmptyVersion,
	)
	s.True(fetcher.HasNext())
	batch, err := fetcher.Next()
	s.Nil(err)
	s.Equal(blob, batch.RawEventBatch)

	s.True(fetcher.HasNext())
	batch, err = fetcher.Next()
	s.Nil(err)
	s.Equal(blob, batch.RawEventBatch)

	s.False(fetcher.HasNext())
}

func (s *historyPaginatedFetcherSuite) TestGetHistory() {
	workflowID := "some random workflow ID"
	runID := uuid.NewString()
	startEventID := int64(123)
	endEventID := int64(345)
	version := int64(20)
	nextTokenIn := []byte("some random next token in")
	nextTokenOut := []byte("some random next token out")
	pageSize := int32(59)
	blob := []byte("some random events blob")

	response := adminservice.GetWorkflowExecutionRawHistoryV2Response_builder{
		HistoryBatches: []*commonpb.DataBlob{commonpb.DataBlob_builder{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         blob,
		}.Build()},
		NextPageToken: nextTokenOut,
	}.Build()
	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), adminservice.GetWorkflowExecutionRawHistoryV2Request_builder{
		NamespaceId: s.namespaceID.String(),
		Execution: commonpb.WorkflowExecution_builder{
			WorkflowId: workflowID,
			RunId:      runID,
		}.Build(),
		StartEventId:      startEventID,
		StartEventVersion: version,
		EndEventId:        endEventID,
		EndEventVersion:   version,
		MaximumPageSize:   pageSize,
		NextPageToken:     nextTokenIn,
	}.Build()).Return(response, nil)

	out, token, err := s.fetcher.getHistory(
		context.Background(),
		cluster.TestCurrentClusterName,
		s.namespaceID,
		workflowID,
		runID,
		startEventID,
		version,
		endEventID,
		version,
		nextTokenIn,
		pageSize,
		false,
	)
	s.Nil(err)
	s.Equal(token, nextTokenOut)
	s.Equal(out[0].RawEventBatch.GetData(), blob)
}

func (s *historyPaginatedFetcherSuite) serializeEvents(events []*historypb.HistoryEvent) *commonpb.DataBlob {
	blob, err := s.serializer.SerializeEvents(events)
	s.Nil(err)
	return commonpb.DataBlob_builder{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         blob.GetData(),
	}.Build()
}
