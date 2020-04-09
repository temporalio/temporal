package xdc

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/temporal-proto/common"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/adminservicemock"
	eventgenpb "github.com/temporalio/temporal/.gen/proto/event"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/historyservicemock"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	nDCHistoryResenderSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockNamespaceCache *cache.MockNamespaceCache
		mockAdminClient    *adminservicemock.MockAdminServiceClient
		mockHistoryClient  *historyservicemock.MockHistoryServiceClient

		namespaceID string
		namespace   string

		mockClusterMetadata *mocks.ClusterMetadata

		serializer persistence.PayloadSerializer
		logger     log.Logger

		rereplicator *NDCHistoryResenderImpl
	}
)

func TestNDCHistoryResenderSuite(t *testing.T) {
	s := new(nDCHistoryResenderSuite)
	suite.Run(t, s)
}

func (s *nDCHistoryResenderSuite) SetupSuite() {
}

func (s *nDCHistoryResenderSuite) TearDownSuite() {

}

func (s *nDCHistoryResenderSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockAdminClient = adminservicemock.NewMockAdminServiceClient(s.controller)
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockNamespaceCache = cache.NewMockNamespaceCache(s.controller)

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockClusterMetadata.On("IsGlobalNamespaceEnabled").Return(true)

	s.namespaceID = uuid.New()
	s.namespace = "some random namespace name"
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{ID: s.namespaceID, Name: s.namespace},
		&persistence.NamespaceConfig{Retention: 1},
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		1234,
		nil,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(s.namespace).Return(namespaceEntry, nil).AnyTimes()
	s.serializer = persistence.NewPayloadSerializer()

	s.rereplicator = NewNDCHistoryResender(
		s.mockNamespaceCache,
		s.mockAdminClient,
		func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error {
			_, err := s.mockHistoryClient.ReplicateEventsV2(ctx, request)
			return err
		},
		persistence.NewPayloadSerializer(),
		s.logger,
	)
}

func (s *nDCHistoryResenderSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *nDCHistoryResenderSuite) TestSendSingleWorkflowHistory() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	startEventID := int64(123)
	startEventVersion := int64(100)
	token := []byte{1}
	pageSize := defaultPageSize
	eventBatch := []*eventpb.HistoryEvent{
		{
			EventId:   2,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_DecisionTaskScheduled,
		},
		{
			EventId:   3,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_DecisionTaskStarted,
		},
	}
	blob := s.serializeEvents(eventBatch)
	versionHistoryItems := []*eventgenpb.VersionHistoryItem{
		{
			EventId: 1,
			Version: 1,
		},
	}

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(
		gomock.Any(),
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Namespace: s.namespace,
			Execution: &executionpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			StartEventId:      startEventID,
			StartEventVersion: startEventVersion,
			EndEventId:        common.EmptyEventID,
			EndEventVersion:   common.EmptyVersion,
			MaximumPageSize:   pageSize,
			NextPageToken:     nil,
		}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{blob},
		NextPageToken:  token,
		VersionHistory: &eventgenpb.VersionHistory{
			Items: versionHistoryItems,
		},
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(
		gomock.Any(),
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Namespace: s.namespace,
			Execution: &executionpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			StartEventId:      startEventID,
			StartEventVersion: startEventVersion,
			EndEventId:        common.EmptyEventID,
			EndEventVersion:   common.EmptyVersion,
			MaximumPageSize:   pageSize,
			NextPageToken:     token,
		}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{blob},
		NextPageToken:  nil,
		VersionHistory: &eventgenpb.VersionHistory{
			Items: versionHistoryItems,
		},
	}, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateEventsV2(
		gomock.Any(),
		&historyservice.ReplicateEventsV2Request{
			NamespaceId: s.namespaceID,
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			VersionHistoryItems: versionHistoryItems,
			Events:              blob,
		}).Return(nil, nil).Times(2)

	err := s.rereplicator.SendSingleWorkflowHistory(
		s.namespaceID,
		workflowID,
		runID,
		startEventID,
		startEventVersion,
		common.EmptyEventID,
		common.EmptyVersion,
	)

	s.Nil(err)
}

func (s *nDCHistoryResenderSuite) TestCreateReplicateRawEventsRequest() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	blob := &commonpb.DataBlob{
		EncodingType: commonpb.EncodingType_Proto3,
		Data:         []byte("some random history blob"),
	}
	versionHistoryItems := []*eventgenpb.VersionHistoryItem{
		{
			EventId: 1,
			Version: 1,
		},
	}

	s.Equal(&historyservice.ReplicateEventsV2Request{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &executionpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		VersionHistoryItems: versionHistoryItems,
		Events:              blob,
	}, s.rereplicator.createReplicationRawRequest(
		s.namespaceID,
		workflowID,
		runID,
		blob,
		versionHistoryItems))
}

func (s *nDCHistoryResenderSuite) TestSendReplicationRawRequest() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	item := &eventgenpb.VersionHistoryItem{
		EventId: 1,
		Version: 1,
	}
	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &executionpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Events: &commonpb.DataBlob{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         []byte("some random history blob"),
		},
		VersionHistoryItems: []*eventgenpb.VersionHistoryItem{item},
	}

	s.mockHistoryClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(nil, nil).Times(1)
	err := s.rereplicator.sendReplicationRawRequest(request)
	s.Nil(err)
}

func (s *nDCHistoryResenderSuite) TestSendReplicationRawRequest_Err() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	item := &eventgenpb.VersionHistoryItem{
		EventId: 1,
		Version: 1,
	}
	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &executionpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Events: &commonpb.DataBlob{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         []byte("some random history blob"),
		},
		VersionHistoryItems: []*eventgenpb.VersionHistoryItem{item},
	}
	retryErr := serviceerror.NewRetryTaskV2(
		"",
		s.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		common.EmptyEventID,
		common.EmptyVersion,
	)

	s.mockHistoryClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(nil, retryErr).Times(1)
	err := s.rereplicator.sendReplicationRawRequest(request)
	s.Equal(retryErr, err)
}

func (s *nDCHistoryResenderSuite) TestGetHistory() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	startEventID := int64(123)
	endEventID := int64(345)
	version := int64(20)
	nextTokenIn := []byte("some random next token in")
	nextTokenOut := []byte("some random next token out")
	pageSize := int32(59)
	blob := []byte("some random events blob")

	response := &adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         blob,
		}},
		NextPageToken: nextTokenOut,
	}
	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		Namespace: s.namespace,
		Execution: &executionpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		StartEventId:      startEventID,
		StartEventVersion: version,
		EndEventId:        endEventID,
		EndEventVersion:   version,
		MaximumPageSize:   pageSize,
		NextPageToken:     nextTokenIn,
	}).Return(response, nil).Times(1)

	out, err := s.rereplicator.getHistory(
		s.namespaceID,
		workflowID,
		runID,
		startEventID,
		version,
		endEventID,
		version,
		nextTokenIn,
		pageSize)
	s.Nil(err)
	s.Equal(response, out)
}

func (s *nDCHistoryResenderSuite) serializeEvents(events []*eventpb.HistoryEvent) *commonpb.DataBlob {
	blob, err := s.serializer.SerializeBatchEvents(events, common.EncodingTypeProto3)
	s.Nil(err)
	return &commonpb.DataBlob{
		EncodingType: commonpb.EncodingType_Proto3,
		Data:         blob.Data,
	}
}
