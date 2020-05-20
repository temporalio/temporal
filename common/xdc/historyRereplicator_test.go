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

package xdc

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/temporal-proto/common"
	eventpb "go.temporal.io/temporal-proto/event"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.uber.org/zap"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/adminservicemock"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/historyservicemock"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	replicationgenpb "github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	historyRereplicatorSuite struct {
		suite.Suite

		namespaceID       string
		namespace         string
		targetClusterName string

		mockClusterMetadata *mocks.ClusterMetadata
		mockAdminClient     *adminservicemock.MockAdminServiceClient
		mockHistoryClient   *historyservicemock.MockHistoryServiceClient
		serializer          persistence.PayloadSerializer
		logger              log.Logger

		controller         *gomock.Controller
		mockNamespaceCache *cache.MockNamespaceCache
		rereplicator       *HistoryRereplicatorImpl
	}
)

func TestHistoryRereplicatorSuite(t *testing.T) {
	s := new(historyRereplicatorSuite)
	suite.Run(t, s)
}

func (s *historyRereplicatorSuite) SetupSuite() {
}

func (s *historyRereplicatorSuite) TearDownSuite() {

}

func (s *historyRereplicatorSuite) SetupTest() {
	zapLogger, err := zap.NewDevelopment()
	s.Require().NoError(err)
	s.logger = loggerimpl.NewLogger(zapLogger)
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockClusterMetadata.On("IsGlobalNamespaceEnabled").Return(true)

	s.controller = gomock.NewController(s.T())
	s.mockAdminClient = adminservicemock.NewMockAdminServiceClient(s.controller)
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockNamespaceCache = cache.NewMockNamespaceCache(s.controller)

	s.namespaceID = uuid.New()
	s.namespace = "some random namespace name"
	s.targetClusterName = "some random target cluster name"
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: s.namespaceID, Name: s.namespace},
		&persistenceblobs.NamespaceConfig{RetentionDays: 1},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
		nil,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(s.namespace).Return(namespaceEntry, nil).AnyTimes()
	s.serializer = persistence.NewPayloadSerializer()

	s.rereplicator = NewHistoryRereplicator(
		s.targetClusterName,
		s.mockNamespaceCache,
		s.mockAdminClient,
		func(ctx context.Context, request *historyservice.ReplicateRawEventsRequest) error {
			_, err := s.mockHistoryClient.ReplicateRawEvents(ctx, request)
			return err
		},
		persistence.NewPayloadSerializer(),
		30*time.Second,
		nil,
		s.logger,
	)
}

func (s *historyRereplicatorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *historyRereplicatorSuite) TestSendMultiWorkflowHistory_SameRunID() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	firstEventID := int64(123)
	nextEventID := firstEventID + 100
	pageSize := defaultPageSize
	replicationInfo := map[string]*replicationgenpb.ReplicationInfo{
		"random data center": {
			Version:     777,
			LastEventId: 999,
		},
	}
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

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    firstEventID,
		NextEventId:     nextEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{blob},
		NextPageToken:   nil,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &historyservice.ReplicateRawEventsRequest{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		ReplicationInfo: replicationInfo,
		History: &commonpb.DataBlob{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         blob.Data,
		},
		NewRunHistory: nil,
	}).Return(nil, nil).Times(1)

	err := s.rereplicator.SendMultiWorkflowHistory(s.namespaceID, workflowID, runID, firstEventID, runID, nextEventID)
	s.Nil(err)
}

func (s *historyRereplicatorSuite) TestSendMultiWorkflowHistory_DiffRunID_Continued() {
	workflowID := "some random workflow ID"
	pageSize := defaultPageSize
	beginingEventID := int64(133)
	endingEventID := int64(20)

	// beginingRunID -> midRunID1; not continue relationship; midRunID2 -> endingRunID

	beginingRunID := "00001111-2222-3333-4444-555566661111"
	beginingReplicationInfo := map[string]*replicationgenpb.ReplicationInfo{
		"random data center 1": {
			Version:     111,
			LastEventId: 222,
		},
	}

	midRunID1 := "00001111-2222-3333-4444-555566662222"
	midReplicationInfo1 := map[string]*replicationgenpb.ReplicationInfo{
		"random data center 2": {
			Version:     111,
			LastEventId: 222,
		},
	}

	midRunID2 := "00001111-2222-3333-4444-555566663333"
	midReplicationInfo2 := map[string]*replicationgenpb.ReplicationInfo{
		"random data center 3": {
			Version:     111,
			LastEventId: 222,
		},
	}

	endingRunID := "00001111-2222-3333-4444-555566664444"
	endingReplicationInfo := map[string]*replicationgenpb.ReplicationInfo{
		"random data center 4": {
			Version:     777,
			LastEventId: 888,
		},
	}

	beginingEventBatch := []*eventpb.HistoryEvent{
		{
			EventId:   4,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_DecisionTaskCompleted,
		},
		{
			EventId:   5,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_WorkflowExecutionContinuedAsNew,
			Attributes: &eventpb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &eventpb.WorkflowExecutionContinuedAsNewEventAttributes{
				NewExecutionRunId: midRunID1,
			}},
		},
	}
	beginingBlob := s.serializeEvents(beginingEventBatch)

	midEventBatch1 := []*eventpb.HistoryEvent{
		{
			EventId:   1,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_WorkflowExecutionStarted,
			Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &eventpb.WorkflowExecutionStartedEventAttributes{
				ContinuedExecutionRunId: beginingRunID,
			}},
		},
		{
			EventId:   5,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_WorkflowExecutionCompleted,
		},
	}
	midBlob1 := s.serializeEvents(midEventBatch1)

	midEventBatch2 := []*eventpb.HistoryEvent{
		{
			EventId:   1,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_WorkflowExecutionStarted,
			Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &eventpb.WorkflowExecutionStartedEventAttributes{
				ContinuedExecutionRunId: "",
			}},
		},
		{
			EventId:   5,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_WorkflowExecutionContinuedAsNew,
			Attributes: &eventpb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &eventpb.WorkflowExecutionContinuedAsNewEventAttributes{
				NewExecutionRunId: endingRunID,
			}},
		},
	}
	midBlob2 := s.serializeEvents(midEventBatch2)

	endingEventBatch := []*eventpb.HistoryEvent{
		{
			EventId:   1,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_WorkflowExecutionStarted,
			Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &eventpb.WorkflowExecutionStartedEventAttributes{
				ContinuedExecutionRunId: midRunID2,
			}},
		},
		{
			EventId:   2,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_DecisionTaskScheduled,
		},
	}
	endingBlob := s.serializeEvents(endingEventBatch)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      beginingRunID,
		},
		FirstEventId:    beginingEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{beginingBlob},
		NextPageToken:   nil,
		ReplicationInfo: beginingReplicationInfo,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      midRunID1,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: 1,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{midBlob1},
		NextPageToken:   nil,
		ReplicationInfo: midReplicationInfo1,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      midRunID1,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{midBlob1},
		NextPageToken:   nil,
		ReplicationInfo: midReplicationInfo1,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      endingRunID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: 1,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{endingBlob},
		NextPageToken:   nil,
		ReplicationInfo: endingReplicationInfo,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      midRunID2,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: 1,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{midBlob2},
		NextPageToken:   nil,
		ReplicationInfo: midReplicationInfo2,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      midRunID2,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{midBlob2},
		NextPageToken:   nil,
		ReplicationInfo: midReplicationInfo2,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      endingRunID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: 1,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{endingBlob},
		NextPageToken:   nil,
		ReplicationInfo: endingReplicationInfo,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      endingRunID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     endingEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{endingBlob},
		NextPageToken:   nil,
		ReplicationInfo: endingReplicationInfo,
	}, nil).Times(1)

	// ReplicateRawEvents is already tested, just count how many times this is called
	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), gomock.Any()).Return(nil, nil).Times(4)

	err := s.rereplicator.SendMultiWorkflowHistory(s.namespaceID, workflowID,
		beginingRunID, beginingEventID, endingRunID, endingEventID)
	s.Nil(err)
}

func (s *historyRereplicatorSuite) TestSendSingleWorkflowHistory_NotContinueAsNew() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	nextToken := []byte("some random next token")
	pageSize := defaultPageSize
	replicationInfo := map[string]*replicationgenpb.ReplicationInfo{
		"random data center": {
			Version:     777,
			LastEventId: 999,
		},
	}

	eventBatch1 := []*eventpb.HistoryEvent{
		{
			EventId:   1,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_WorkflowExecutionStarted,
		},
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
	blob1 := s.serializeEvents(eventBatch1)

	eventBatch2 := []*eventpb.HistoryEvent{
		{
			EventId:   4,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_DecisionTaskCompleted,
		},
		{
			EventId:   5,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_WorkflowExecutionCompleted,
		},
	}
	blob2 := s.serializeEvents(eventBatch2)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{blob1},
		NextPageToken:   nextToken,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nextToken,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{blob2},
		NextPageToken:   nil,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &historyservice.ReplicateRawEventsRequest{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		ReplicationInfo: replicationInfo,
		History: &commonpb.DataBlob{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         blob1.Data,
		},
		NewRunHistory: nil,
	}).Return(nil, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &historyservice.ReplicateRawEventsRequest{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		ReplicationInfo: replicationInfo,
		History: &commonpb.DataBlob{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         blob2.Data,
		},
		NewRunHistory: nil,
	}).Return(nil, nil).Times(1)

	nextRunID, err := s.getDummyRereplicationContext().sendSingleWorkflowHistory(s.namespaceID, workflowID, runID, common.FirstEventID, common.EndEventID)
	s.Nil(err)
	s.Equal("", nextRunID)
}

func (s *historyRereplicatorSuite) TestSendSingleWorkflowHistory_ContinueAsNew() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	newRunID := uuid.New()
	nextToken := []byte("some random next token")
	pageSize := defaultPageSize
	replicationInfo := map[string]*replicationgenpb.ReplicationInfo{
		"random data center": {
			Version:     777,
			LastEventId: 999,
		},
	}
	replicationInfoNew := map[string]*replicationgenpb.ReplicationInfo{
		"random data center": {
			Version:     222,
			LastEventId: 111,
		},
	}

	eventBatch1 := []*eventpb.HistoryEvent{
		{
			EventId:   1,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_WorkflowExecutionStarted,
		},
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
	blob1 := s.serializeEvents(eventBatch1)

	eventBatch2 := []*eventpb.HistoryEvent{
		{
			EventId:   4,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_DecisionTaskCompleted,
		},
		{
			EventId:   5,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_WorkflowExecutionContinuedAsNew,
			Attributes: &eventpb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &eventpb.WorkflowExecutionContinuedAsNewEventAttributes{
				NewExecutionRunId: newRunID,
			}},
		},
	}
	blob2 := s.serializeEvents(eventBatch2)

	eventBatchNew := []*eventpb.HistoryEvent{
		{
			EventId:   1,
			Version:   223,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_WorkflowExecutionStarted,
			Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &eventpb.WorkflowExecutionStartedEventAttributes{
				ContinuedExecutionRunId: runID,
			}},
		},
	}
	blobNew := s.serializeEvents(eventBatchNew)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{blob1},
		NextPageToken:   nextToken,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nextToken,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{blob2},
		NextPageToken:   nil,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      newRunID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: 1,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{blobNew},
		NextPageToken:   nil,
		ReplicationInfo: replicationInfoNew,
	}, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &historyservice.ReplicateRawEventsRequest{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		ReplicationInfo: replicationInfo,
		History: &commonpb.DataBlob{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         blob1.Data,
		},
		NewRunHistory: nil,
	}).Return(nil, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &historyservice.ReplicateRawEventsRequest{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		ReplicationInfo: replicationInfo,
		History: &commonpb.DataBlob{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         blob2.Data,
		},
		NewRunHistory: &commonpb.DataBlob{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         blobNew.Data,
		},
	}).Return(nil, nil).Times(1)

	nextRunID, err := s.getDummyRereplicationContext().sendSingleWorkflowHistory(s.namespaceID, workflowID, runID, common.FirstEventID, common.EndEventID)
	s.Nil(err)
	s.Equal(newRunID, nextRunID)
}

func (s *historyRereplicatorSuite) TestEventIDRange() {
	// test case where begining run ID != ending run ID
	beginingRunID := "00001111-2222-3333-4444-555566667777"
	beginingEventID := int64(144)
	endingRunID := "00001111-2222-3333-4444-555566668888"
	endingEventID := int64(1)

	runID := beginingRunID
	firstEventID, nextEventID := s.getDummyRereplicationContext().eventIDRange(runID, beginingRunID, beginingEventID, endingRunID, endingEventID)
	s.Equal(beginingEventID, firstEventID)
	s.Equal(common.EndEventID, nextEventID)

	runID = uuid.New()
	firstEventID, nextEventID = s.getDummyRereplicationContext().eventIDRange(runID, beginingRunID, beginingEventID, endingRunID, endingEventID)
	s.Equal(common.FirstEventID, firstEventID)
	s.Equal(common.EndEventID, nextEventID)

	runID = endingRunID
	firstEventID, nextEventID = s.getDummyRereplicationContext().eventIDRange(runID, beginingRunID, beginingEventID, endingRunID, endingEventID)
	s.Equal(common.FirstEventID, firstEventID)
	s.Equal(endingEventID, nextEventID)

	// test case where begining run ID != ending run ID
	beginingRunID = "00001111-2222-3333-4444-555566667777"
	beginingEventID = int64(144)
	endingRunID = beginingRunID
	endingEventID = endingEventID + 100
	runID = beginingRunID
	firstEventID, nextEventID = s.getDummyRereplicationContext().eventIDRange(runID, beginingRunID, beginingEventID, endingRunID, endingEventID)
	s.Equal(beginingEventID, firstEventID)
	s.Equal(endingEventID, nextEventID)
}

func (s *historyRereplicatorSuite) TestCreateReplicationRawRequest() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	blob := &commonpb.DataBlob{
		EncodingType: commonpb.EncodingType_Proto3,
		Data:         []byte("some random history blob"),
	}
	replicationInfo := map[string]*replicationgenpb.ReplicationInfo{
		"random data center": {
			Version:     777,
			LastEventId: 999,
		},
	}

	s.Equal(&historyservice.ReplicateRawEventsRequest{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		ReplicationInfo: replicationInfo,
		History:         blob,
		NewRunHistory:   nil,
	}, s.getDummyRereplicationContext().createReplicationRawRequest(s.namespaceID, workflowID, runID, blob, replicationInfo))
}

func (s *historyRereplicatorSuite) TestSendReplicationRawRequest() {
	// test that nil request will be a no op
	s.Nil(s.getDummyRereplicationContext().sendReplicationRawRequest(nil))

	request := &historyservice.ReplicateRawEventsRequest{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "some random workflow ID",
			RunId:      uuid.New(),
		},
		ReplicationInfo: map[string]*replicationgenpb.ReplicationInfo{
			"random data center": {
				Version:     777,
				LastEventId: 999,
			},
		},
		History: &commonpb.DataBlob{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         []byte("some random history blob"),
		},
		NewRunHistory: &commonpb.DataBlob{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         []byte("some random new run history blob"),
		},
	}

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), request).Return(nil, nil).Times(1)
	err := s.getDummyRereplicationContext().sendReplicationRawRequest(request)
	s.Nil(err)
}

func (s *historyRereplicatorSuite) TestSendReplicationRawRequest_HistoryReset_MissingHistory() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	replicationInfo := map[string]*replicationgenpb.ReplicationInfo{
		"random data center": {
			Version:     777,
			LastEventId: 999,
		},
	}
	request := &historyservice.ReplicateRawEventsRequest{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		ReplicationInfo: replicationInfo,
		History: &commonpb.DataBlob{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         []byte("some random history blob"),
		},
		NewRunHistory: &commonpb.DataBlob{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         []byte("some random new run history blob"),
		},
	}

	rereplicationContext := newHistoryRereplicationContext(s.namespaceID, workflowID, runID, int64(123), uuid.New(), int64(111), s.rereplicator)
	retryErr := serviceerror.NewRetryTask(
		"retry task status",
		s.namespaceID,
		workflowID,
		runID,
		rereplicationContext.beginningFirstEventID-10)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), request).Return(nil, retryErr).Times(1)

	missingEventBatch := []*eventpb.HistoryEvent{
		{
			EventId:   1,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_WorkflowExecutionStarted,
		},
	}
	missingBlob := s.serializeEvents(missingEventBatch)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    retryErr.NextEventId,
		NextEventId:     rereplicationContext.beginningFirstEventID,
		MaximumPageSize: defaultPageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{missingBlob},
		NextPageToken:   nil,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)
	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &historyservice.ReplicateRawEventsRequest{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		ReplicationInfo: replicationInfo,
		History:         missingBlob,
		NewRunHistory:   nil,
	}).Return(nil, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), request).Return(nil, nil).Times(1)

	err := rereplicationContext.sendReplicationRawRequest(request)
	s.Nil(err)
}

func (s *historyRereplicatorSuite) TestSendReplicationRawRequest_Err() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	replicationInfo := map[string]*replicationgenpb.ReplicationInfo{
		"random data center": {
			Version:     777,
			LastEventId: 999,
		},
	}
	request := &historyservice.ReplicateRawEventsRequest{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		ReplicationInfo: replicationInfo,
		History: &commonpb.DataBlob{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         []byte("some random history blob"),
		},
		NewRunHistory: &commonpb.DataBlob{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         []byte("some random new run history blob"),
		},
	}

	rereplicationContext := newHistoryRereplicationContext(s.namespaceID, workflowID, runID, int64(123), uuid.New(), int64(111), s.rereplicator)
	rereplicationContext.rpcCalls = 1 // so this will be the second API call for rereplication
	retryErr := serviceerror.NewRetryTask(
		"",
		s.namespaceID,
		workflowID,
		runID,
		rereplicationContext.beginningFirstEventID-10)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), request).Return(nil, retryErr).Times(1)

	err := rereplicationContext.sendReplicationRawRequest(request)
	s.Equal(retryErr, err)
}

func (s *historyRereplicatorSuite) TestHandleEmptyHistory_SeenMoreThanOnce() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	lastVersion := int64(777)
	lastEventID := int64(999)
	replicationInfo := map[string]*replicationgenpb.ReplicationInfo{
		s.targetClusterName: {
			Version:     lastVersion,
			LastEventId: lastEventID,
		},
	}

	rereplicationContext := newHistoryRereplicationContext(s.namespaceID, workflowID, runID, int64(123), uuid.New(), int64(111), s.rereplicator)
	rereplicationContext.seenEmptyEvents = true
	err := rereplicationContext.handleEmptyHistory(s.namespaceID, workflowID, runID, replicationInfo)
	s.Equal(ErrNoHistoryRawEventBatches, err)
}

func (s *historyRereplicatorSuite) TestHandleEmptyHistory_FoundReplicationInfoEntry() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	lastVersion := int64(777)
	lastEventID := int64(999)
	replicationInfo := map[string]*replicationgenpb.ReplicationInfo{
		s.targetClusterName: {
			Version:     lastVersion,
			LastEventId: lastEventID,
		},
	}
	eventBatch := []*eventpb.HistoryEvent{
		{
			EventId:   lastEventID + 1,
			Version:   lastVersion + 1,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_TimerFired,
		},
	}
	blob := s.serializeEvents(eventBatch)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    lastEventID + 1,
		NextEventId:     common.EndEventID,
		MaximumPageSize: defaultPageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{blob},
		NextPageToken:   nil,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &historyservice.ReplicateRawEventsRequest{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		ReplicationInfo: replicationInfo,
		History:         blob,
		NewRunHistory:   nil,
	}).Return(nil, nil).Times(1)

	rereplicationContext := newHistoryRereplicationContext(s.namespaceID, workflowID, runID, int64(123), uuid.New(), int64(111), s.rereplicator)
	err := rereplicationContext.handleEmptyHistory(s.namespaceID, workflowID, runID, replicationInfo)
	s.Nil(err)
}

func (s *historyRereplicatorSuite) TestHandleEmptyHistory_NoReplicationInfoEntry() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	lastVersion := int64(777)
	lastEventID := int64(999)
	replicationInfo := map[string]*replicationgenpb.ReplicationInfo{
		"some randon cluster": {
			Version:     lastVersion,
			LastEventId: lastEventID,
		},
	}
	eventBatch := []*eventpb.HistoryEvent{
		{
			EventId:   common.FirstEventID,
			Version:   1,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_WorkflowExecutionStarted,
		},
	}
	blob := s.serializeEvents(eventBatch)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: defaultPageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonpb.DataBlob{blob},
		NextPageToken:   nil,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &historyservice.ReplicateRawEventsRequest{
		NamespaceId: s.namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		ReplicationInfo: replicationInfo,
		History:         blob,
		NewRunHistory:   nil,
	}).Return(nil, nil).Times(1)

	rereplicationContext := newHistoryRereplicationContext(s.namespaceID, workflowID, runID, int64(123), uuid.New(), int64(111), s.rereplicator)
	err := rereplicationContext.handleEmptyHistory(s.namespaceID, workflowID, runID, replicationInfo)
	s.Nil(err)
}

func (s *historyRereplicatorSuite) TestGetHistory() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	firstEventID := int64(123)
	nextEventID := int64(345)
	nextTokenIn := []byte("some random next token in")
	nextTokenOut := []byte("some random next token out")
	pageSize := int32(59)
	blob := []byte("some random events blob")

	response := &adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches: []*commonpb.DataBlob{{
			EncodingType: commonpb.EncodingType_Proto3,
			Data:         blob,
		}},
		NextPageToken: nextTokenOut,
		ReplicationInfo: map[string]*replicationgenpb.ReplicationInfo{
			"random data center": {
				Version:     777,
				LastEventId: 999,
			},
		},
	}
	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    firstEventID,
		NextEventId:     nextEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nextTokenIn,
	}).Return(response, nil).Times(1)

	out, err := s.getDummyRereplicationContext().getHistory(s.namespaceID, workflowID, runID, firstEventID, nextEventID, nextTokenIn, pageSize)
	s.Nil(err)
	s.Equal(response, out)
}

func (s *historyRereplicatorSuite) TestGetPrevEventID() {
	workflowID := "some random workflow ID"
	currentRunID := uuid.New()

	prepareFn := func(prevRunID string) {
		eventBatch := []*eventpb.HistoryEvent{
			{
				EventId:   1,
				Version:   123,
				Timestamp: time.Now().UnixNano(),
				EventType: eventpb.EventType_WorkflowExecutionStarted,
				Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &eventpb.WorkflowExecutionStartedEventAttributes{
					ContinuedExecutionRunId: prevRunID,
				}},
			},
			{
				EventId:   2,
				Version:   223,
				Timestamp: time.Now().UnixNano(),
				EventType: eventpb.EventType_DecisionTaskScheduled,
			},
		}
		blob, err := s.serializer.SerializeBatchEvents(eventBatch, common.EncodingTypeProto3)
		s.Nil(err)

		s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      currentRunID,
			},
			FirstEventId:    common.FirstEventID,
			NextEventId:     common.EndEventID,
			MaximumPageSize: 1,
			NextPageToken:   nil,
		}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
			HistoryBatches: []*commonpb.DataBlob{{
				EncodingType: commonpb.EncodingType_Proto3,
				Data:         blob.Data,
			}},
		}, nil).Times(1)
	}

	// has prev run
	prevRunID := uuid.New()
	prepareFn(prevRunID)
	runID, err := s.getDummyRereplicationContext().getPrevRunID(s.namespaceID, workflowID, currentRunID)
	s.Nil(err)
	s.Equal(prevRunID, runID)

	// no prev run
	prepareFn("")
	runID, err = s.getDummyRereplicationContext().getPrevRunID(s.namespaceID, workflowID, currentRunID)
	s.Nil(err)
	s.Equal("", runID)
}

func (s *historyRereplicatorSuite) TestGetPrevEventID_EmptyEvents() {
	workflowID := "some random workflow ID"
	currentRunID := uuid.New()

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      currentRunID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: 1,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches: []*commonpb.DataBlob{},
	}, nil).Times(1)

	runID, err := s.getDummyRereplicationContext().getPrevRunID(s.namespaceID, workflowID, currentRunID)
	s.IsType(&serviceerror.NotFound{}, err)
	s.Equal("", runID)
}

func (s *historyRereplicatorSuite) TestGetNextRunID_ContinueAsNew() {
	nextRunID := uuid.New()
	eventBatchIn := []*eventpb.HistoryEvent{
		{
			EventId:   233,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_DecisionTaskCompleted,
		},
		{
			EventId:   234,
			Version:   223,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_WorkflowExecutionContinuedAsNew,
			Attributes: &eventpb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &eventpb.WorkflowExecutionContinuedAsNewEventAttributes{
				NewExecutionRunId: nextRunID,
			}},
		},
	}
	blob, err := s.serializer.SerializeBatchEvents(eventBatchIn, common.EncodingTypeProto3)
	s.Nil(err)

	runID, err := s.getDummyRereplicationContext().getNextRunID(&commonpb.DataBlob{
		EncodingType: commonpb.EncodingType_Proto3,
		Data:         blob.Data,
	})
	s.Nil(err)
	s.Equal(nextRunID, runID)
}

func (s *historyRereplicatorSuite) TestGetNextRunID_NotContinueAsNew() {
	eventBatchIn := []*eventpb.HistoryEvent{
		{
			EventId:   233,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: eventpb.EventType_DecisionTaskCompleted,
		},
		{
			EventId:    234,
			Version:    223,
			Timestamp:  time.Now().UnixNano(),
			EventType:  eventpb.EventType_WorkflowExecutionCanceled,
			Attributes: &eventpb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: &eventpb.WorkflowExecutionCancelRequestedEventAttributes{}},
		},
	}
	blob, err := s.serializer.SerializeBatchEvents(eventBatchIn, common.EncodingTypeProto3)
	s.Nil(err)

	runID, err := s.getDummyRereplicationContext().getNextRunID(&commonpb.DataBlob{
		EncodingType: commonpb.EncodingType_Proto3,
		Data:         blob.Data,
	})
	s.Nil(err)
	s.Equal("", runID)
}

func (s *historyRereplicatorSuite) TestDeserializeBlob() {
	eventBatchIn := []*eventpb.HistoryEvent{
		{
			EventId:    1,
			Version:    123,
			Timestamp:  time.Now().UnixNano(),
			EventType:  eventpb.EventType_WorkflowExecutionStarted,
			Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &eventpb.WorkflowExecutionStartedEventAttributes{}},
		},
		{
			EventId:    2,
			Version:    223,
			Timestamp:  time.Now().UnixNano(),
			EventType:  eventpb.EventType_DecisionTaskScheduled,
			Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{}},
		},
	}

	blob, err := s.serializer.SerializeBatchEvents(eventBatchIn, common.EncodingTypeProto3)
	s.Nil(err)

	eventBatchOut, err := s.getDummyRereplicationContext().deserializeBlob(&commonpb.DataBlob{
		EncodingType: commonpb.EncodingType_Proto3,
		Data:         blob.Data,
	})
	s.Nil(err)
	s.Equal(eventBatchIn, eventBatchOut)
}

func (s *historyRereplicatorSuite) serializeEvents(events []*eventpb.HistoryEvent) *commonpb.DataBlob {
	blob, err := s.serializer.SerializeBatchEvents(events, common.EncodingTypeProto3)
	s.Nil(err)
	return &commonpb.DataBlob{
		EncodingType: commonpb.EncodingType_Proto3,
		Data:         blob.Data,
	}
}

func (s *historyRereplicatorSuite) getDummyRereplicationContext() *historyRereplicationContext {
	return &historyRereplicationContext{
		rereplicator: s.rereplicator,
		logger:       s.rereplicator.logger,
		ctx:          context.Background(),
	}
}
