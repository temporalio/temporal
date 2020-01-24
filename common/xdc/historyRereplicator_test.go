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

package xdc

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.uber.org/zap"

	"github.com/temporalio/temporal/.gen/go/history"
	"github.com/temporalio/temporal/.gen/go/history/historyservicetest"
	"github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/adminservicemock"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/service/frontend/adapter"
)

type (
	historyRereplicatorSuite struct {
		suite.Suite

		domainID          string
		domainName        string
		targetClusterName string

		mockClusterMetadata *mocks.ClusterMetadata
		mockAdminClient     *adminservicemock.MockAdminServiceYARPCClient
		mockHistoryClient   *historyservicetest.MockClient
		serializer          persistence.PayloadSerializer
		logger              log.Logger

		controller      *gomock.Controller
		mockDomainCache *cache.MockDomainCache
		rereplicator    *HistoryRereplicatorImpl
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
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)

	s.controller = gomock.NewController(s.T())
	s.mockAdminClient = adminservicemock.NewMockAdminServiceYARPCClient(s.controller)
	s.mockHistoryClient = historyservicetest.NewMockClient(s.controller)
	s.mockDomainCache = cache.NewMockDomainCache(s.controller)

	s.domainID = uuid.New()
	s.domainName = "some random domain name"
	s.targetClusterName = "some random target cluster name"
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: s.domainID, Name: s.domainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		1234,
		nil,
	)
	s.mockDomainCache.EXPECT().GetDomainByID(s.domainID).Return(domainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(s.domainName).Return(domainEntry, nil).AnyTimes()
	s.serializer = persistence.NewPayloadSerializer()

	s.rereplicator = NewHistoryRereplicator(
		s.targetClusterName,
		s.mockDomainCache,
		s.mockAdminClient,
		func(ctx context.Context, request *history.ReplicateRawEventsRequest) error {
			return s.mockHistoryClient.ReplicateRawEvents(ctx, request)
		},
		persistence.NewPayloadSerializer(),
		30*time.Second,
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
	replicationInfo := map[string]*commonproto.ReplicationInfo{
		"random data center": {
			Version:     777,
			LastEventId: 999,
		},
	}
	eventBatch := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(2),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskScheduled.Ptr(),
		},
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(3),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskStarted.Ptr(),
		},
	}
	blob := s.serializeEvents(eventBatch)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    firstEventID,
		NextEventId:     nextEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{blob},
		NextPageToken:   nil,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &history.ReplicateRawEventsRequest{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		ReplicationInfo: adapter.ToThriftReplicationInfos(replicationInfo),
		History: &shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         blob.Data,
		},
		NewRunHistory: nil,
	}).Return(nil).Times(1)

	err := s.rereplicator.SendMultiWorkflowHistory(s.domainID, workflowID, runID, firstEventID, runID, nextEventID)
	s.Nil(err)
}

func (s *historyRereplicatorSuite) TestSendMultiWorkflowHistory_DiffRunID_Continued() {
	workflowID := "some random workflow ID"
	pageSize := defaultPageSize
	beginingEventID := int64(133)
	endingEventID := int64(20)

	// beginingRunID -> midRunID1; not continue relationship; midRunID2 -> endingRunID

	beginingRunID := "00001111-2222-3333-4444-555566661111"
	beginingReplicationInfo := map[string]*commonproto.ReplicationInfo{
		"random data center 1": {
			Version:     111,
			LastEventId: 222,
		},
	}

	midRunID1 := "00001111-2222-3333-4444-555566662222"
	midReplicationInfo1 := map[string]*commonproto.ReplicationInfo{
		"random data center 2": {
			Version:     111,
			LastEventId: 222,
		},
	}

	midRunID2 := "00001111-2222-3333-4444-555566663333"
	midReplicationInfo2 := map[string]*commonproto.ReplicationInfo{
		"random data center 3": {
			Version:     111,
			LastEventId: 222,
		},
	}

	endingRunID := "00001111-2222-3333-4444-555566664444"
	endingReplicationInfo := map[string]*commonproto.ReplicationInfo{
		"random data center 4": {
			Version:     777,
			LastEventId: 888,
		},
	}

	beginingEventBatch := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(4),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskCompleted.Ptr(),
		},
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(5),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionContinuedAsNew.Ptr(),
			WorkflowExecutionContinuedAsNewEventAttributes: &shared.WorkflowExecutionContinuedAsNewEventAttributes{
				NewExecutionRunId: common.StringPtr(midRunID1),
			},
		},
	}
	beginingBlob := s.serializeEvents(beginingEventBatch)

	midEventBatch1 := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(1),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
			WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
				ContinuedExecutionRunId: common.StringPtr(beginingRunID),
			},
		},
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(5),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionCompleted.Ptr(),
		},
	}
	midBlob1 := s.serializeEvents(midEventBatch1)

	midEventBatch2 := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(1),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
			WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
				ContinuedExecutionRunId: nil,
			},
		},
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(5),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionContinuedAsNew.Ptr(),
			WorkflowExecutionContinuedAsNewEventAttributes: &shared.WorkflowExecutionContinuedAsNewEventAttributes{
				NewExecutionRunId: common.StringPtr(endingRunID),
			},
		},
	}
	midBlob2 := s.serializeEvents(midEventBatch2)

	endingEventBatch := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(1),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
			WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
				ContinuedExecutionRunId: common.StringPtr(midRunID2),
			},
		},
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(2),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskScheduled.Ptr(),
		},
	}
	endingBlob := s.serializeEvents(endingEventBatch)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      beginingRunID,
		},
		FirstEventId:    beginingEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{beginingBlob},
		NextPageToken:   nil,
		ReplicationInfo: beginingReplicationInfo,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      midRunID1,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: 1,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{midBlob1},
		NextPageToken:   nil,
		ReplicationInfo: midReplicationInfo1,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      midRunID1,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{midBlob1},
		NextPageToken:   nil,
		ReplicationInfo: midReplicationInfo1,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      endingRunID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: 1,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{endingBlob},
		NextPageToken:   nil,
		ReplicationInfo: endingReplicationInfo,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      midRunID2,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: 1,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{midBlob2},
		NextPageToken:   nil,
		ReplicationInfo: midReplicationInfo2,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      midRunID2,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{midBlob2},
		NextPageToken:   nil,
		ReplicationInfo: midReplicationInfo2,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      endingRunID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: 1,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{endingBlob},
		NextPageToken:   nil,
		ReplicationInfo: endingReplicationInfo,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      endingRunID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     endingEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{endingBlob},
		NextPageToken:   nil,
		ReplicationInfo: endingReplicationInfo,
	}, nil).Times(1)

	// ReplicateRawEvents is already tested, just count how many times this is called
	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), gomock.Any()).Return(nil).Times(4)

	err := s.rereplicator.SendMultiWorkflowHistory(s.domainID, workflowID,
		beginingRunID, beginingEventID, endingRunID, endingEventID)
	s.Nil(err)
}

func (s *historyRereplicatorSuite) TestSendSingleWorkflowHistory_NotContinueAsNew() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	nextToken := []byte("some random next token")
	pageSize := defaultPageSize
	replicationInfo := map[string]*commonproto.ReplicationInfo{
		"random data center": {
			Version:     777,
			LastEventId: 999,
		},
	}

	eventBatch1 := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(1),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
		},
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(2),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskScheduled.Ptr(),
		},
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(3),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskStarted.Ptr(),
		},
	}
	blob1 := s.serializeEvents(eventBatch1)

	eventBatch2 := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(4),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskCompleted.Ptr(),
		},
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(5),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionCompleted.Ptr(),
		},
	}
	blob2 := s.serializeEvents(eventBatch2)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{blob1},
		NextPageToken:   nextToken,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nextToken,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{blob2},
		NextPageToken:   nil,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &history.ReplicateRawEventsRequest{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		ReplicationInfo: adapter.ToThriftReplicationInfos(replicationInfo),
		History: &shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         blob1.Data,
		},
		NewRunHistory: nil,
	}).Return(nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &history.ReplicateRawEventsRequest{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		ReplicationInfo: adapter.ToThriftReplicationInfos(replicationInfo),
		History: &shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         blob2.Data,
		},
		NewRunHistory: nil,
	}).Return(nil).Times(1)

	nextRunID, err := s.getDummyRereplicationContext().sendSingleWorkflowHistory(s.domainID, workflowID, runID, common.FirstEventID, common.EndEventID)
	s.Nil(err)
	s.Equal("", nextRunID)
}

func (s *historyRereplicatorSuite) TestSendSingleWorkflowHistory_ContinueAsNew() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	newRunID := uuid.New()
	nextToken := []byte("some random next token")
	pageSize := defaultPageSize
	replicationInfo := map[string]*commonproto.ReplicationInfo{
		"random data center": {
			Version:     777,
			LastEventId: 999,
		},
	}
	replicationInfoNew := map[string]*commonproto.ReplicationInfo{
		"random data center": {
			Version:     222,
			LastEventId: 111,
		},
	}

	eventBatch1 := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(1),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
		},
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(2),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskScheduled.Ptr(),
		},
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(3),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskStarted.Ptr(),
		},
	}
	blob1 := s.serializeEvents(eventBatch1)

	eventBatch2 := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(4),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskCompleted.Ptr(),
		},
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(5),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionContinuedAsNew.Ptr(),
			WorkflowExecutionContinuedAsNewEventAttributes: &shared.WorkflowExecutionContinuedAsNewEventAttributes{
				NewExecutionRunId: common.StringPtr(newRunID),
			},
		},
	}
	blob2 := s.serializeEvents(eventBatch2)

	eventBatchNew := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(1),
			Version:   common.Int64Ptr(223),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
			WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
				ContinuedExecutionRunId: common.StringPtr(runID),
			},
		},
	}
	blobNew := s.serializeEvents(eventBatchNew)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{blob1},
		NextPageToken:   nextToken,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nextToken,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{blob2},
		NextPageToken:   nil,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      newRunID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: 1,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{blobNew},
		NextPageToken:   nil,
		ReplicationInfo: replicationInfoNew,
	}, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &history.ReplicateRawEventsRequest{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		ReplicationInfo: adapter.ToThriftReplicationInfos(replicationInfo),
		History: &shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         blob1.Data,
		},
		NewRunHistory: nil,
	}).Return(nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &history.ReplicateRawEventsRequest{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		ReplicationInfo: adapter.ToThriftReplicationInfos(replicationInfo),
		History: &shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         blob2.Data,
		},
		NewRunHistory: &shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         blobNew.Data,
		},
	}).Return(nil).Times(1)

	nextRunID, err := s.getDummyRereplicationContext().sendSingleWorkflowHistory(s.domainID, workflowID, runID, common.FirstEventID, common.EndEventID)
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
	blob := &commonproto.DataBlob{
		EncodingType: enums.EncodingTypeThriftRW,
		Data:         []byte("some random history blob"),
	}
	replicationInfo := map[string]*commonproto.ReplicationInfo{
		"random data center": &commonproto.ReplicationInfo{
			Version:     777,
			LastEventId: 999,
		},
	}

	s.Equal(&history.ReplicateRawEventsRequest{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		ReplicationInfo: adapter.ToThriftReplicationInfos(replicationInfo),
		History:         adapter.ToThriftDataBlob(blob),
		NewRunHistory:   nil,
	}, s.getDummyRereplicationContext().createReplicationRawRequest(s.domainID, workflowID, runID, blob, replicationInfo))
}

func (s *historyRereplicatorSuite) TestSendReplicationRawRequest() {
	// test that nil request will be a no op
	s.Nil(s.getDummyRereplicationContext().sendReplicationRawRequest(nil))

	request := &history.ReplicateRawEventsRequest{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("some random workflow ID"),
			RunId:      common.StringPtr(uuid.New()),
		},
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			"random data center": &shared.ReplicationInfo{
				Version:     common.Int64Ptr(777),
				LastEventId: common.Int64Ptr(999),
			},
		},
		History: &shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         []byte("some random history blob"),
		},
		NewRunHistory: &shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         []byte("some random new run history blob"),
		},
	}

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), request).Return(nil).Times(1)
	err := s.getDummyRereplicationContext().sendReplicationRawRequest(request)
	s.Nil(err)
}

func (s *historyRereplicatorSuite) TestSendReplicationRawRequest_HistoryReset_MissingHistory() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	replicationInfo := map[string]*commonproto.ReplicationInfo{
		"random data center": {
			Version:     777,
			LastEventId: 999,
		},
	}
	request := &history.ReplicateRawEventsRequest{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		ReplicationInfo: adapter.ToThriftReplicationInfos(replicationInfo),
		History: &shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         []byte("some random history blob"),
		},
		NewRunHistory: &shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         []byte("some random new run history blob"),
		},
	}

	rereplicationContext := newHistoryRereplicationContext(s.domainID, workflowID, runID, int64(123), uuid.New(), int64(111), s.rereplicator)
	retryErr := &shared.RetryTaskError{
		DomainId:    common.StringPtr(s.domainID),
		WorkflowId:  common.StringPtr(workflowID),
		RunId:       common.StringPtr(runID),
		NextEventId: common.Int64Ptr(rereplicationContext.beginningFirstEventID - 10),
	}
	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), request).Return(retryErr).Times(1)

	missingEventBatch := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(1),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
		},
	}
	missingBlob := s.serializeEvents(missingEventBatch)
	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    retryErr.GetNextEventId(),
		NextEventId:     rereplicationContext.beginningFirstEventID,
		MaximumPageSize: defaultPageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{missingBlob},
		NextPageToken:   nil,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)
	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &history.ReplicateRawEventsRequest{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		ReplicationInfo: adapter.ToThriftReplicationInfos(replicationInfo),
		History:         adapter.ToThriftDataBlob(missingBlob),
		NewRunHistory:   nil,
	}).Return(nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), request).Return(nil).Times(1)

	err := rereplicationContext.sendReplicationRawRequest(request)
	s.Nil(err)
}

func (s *historyRereplicatorSuite) TestSendReplicationRawRequest_Err() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	replicationInfo := map[string]*shared.ReplicationInfo{
		"random data center": {
			Version:     common.Int64Ptr(777),
			LastEventId: common.Int64Ptr(999),
		},
	}
	request := &history.ReplicateRawEventsRequest{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		ReplicationInfo: replicationInfo,
		History: &shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         []byte("some random history blob"),
		},
		NewRunHistory: &shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         []byte("some random new run history blob"),
		},
	}

	rereplicationContext := newHistoryRereplicationContext(s.domainID, workflowID, runID, int64(123), uuid.New(), int64(111), s.rereplicator)
	rereplicationContext.rpcCalls = 1 // so this will be the second API call for rereplication
	retryErr := &shared.RetryTaskError{
		DomainId:    common.StringPtr(s.domainID),
		WorkflowId:  common.StringPtr(workflowID),
		RunId:       common.StringPtr(runID),
		NextEventId: common.Int64Ptr(rereplicationContext.beginningFirstEventID - 10),
	}
	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), request).Return(retryErr).Times(1)

	err := rereplicationContext.sendReplicationRawRequest(request)
	s.Equal(retryErr, err)
}

func (s *historyRereplicatorSuite) TestHandleEmptyHistory_SeenMoreThanOnce() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	lastVersion := int64(777)
	lastEventID := int64(999)
	replicationInfo := map[string]*commonproto.ReplicationInfo{
		s.targetClusterName: {
			Version:     lastVersion,
			LastEventId: lastEventID,
		},
	}

	rereplicationContext := newHistoryRereplicationContext(s.domainID, workflowID, runID, int64(123), uuid.New(), int64(111), s.rereplicator)
	rereplicationContext.seenEmptyEvents = true
	err := rereplicationContext.handleEmptyHistory(s.domainID, workflowID, runID, replicationInfo)
	s.Equal(ErrNoHistoryRawEventBatches, err)
}

func (s *historyRereplicatorSuite) TestHandleEmptyHistory_FoundReplicationInfoEntry() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	lastVersion := int64(777)
	lastEventID := int64(999)
	replicationInfo := map[string]*commonproto.ReplicationInfo{
		s.targetClusterName: {
			Version:     lastVersion,
			LastEventId: lastEventID,
		},
	}
	eventBatch := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(lastEventID + 1),
			Version:   common.Int64Ptr(lastVersion + 1),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeTimerFired.Ptr(),
		},
	}
	blob := s.serializeEvents(eventBatch)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    lastEventID + 1,
		NextEventId:     common.EndEventID,
		MaximumPageSize: defaultPageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{blob},
		NextPageToken:   nil,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &history.ReplicateRawEventsRequest{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		ReplicationInfo: adapter.ToThriftReplicationInfos(replicationInfo),
		History:         adapter.ToThriftDataBlob(blob),
		NewRunHistory:   nil,
	}).Return(nil).Times(1)

	rereplicationContext := newHistoryRereplicationContext(s.domainID, workflowID, runID, int64(123), uuid.New(), int64(111), s.rereplicator)
	err := rereplicationContext.handleEmptyHistory(s.domainID, workflowID, runID, replicationInfo)
	s.Nil(err)
}

func (s *historyRereplicatorSuite) TestHandleEmptyHistory_NoReplicationInfoEntry() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	lastVersion := int64(777)
	lastEventID := int64(999)
	replicationInfo := map[string]*commonproto.ReplicationInfo{
		"some randon cluster": {
			Version:     lastVersion,
			LastEventId: lastEventID,
		},
	}
	eventBatch := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(common.FirstEventID),
			Version:   common.Int64Ptr(1),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
		},
	}
	blob := s.serializeEvents(eventBatch)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: defaultPageSize,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  []*commonproto.DataBlob{blob},
		NextPageToken:   nil,
		ReplicationInfo: replicationInfo,
	}, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateRawEvents(gomock.Any(), &history.ReplicateRawEventsRequest{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		ReplicationInfo: adapter.ToThriftReplicationInfos(replicationInfo),
		History:         adapter.ToThriftDataBlob(blob),
		NewRunHistory:   nil,
	}).Return(nil).Times(1)

	rereplicationContext := newHistoryRereplicationContext(s.domainID, workflowID, runID, int64(123), uuid.New(), int64(111), s.rereplicator)
	err := rereplicationContext.handleEmptyHistory(s.domainID, workflowID, runID, replicationInfo)
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
		HistoryBatches: []*commonproto.DataBlob{{
			EncodingType: enums.EncodingTypeThriftRW,
			Data:         blob,
		}},
		NextPageToken: nextTokenOut,
		ReplicationInfo: map[string]*commonproto.ReplicationInfo{
			"random data center": {
				Version:     777,
				LastEventId: 999,
			},
		},
	}
	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    firstEventID,
		NextEventId:     nextEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   nextTokenIn,
	}).Return(response, nil).Times(1)

	out, err := s.getDummyRereplicationContext().getHistory(s.domainID, workflowID, runID, firstEventID, nextEventID, nextTokenIn, pageSize)
	s.Nil(err)
	s.Equal(response, out)
}

func (s *historyRereplicatorSuite) TestGetPrevEventID() {
	workflowID := "some random workflow ID"
	currentRunID := uuid.New()

	prepareFn := func(prevRunID *string) {
		eventBatch := []*shared.HistoryEvent{
			&shared.HistoryEvent{
				EventId:   common.Int64Ptr(1),
				Version:   common.Int64Ptr(123),
				Timestamp: common.Int64Ptr(time.Now().UnixNano()),
				EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
				WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
					ContinuedExecutionRunId: prevRunID,
				},
			},
			&shared.HistoryEvent{
				EventId:   common.Int64Ptr(2),
				Version:   common.Int64Ptr(223),
				Timestamp: common.Int64Ptr(time.Now().UnixNano()),
				EventType: shared.EventTypeDecisionTaskScheduled.Ptr(),
			},
		}
		blob, err := s.serializer.SerializeBatchEvents(eventBatch, common.EncodingTypeThriftRW)
		s.Nil(err)

		s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
			Domain: s.domainName,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      currentRunID,
			},
			FirstEventId:    common.FirstEventID,
			NextEventId:     common.EndEventID,
			MaximumPageSize: 1,
			NextPageToken:   nil,
		}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
			HistoryBatches: []*commonproto.DataBlob{{
				EncodingType: enums.EncodingTypeThriftRW,
				Data:         blob.Data,
			}},
		}, nil).Times(1)
	}

	// has prev run
	prevRunID := uuid.New()
	prepareFn(common.StringPtr(prevRunID))
	runID, err := s.getDummyRereplicationContext().getPrevRunID(s.domainID, workflowID, currentRunID)
	s.Nil(err)
	s.Equal(prevRunID, runID)

	// no prev run
	prepareFn(nil)
	runID, err = s.getDummyRereplicationContext().getPrevRunID(s.domainID, workflowID, currentRunID)
	s.Nil(err)
	s.Equal("", runID)
}

func (s *historyRereplicatorSuite) TestGetPrevEventID_EmptyEvents() {
	workflowID := "some random workflow ID"
	currentRunID := uuid.New()

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      currentRunID,
		},
		FirstEventId:    common.FirstEventID,
		NextEventId:     common.EndEventID,
		MaximumPageSize: 1,
		NextPageToken:   nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches: []*commonproto.DataBlob{},
	}, nil).Times(1)

	runID, err := s.getDummyRereplicationContext().getPrevRunID(s.domainID, workflowID, currentRunID)
	s.IsType(&shared.EntityNotExistsError{}, err)
	s.Equal("", runID)
}

func (s *historyRereplicatorSuite) TestGetNextRunID_ContinueAsNew() {
	nextRunID := uuid.New()
	eventBatchIn := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(233),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskCompleted.Ptr(),
		},
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(234),
			Version:   common.Int64Ptr(223),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionContinuedAsNew.Ptr(),
			WorkflowExecutionContinuedAsNewEventAttributes: &shared.WorkflowExecutionContinuedAsNewEventAttributes{
				NewExecutionRunId: common.StringPtr(nextRunID),
			},
		},
	}
	blob, err := s.serializer.SerializeBatchEvents(eventBatchIn, common.EncodingTypeThriftRW)
	s.Nil(err)

	runID, err := s.getDummyRereplicationContext().getNextRunID(&commonproto.DataBlob{
		EncodingType: enums.EncodingTypeThriftRW,
		Data:         blob.Data,
	})
	s.Nil(err)
	s.Equal(nextRunID, runID)
}

func (s *historyRereplicatorSuite) TestGetNextRunID_NotContinueAsNew() {
	eventBatchIn := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(233),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskCompleted.Ptr(),
		},
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(234),
			Version:   common.Int64Ptr(223),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionCanceled.Ptr(),
			WorkflowExecutionCancelRequestedEventAttributes: &shared.WorkflowExecutionCancelRequestedEventAttributes{},
		},
	}
	blob, err := s.serializer.SerializeBatchEvents(eventBatchIn, common.EncodingTypeThriftRW)
	s.Nil(err)

	runID, err := s.getDummyRereplicationContext().getNextRunID(&commonproto.DataBlob{
		EncodingType: enums.EncodingTypeThriftRW,
		Data:         blob.Data,
	})
	s.Nil(err)
	s.Equal("", runID)
}

func (s *historyRereplicatorSuite) TestDeserializeBlob() {
	eventBatchIn := []*shared.HistoryEvent{
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(1),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
		},
		&shared.HistoryEvent{
			EventId:   common.Int64Ptr(2),
			Version:   common.Int64Ptr(223),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskScheduled.Ptr(),
		},
	}

	blob, err := s.serializer.SerializeBatchEvents(eventBatchIn, common.EncodingTypeThriftRW)
	s.Nil(err)

	eventBatchOut, err := s.getDummyRereplicationContext().deserializeBlob(&commonproto.DataBlob{
		EncodingType: enums.EncodingTypeThriftRW,
		Data:         blob.Data,
	})
	s.Nil(err)
	s.Equal(eventBatchIn, eventBatchOut)
}

func (s *historyRereplicatorSuite) serializeEvents(events []*shared.HistoryEvent) *commonproto.DataBlob {
	blob, err := s.serializer.SerializeBatchEvents(events, common.EncodingTypeThriftRW)
	s.Nil(err)
	return &commonproto.DataBlob{
		EncodingType: enums.EncodingTypeThriftRW,
		Data:         blob.Data,
	}
}

func (s *historyRereplicatorSuite) getDummyRereplicationContext() *historyRereplicationContext {
	return &historyRereplicationContext{
		rereplicator: s.rereplicator,
		logger:       s.rereplicator.logger,
	}
}
