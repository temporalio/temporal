// Copyright (c) 2019 Uber Technologies, Inc.
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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/adminservicemock"
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

		controller        *gomock.Controller
		mockDomainCache   *cache.MockDomainCache
		mockAdminClient   *adminservicemock.MockAdminServiceClient
		mockHistoryClient *historyservicemock.MockHistoryServiceClient

		domainID   string
		domainName string

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
	s.mockDomainCache = cache.NewMockDomainCache(s.controller)

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)

	s.domainID = uuid.New()
	s.domainName = "some random domain name"
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

	s.rereplicator = NewNDCHistoryResender(
		s.mockDomainCache,
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
	eventBatch := []*commonproto.HistoryEvent{
		{
			EventId:   2,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: enums.EventTypeDecisionTaskScheduled,
		},
		{
			EventId:   3,
			Version:   123,
			Timestamp: time.Now().UnixNano(),
			EventType: enums.EventTypeDecisionTaskStarted,
		},
	}
	blob := s.serializeEvents(eventBatch)
	versionHistoryItems := []*commonproto.VersionHistoryItem{
		{
			EventID: 1,
			Version: 1,
		},
	}

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(
		gomock.Any(),
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Domain: s.domainName,
			Execution: &commonproto.WorkflowExecution{
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
		HistoryBatches: []*commonproto.DataBlob{blob},
		NextPageToken:  token,
		VersionHistory: &commonproto.VersionHistory{
			Items: versionHistoryItems,
		},
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(
		gomock.Any(),
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Domain: s.domainName,
			Execution: &commonproto.WorkflowExecution{
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
		HistoryBatches: []*commonproto.DataBlob{blob},
		NextPageToken:  nil,
		VersionHistory: &commonproto.VersionHistory{
			Items: versionHistoryItems,
		},
	}, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateEventsV2(
		gomock.Any(),
		&historyservice.ReplicateEventsV2Request{
			DomainUUID: s.domainID,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			VersionHistoryItems: versionHistoryItems,
			Events:              blob,
		}).Return(nil, nil).Times(2)

	err := s.rereplicator.SendSingleWorkflowHistory(
		s.domainID,
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
	blob := &commonproto.DataBlob{
		EncodingType: enums.EncodingTypeThriftRW,
		Data:         []byte("some random history blob"),
	}
	versionHistoryItems := []*commonproto.VersionHistoryItem{
		{
			EventID: 1,
			Version: 1,
		},
	}

	s.Equal(&historyservice.ReplicateEventsV2Request{
		DomainUUID: s.domainID,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		VersionHistoryItems: versionHistoryItems,
		Events:              blob,
	}, s.rereplicator.createReplicationRawRequest(
		s.domainID,
		workflowID,
		runID,
		blob,
		versionHistoryItems))
}

func (s *nDCHistoryResenderSuite) TestSendReplicationRawRequest() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	item := &commonproto.VersionHistoryItem{
		EventID: 1,
		Version: 1,
	}
	request := &historyservice.ReplicateEventsV2Request{
		DomainUUID: s.domainID,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Events: &commonproto.DataBlob{
			EncodingType: enums.EncodingTypeThriftRW,
			Data:         []byte("some random history blob"),
		},
		VersionHistoryItems: []*commonproto.VersionHistoryItem{item},
	}

	s.mockHistoryClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(nil, nil).Times(1)
	err := s.rereplicator.sendReplicationRawRequest(request)
	s.Nil(err)
}

func (s *nDCHistoryResenderSuite) TestSendReplicationRawRequest_Err() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	item := &commonproto.VersionHistoryItem{
		EventID: 1,
		Version: 1,
	}
	request := &historyservice.ReplicateEventsV2Request{
		DomainUUID: s.domainID,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Events: &commonproto.DataBlob{
			EncodingType: enums.EncodingTypeThriftRW,
			Data:         []byte("some random history blob"),
		},
		VersionHistoryItems: []*commonproto.VersionHistoryItem{item},
	}
	retryErr := serviceerror.NewRetryTaskV2(
		"",
		s.domainID,
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
		HistoryBatches: []*commonproto.DataBlob{{
			EncodingType: enums.EncodingTypeThriftRW,
			Data:         blob,
		}},
		NextPageToken: nextTokenOut,
	}
	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
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
		s.domainID,
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

func (s *nDCHistoryResenderSuite) serializeEvents(events []*commonproto.HistoryEvent) *commonproto.DataBlob {
	blob, err := s.serializer.SerializeBatchEvents(events, common.EncodingTypeProto3)
	s.Nil(err)
	return &commonproto.DataBlob{
		EncodingType: enums.EncodingTypeProto3,
		Data:         blob.Data,
	}
}
