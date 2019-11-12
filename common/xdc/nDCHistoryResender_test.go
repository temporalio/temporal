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

	"github.com/uber/cadence/.gen/go/admin"
	"github.com/uber/cadence/.gen/go/admin/adminservicetest"
	"github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/history/historyservicetest"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
)

type (
	nDCHistoryResenderSuite struct {
		suite.Suite
		*require.Assertions

		controller        *gomock.Controller
		mockDomainCache   *cache.MockDomainCache
		mockAdminClient   *adminservicetest.MockClient
		mockHistoryClient *historyservicetest.MockClient

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
	s.mockAdminClient = adminservicetest.NewMockClient(s.controller)
	s.mockHistoryClient = historyservicetest.NewMockClient(s.controller)
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
		func(ctx context.Context, request *history.ReplicateEventsV2Request) error {
			return s.mockHistoryClient.ReplicateEventsV2(ctx, request)
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
	eventBatch := []*shared.HistoryEvent{
		{
			EventId:   common.Int64Ptr(2),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskScheduled.Ptr(),
		},
		{
			EventId:   common.Int64Ptr(3),
			Version:   common.Int64Ptr(123),
			Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			EventType: shared.EventTypeDecisionTaskStarted.Ptr(),
		},
	}
	blob := s.serializeEvents(eventBatch)
	versionHistoryItems := []*shared.VersionHistoryItem{
		{
			EventID: common.Int64Ptr(1),
			Version: common.Int64Ptr(1),
		},
	}

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(
		gomock.Any(),
		&admin.GetWorkflowExecutionRawHistoryV2Request{
			Domain: common.StringPtr(s.domainName),
			Execution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			StartEventId:      common.Int64Ptr(startEventID),
			StartEventVersion: common.Int64Ptr(startEventVersion),
			MaximumPageSize:   common.Int32Ptr(pageSize),
			NextPageToken:     nil,
		}).Return(&admin.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*shared.DataBlob{blob},
		NextPageToken:  token,
		VersionHistory: &shared.VersionHistory{
			Items: versionHistoryItems,
		},
	}, nil).Times(1)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(
		gomock.Any(),
		&admin.GetWorkflowExecutionRawHistoryV2Request{
			Domain: common.StringPtr(s.domainName),
			Execution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			StartEventId:      common.Int64Ptr(startEventID),
			StartEventVersion: common.Int64Ptr(startEventVersion),
			MaximumPageSize:   common.Int32Ptr(pageSize),
			NextPageToken:     token,
		}).Return(&admin.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*shared.DataBlob{blob},
		NextPageToken:  nil,
		VersionHistory: &shared.VersionHistory{
			Items: versionHistoryItems,
		},
	}, nil).Times(1)

	s.mockHistoryClient.EXPECT().ReplicateEventsV2(
		gomock.Any(),
		&history.ReplicateEventsV2Request{
			DomainUUID: common.StringPtr(s.domainID),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			VersionHistoryItems: versionHistoryItems,
			Events:              blob,
		}).Return(nil).Times(2)

	err := s.rereplicator.SendSingleWorkflowHistory(
		s.domainID,
		workflowID,
		runID,
		common.Int64Ptr(startEventID),
		common.Int64Ptr(startEventVersion),
		nil,
		nil,
	)

	s.Nil(err)
}

func (s *nDCHistoryResenderSuite) TestCreateReplicateRawEventsRequest() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	blob := &shared.DataBlob{
		EncodingType: shared.EncodingTypeThriftRW.Ptr(),
		Data:         []byte("some random history blob"),
	}
	versionHistoryItems := []*shared.VersionHistoryItem{
		{
			EventID: common.Int64Ptr(1),
			Version: common.Int64Ptr(1),
		},
	}

	s.Equal(&history.ReplicateEventsV2Request{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
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
	item := &shared.VersionHistoryItem{
		EventID: common.Int64Ptr(1),
		Version: common.Int64Ptr(1),
	}
	request := &history.ReplicateEventsV2Request{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		Events: &shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         []byte("some random history blob"),
		},
		VersionHistoryItems: []*shared.VersionHistoryItem{item},
	}

	s.mockHistoryClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(nil).Times(1)
	err := s.rereplicator.sendReplicationRawRequest(request)
	s.Nil(err)
}

func (s *nDCHistoryResenderSuite) TestSendReplicationRawRequest_Err() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	item := &shared.VersionHistoryItem{
		EventID: common.Int64Ptr(1),
		Version: common.Int64Ptr(1),
	}
	request := &history.ReplicateEventsV2Request{
		DomainUUID: common.StringPtr(s.domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		Events: &shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         []byte("some random history blob"),
		},
		VersionHistoryItems: []*shared.VersionHistoryItem{item},
	}
	retryErr := &shared.RetryTaskV2Error{
		DomainId:   common.StringPtr(s.domainID),
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}

	s.mockHistoryClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(retryErr).Times(1)
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

	response := &admin.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*shared.DataBlob{&shared.DataBlob{
			EncodingType: shared.EncodingTypeThriftRW.Ptr(),
			Data:         blob,
		}},
		NextPageToken: nextTokenOut,
	}
	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &admin.GetWorkflowExecutionRawHistoryV2Request{
		Domain: common.StringPtr(s.domainName),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		StartEventId:      common.Int64Ptr(startEventID),
		StartEventVersion: common.Int64Ptr(version),
		EndEventId:        common.Int64Ptr(endEventID),
		EndEventVersion:   common.Int64Ptr(version),
		MaximumPageSize:   common.Int32Ptr(pageSize),
		NextPageToken:     nextTokenIn,
	}).Return(response, nil).Times(1)

	out, err := s.rereplicator.getHistory(
		s.domainID,
		workflowID,
		runID,
		&startEventID,
		&version,
		&endEventID,
		&version,
		nextTokenIn,
		pageSize)
	s.Nil(err)
	s.Equal(response, out)
}

func (s *nDCHistoryResenderSuite) serializeEvents(events []*shared.HistoryEvent) *shared.DataBlob {
	blob, err := s.serializer.SerializeBatchEvents(events, common.EncodingTypeThriftRW)
	s.Nil(err)
	return &shared.DataBlob{
		EncodingType: shared.EncodingTypeThriftRW.Ptr(),
		Data:         blob.Data,
	}
}
