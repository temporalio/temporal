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

	"github.com/pborman/uuid"
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
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	nDCHistoryResenderSuite struct {
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
		config     *configs.Config
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
	s.mockClusterMetadata = cluster.NewMockMetadata(s.controller)
	s.mockClientBean = client.NewMockBean(s.controller)
	s.mockAdminClient = adminservicemock.NewMockAdminServiceClient(s.controller)
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockNamespaceCache = namespace.NewMockRegistry(s.controller)

	s.mockClientBean.EXPECT().GetRemoteAdminClient(gomock.Any()).Return(s.mockAdminClient, nil).AnyTimes()

	s.logger = log.NewTestLogger()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	s.namespaceID = namespace.ID(uuid.New())
	s.namespace = "some random namespace name"
	s.config = tests.NewDynamicConfig()
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
	s.serializer = serialization.NewSerializer()
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
	eventBatch := []*historypb.HistoryEvent{
		{
			EventId:   2,
			Version:   123,
			EventTime: timestamppb.New(time.Now().UTC()),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		},
		{
			EventId:   3,
			Version:   123,
			EventTime: timestamppb.New(time.Now().UTC()),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		},
	}
	blob := s.serializeEvents(eventBatch)
	versionHistoryItems := []*historyspb.VersionHistoryItem{
		{
			EventId: 1,
			Version: 1,
		},
	}

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(
		gomock.Any(),
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: s.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
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
		VersionHistory: &historyspb.VersionHistory{
			Items: versionHistoryItems,
		},
	}, nil)

	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(
		gomock.Any(),
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: s.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
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
		VersionHistory: &historyspb.VersionHistory{
			Items: versionHistoryItems,
		},
	}, nil)

	functionCalled := false
	rereplicator := NewNDCHistoryResender(
		s.mockNamespaceCache,
		s.mockClientBean,
		func(
			ctx context.Context,
			sourceClusterName string,
			namespaceId namespace.ID,
			workflowId string,
			runId string,
			events [][]*historypb.HistoryEvent,
			versionHistory []*historyspb.VersionHistoryItem,
		) error {
			functionCalled = true
			return nil
		},
		serialization.NewSerializer(),
		nil,
		s.logger,
		s.config,
	)

	err := rereplicator.SendSingleWorkflowHistory(
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
	s.True(functionCalled)
	s.Nil(err)
}

func (s *nDCHistoryResenderSuite) TestSendSingleWorkflowHistory_Batching() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	startEventID := int64(123)
	startEventVersion := int64(100)
	pageSize := defaultPageSize
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
	versionHistoryItems0 := []*historyspb.VersionHistoryItem{
		{EventId: 1, Version: 1},
	}
	versionHistoryItems1 := []*historyspb.VersionHistoryItem{
		{EventId: 2, Version: 1},
	}

	mockGetHistoryCall := func(events []*historypb.HistoryEvent, vh []*historyspb.VersionHistoryItem, inputToken []byte, returnToken []byte) {
		blob := s.serializeEvents(events)
		s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(
			gomock.Any(),
			&adminservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId: s.namespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				StartEventId:      startEventID,
				StartEventVersion: startEventVersion,
				EndEventId:        common.EmptyEventID,
				EndEventVersion:   common.EmptyVersion,
				MaximumPageSize:   pageSize,
				NextPageToken:     inputToken,
			}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
			HistoryBatches: []*commonpb.DataBlob{blob},
			NextPageToken:  returnToken,
			VersionHistory: &historyspb.VersionHistory{
				Items: vh,
			},
		}, nil)
	}
	token0 := []byte{0}
	token1 := []byte{1}
	token2 := []byte{2}
	token3 := []byte{3}
	mockGetHistoryCall(eventBatch0, versionHistoryItems0, nil, token0)
	mockGetHistoryCall(eventBatch1, versionHistoryItems0, token0, token1)
	mockGetHistoryCall(eventBatch2, versionHistoryItems0, token1, token2)
	mockGetHistoryCall(eventBatch3, versionHistoryItems1, token2, token3)
	mockGetHistoryCall(eventBatch4, versionHistoryItems1, token3, nil)

	functionCallTimes := 0
	var calledEvents [][]*historypb.HistoryEvent
	rereplicator := NewNDCHistoryResender(
		s.mockNamespaceCache,
		s.mockClientBean,
		func(
			ctx context.Context,
			sourceClusterName string,
			namespaceId namespace.ID,
			workflowId string,
			runId string,
			events [][]*historypb.HistoryEvent,
			versionHistory []*historyspb.VersionHistoryItem,
		) error {
			functionCallTimes++
			calledEvents = append(calledEvents, events...)
			return nil
		},
		serialization.NewSerializer(),
		nil,
		s.logger,
		s.config,
	)

	err := rereplicator.SendSingleWorkflowHistory(
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
	s.Nil(err)
	s.Equal(3, functionCallTimes)
	s.Equal(5, len(calledEvents))
	eventId := int64(1)
	for _, events := range calledEvents {
		for _, event := range events {
			event.EventId = eventId
			eventId++
		}
	}
}

func (s *nDCHistoryResenderSuite) TestSendSingleWorkflowHistory_Batching_ApplyWithSameEventVersion() {
	workflowID := "some random workflow ID"
	runID := uuid.New()
	startEventID := int64(123)
	startEventVersion := int64(100)
	pageSize := defaultPageSize
	s.config.ReplicationResendMaxBatchCount = dynamicconfig.GetIntPropertyFn(2)

	eventBatch0 := []*historypb.HistoryEvent{
		{EventId: 1},
		{EventId: 2},
	}
	eventBatch1 := []*historypb.HistoryEvent{
		{EventId: 3, Version: 124},
		{EventId: 4, Version: 124},
	}
	eventBatch2 := []*historypb.HistoryEvent{
		{EventId: 5, Version: 125},
		{EventId: 6, Version: 125},
	}
	eventBatch3 := []*historypb.HistoryEvent{
		{EventId: 7, Version: 126},
		{EventId: 8, Version: 126},
	}
	eventBatch4 := []*historypb.HistoryEvent{
		{EventId: 9, Version: 127},
		{EventId: 10, Version: 127},
	}
	versionHistoryItems := []*historyspb.VersionHistoryItem{
		{EventId: 2},
		{EventId: 4, Version: 124},
		{EventId: 6, Version: 125},
		{EventId: 8, Version: 126},
		{EventId: 10, Version: 127},
	}
	mockGetHistoryCall := func(events []*historypb.HistoryEvent, vh []*historyspb.VersionHistoryItem, inputToken []byte, returnToken []byte) {
		blob := s.serializeEvents(events)
		s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(
			gomock.Any(),
			&adminservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId: s.namespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				StartEventId:      startEventID,
				StartEventVersion: startEventVersion,
				EndEventId:        common.EmptyEventID,
				EndEventVersion:   common.EmptyVersion,
				MaximumPageSize:   pageSize,
				NextPageToken:     inputToken,
			}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
			HistoryBatches: []*commonpb.DataBlob{blob},
			NextPageToken:  returnToken,
			VersionHistory: &historyspb.VersionHistory{
				Items: vh,
			},
		}, nil)
	}
	token0 := []byte{0}
	token1 := []byte{1}
	token2 := []byte{2}
	token3 := []byte{3}
	mockGetHistoryCall(eventBatch0, versionHistoryItems, nil, token0)
	mockGetHistoryCall(eventBatch1, versionHistoryItems, token0, token1)
	mockGetHistoryCall(eventBatch2, versionHistoryItems, token1, token2)
	mockGetHistoryCall(eventBatch3, versionHistoryItems, token2, token3)
	mockGetHistoryCall(eventBatch4, versionHistoryItems, token3, nil)

	functionCallTimes := 0
	var calledEvents [][]*historypb.HistoryEvent
	rereplicator := NewNDCHistoryResender(
		s.mockNamespaceCache,
		s.mockClientBean,
		func(
			ctx context.Context,
			sourceClusterName string,
			namespaceId namespace.ID,
			workflowId string,
			runId string,
			events [][]*historypb.HistoryEvent,
			versionHistory []*historyspb.VersionHistoryItem,
		) error {
			functionCallTimes++
			calledEvents = append(calledEvents, events...)
			return nil
		},
		serialization.NewSerializer(),
		nil,
		s.logger,
		s.config,
	)

	err := rereplicator.SendSingleWorkflowHistory(
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
	s.Nil(err)
	s.Equal(5, functionCallTimes)
	s.Equal(5, len(calledEvents))
	eventId := int64(1)
	for _, events := range calledEvents {
		for _, event := range events {
			event.EventId = eventId
			eventId++
		}
	}
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
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         blob,
		}},
		NextPageToken: nextTokenOut,
	}
	s.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: s.namespaceID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		StartEventId:      startEventID,
		StartEventVersion: version,
		EndEventId:        endEventID,
		EndEventVersion:   version,
		MaximumPageSize:   pageSize,
		NextPageToken:     nextTokenIn,
	}).Return(response, nil)

	rereplicator := NewNDCHistoryResender(
		s.mockNamespaceCache,
		s.mockClientBean,
		func(
			ctx context.Context,
			sourceClusterName string,
			namespaceId namespace.ID,
			workflowId string,
			runId string,
			events [][]*historypb.HistoryEvent,
			versionHistory []*historyspb.VersionHistoryItem,
		) error {
			return nil
		},
		serialization.NewSerializer(),
		nil,
		s.logger,
		s.config,
	)
	out, err := rereplicator.getHistory(
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
		pageSize)
	s.Nil(err)
	s.Equal(response, out)
}

func (s *nDCHistoryResenderSuite) serializeEvents(events []*historypb.HistoryEvent) *commonpb.DataBlob {
	blob, err := s.serializer.SerializeEvents(events, enumspb.ENCODING_TYPE_PROTO3)
	s.Nil(err)
	return &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         blob.Data,
	}
}
