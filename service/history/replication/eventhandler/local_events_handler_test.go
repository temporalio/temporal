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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/shard"
)

type (
	localEventsHandlerSuite struct {
		suite.Suite
		*require.Assertions
		controller           *gomock.Controller
		clusterMetadata      *cluster.MockMetadata
		shardController      *shard.MockController
		logger               log.Logger
		eventSerializer      serialization.Serializer
		remoteHistoryFetcher *MockHistoryPaginatedFetcher

		localEventsHandler LocalGeneratedEventsHandler
	}
)

func TestLocalEventsHandlerSuite(t *testing.T) {
	s := new(localEventsHandlerSuite)
	suite.Run(t, s)
}

func (s *localEventsHandlerSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *localEventsHandlerSuite) TearDownSuite() {

}

func (s *localEventsHandlerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.logger = log.NewNoopLogger()
	s.eventSerializer = serialization.NewSerializer()
	s.remoteHistoryFetcher = NewMockHistoryPaginatedFetcher(s.controller)

	s.localEventsHandler = NewLocalEventsHandler(
		s.clusterMetadata,
		s.shardController,
		s.logger,
		s.eventSerializer,
		s.remoteHistoryFetcher,
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

func (s *localEventsHandlerSuite) TestHandleLocalHistoryEvents_AlreadyExist() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()
	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

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
	batch := serializeEvents(s.eventSerializer, historyEvents)

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
	engine.EXPECT().GetMutableState(gomock.Any(), &historyservice.GetMutableStateRequest{
		NamespaceId: namespaceId,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runId,
		},
	}).Return(nil, nil).Times(1)
	engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &ImportWorkflowExecutionRequestMatcher{request}).Return(
		&historyservice.ImportWorkflowExecutionResponse{
			Token:         nil,
			EventsApplied: false,
		}, nil).Times(1)

	err := s.localEventsHandler.HandleLocalGeneratedHistoryEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		versionHistory.Items,
		historyEvents,
	)
	s.Nil(err)
}

func (s *localEventsHandlerSuite) TestHandleLocalHistoryEvents_IncludeLastEvent_AppliedAndDirectCommit() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 1}, // Last local event is 10
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
		engine.EXPECT().GetMutableState(gomock.Any(), &historyservice.GetMutableStateRequest{
			NamespaceId: namespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowId,
				RunId:      runId,
			},
		}).Return(nil, nil).Times(1),
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

	err := s.localEventsHandler.HandleLocalGeneratedHistoryEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		versionHistory.Items,
		[][]*historypb.HistoryEvent{historyEvents},
	)
	s.Nil(err)
}

func (s *localEventsHandlerSuite) TestHandleHistoryEvents_LocalOnly_ImportAllLocalAndCommit() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 5, Version: 3},
			{EventId: 20, Version: 1001},
			{EventId: 25, Version: 2002},
		},
	}
	initialHistoryEvents := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 7,
				Version: 1001,
			},
			{
				EventId: 8,
				Version: 1001,
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
	batch := serializeEvents(s.eventSerializer, initialHistoryEvents)
	historyBatch1 := HistoryBatch{
		RawEventBatch:  batch[0],
		VersionHistory: versionHistory,
	}
	historyBatch2 := HistoryBatch{
		RawEventBatch:  batch[0],
		VersionHistory: versionHistory,
	}

	times := 0
	fetcher := collection.NewPagingIterator(func(paginationToken []byte) ([]HistoryBatch, []byte, error) {
		if times < historyImportBlobSize {
			times++
			return []HistoryBatch{historyBatch1}, []byte{1, 1, 0}, nil
		}
		return []HistoryBatch{historyBatch2}, nil, nil
	})

	returnToken1 := []byte{1, 0, 0, 1, 1, 1, 1, 0}
	returnToken2 := []byte{1, 0, 0, 1, 1, 1, 1, 1}
	returnToken3 := []byte{1, 1, 0, 1, 1, 1, 1, 1}

	fetchedBlob1 := []*commonpb.DataBlob{}
	for i := 0; i < historyImportBlobSize; i++ {
		fetchedBlob1 = append(fetchedBlob1, batch[0])
	}
	fetchedBlob2 := []*commonpb.DataBlob{batch[0]}

	gomock.InOrder(
		engine.EXPECT().GetMutableState(gomock.Any(), &historyservice.GetMutableStateRequest{
			NamespaceId: namespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowId,
				RunId:      runId,
			},
		}).Return(nil, nil).Times(1),
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
			int64(9),
			int64(1001),
			int64(20),
			int64(1001),
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

	err := s.localEventsHandler.HandleLocalGeneratedHistoryEvents(
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
