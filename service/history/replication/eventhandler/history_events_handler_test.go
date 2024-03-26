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
	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
)

type (
	historyEventHandlerSuite struct {
		suite.Suite
		*require.Assertions
		controller      *gomock.Controller
		clusterMetadata *cluster.MockMetadata

		localEventsHandler *MockLocalGeneratedEventsHandler
		remoteEventHandler *MockRemoteGeneratedEventsHandler

		historyEventHandler *historyEventsHandlerImpl
	}
)

func TestHistoryEventHandlerSuite(t *testing.T) {
	s := new(historyEventHandlerSuite)
	suite.Run(t, s)
}

func (s *historyEventHandlerSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *historyEventHandlerSuite) TearDownSuite() {

}

func (s *historyEventHandlerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.localEventsHandler = NewMockLocalGeneratedEventsHandler(s.controller)
	s.remoteEventHandler = NewMockRemoteGeneratedEventsHandler(s.controller)
	s.historyEventHandler = &historyEventsHandlerImpl{
		s.clusterMetadata,
		s.localEventsHandler,
		s.remoteEventHandler,
	}
}

func (s *historyEventHandlerSuite) TestHandleHistoryEvents_RemoteOnly() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{1, 0, 1},
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 1},
			{EventId: 15, Version: 2},
		},
	}
	historyEvents1 := []*historypb.HistoryEvent{
		{
			EventId: 11,
		},
		{
			EventId: 12,
		},
	}
	historyEvents2 := []*historypb.HistoryEvent{
		{
			EventId: 13,
		},
		{
			EventId: 14,
		},
	}
	historyEvents := [][]*historypb.HistoryEvent{historyEvents1, historyEvents2}
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}

	s.remoteEventHandler.EXPECT().HandleRemoteGeneratedHistoryEvents(
		gomock.Any(),
		workflowKey,
		nil,
		versionHistory.Items,
		historyEvents,
		nil,
		"",
	).Return(nil).Times(1)

	err := s.historyEventHandler.HandleHistoryEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		nil,
		versionHistory.Items,
		historyEvents,
		nil,
		"",
	)
	s.Nil(err)
}

func (s *historyEventHandlerSuite) TestHandleHistoryEvents_LocalOnly() {
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

	s.localEventsHandler.EXPECT().HandleLocalGeneratedHistoryEvents(
		gomock.Any(),
		remoteCluster,
		workflowKey,
		versionHistory.Items,
		[][]*historypb.HistoryEvent{historyEvents},
	).Return(nil).Times(1)

	err := s.historyEventHandler.HandleHistoryEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		nil,
		versionHistory.Items,
		[][]*historypb.HistoryEvent{historyEvents},
		nil,
		"",
	)
	s.Nil(err)
}

func (s *historyEventHandlerSuite) TestHandleHistoryEvents_LocalAndRemote_HandleLocalThenRemote() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1)) // current cluster ID is 1
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 5, Version: 3},
			{EventId: 10, Version: 1001},
			{EventId: 13, Version: 1002},
			{EventId: 15, Version: 1003},
		},
	}
	localHistoryEvents := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 5,
				Version: 3,
			},
		},
		{
			{
				EventId: 6,
				Version: 1,
			},
		},
		{
			{
				EventId: 7,
				Version: 1,
			},
			{
				EventId: 8,
				Version: 1,
			},
			{
				EventId: 9,
				Version: 1,
			},
			{
				EventId: 10,
				Version: 1,
			},
		},
	}
	remoteHistoryEvents := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 11,
				Version: 2,
			},
			{
				EventId: 12,
				Version: 2,
			},
			{
				EventId: 13,
				Version: 2,
			},
		},
		{
			{
				EventId: 14,
				Version: 1003,
			},
			{
				EventId: 15,
				Version: 1003,
			},
		},
	}
	initialHistoryEvents := append(localHistoryEvents, remoteHistoryEvents...)
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}

	gomock.InOrder(
		s.localEventsHandler.EXPECT().HandleLocalGeneratedHistoryEvents(
			gomock.Any(),
			remoteCluster,
			workflowKey,
			versionHistory.Items,
			localHistoryEvents,
		).Return(nil).Times(1),

		s.remoteEventHandler.EXPECT().HandleRemoteGeneratedHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory.Items,
			remoteHistoryEvents,
			nil,
			"",
		).Return(nil).Times(1),
	)

	err := s.historyEventHandler.HandleHistoryEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		nil,
		versionHistory.Items,
		initialHistoryEvents,
		nil,
		"",
	)
	s.Nil(err)
}
