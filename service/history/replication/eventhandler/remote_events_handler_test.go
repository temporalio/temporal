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
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
)

type (
	futureEventsHandlerSuite struct {
		suite.Suite
		*require.Assertions
		controller      *gomock.Controller
		shardController *shard.MockController

		futureEventHandler RemoteGeneratedEventsHandler
	}
)

func TestFutureEventsHandlerSuite(t *testing.T) {
	s := new(futureEventsHandlerSuite)
	suite.Run(t, s)
}

func (s *futureEventsHandlerSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *futureEventsHandlerSuite) TearDownSuite() {

}

func (s *futureEventsHandlerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.shardController = shard.NewMockController(s.controller)
	s.futureEventHandler = NewRemoteGeneratedEventsHandler(
		s.shardController,
	)
}

func (s *futureEventsHandlerSuite) TestHandleFutureHistoryEvents() {
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{1, 0, 1},
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 1},
			{EventId: 15, Version: 2},
		},
	}
	historyEvents := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 11,
			},
			{
				EventId: 12,
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
	engine.EXPECT().ReplicateHistoryEvents(
		gomock.Any(),
		workflowKey,
		nil,
		versionHistory.Items,
		historyEvents,
		nil,
		"",
	).Times(1)

	err := s.futureEventHandler.HandleRemoteGeneratedHistoryEvents(
		context.Background(),
		workflowKey,
		nil,
		versionHistory.Items,
		historyEvents,
		nil,
		"",
	)
	s.Nil(err)
}
