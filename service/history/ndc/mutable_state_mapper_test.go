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

package ndc

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	mutableStateMapperSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		logger             log.Logger
		shardContext       shard.MockContext
		branchMgrProvider  branchMgrProvider
		mockBranchMgr      *MockBranchMgr
		mutableStateMapper *MutableStateMapperImpl
		clusterMetadata    *cluster.MockMetadata
	}
)

func TestMutableStateMapperSuite(t *testing.T) {
	s := new(mutableStateMapperSuite)
	suite.Run(t, s)
}

func (s *mutableStateMapperSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.mockBranchMgr = NewMockBranchMgr(s.controller)
	s.branchMgrProvider = func(
		wfContext workflow.Context,
		mutableState workflow.MutableState,
		logger log.Logger) BranchMgr {
		return s.mockBranchMgr
	}
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).Return("some random cluster name").AnyTimes()
	s.mutableStateMapper = NewMutableStateMapping(
		nil,
		nil,
		s.branchMgrProvider,
		nil,
		nil)
}

func (s *mutableStateMapperSuite) TearDownSuite() {

}

func (s *mutableStateMapperSuite) TestGetOrCreateHistoryBranch_ValidEventBatch_NoDedupe() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.New(),
		RunID:      uuid.New(),
	}
	eventSlices := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 11,
			},
			{
				EventId: 12,
			},
		},
		{
			{
				EventId: 13,
			},
			{
				EventId: 14,
			},
		},
	}
	task, _ := newReplicationTask(
		s.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		eventSlices,
		nil,
		"",
	)
	s.mockBranchMgr.EXPECT().
		GetOrCreate(gomock.Any(), gomock.Any(), int64(11), gomock.Any()).Return(true, int32(0), nil).Times(1)

	_, out, err := s.mutableStateMapper.GetOrCreateHistoryBranch(context.Background(), nil, nil, task)
	s.NoError(err)
	s.Equal(int32(0), out.BranchIndex)
	s.Equal(0, out.EventsApplyIndex)
	s.True(out.DoContinue)
}

func (s *mutableStateMapperSuite) TestGetOrCreateHistoryBranch_ValidEventBatch_FirstBatchDedupe() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.New(),
		RunID:      uuid.New(),
	}
	eventSlices := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 11,
			},
			{
				EventId: 12,
			},
		},
		{
			{
				EventId: 13,
			},
			{
				EventId: 14,
			},
		},
	}
	task, _ := newReplicationTask(
		s.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		eventSlices,
		nil,
		"",
	)
	s.mockBranchMgr.EXPECT().
		GetOrCreate(gomock.Any(), gomock.Any(), int64(11), gomock.Any()).Return(false, int32(0), nil).Times(1)
	s.mockBranchMgr.EXPECT().
		GetOrCreate(gomock.Any(), gomock.Any(), int64(13), gomock.Any()).Return(true, int32(0), nil).Times(1)
	_, out, err := s.mutableStateMapper.GetOrCreateHistoryBranch(context.Background(), nil, nil, task)
	s.NoError(err)
	s.Equal(int32(0), out.BranchIndex)
	s.Equal(1, out.EventsApplyIndex)
	s.True(out.DoContinue)
}

func (s *mutableStateMapperSuite) TestGetOrCreateHistoryBranch_ValidEventBatch_AllBatchDedupe() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.New(),
		RunID:      uuid.New(),
	}
	eventSlices := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 11,
			},
			{
				EventId: 12,
			},
		},
		{
			{
				EventId: 13,
			},
			{
				EventId: 14,
			},
		},
	}
	task, _ := newReplicationTask(
		s.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		eventSlices,
		nil,
		"",
	)
	s.mockBranchMgr.EXPECT().
		GetOrCreate(gomock.Any(), gomock.Any(), int64(11), gomock.Any()).Return(false, int32(0), nil).Times(1)
	s.mockBranchMgr.EXPECT().
		GetOrCreate(gomock.Any(), gomock.Any(), int64(13), gomock.Any()).Return(false, int32(0), nil).Times(1)
	_, out, err := s.mutableStateMapper.GetOrCreateHistoryBranch(context.Background(), nil, nil, task)
	s.NoError(err)
	s.Equal(int32(0), out.BranchIndex)
	s.Equal(0, out.EventsApplyIndex)
	s.False(out.DoContinue)
}
