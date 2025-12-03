package ndc

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

type (
	mutableStateMapperSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		logger             log.Logger
		shardContext       historyi.MockShardContext
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
		wfContext historyi.WorkflowContext,
		mutableState historyi.MutableState,
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
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
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
		nil,
		false,
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
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
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
		nil,
		false,
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
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
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
		nil,
		false,
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
