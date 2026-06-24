package ndc

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

type (
	mutableStateMapperSuite struct {
		suite.Suite
		*require.Assertions

		controller           *gomock.Controller
		logger               log.Logger
		shardContext         historyi.MockShardContext
		branchMgrProvider    branchMgrProvider
		mockBranchMgr        *MockBranchMgr
		mockConflictResolver *MockConflictResolver
		mockFlusher          *MockBufferEventFlusher
		mockRebuilder        *workflow.MockMutableStateRebuilder
		mutableStateMapper   *MutableStateMapperImpl
		clusterMetadata      *cluster.MockMetadata
	}
)

func TestMutableStateMapperSuite(t *testing.T) {
	s := new(mutableStateMapperSuite)
	suite.Run(t, s)
}

func (s *mutableStateMapperSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.logger = log.NewTestLogger()

	s.mockBranchMgr = NewMockBranchMgr(s.controller)
	s.branchMgrProvider = func(
		wfContext historyi.WorkflowContext,
		mutableState historyi.MutableState,
		logger log.Logger) BranchMgr {
		return s.mockBranchMgr
	}
	s.mockConflictResolver = NewMockConflictResolver(s.controller)
	s.mockFlusher = NewMockBufferEventFlusher(s.controller)
	s.mockRebuilder = workflow.NewMockMutableStateRebuilder(s.controller)
	conflictResolverProvider := func(
		wfContext historyi.WorkflowContext,
		mutableState historyi.MutableState,
		logger log.Logger) ConflictResolver {
		return s.mockConflictResolver
	}
	flusherProvider := func(
		wfContext historyi.WorkflowContext,
		mutableState historyi.MutableState,
		logger log.Logger) BufferEventFlusher {
		return s.mockFlusher
	}
	rebuilderProvider := func(
		mutableState historyi.MutableState,
		logger log.Logger) workflow.MutableStateRebuilder {
		return s.mockRebuilder
	}
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).Return("some random cluster name").AnyTimes()
	s.mutableStateMapper = NewMutableStateMapping(
		nil,
		flusherProvider,
		s.branchMgrProvider,
		conflictResolverProvider,
		rebuilderProvider)
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

func (s *mutableStateMapperSuite) msMapperNewTask(eventSlices [][]*historypb.HistoryEvent) *replicationTaskImpl {
	workflowKey := definition.WorkflowKey{
		NamespaceID: uuid.NewString(),
		WorkflowID:  uuid.NewString(),
		RunID:       uuid.NewString(),
	}
	task, err := newReplicationTask(
		s.clusterMetadata,
		s.logger,
		workflowKey,
		nil,
		nil,
		eventSlices,
		nil,
		"",
		nil,
		false,
	)
	s.NoError(err)
	return task
}

func msMapperSingleBatch() [][]*historypb.HistoryEvent {
	return [][]*historypb.HistoryEvent{
		{
			{EventId: 11},
			{EventId: 12},
		},
	}
}

func (s *mutableStateMapperSuite) TestGetOrCreateHistoryBranch_RetryReplication() {
	task := s.msMapperNewTask(msMapperSingleBatch())
	retryErr := serviceerrors.NewRetryReplication("retry", "ns", "wf", "run", 1, 1, 2, 2)
	s.mockBranchMgr.EXPECT().
		GetOrCreate(gomock.Any(), gomock.Any(), int64(11), gomock.Any()).Return(false, int32(0), retryErr).Times(1)

	ms, out, err := s.mutableStateMapper.GetOrCreateHistoryBranch(context.Background(), nil, nil, task)
	s.Error(err)
	s.Nil(ms)
	s.Equal(PrepareHistoryBranchOut{}, out)
}

func (s *mutableStateMapperSuite) TestGetOrCreateHistoryBranch_DefaultError() {
	task := s.msMapperNewTask(msMapperSingleBatch())
	someErr := errors.New("some random error")
	s.mockBranchMgr.EXPECT().
		GetOrCreate(gomock.Any(), gomock.Any(), int64(11), gomock.Any()).Return(false, int32(0), someErr).Times(1)

	ms, out, err := s.mutableStateMapper.GetOrCreateHistoryBranch(context.Background(), nil, nil, task)
	s.Equal(someErr, err)
	s.Nil(ms)
	s.Equal(PrepareHistoryBranchOut{}, out)
}

func (s *mutableStateMapperSuite) TestCreateHistoryBranch_Success() {
	task := s.msMapperNewTask(msMapperSingleBatch())
	s.mockBranchMgr.EXPECT().
		Create(gomock.Any(), gomock.Any(), int64(11), gomock.Any()).Return(true, int32(2), nil).Times(1)

	ms, out, err := s.mutableStateMapper.CreateHistoryBranch(context.Background(), nil, nil, task)
	s.NoError(err)
	s.Nil(ms)
	s.Equal(int32(2), out.BranchIndex)
	s.True(out.DoContinue)
}

func (s *mutableStateMapperSuite) TestCreateHistoryBranch_RetryReplication() {
	task := s.msMapperNewTask(msMapperSingleBatch())
	retryErr := serviceerrors.NewRetryReplication("retry", "ns", "wf", "run", 1, 1, 2, 2)
	s.mockBranchMgr.EXPECT().
		Create(gomock.Any(), gomock.Any(), int64(11), gomock.Any()).Return(false, int32(0), retryErr).Times(1)

	ms, out, err := s.mutableStateMapper.CreateHistoryBranch(context.Background(), nil, nil, task)
	s.Error(err)
	s.Nil(ms)
	s.Equal(PrepareHistoryBranchOut{}, out)
}

func (s *mutableStateMapperSuite) TestCreateHistoryBranch_DefaultError() {
	task := s.msMapperNewTask(msMapperSingleBatch())
	someErr := errors.New("some random error")
	s.mockBranchMgr.EXPECT().
		Create(gomock.Any(), gomock.Any(), int64(11), gomock.Any()).Return(false, int32(0), someErr).Times(1)

	ms, out, err := s.mutableStateMapper.CreateHistoryBranch(context.Background(), nil, nil, task)
	s.Equal(someErr, err)
	s.Nil(ms)
	s.Equal(PrepareHistoryBranchOut{}, out)
}

func (s *mutableStateMapperSuite) TestFlushBufferEvents_Success() {
	task := s.msMapperNewTask(msMapperSingleBatch())
	flushedMS := historyi.NewMockMutableState(s.controller)
	s.mockFlusher.EXPECT().flush(gomock.Any()).Return(nil, flushedMS, nil).Times(1)

	ms, _, err := s.mutableStateMapper.FlushBufferEvents(context.Background(), nil, nil, task)
	s.NoError(err)
	s.Equal(flushedMS, ms)
}

func (s *mutableStateMapperSuite) TestFlushBufferEvents_Error() {
	task := s.msMapperNewTask(msMapperSingleBatch())
	someErr := errors.New("some random error")
	s.mockFlusher.EXPECT().flush(gomock.Any()).Return(nil, nil, someErr).Times(1)

	ms, _, err := s.mutableStateMapper.FlushBufferEvents(context.Background(), nil, nil, task)
	s.Equal(someErr, err)
	s.Nil(ms)
}

func (s *mutableStateMapperSuite) msMapperRebuildIn() GetOrRebuildMutableStateIn {
	task := s.msMapperNewTask(msMapperSingleBatch())
	return GetOrRebuildMutableStateIn{
		replicationTask: task,
		BranchIndex:     1,
	}
}

func (s *mutableStateMapperSuite) TestGetOrRebuildCurrentMutableState_Success() {
	in := s.msMapperRebuildIn()
	rebuiltMS := historyi.NewMockMutableState(s.controller)
	s.mockConflictResolver.EXPECT().
		GetOrRebuildCurrentMutableState(gomock.Any(), int32(1), gomock.Any()).Return(rebuiltMS, true, nil).Times(1)

	ms, isRebuilt, err := s.mutableStateMapper.GetOrRebuildCurrentMutableState(context.Background(), nil, nil, in)
	s.NoError(err)
	s.True(isRebuilt)
	s.Equal(rebuiltMS, ms)
}

func (s *mutableStateMapperSuite) TestGetOrRebuildCurrentMutableState_Error() {
	in := s.msMapperRebuildIn()
	someErr := errors.New("some random error")
	s.mockConflictResolver.EXPECT().
		GetOrRebuildCurrentMutableState(gomock.Any(), int32(1), gomock.Any()).Return(nil, false, someErr).Times(1)

	_, isRebuilt, err := s.mutableStateMapper.GetOrRebuildCurrentMutableState(context.Background(), nil, nil, in)
	s.Equal(someErr, err)
	s.False(isRebuilt)
}

func (s *mutableStateMapperSuite) TestGetOrRebuildMutableState_Success() {
	in := s.msMapperRebuildIn()
	rebuiltMS := historyi.NewMockMutableState(s.controller)
	s.mockConflictResolver.EXPECT().
		GetOrRebuildMutableState(gomock.Any(), int32(1)).Return(rebuiltMS, true, nil).Times(1)

	ms, isRebuilt, err := s.mutableStateMapper.GetOrRebuildMutableState(context.Background(), nil, nil, in)
	s.NoError(err)
	s.True(isRebuilt)
	s.Equal(rebuiltMS, ms)
}

func (s *mutableStateMapperSuite) TestGetOrRebuildMutableState_Error() {
	in := s.msMapperRebuildIn()
	someErr := errors.New("some random error")
	s.mockConflictResolver.EXPECT().
		GetOrRebuildMutableState(gomock.Any(), int32(1)).Return(nil, false, someErr).Times(1)

	_, isRebuilt, err := s.mutableStateMapper.GetOrRebuildMutableState(context.Background(), nil, nil, in)
	s.Equal(someErr, err)
	s.False(isRebuilt)
}

func (s *mutableStateMapperSuite) TestApplyEvents_Success() {
	task := s.msMapperNewTask(msMapperSingleBatch())
	newMS := historyi.NewMockMutableState(s.controller)
	s.mockRebuilder.EXPECT().
		ApplyEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(newMS, nil).Times(1)

	ms, gotNewMS, err := s.mutableStateMapper.ApplyEvents(context.Background(), nil, nil, task)
	s.NoError(err)
	s.Nil(ms)
	s.Equal(newMS, gotNewMS)
}

func (s *mutableStateMapperSuite) TestApplyEvents_Error() {
	task := s.msMapperNewTask(msMapperSingleBatch())
	someErr := errors.New("some random error")
	s.mockRebuilder.EXPECT().
		ApplyEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, someErr).Times(1)

	ms, gotNewMS, err := s.mutableStateMapper.ApplyEvents(context.Background(), nil, nil, task)
	s.Equal(someErr, err)
	s.Nil(ms)
	s.Nil(gotNewMS)
}

func (s *mutableStateMapperSuite) TestNewMutableStateMapping() {
	mapper := NewMutableStateMapping(nil, nil, nil, nil, nil)
	s.NotNil(mapper)
}
