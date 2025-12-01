package api

import (
	"context"
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type (
	workflowConsistencyCheckerSuite struct {
		suite.Suite
		*require.Assertions

		controller    *gomock.Controller
		shardContext  *historyi.MockShardContext
		workflowCache *wcache.MockCache
		config        *configs.Config

		shardID      int32
		namespaceID  string
		workflowID   string
		currentRunID string

		checker *WorkflowConsistencyCheckerImpl
	}
)

func TestWorkflowConsistencyCheckerSuite(t *testing.T) {
	s := new(workflowConsistencyCheckerSuite)
	suite.Run(t, s)
}

func (s *workflowConsistencyCheckerSuite) SetupSuite() {
}

func (s *workflowConsistencyCheckerSuite) TearDownSuite() {
}

func (s *workflowConsistencyCheckerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.shardContext = historyi.NewMockShardContext(s.controller)
	s.workflowCache = wcache.NewMockCache(s.controller)
	s.config = tests.NewDynamicConfig()

	s.shardID = rand.Int31()
	s.namespaceID = uuid.New().String()
	s.workflowID = uuid.New().String()
	s.currentRunID = uuid.New().String()

	s.shardContext.EXPECT().GetShardID().Return(s.shardID).AnyTimes()
	s.shardContext.EXPECT().GetConfig().Return(s.config).AnyTimes()

	s.checker = NewWorkflowConsistencyChecker(s.shardContext, s.workflowCache)
}

func (s *workflowConsistencyCheckerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *workflowConsistencyCheckerSuite) TestGetWorkflowContextValidatedByCheck_Success_PassCheck() {
	ctx := context.Background()

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	released := false
	releaseFn := func(err error) { released = true }

	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		ctx,
		s.shardContext,
		namespace.ID(s.namespaceID),
		protomock.Eq(&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.currentRunID,
		}),
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	).Return(wfContext, releaseFn, nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.shardContext).Return(mutableState, nil)

	workflowLease, err := s.checker.GetWorkflowLease(
		ctx, nil,
		definition.NewWorkflowKey(s.namespaceID, s.workflowID, s.currentRunID),
		locks.PriorityHigh,
	)
	s.NoError(err)
	s.Equal(mutableState, workflowLease.GetMutableState())
	s.False(released)
}
func (s *workflowConsistencyCheckerSuite) TestGetCurrentRunID_Success() {
	ctx := context.Background()

	released := false
	releaseFn := func(err error) { released = true }

	s.workflowCache.EXPECT().GetOrCreateCurrentExecution(
		ctx,
		s.shardContext,
		namespace.ID(s.namespaceID),
		s.workflowID,
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	).Return(releaseFn, nil)
	s.shardContext.EXPECT().GetCurrentExecution(
		ctx,
		&persistence.GetCurrentExecutionRequest{
			ShardID:     s.shardContext.GetShardID(),
			NamespaceID: s.namespaceID,
			WorkflowID:  s.workflowID,
			ArchetypeID: chasm.WorkflowArchetypeID,
		},
	).Return(&persistence.GetCurrentExecutionResponse{RunID: s.currentRunID}, nil)

	runID, err := s.checker.GetCurrentWorkflowRunID(ctx, s.namespaceID, s.workflowID, locks.PriorityHigh)
	s.NoError(err)
	s.Equal(s.currentRunID, runID)
	s.True(released)
}

func (s *workflowConsistencyCheckerSuite) TestGetCurrentRunID_Error() {
	ctx := context.Background()

	released := false
	releaseFn := func(err error) { released = true }

	s.workflowCache.EXPECT().GetOrCreateCurrentExecution(
		ctx,
		s.shardContext,
		namespace.ID(s.namespaceID),
		s.workflowID,
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	).Return(releaseFn, nil)
	s.shardContext.EXPECT().GetCurrentExecution(
		ctx,
		&persistence.GetCurrentExecutionRequest{
			ShardID:     s.shardContext.GetShardID(),
			NamespaceID: s.namespaceID,
			WorkflowID:  s.workflowID,
			ArchetypeID: chasm.WorkflowArchetypeID,
		},
	).Return(nil, serviceerror.NewUnavailable(""))

	runID, err := s.checker.GetCurrentWorkflowRunID(ctx, s.namespaceID, s.workflowID, locks.PriorityHigh)
	s.IsType(&serviceerror.Unavailable{}, err)
	s.Empty(runID)
	s.True(released)
}

func (s *workflowConsistencyCheckerSuite) Test_clockConsistencyCheck() {
	err := s.checker.clockConsistencyCheck(nil)
	s.NoError(err)

	reqClock := &clockspb.VectorClock{
		ShardId:   1,
		Clock:     10,
		ClusterId: 1,
	}

	// not compatible - different shard id
	differentShardClock := &clockspb.VectorClock{
		ShardId:   2,
		Clock:     1,
		ClusterId: 1,
	}
	s.shardContext.EXPECT().CurrentVectorClock().Return(differentShardClock)
	err = s.checker.clockConsistencyCheck(reqClock)
	s.NoError(err)

	// not compatible - different cluster id
	differentClusterClock := &clockspb.VectorClock{
		ShardId:   1,
		Clock:     1,
		ClusterId: 2,
	}
	s.shardContext.EXPECT().CurrentVectorClock().Return(differentClusterClock)
	err = s.checker.clockConsistencyCheck(reqClock)
	s.NoError(err)

	// not compatible - shard context clock is missing
	s.shardContext.EXPECT().CurrentVectorClock().Return(nil)
	err = s.checker.clockConsistencyCheck(reqClock)
	s.NoError(err)

	// shard clock ahead
	shardClock := &clockspb.VectorClock{
		ShardId:   1,
		Clock:     20,
		ClusterId: 1,
	}
	s.shardContext.EXPECT().CurrentVectorClock().Return(shardClock)
	err = s.checker.clockConsistencyCheck(reqClock)
	s.NoError(err)

	// shard clock behind
	shardClock = &clockspb.VectorClock{
		ShardId:   1,
		Clock:     1,
		ClusterId: 1,
	}
	s.shardContext.EXPECT().CurrentVectorClock().Return(shardClock)
	s.shardContext.EXPECT().UnloadForOwnershipLost()
	err = s.checker.clockConsistencyCheck(reqClock)
	s.Error(err)
}
