package ndc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/testhooks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

type (
	resetterSuite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		mockShard               *shard.ContextTest
		mockBaseMutableState    *historyi.MockMutableState
		mockRebuiltMutableState *historyi.MockMutableState
		mockTransactionMgr      *MockTransactionManager
		mockStateBuilder        *MockStateRebuilder

		logger          log.Logger
		mockExecManager *persistence.MockExecutionManager

		namespaceID namespace.ID
		namespace   namespace.Name
		workflowID  string
		baseRunID   string
		newContext  historyi.WorkflowContext
		newRunID    string

		workflowResetter *resetterImpl
	}
)

func TestResetterSuite(t *testing.T) {
	s := new(resetterSuite)
	suite.Run(t, s)
}

func (s *resetterSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockBaseMutableState = historyi.NewMockMutableState(s.controller)
	s.mockRebuiltMutableState = historyi.NewMockMutableState(s.controller)
	s.mockTransactionMgr = NewMockTransactionManager(s.controller)
	s.mockStateBuilder = NewMockStateRebuilder(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	s.mockExecManager = s.mockShard.Resource.ExecutionMgr

	s.logger = s.mockShard.GetLogger()

	s.namespaceID = namespace.ID(uuid.NewString())
	s.namespace = "some random namespace name"
	s.workflowID = "some random workflow ID"
	s.baseRunID = uuid.NewString()
	s.newContext = workflow.NewContext(
		s.mockShard.GetConfig(),
		definition.NewWorkflowKey(
			s.namespaceID.String(),
			s.workflowID,
			s.newRunID,
		),
		chasm.WorkflowArchetypeID,
		s.logger,
		s.mockShard.GetThrottledLogger(),
		s.mockShard.GetMetricsHandler(),
		nil,
		testhooks.TestHooks{},
	)
	s.newRunID = uuid.NewString()

	s.workflowResetter = NewResetter(
		s.mockShard, s.mockTransactionMgr, s.namespaceID, s.workflowID, s.baseRunID, s.newContext, s.newRunID, s.logger,
	)
	s.workflowResetter.stateRebuilder = s.mockStateBuilder
}

func (s *resetterSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *resetterSuite) TestResetWorkflow_HoldsBaseLeaseDuringFork() {
	for _, forkFails := range []bool{false, true} {
		name := "fork succeeds"
		if forkFails {
			name = "fork fails"
		}
		s.Run(name, func() {
			r := require.New(s.T())
			ctx := s.T().Context()
			baseContext := workflow.NewContext(
				s.mockShard.GetConfig(), definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, s.baseRunID),
				chasm.WorkflowArchetypeID, s.logger, s.mockShard.GetThrottledLogger(), s.mockShard.GetMetricsHandler(), nil, testhooks.TestHooks{},
			)
			r.NoError(baseContext.Lock(ctx, locks.PriorityLow))
			var releaseCount int
			var releaseErr error
			release := func(err error) {
				releaseCount++
				releaseErr = err
				baseContext.Unlock()
			}
			baseState := historyi.NewMockMutableState(s.controller)
			baseState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				VersionHistories: versionhistory.NewVersionHistories(versionhistory.NewVersionHistory(
					[]byte("base"), []*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(100, 1)},
				)),
			})
			baseWorkflow := NewMockWorkflow(s.controller)
			baseWorkflow.EXPECT().GetMutableState().Return(baseState)
			baseWorkflow.EXPECT().GetReleaseFn().Return(release)
			s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, s.namespaceID, s.workflowID, s.baseRunID, chasm.WorkflowArchetypeID).Return(baseWorkflow, nil)
			forkErr := errors.New("fork failed")
			rebuildErr := errors.New("stop after fork")
			s.mockExecManager.EXPECT().ForkHistoryBranch(gomock.Any(), gomock.Any()).DoAndReturn(
				func(context.Context, *persistence.ForkHistoryBranchRequest) (*persistence.ForkHistoryBranchResponse, error) {
					// A competing history cleanup uses the same workflow lock.
					waitCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
					defer cancel()
					lockErr := baseContext.Lock(waitCtx, locks.PriorityLow)
					if lockErr == nil {
						baseContext.Unlock()
					}
					r.ErrorIs(lockErr, context.DeadlineExceeded)
					if forkFails {
						return nil, forkErr
					}
					return &persistence.ForkHistoryBranchResponse{NewBranchToken: []byte("reset")}, nil
				})
			if !forkFails {
				s.mockStateBuilder.EXPECT().Rebuild(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
					func(context.Context, time.Time, definition.WorkflowKey, []byte, int64, *int64, definition.WorkflowKey, []byte, string) (historyi.MutableState, RebuildStats, error) {
						r.Equal(1, releaseCount)
						return nil, RebuildStats{}, rebuildErr
					})
			}
			_, err := s.workflowResetter.resetWorkflow(ctx, time.Now(), 90, 1, 91, 1)
			r.Equal(1, releaseCount)
			if forkFails {
				r.ErrorIs(err, forkErr)
				r.ErrorIs(releaseErr, forkErr)
			} else {
				r.ErrorIs(err, rebuildErr)
				r.NoError(releaseErr)
			}
			// The lease is available again after either fork outcome.
			r.NoError(baseContext.Lock(ctx, locks.PriorityLow))
			baseContext.Unlock()
		})
	}
}

func (s *resetterSuite) TestResetWorkflow_NoError() {
	ctx := context.Background()
	now := time.Now().UTC()

	branchToken := []byte("some random branch token")
	lastEventID := int64(500)
	version := int64(123)
	versionHistory := versionhistory.NewVersionHistory(
		branchToken,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID, version)},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	baseEventID := lastEventID - 100
	baseVersion := version
	incomingFirstEventID := baseEventID + 12
	incomingVersion := baseVersion + 3

	rebuildStats := RebuildStats{
		HistorySize:          9999,
		ExternalPayloadSize:  1234,
		ExternalPayloadCount: 56,
	}
	newBranchToken := []byte("other random branch token")
	resurrectedBranchToken := []byte("resurrected branch token")

	s.mockBaseMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	mockBaseWorkflowReleaseFnCalled := false
	mockBaseWorkflowReleaseFn := func(err error) {
		mockBaseWorkflowReleaseFnCalled = true
	}
	mockBaseWorkflow := NewMockWorkflow(s.controller)
	mockBaseWorkflow.EXPECT().GetMutableState().Return(s.mockBaseMutableState).AnyTimes()
	mockBaseWorkflow.EXPECT().GetReleaseFn().Return(mockBaseWorkflowReleaseFn)

	s.mockTransactionMgr.EXPECT().LoadWorkflow(
		ctx,
		s.namespaceID,
		s.workflowID,
		s.baseRunID,
		chasm.WorkflowArchetypeID,
	).Return(mockBaseWorkflow, nil)

	s.mockStateBuilder.EXPECT().Rebuild(
		ctx,
		now,
		definition.NewWorkflowKey(
			s.namespaceID.String(),
			s.workflowID,
			s.baseRunID,
		),
		resurrectedBranchToken,
		baseEventID,
		new(baseVersion),
		definition.NewWorkflowKey(
			s.namespaceID.String(),
			s.workflowID,
			s.newRunID,
		),
		newBranchToken,
		gomock.Any(),
	).Return(s.mockRebuiltMutableState, rebuildStats, nil)
	s.mockRebuiltMutableState.EXPECT().AddHistorySize(rebuildStats.HistorySize)
	s.mockRebuiltMutableState.EXPECT().AddExternalPayloadSize(rebuildStats.ExternalPayloadSize)
	s.mockRebuiltMutableState.EXPECT().AddExternalPayloadCount(rebuildStats.ExternalPayloadCount)

	shardID := s.mockShard.GetShardID()
	s.mockExecManager.EXPECT().ForkHistoryBranch(gomock.Any(), &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: branchToken,
		ForkNodeID:      baseEventID + 1,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(s.namespaceID.String(), s.workflowID, s.newRunID),
		ShardID:         shardID,
		NamespaceID:     s.namespaceID.String(),
		NewRunID:        s.newRunID,
	}).Return(&persistence.ForkHistoryBranchResponse{NewBranchToken: newBranchToken, BaseBranchToken: resurrectedBranchToken}, nil)

	s.mockRebuiltMutableState.EXPECT().RefreshExpirationTimeoutTask(gomock.Any()).Return(nil)

	rebuiltMutableState, err := s.workflowResetter.resetWorkflow(
		ctx,
		now,
		baseEventID,
		baseVersion,
		incomingFirstEventID,
		incomingVersion,
	)
	s.NoError(err)
	s.Equal(s.mockRebuiltMutableState, rebuiltMutableState)
	s.True(mockBaseWorkflowReleaseFnCalled)
}

func (s *resetterSuite) TestResetWorkflow_NoError_BaseBranchToken_Nil() {
	ctx := context.Background()
	now := time.Now().UTC()

	branchToken := []byte("some random branch token")
	lastEventID := int64(500)
	version := int64(123)
	versionHistory := versionhistory.NewVersionHistory(
		branchToken,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID, version)},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	baseEventID := lastEventID - 100
	baseVersion := version
	incomingFirstEventID := baseEventID + 12
	incomingVersion := baseVersion + 3

	rebuildStats := RebuildStats{
		HistorySize:          9999,
		ExternalPayloadSize:  1234,
		ExternalPayloadCount: 56,
	}
	newBranchToken := []byte("other random branch token")

	s.mockBaseMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	mockBaseWorkflowReleaseFnCalled := false
	mockBaseWorkflowReleaseFn := func(err error) {
		mockBaseWorkflowReleaseFnCalled = true
	}
	mockBaseWorkflow := NewMockWorkflow(s.controller)
	mockBaseWorkflow.EXPECT().GetMutableState().Return(s.mockBaseMutableState).AnyTimes()
	mockBaseWorkflow.EXPECT().GetReleaseFn().Return(mockBaseWorkflowReleaseFn)

	s.mockTransactionMgr.EXPECT().LoadWorkflow(
		ctx,
		s.namespaceID,
		s.workflowID,
		s.baseRunID,
		chasm.WorkflowArchetypeID,
	).Return(mockBaseWorkflow, nil)

	s.mockStateBuilder.EXPECT().Rebuild(
		ctx,
		now,
		definition.NewWorkflowKey(
			s.namespaceID.String(),
			s.workflowID,
			s.baseRunID,
		),
		branchToken,
		baseEventID,
		new(baseVersion),
		definition.NewWorkflowKey(
			s.namespaceID.String(),
			s.workflowID,
			s.newRunID,
		),
		newBranchToken,
		gomock.Any(),
	).Return(s.mockRebuiltMutableState, rebuildStats, nil)
	s.mockRebuiltMutableState.EXPECT().AddHistorySize(rebuildStats.HistorySize)
	s.mockRebuiltMutableState.EXPECT().AddExternalPayloadSize(rebuildStats.ExternalPayloadSize)
	s.mockRebuiltMutableState.EXPECT().AddExternalPayloadCount(rebuildStats.ExternalPayloadCount)

	shardID := s.mockShard.GetShardID()
	s.mockExecManager.EXPECT().ForkHistoryBranch(gomock.Any(), &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: branchToken,
		ForkNodeID:      baseEventID + 1,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(s.namespaceID.String(), s.workflowID, s.newRunID),
		ShardID:         shardID,
		NamespaceID:     s.namespaceID.String(),
		NewRunID:        s.newRunID,
	}).Return(&persistence.ForkHistoryBranchResponse{NewBranchToken: newBranchToken, BaseBranchToken: nil}, nil)

	s.mockRebuiltMutableState.EXPECT().RefreshExpirationTimeoutTask(gomock.Any()).Return(nil)

	rebuiltMutableState, err := s.workflowResetter.resetWorkflow(
		ctx,
		now,
		baseEventID,
		baseVersion,
		incomingFirstEventID,
		incomingVersion,
	)
	s.NoError(err)
	s.Equal(s.mockRebuiltMutableState, rebuiltMutableState)
	s.True(mockBaseWorkflowReleaseFnCalled)
}

func (s *resetterSuite) TestResetWorkflow_Error() {
	ctx := context.Background()
	now := time.Now().UTC()

	branchToken := []byte("some random branch token")
	lastEventID := int64(500)
	version := int64(123)
	versionHistory := versionhistory.NewVersionHistory(
		branchToken,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID, version)},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	baseEventID := lastEventID + 100
	baseVersion := version
	incomingFirstEventID := baseEventID + 12
	incomingFirstEventVersion := baseVersion + 3

	s.mockBaseMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	mockBaseWorkflowReleaseFn := func(err error) {
	}
	mockBaseWorkflow := NewMockWorkflow(s.controller)
	mockBaseWorkflow.EXPECT().GetMutableState().Return(s.mockBaseMutableState).AnyTimes()
	mockBaseWorkflow.EXPECT().GetReleaseFn().Return(mockBaseWorkflowReleaseFn)

	s.mockTransactionMgr.EXPECT().LoadWorkflow(
		ctx,
		s.namespaceID,
		s.workflowID,
		s.baseRunID,
		chasm.WorkflowArchetypeID,
	).Return(mockBaseWorkflow, nil)

	rebuiltMutableState, err := s.workflowResetter.resetWorkflow(
		ctx,
		now,
		baseEventID,
		baseVersion,
		incomingFirstEventID,
		incomingFirstEventVersion,
	)
	s.Error(err)
	s.ErrorAs(err, new(*serviceerrors.RetryReplication))
	s.Nil(rebuiltMutableState)

	retryErr, isRetryError := err.(*serviceerrors.RetryReplication)
	s.True(isRetryError)
	expectedErr := serviceerrors.NewRetryReplication(
		resendOnResetWorkflowMessage,
		s.namespaceID.String(),
		s.workflowID,
		s.newRunID,
		common.EmptyEventID,
		common.EmptyVersion,
		incomingFirstEventID,
		incomingFirstEventVersion,
	)
	s.Equal(expectedErr, retryErr)
}
