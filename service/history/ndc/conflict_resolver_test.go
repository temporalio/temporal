package ndc

import (
	"context"
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/versionhistory"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	conflictResolverSuite struct {
		suite.Suite
		*require.Assertions

		controller       *gomock.Controller
		mockShard        *shard.ContextTest
		mockContext      *historyi.MockWorkflowContext
		mockMutableState *historyi.MockMutableState
		mockStateBuilder *MockStateRebuilder

		logger log.Logger

		namespaceID string
		namespace   string
		workflowID  string
		runID       string

		nDCConflictResolver *ConflictResolverImpl
	}
)

func TestNDCConflictResolverSuite(t *testing.T) {
	s := new(conflictResolverSuite)
	suite.Run(t, s)
}

func (s *conflictResolverSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockContext = historyi.NewMockWorkflowContext(s.controller)
	s.mockMutableState = historyi.NewMockMutableState(s.controller)
	s.mockStateBuilder = NewMockStateRebuilder(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	s.logger = s.mockShard.GetLogger()

	s.namespaceID = uuid.NewString()
	s.namespace = "some random namespace name"
	s.workflowID = "some random workflow ID"
	s.runID = uuid.NewString()

	s.nDCConflictResolver = NewConflictResolver(
		s.mockShard, s.mockContext, s.mockMutableState, s.logger,
	)
	s.nDCConflictResolver.stateRebuilder = s.mockStateBuilder
}

func (s *conflictResolverSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *conflictResolverSuite) TestRebuild() {
	ctx := context.Background()
	updateCondition := int64(59)
	dbVersion := int64(1444)
	requestID := uuid.NewString()
	version := int64(12)
	historySize := int64(12345)
	externalPayloadSize := int64(6789)
	externalPayloadCount := int64(42)

	branchToken0 := []byte("some random branch token")
	lastEventID0 := int64(5)
	versionHistory0 := versionhistory.NewVersionHistory(
		branchToken0,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID0, version)},
	)
	branchToken1 := []byte("other random branch token")
	lastEventID1 := int64(2)
	versionHistory1 := versionhistory.NewVersionHistory(
		branchToken1,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID1, version)},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory0)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, versionHistory1)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetUpdateCondition().Return(updateCondition, dbVersion).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
		RequestIds: map[string]*persistencespb.RequestIDInfo{
			requestID: {EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		},
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetHistorySize().Return(historySize).AnyTimes()
	s.mockMutableState.EXPECT().GetExternalPayloadSize().Return(externalPayloadSize).AnyTimes()
	s.mockMutableState.EXPECT().GetExternalPayloadCount().Return(externalPayloadCount).AnyTimes()

	workflowKey := definition.NewWorkflowKey(
		s.namespaceID,
		s.workflowID,
		s.runID,
	)
	mockRebuildMutableState := historyi.NewMockMutableState(s.controller)
	mockRebuildMutableState.EXPECT().GetExecutionInfo().Return(
		&persistencespb.WorkflowExecutionInfo{
			VersionHistories: versionhistory.NewVersionHistories(
				versionhistory.NewVersionHistory(
					branchToken1,
					[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID1, version)},
				),
			),
		},
	).AnyTimes()
	mockRebuildMutableState.EXPECT().AddHistorySize(historySize)
	mockRebuildMutableState.EXPECT().AddExternalPayloadSize(externalPayloadSize)
	mockRebuildMutableState.EXPECT().AddExternalPayloadCount(externalPayloadCount)
	mockRebuildMutableState.EXPECT().SetUpdateCondition(updateCondition, dbVersion)

	s.mockStateBuilder.EXPECT().Rebuild(
		ctx,
		gomock.Any(),
		workflowKey,
		branchToken1,
		lastEventID1,
		new(version),
		workflowKey,
		branchToken1,
		requestID,
	).Return(mockRebuildMutableState, RebuildStats{
		HistorySize:          rand.Int63(),
		ExternalPayloadSize:  rand.Int63(),
		ExternalPayloadCount: rand.Int63(),
	}, nil)

	s.mockContext.EXPECT().Clear()
	rebuiltMutableState, err := s.nDCConflictResolver.rebuild(ctx, 1)
	s.NoError(err)
	s.NotNil(rebuiltMutableState)
	s.Equal(int32(1), versionHistories.GetCurrentVersionHistoryIndex())
}

func (s *conflictResolverSuite) TestGetOrRebuildCurrentMutableState_NoRebuild_NotCurrent() {
	branchToken0 := []byte("some random branch token")
	lastEventID0 := int64(2)
	version0 := int64(12)
	versionHistoryItem0 := versionhistory.NewVersionHistoryItem(lastEventID0, version0)
	versionHistory0 := versionhistory.NewVersionHistory(
		branchToken0,
		[]*historyspb.VersionHistoryItem{versionHistoryItem0},
	)
	branchToken1 := []byte("another random branch token")
	lastEventID1 := lastEventID0 + 1
	version1 := version0 + 1
	versionHistoryItem1 := versionhistory.NewVersionHistoryItem(lastEventID1, version1)
	versionHistory1 := versionhistory.NewVersionHistory(
		branchToken1,
		[]*historyspb.VersionHistoryItem{versionHistoryItem0, versionHistoryItem1},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory0)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, versionHistory1)
	s.NoError(err)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	rebuiltMutableState, isRebuilt, err := s.nDCConflictResolver.GetOrRebuildCurrentMutableState(context.Background(), 0, version0)
	s.NoError(err)
	s.False(isRebuilt)
	s.Equal(s.mockMutableState, rebuiltMutableState)
}

func (s *conflictResolverSuite) TestGetOrRebuildCurrentMutableState_NoRebuild_SameIndex() {
	branchToken := []byte("some random branch token")
	lastEventID := int64(2)
	version := int64(12)
	versionHistoryItem := versionhistory.NewVersionHistoryItem(lastEventID, version)
	versionHistory := versionhistory.NewVersionHistory(
		branchToken,
		[]*historyspb.VersionHistoryItem{versionHistoryItem},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	rebuiltMutableState, isRebuilt, err := s.nDCConflictResolver.GetOrRebuildCurrentMutableState(context.Background(), 0, version)
	s.NoError(err)
	s.False(isRebuilt)
	s.Equal(s.mockMutableState, rebuiltMutableState)
}

func (s *conflictResolverSuite) TestGetOrRebuildCurrentMutableState_Rebuild() {
	ctx := context.Background()
	updateCondition := int64(59)
	dbVersion := int64(1444)
	version := int64(12)
	incomingVersion := version + 1
	historySize := int64(12345)

	// current branch
	branchToken0 := []byte("some random branch token")
	lastEventID0 := int64(2)

	versionHistoryItem0 := versionhistory.NewVersionHistoryItem(lastEventID0, version)
	versionHistory0 := versionhistory.NewVersionHistory(
		branchToken0,
		[]*historyspb.VersionHistoryItem{versionHistoryItem0},
	)

	// stale branch, used for Rebuild
	branchToken1 := []byte("other random branch token")
	lastEventID1 := lastEventID0 - 1
	versionHistoryItem1 := versionhistory.NewVersionHistoryItem(lastEventID1, version)
	versionHistory1 := versionhistory.NewVersionHistory(
		branchToken1,
		[]*historyspb.VersionHistoryItem{versionHistoryItem1},
	)

	versionHistories := versionhistory.NewVersionHistories(versionHistory0)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, versionHistory1)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetUpdateCondition().Return(updateCondition, dbVersion).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetHistorySize().Return(historySize).AnyTimes()
	externalPayloadSize := int64(6789)
	externalPayloadCount := int64(42)
	s.mockMutableState.EXPECT().GetExternalPayloadSize().Return(externalPayloadSize).AnyTimes()
	s.mockMutableState.EXPECT().GetExternalPayloadCount().Return(externalPayloadCount).AnyTimes()

	workflowKey := definition.NewWorkflowKey(
		s.namespaceID,
		s.workflowID,
		s.runID,
	)
	mockRebuildMutableState := historyi.NewMockMutableState(s.controller)
	mockRebuildMutableState.EXPECT().GetExecutionInfo().Return(
		&persistencespb.WorkflowExecutionInfo{
			VersionHistories: versionhistory.NewVersionHistories(
				versionhistory.NewVersionHistory(
					branchToken1,
					[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID1, version)},
				),
			),
		},
	).AnyTimes()
	mockRebuildMutableState.EXPECT().AddHistorySize(historySize)
	mockRebuildMutableState.EXPECT().AddExternalPayloadSize(externalPayloadSize)
	mockRebuildMutableState.EXPECT().AddExternalPayloadCount(externalPayloadCount)
	mockRebuildMutableState.EXPECT().SetUpdateCondition(updateCondition, dbVersion)

	s.mockStateBuilder.EXPECT().Rebuild(
		ctx,
		gomock.Any(),
		workflowKey,
		branchToken1,
		lastEventID1,
		new(version),
		workflowKey,
		branchToken1,
		gomock.Any(),
	).Return(mockRebuildMutableState, RebuildStats{
		HistorySize:          rand.Int63(),
		ExternalPayloadSize:  rand.Int63(),
		ExternalPayloadCount: rand.Int63(),
	}, nil)

	s.mockContext.EXPECT().Clear()
	rebuiltMutableState, isRebuilt, err := s.nDCConflictResolver.GetOrRebuildCurrentMutableState(ctx, 1, incomingVersion)
	s.NoError(err)
	s.NotNil(rebuiltMutableState)
	s.True(isRebuilt)
}

func (s *conflictResolverSuite) TestGetOrRebuildCurrentMutableState_SameVersionDifferentIndex_Error() {
	conflictResEmptyVersionHistory := versionhistory.NewVersionHistory(
		[]byte("conflictRes branch token"),
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(int64(2), int64(12))},
	)
	versionHistories := versionhistory.NewVersionHistories(conflictResEmptyVersionHistory)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	// incomingVersion == currentLastItem.GetVersion() but branchIndex != currentVersionHistoryIndex
	_, _, err := s.nDCConflictResolver.GetOrRebuildCurrentMutableState(context.Background(), 1, int64(12))
	s.Error(err)
	_, ok := err.(*serviceerror.InvalidArgument)
	s.True(ok)
}

func (s *conflictResolverSuite) TestGetOrRebuildCurrentMutableState_GetVersionHistoryError() {
	// empty version histories -> GetVersionHistory returns error
	versionHistories := &historyspb.VersionHistories{}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	_, _, err := s.nDCConflictResolver.GetOrRebuildCurrentMutableState(context.Background(), 0, int64(12))
	s.Error(err)
}

func (s *conflictResolverSuite) TestGetOrRebuildCurrentMutableState_GetLastItemError() {
	// version history with no items -> GetLastVersionHistoryItem returns error
	conflictResEmptyVH := versionhistory.NewVersionHistory([]byte("conflictRes token"), nil)
	versionHistories := versionhistory.NewVersionHistories(conflictResEmptyVH)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	_, _, err := s.nDCConflictResolver.GetOrRebuildCurrentMutableState(context.Background(), 0, int64(12))
	s.Error(err)
}

func (s *conflictResolverSuite) TestRebuild_GetVersionHistoryError() {
	// requesting a branchIndex out of range from rebuild's GetVersionHistory
	conflictResVH := versionhistory.NewVersionHistory(
		[]byte("conflictRes branch token"),
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(int64(2), int64(12))},
	)
	versionHistories := versionhistory.NewVersionHistories(conflictResVH)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	_, err := s.nDCConflictResolver.rebuild(context.Background(), 99)
	s.Error(err)
}

func (s *conflictResolverSuite) TestRebuild_GetLastItemError() {
	// branchIndex 1 points to a version history without items
	conflictResVH0 := versionhistory.NewVersionHistory(
		[]byte("conflictRes branch token 0"),
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(int64(2), int64(12))},
	)
	versionHistories := versionhistory.NewVersionHistories(conflictResVH0)
	conflictResVH1 := versionhistory.NewVersionHistory([]byte("conflictRes branch token 1"), nil)
	versionHistories.Histories = append(versionHistories.Histories, conflictResVH1)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	_, err := s.nDCConflictResolver.rebuild(context.Background(), 1)
	s.Error(err)
}

func (s *conflictResolverSuite) TestRebuild_StateRebuilderError() {
	ctx := context.Background()
	version := int64(12)
	conflictResVH0 := versionhistory.NewVersionHistory(
		[]byte("conflictRes branch token 0"),
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(int64(5), version)},
	)
	conflictResVH1 := versionhistory.NewVersionHistory(
		[]byte("conflictRes branch token 1"),
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(int64(2), version)},
	)
	versionHistories := versionhistory.NewVersionHistories(conflictResVH0)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, conflictResVH1)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: s.runID}).AnyTimes()
	s.mockMutableState.EXPECT().GetHistorySize().Return(int64(1)).AnyTimes()
	s.mockMutableState.EXPECT().GetExternalPayloadSize().Return(int64(0)).AnyTimes()
	s.mockMutableState.EXPECT().GetExternalPayloadCount().Return(int64(0)).AnyTimes()

	s.mockStateBuilder.EXPECT().Rebuild(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil, RebuildStats{}, serviceerror.NewInternal("rebuild failed"))

	_, err = s.nDCConflictResolver.rebuild(ctx, 1)
	s.Error(err)
}

func (s *conflictResolverSuite) TestRebuild_VersionHistoryMismatch() {
	ctx := context.Background()
	version := int64(12)
	conflictResVH0 := versionhistory.NewVersionHistory(
		[]byte("conflictRes branch token 0"),
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(int64(5), version)},
	)
	conflictResVH1 := versionhistory.NewVersionHistory(
		[]byte("conflictRes branch token 1"),
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(int64(2), version)},
	)
	versionHistories := versionhistory.NewVersionHistories(conflictResVH0)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, conflictResVH1)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: s.runID}).AnyTimes()
	s.mockMutableState.EXPECT().GetHistorySize().Return(int64(1)).AnyTimes()
	s.mockMutableState.EXPECT().GetExternalPayloadSize().Return(int64(0)).AnyTimes()
	s.mockMutableState.EXPECT().GetExternalPayloadCount().Return(int64(0)).AnyTimes()

	// rebuilt version history does NOT match the replay version history -> Internal error
	mockRebuildMutableState := historyi.NewMockMutableState(s.controller)
	mockRebuildMutableState.EXPECT().GetExecutionInfo().Return(
		&persistencespb.WorkflowExecutionInfo{
			VersionHistories: versionhistory.NewVersionHistories(
				versionhistory.NewVersionHistory(
					[]byte("mismatched branch token"),
					[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(int64(99), version)},
				),
			),
		},
	).AnyTimes()

	s.mockStateBuilder.EXPECT().Rebuild(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(mockRebuildMutableState, RebuildStats{}, nil)

	_, err = s.nDCConflictResolver.rebuild(ctx, 1)
	s.Error(err)
	_, ok := err.(*serviceerror.Internal)
	s.True(ok)
}

func (s *conflictResolverSuite) TestGetOrRebuildMutableState_RebuildError() {
	// drive rebuild() error through getOrRebuildMutableStateByIndex via GetOrRebuildMutableState
	conflictResVH0 := versionhistory.NewVersionHistory(
		[]byte("conflictRes branch token 0"),
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(int64(5), int64(12))},
	)
	conflictResVH1 := versionhistory.NewVersionHistory(
		[]byte("conflictRes branch token 1"),
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(int64(2), int64(12))},
	)
	versionHistories := versionhistory.NewVersionHistories(conflictResVH0)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, conflictResVH1)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: s.runID}).AnyTimes()
	s.mockMutableState.EXPECT().GetHistorySize().Return(int64(1)).AnyTimes()
	s.mockMutableState.EXPECT().GetExternalPayloadSize().Return(int64(0)).AnyTimes()
	s.mockMutableState.EXPECT().GetExternalPayloadCount().Return(int64(0)).AnyTimes()

	s.mockStateBuilder.EXPECT().Rebuild(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil, RebuildStats{}, serviceerror.NewInternal("rebuild failed"))

	// pick the non-current branch index so getOrRebuildMutableStateByIndex calls rebuild
	currentIdx := versionHistories.GetCurrentVersionHistoryIndex()
	nonCurrentIdx := int32(0)
	if currentIdx == 0 {
		nonCurrentIdx = 1
	}
	_, _, err = s.nDCConflictResolver.GetOrRebuildMutableState(context.Background(), nonCurrentIdx)
	s.Error(err)
}

func (s *conflictResolverSuite) TestRebuild_RebuiltVersionHistoryEmptyError() {
	ctx := context.Background()
	version := int64(12)
	conflictResVH0 := versionhistory.NewVersionHistory(
		[]byte("conflictRes branch token 0"),
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(int64(5), version)},
	)
	conflictResVH1 := versionhistory.NewVersionHistory(
		[]byte("conflictRes branch token 1"),
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(int64(2), version)},
	)
	versionHistories := versionhistory.NewVersionHistories(conflictResVH0)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, conflictResVH1)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: s.runID}).AnyTimes()
	s.mockMutableState.EXPECT().GetHistorySize().Return(int64(1)).AnyTimes()
	s.mockMutableState.EXPECT().GetExternalPayloadSize().Return(int64(0)).AnyTimes()
	s.mockMutableState.EXPECT().GetExternalPayloadCount().Return(int64(0)).AnyTimes()

	// rebuilt MS returns empty version histories -> GetCurrentVersionHistory errors
	mockRebuildMutableState := historyi.NewMockMutableState(s.controller)
	mockRebuildMutableState.EXPECT().GetExecutionInfo().Return(
		&persistencespb.WorkflowExecutionInfo{VersionHistories: &historyspb.VersionHistories{}},
	).AnyTimes()

	s.mockStateBuilder.EXPECT().Rebuild(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(mockRebuildMutableState, RebuildStats{}, nil)

	_, err = s.nDCConflictResolver.rebuild(ctx, 1)
	s.Error(err)
}

func (s *conflictResolverSuite) TestGetOrRebuildMutableState_NoRebuild_SameIndex() {
	branchToken := []byte("some random branch token")
	lastEventID := int64(2)
	version := int64(12)
	versionHistoryItem := versionhistory.NewVersionHistoryItem(lastEventID, version)
	versionHistory := versionhistory.NewVersionHistory(
		branchToken,
		[]*historyspb.VersionHistoryItem{versionHistoryItem},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	rebuiltMutableState, isRebuilt, err := s.nDCConflictResolver.GetOrRebuildMutableState(context.Background(), 0)
	s.NoError(err)
	s.False(isRebuilt)
	s.Equal(s.mockMutableState, rebuiltMutableState)
}

func (s *conflictResolverSuite) TestGetOrRebuildMutableState_Rebuild() {
	ctx := context.Background()
	updateCondition := int64(59)
	dbVersion := int64(1444)
	version := int64(12)
	historySize := int64(12345)
	externalPayloadSize := int64(6789)
	externalPayloadCount := int64(42)

	// current branch
	branchToken0 := []byte("some random branch token")
	lastEventID0 := int64(2)

	versionHistoryItem0 := versionhistory.NewVersionHistoryItem(lastEventID0, version)
	versionHistory0 := versionhistory.NewVersionHistory(
		branchToken0,
		[]*historyspb.VersionHistoryItem{versionHistoryItem0},
	)

	// stale branch, used for Rebuild
	branchToken1 := []byte("other random branch token")
	lastEventID1 := lastEventID0 - 1
	versionHistoryItem1 := versionhistory.NewVersionHistoryItem(lastEventID1, version)
	versionHistory1 := versionhistory.NewVersionHistory(
		branchToken1,
		[]*historyspb.VersionHistoryItem{versionHistoryItem1},
	)

	versionHistories := versionhistory.NewVersionHistories(versionHistory0)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, versionHistory1)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetUpdateCondition().Return(updateCondition, dbVersion).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetHistorySize().Return(historySize).AnyTimes()
	s.mockMutableState.EXPECT().GetExternalPayloadSize().Return(externalPayloadSize).AnyTimes()
	s.mockMutableState.EXPECT().GetExternalPayloadCount().Return(externalPayloadCount).AnyTimes()

	workflowKey := definition.NewWorkflowKey(
		s.namespaceID,
		s.workflowID,
		s.runID,
	)
	mockRebuildMutableState := historyi.NewMockMutableState(s.controller)
	mockRebuildMutableState.EXPECT().GetExecutionInfo().Return(
		&persistencespb.WorkflowExecutionInfo{
			VersionHistories: versionhistory.NewVersionHistories(
				versionhistory.NewVersionHistory(
					branchToken1,
					[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID1, version)},
				),
			),
		},
	).AnyTimes()
	mockRebuildMutableState.EXPECT().AddHistorySize(historySize)
	mockRebuildMutableState.EXPECT().AddExternalPayloadSize(externalPayloadSize)
	mockRebuildMutableState.EXPECT().AddExternalPayloadCount(externalPayloadCount)
	mockRebuildMutableState.EXPECT().SetUpdateCondition(updateCondition, dbVersion)

	s.mockStateBuilder.EXPECT().Rebuild(
		ctx,
		gomock.Any(),
		workflowKey,
		branchToken1,
		lastEventID1,
		new(version),
		workflowKey,
		branchToken1,
		gomock.Any(),
	).Return(mockRebuildMutableState, RebuildStats{
		HistorySize:          rand.Int63(),
		ExternalPayloadSize:  rand.Int63(),
		ExternalPayloadCount: rand.Int63(),
	}, nil)

	s.mockContext.EXPECT().Clear()
	rebuiltMutableState, isRebuilt, err := s.nDCConflictResolver.GetOrRebuildMutableState(ctx, 1)
	s.NoError(err)
	s.NotNil(rebuiltMutableState)
	s.True(isRebuilt)
}
