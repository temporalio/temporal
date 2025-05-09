package resetworkflow

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

type apiSuite struct {
	suite.Suite
	*require.Assertions

	controller *gomock.Controller

	baseRunID    string
	currentRunID string
	namespaceID  namespace.ID
	workflowID   string

	// mockShardContext               *historyi.MockShardContext
	mockShardContext               *shard.ContextTest
	mockWorkflowConsistencyChecker *api.MockWorkflowConsistencyChecker
	// mockWorkflowLease              *historyi.MockWorkflowLease
	mockCurrentMutableState *historyi.MockMutableState
	mockNamespaceCache      *namespace.MockRegistry
	mockClusterMetadata     *cluster.MockMetadata
	mockExecutionManager    *persistence.MockExecutionManager
	mockResetMutableState   *historyi.MockMutableState
	// mockConfig                     *shard.MockConfig
	// mockExecutionInfo              *persistence.MockWorkflowExecutionInfo
	// mockExecutionState             *persistence.MockWorkflowExecutionState
	// mockWorkflowCache              *persistence.MockWorkflowCache
}

func TestAPISuite(t *testing.T) {
	s := new(apiSuite)
	suite.Run(t, s)
}

func (s *apiSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	randomEventID := int64(2208)
	s.baseRunID = uuid.NewString()
	s.currentRunID = uuid.NewString()
	s.namespaceID = tests.NamespaceID
	s.workflowID = uuid.NewString()

	// s.mockShardContext = historyi.NewMockShardContext(s.controller)
	s.mockShardContext = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	mockRegistry := s.mockShardContext.GetNamespaceRegistry().(*namespace.MockRegistry)
	mockRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockShardContext.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	// s.mockShardContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	// s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)
	// s.mockShardContext.EXPECT().GetExecutionManager().Return(s.mockExecutionManager).AnyTimes()
	// s.mockShardContext.EXPECT().GetEventsCache()

	s.mockWorkflowConsistencyChecker = api.NewMockWorkflowConsistencyChecker(s.controller)
	// s.mockWorkflowLease = ndc.NewMockWorkflow(s.controller)
	s.mockCurrentMutableState = historyi.NewMockMutableState(s.controller)
	s.mockNamespaceCache = namespace.NewMockRegistry(s.controller)
	s.mockClusterMetadata = cluster.NewMockMetadata(s.controller)
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)
	// s.mockExecutionInfo = persistence.NewMockWorkflowExecutionInfo(s.controller)
	// s.mockExecutionState = persistence.NewMockWorkflowExecutionState(s.controller)
	// s.mockWorkflowCache = persistence.NewMockWorkflowCache(s.controller)
	s.mockCurrentMutableState.EXPECT().GetNextEventID().Return(randomEventID).AnyTimes()
	s.mockCurrentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:  s.currentRunID,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}).AnyTimes()
	s.mockCurrentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionhistory.NewVersionHistories(&historyspb.VersionHistory{
			BranchToken: []byte{1, 2, 3},
			Items: []*historyspb.VersionHistoryItem{
				{EventId: 123, Version: 0},
			},
		}),
	}).AnyTimes()
	s.mockResetMutableState = historyi.NewMockMutableState(s.controller)

	s.mockWorkflowConsistencyChecker.EXPECT().GetWorkflowCache().Return(nil).AnyTimes()
	s.mockWorkflowConsistencyChecker.EXPECT().GetCurrentRunID(
		gomock.Any(),
		s.namespaceID.String(),
		s.workflowID,
		locks.PriorityHigh,
	).Return(s.currentRunID, nil)

	s.mockWorkflowConsistencyChecker.EXPECT().GetWorkflowLease(
		gomock.Any(),
		nil,
		definition.NewWorkflowKey(
			s.namespaceID.String(),
			s.workflowID,
			s.currentRunID,
		),
		locks.PriorityHigh,
	).Return(api.NewWorkflowLease(nil, func(err error) {}, s.mockCurrentMutableState), nil)

	// s.mockWorkflowConsistencyChecker.EXPECT().GetCurrentRunID(
	// 	gomock.Any(),
	// 	gomock.Any(),
	// 	gomock.Any(),
	// 	gomock.Any(),
	// ).Return(currentRunID, nil)
	// namespaceRegistry := namespace.NewMockRegistry(s.controller)
	// namespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	// s.mockShardContext.EXPECT().GetNamespaceRegistry().Return(namespaceRegistry).AnyTimes()
}

func (s *apiSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *apiSuite) TestInvoke_Success_SimpleReset() {
	ctx := context.Background()
	workflowTaskFinishEventID := int64(5)
	baseRunID := s.currentRunID
	requestID := uuid.NewString()

	resetRequest := &historyservice.ResetWorkflowExecutionRequest{
		NamespaceId: s.namespaceID.String(),
		ResetRequest: &workflowservice.ResetWorkflowExecutionRequest{
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: s.workflowID,
				RunId:      baseRunID,
			},
			WorkflowTaskFinishEventId: workflowTaskFinishEventID,
			RequestId:                 requestID,
			Reason:                    "test-reset-reason",
			ResetReapplyType:          enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
		},
	}

	s.mockCurrentMutableState.EXPECT().UpdateResetRunID(gomock.Any()).AnyTimes()
	s.mockCurrentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockCurrentMutableState.EXPECT().GetStartedWorkflowTask().Return(nil)
	s.mockCurrentMutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&historypb.HistoryEvent{}, nil)
	s.mockCurrentMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	currentMutation := &persistence.WorkflowMutation{}
	currentEventsSeq := []*persistence.WorkflowEvents{{}}
	s.mockCurrentMutableState.EXPECT().CloseTransactionAsMutation(historyi.TransactionPolicyActive).Return(currentMutation, currentEventsSeq, nil)
	s.mockShardContext.Resource.ExecutionMgr.EXPECT().ForkHistoryBranch(gomock.Any(), gomock.Any()).Return(
		&persistence.ForkHistoryBranchResponse{NewBranchToken: []byte("TEST reset branch token")}, nil,
	)
	s.mockShardContext.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	smReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
	s.NoError(err)
	s.mockCurrentMutableState.EXPECT().HSM().Return(root).AnyTimes()

	// baseEvent1 := &historypb.HistoryEvent{
	// 	EventId:    124,
	// 	EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
	// 	Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{}},
	// }
	s.mockShardContext.Resource.ExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchByBatchResponse{
		// History:       []*historypb.History{{Events: []*historypb.HistoryEvent{baseEvent1}}},
		History:       []*historypb.History{},
		NextPageToken: nil,
	}, nil)

	// mockNamespace := namespace.NewGlobalNamespaceForTest(
	// 	&namespace.Namespace{ID: namespaceID, Name: namespace.Name("test-namespace")},
	// 	&namespace.Config{Retention: namespace.DefaultRetention},
	// 	&namespace.ReplicationConfig{ActiveClusterName: "active"},
	// 	0,
	// )

	// s.mockShardContext.EXPECT().GetNamespaceRegistry().Return(s.mockNamespaceCache).AnyTimes()
	// s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(mockNamespace, nil).AnyTimes()
	// s.mockConfig.EXPECT().AllowResetWithPendingChildren(mockNamespace.Name().String()).Return(false).AnyTimes()
	// s.mockShardContext.EXPECT().GetLogger().Return(log.NewTestLogger()).AnyTimes()
	// s.mockShardContext.EXPECT().GetClusterMetadata().Return(s.mockClusterMetadata).AnyTimes()
	// s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("active").AnyTimes()

	// s.mockWorkflowLease.EXPECT().GetMutableState().Return(s.mockMutableState).AnyTimes()
	// s.mockWorkflowLease.EXPECT().GetReleaseFn().Return(func(error) {}).AnyTimes()
	// s.mockWorkflowLease.EXPECT().GetContext().Return(ctx).AnyTimes()

	// baseRebuildLastEventID := workflowTaskFinishEventID - 1
	// s.mockMutableState.EXPECT().GetNextEventID().Return(workflowTaskFinishEventID + 5).AnyTimes()
	// s.mockMutableState.EXPECT().GetExecutionInfo().Return(s.mockExecutionInfo).AnyTimes()

	// branchToken := []byte{1, 2, 3}
	// versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
	// 	versionhistory.NewVersionHistoryItem(1, 0),
	// 	versionhistory.NewVersionHistoryItem(baseRebuildLastEventID, 0),
	// 	versionhistory.NewVersionHistoryItem(workflowTaskFinishEventID, 0),
	// })
	// versionHistories := versionhistory.NewVersionHistories(versionHistory)
	// s.mockExecutionInfo.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()

	// s.mockMutableState.EXPECT().GetExecutionState().Return(s.mockExecutionState).AnyTimes()
	// s.mockExecutionState.EXPECT().GetCreateRequestId().Return("some-other-request-id").AnyTimes()

	// s.mockWorkflowConsistencyChecker.EXPECT().GetWorkflowCache().Return(s.mockWorkflowCache).AnyTimes()

	// s.mockWorkflowCache.EXPECT().ResetWorkflowExecution(
	// 	ctx,
	// 	namespaceID,
	// 	workflowID,
	// 	baseRunID,
	// 	branchToken,
	// 	baseRebuildLastEventID,
	// 	int64(0),
	// 	workflowTaskFinishEventID+5,
	// 	gomock.AssignableToTypeOf(""),
	// 	requestID,
	// 	gomock.Any(),
	// 	gomock.Any(),
	// 	gomock.Any(),
	// 	gomock.Any(),
	// 	gomock.Any(),
	// 	gomock.Any(),
	// 	map[int64]*persistence.ChildExecutionInfo{},
	// 	map[int64]*persistence.RequestCancelInfo{},
	// 	map[int64]*persistence.SignalInfo{},
	// 	map[string]struct{}{},
	// 	GetResetReapplyExcludeTypes(resetRequest.ResetRequest.GetResetReapplyExcludeTypes(), resetRequest.ResetRequest.GetResetReapplyType()),
	// 	resetRequest.ResetRequest.PostResetOperations,
	// 	false,
	// ).Return(nil)

	resp, err := Invoke(ctx, resetRequest, s.mockShardContext, s.mockWorkflowConsistencyChecker)

	s.NoError(err)
	s.NotNil(resp)
	s.NotEmpty(resp.RunId)
	s.NotEqual(s.currentRunID, resp.RunId, "New RunId should be different from base RunId")
}

type (
	resetWorkflowSuite struct {
		suite.Suite
	}
)

func TestResetWorkflowSuite(t *testing.T) {
	s := new(resetWorkflowSuite)
	suite.Run(t, s)
}

func (s *resetWorkflowSuite) TestGetResetReapplyExcludeTypes() {
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{},
			enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
		),
	)
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {}},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
		),
	)
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {}},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{},
			enumspb.RESET_REAPPLY_TYPE_SIGNAL,
		),
	)
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {},
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {},
		},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enumspb.RESET_REAPPLY_TYPE_SIGNAL,
		),
	)
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {},
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {},
		},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{},
			enumspb.RESET_REAPPLY_TYPE_NONE,
		),
	)
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {},
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {},
		},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enumspb.RESET_REAPPLY_TYPE_NONE,
		),
	)
}
