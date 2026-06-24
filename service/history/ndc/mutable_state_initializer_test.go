package ndc

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type (
	msInitializerSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockShard          *shard.ContextTest
		mockWorkflowCache  *wcache.MockCache
		mockNamespaceCache *namespace.MockRegistry
		logger             log.Logger

		namespaceID string
		workflowID  string
		runID       string

		initializer *MutableStateInitializerImpl
	}
)

func TestMutableStateInitializerSuite(t *testing.T) {
	s := new(msInitializerSuite)
	suite.Run(t, s)
}

func (s *msInitializerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	reg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(reg))
	s.mockShard.SetStateMachineRegistry(reg)
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return("test-cluster").AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).Return("test-cluster").AnyTimes()

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockWorkflowCache = wcache.NewMockCache(s.controller)
	s.logger = s.mockShard.GetLogger()

	s.namespaceID = tests.NamespaceID.String()
	s.workflowID = "some random workflow ID"
	s.runID = uuid.NewString()

	s.initializer = NewMutableStateInitializer(
		s.mockShard,
		s.mockWorkflowCache,
		s.logger,
	)
}

func (s *msInitializerSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *msInitializerSuite) msInitWorkflowKey() definition.WorkflowKey {
	return definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	}
}

func (s *msInitializerSuite) TestNewMutableStateInitializer() {
	s.NotNil(s.initializer)
	s.NotNil(s.initializer.namespaceCache)
	s.Equal(s.mockWorkflowCache, s.initializer.workflowCache)
	s.Equal(s.mockShard, s.initializer.shardContext)
}

func (s *msInitializerSuite) TestInitialize_NamespaceLookupError() {
	workflowKey := s.msInitWorkflowKey()
	lookupErr := serviceerror.NewNotFound("namespace not found")
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(s.namespaceID)).Return(nil, lookupErr).Times(1)

	wf, spec, err := s.initializer.Initialize(context.Background(), workflowKey, nil)
	s.Equal(lookupErr, err)
	s.Nil(wf)
	s.Equal(MutableStateInitializationSpec{}, spec)
}

func (s *msInitializerSuite) TestInitialize_EmptyToken_RoutesToDB() {
	workflowKey := s.msInitWorkflowKey()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(s.namespaceID)).Return(tests.GlobalNamespaceEntry, nil).Times(1)

	// route to InitializeFromDB; fail fast at GetOrCreateWorkflowExecution
	cacheErr := errors.New("cache error")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: s.runID},
		locks.PriorityHigh,
	).Return(nil, nil, cacheErr).Times(1)

	wf, spec, err := s.initializer.Initialize(context.Background(), workflowKey, nil)
	s.Equal(cacheErr, err)
	s.Nil(wf)
	s.Equal(MutableStateInitializationSpec{}, spec)
}

func (s *msInitializerSuite) TestInitialize_NonEmptyToken_RoutesToToken() {
	workflowKey := s.msInitWorkflowKey()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(s.namespaceID)).Return(tests.GlobalNamespaceEntry, nil).Times(1)

	// route to InitializeFromToken; fail fast at deserialize with invalid token
	wf, spec, err := s.initializer.Initialize(context.Background(), workflowKey, []byte("not-json"))
	s.Error(err)
	s.Nil(wf)
	s.Equal(MutableStateInitializationSpec{}, spec)
}

func (s *msInitializerSuite) TestInitializeFromDB_GetOrCreateError() {
	workflowKey := s.msInitWorkflowKey()
	cacheErr := errors.New("cache error")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(s.namespaceID),
		gomock.Any(),
		locks.PriorityHigh,
	).Return(nil, nil, cacheErr).Times(1)

	wf, spec, err := s.initializer.InitializeFromDB(context.Background(), tests.GlobalNamespaceEntry, workflowKey)
	s.Equal(cacheErr, err)
	s.Nil(wf)
	s.Equal(MutableStateInitializationSpec{}, spec)
}

func (s *msInitializerSuite) TestInitializeFromDB_LoadSuccess_NoBufferedEvents() {
	workflowKey := s.msInitWorkflowKey()
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	released := false
	releaseFn := func(err error) { released = true }
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(s.namespaceID),
		gomock.Any(),
		locks.PriorityHigh,
	).Return(mockWeCtx, releaseFn, nil).Times(1)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil).Times(1)

	// flushBufferEvents path with no buffered events / no transient task -> returns same ms
	mockMutableState.EXPECT().HasBufferedEvents().Return(false).Times(1)
	mockMutableState.EXPECT().HasStartedWorkflowTask().Return(false).Times(1)
	mockMutableState.EXPECT().GetUpdateCondition().Return(int64(1), int64(42)).Times(1)
	mockMutableState.EXPECT().GetHistorySize().Return(int64(1024)).Times(1)

	wf, spec, err := s.initializer.InitializeFromDB(context.Background(), tests.GlobalNamespaceEntry, workflowKey)
	s.NoError(err)
	s.NotNil(wf)
	s.Equal(mockMutableState, wf.GetMutableState())
	s.True(spec.ExistsInDB)
	s.False(spec.IsBrandNew)
	s.Equal(int64(42), spec.DBRecordVersion)
	s.Equal(int64(1024), spec.DBHistorySize)
	s.False(released)
}

func (s *msInitializerSuite) TestInitializeFromDB_LoadSuccess_FlushError() {
	workflowKey := s.msInitWorkflowKey()
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	released := false
	releaseFn := func(err error) { released = true }
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(s.namespaceID),
		gomock.Any(),
		locks.PriorityHigh,
	).Return(mockWeCtx, releaseFn, nil).Times(1)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil).Times(1)

	// flush error path: clearing transient workflow task fails
	flushErr := errors.New("clear transient task failed")
	mockMutableState.EXPECT().HasBufferedEvents().Return(false).Times(1)
	mockMutableState.EXPECT().HasStartedWorkflowTask().Return(true).Times(1)
	mockMutableState.EXPECT().IsTransientWorkflowTask().Return(true).Times(1)
	mockMutableState.EXPECT().ClearTransientWorkflowTask().Return(flushErr).Times(1)

	wf, spec, err := s.initializer.InitializeFromDB(context.Background(), tests.GlobalNamespaceEntry, workflowKey)
	s.Equal(flushErr, err)
	s.Nil(wf)
	s.Equal(MutableStateInitializationSpec{}, spec)
	s.True(released)
}

func (s *msInitializerSuite) TestInitializeFromDB_NotFound_BrandNew() {
	workflowKey := s.msInitWorkflowKey()
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	releaseFn := func(err error) {}
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(s.namespaceID),
		gomock.Any(),
		locks.PriorityHigh,
	).Return(mockWeCtx, releaseFn, nil).Times(1)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(nil, serviceerror.NewNotFound("ms not found")).Times(1)

	wf, spec, err := s.initializer.InitializeFromDB(context.Background(), tests.GlobalNamespaceEntry, workflowKey)
	s.NoError(err)
	s.NotNil(wf)
	s.NotNil(wf.GetMutableState())
	s.False(spec.ExistsInDB)
	s.True(spec.IsBrandNew)
	s.Equal(int64(1), spec.DBRecordVersion)
	s.Equal(int64(0), spec.DBHistorySize)
}

func (s *msInitializerSuite) TestInitializeFromDB_LoadDefaultError() {
	workflowKey := s.msInitWorkflowKey()
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	released := false
	releaseFn := func(err error) { released = true }
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(s.namespaceID),
		gomock.Any(),
		locks.PriorityHigh,
	).Return(mockWeCtx, releaseFn, nil).Times(1)
	loadErr := serviceerror.NewInternal("load failed")
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(nil, loadErr).Times(1)

	wf, spec, err := s.initializer.InitializeFromDB(context.Background(), tests.GlobalNamespaceEntry, workflowKey)
	s.Equal(loadErr, err)
	s.Nil(wf)
	s.Equal(MutableStateInitializationSpec{}, spec)
	s.True(released)
}

func (s *msInitializerSuite) TestInitializeFromToken_DeserializeError() {
	workflowKey := s.msInitWorkflowKey()
	wf, spec, err := s.initializer.InitializeFromToken(
		context.Background(),
		tests.GlobalNamespaceEntry,
		workflowKey,
		[]byte("not-json"),
	)
	s.Error(err)
	s.Nil(wf)
	s.Equal(MutableStateInitializationSpec{}, spec)
}

func (s *msInitializerSuite) TestInitializeFromToken_Success() {
	workflowKey := s.msInitWorkflowKey()
	dbState := s.msInitBuildMutableState()
	token, err := s.initializer.serializeBackfillToken(dbState, int64(7), int64(2048), true)
	s.NoError(err)

	wf, spec, err := s.initializer.InitializeFromToken(
		context.Background(),
		tests.GlobalNamespaceEntry,
		workflowKey,
		token,
	)
	s.NoError(err)
	s.NotNil(wf)
	s.NotNil(wf.GetMutableState())
	s.True(spec.ExistsInDB)
	s.False(spec.IsBrandNew)
	s.Equal(int64(7), spec.DBRecordVersion)
	s.Equal(int64(2048), spec.DBHistorySize)
}

func (s *msInitializerSuite) TestSerializeDeserializeBackfillToken_RoundTrip() {
	dbState := s.msInitBuildMutableState()
	token, err := s.initializer.serializeBackfillToken(dbState, int64(11), int64(99), true)
	s.NoError(err)

	gotState, dbRecordVersion, dbHistorySize, existsInDB, err := s.initializer.deserializeBackfillToken(token)
	s.NoError(err)
	s.Equal(int64(11), dbRecordVersion)
	s.Equal(int64(99), dbHistorySize)
	s.True(existsInDB)
	s.Equal(dbState.GetExecutionState().GetRunId(), gotState.GetExecutionState().GetRunId())
	s.Equal(dbState.GetExecutionInfo().GetWorkflowId(), gotState.GetExecutionInfo().GetWorkflowId())
}

func (s *msInitializerSuite) TestDeserializeBackfillToken_InvalidJSON() {
	_, _, _, _, err := s.initializer.deserializeBackfillToken([]byte("not-json"))
	s.Error(err)
}

func (s *msInitializerSuite) TestDeserializeBackfillToken_InvalidProto() {
	// valid JSON token but MutableStateRow is not a valid proto message
	token, err := json.Marshal(MutableStateToken{
		MutableStateRow: []byte("not-a-valid-proto"),
		DBRecordVersion: int64(1),
		DBHistorySize:   int64(2),
		ExistsInDB:      false,
	})
	s.NoError(err)
	_, _, _, _, err = s.initializer.deserializeBackfillToken(token)
	s.Error(err)
}

func (s *msInitializerSuite) msInitBuildMutableState() *persistencespb.WorkflowMutableState {
	return &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			WorkflowId:  s.workflowID,
			NamespaceId: s.namespaceID,
			VersionHistories: &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: []byte("branch-token"),
						Items: []*historyspb.VersionHistoryItem{
							{EventId: int64(100), Version: int64(100)},
						},
					},
				},
			},
			CompletionEventBatchId: int64(5),
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  s.runID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		},
		NextEventId: int64(7),
	}
}
