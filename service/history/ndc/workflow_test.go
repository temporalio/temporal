package ndc

import (
	"reflect"
	"runtime"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type (
	workflowSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockContext         *historyi.MockWorkflowContext
		mockMutableState    *historyi.MockMutableState
		mockClusterMetadata *cluster.MockMetadata

		namespaceID string
		workflowID  string
		runID       string
	}
)

func TestWorkflowSuite(t *testing.T) {
	s := new(workflowSuite)
	suite.Run(t, s)
}

func (s *workflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockContext = historyi.NewMockWorkflowContext(s.controller)
	s.mockMutableState = historyi.NewMockMutableState(s.controller)
	s.mockClusterMetadata = cluster.NewMockMetadata(s.controller)
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.namespaceID = uuid.NewString()
	s.workflowID = "some random workflow ID"
	s.runID = uuid.NewString()
}

func (s *workflowSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *workflowSuite) TestGetMethods() {
	lastRunningClock := int64(144)
	lastEventVersion := int64(12)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastEventVersion, nil).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		LastRunningClock: lastRunningClock,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	nDCWorkflow := NewWorkflow(
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		wcache.NoopReleaseFn,
	)

	s.Equal(s.mockContext, nDCWorkflow.GetContext())
	s.Equal(s.mockMutableState, nDCWorkflow.GetMutableState())
	// NOTE golang does not seem to let people compare functions, easily
	//  link: https://github.com/stretchr/testify/issues/182
	// this is a hack to compare 2 functions, being the same
	expectedReleaseFn := runtime.FuncForPC(reflect.ValueOf(wcache.NoopReleaseFn).Pointer()).Name()
	actualReleaseFn := runtime.FuncForPC(reflect.ValueOf(nDCWorkflow.GetReleaseFn()).Pointer()).Name()
	s.Equal(expectedReleaseFn, actualReleaseFn)
	version, clock, err := nDCWorkflow.GetVectorClock()
	s.NoError(err)
	s.Equal(lastEventVersion, version)
	s.Equal(lastRunningClock, clock)
}

func (s *workflowSuite) TestHappensAfter_LargerVersion() {
	thisLastWriteVersion := int64(0)
	thisLastRunningClock := int64(100)
	thatLastWriteVersion := thisLastWriteVersion - 1
	thatLastRunningClock := int64(123)

	s.True(WorkflowHappensAfter(
		thisLastWriteVersion,
		thisLastRunningClock,
		thatLastWriteVersion,
		thatLastRunningClock,
	))
}

func (s *workflowSuite) TestHappensAfter_SmallerVersion() {
	thisLastWriteVersion := int64(0)
	thisLastRunningClock := int64(100)
	thatLastWriteVersion := thisLastWriteVersion + 1
	thatLastRunningClock := int64(23)

	s.False(WorkflowHappensAfter(
		thisLastWriteVersion,
		thisLastRunningClock,
		thatLastWriteVersion,
		thatLastRunningClock,
	))
}

func (s *workflowSuite) TestHappensAfter_SameVersion_SmallerTaskID() {
	thisLastWriteVersion := int64(0)
	thisLastRunningClock := int64(100)
	thatLastWriteVersion := thisLastWriteVersion
	thatLastRunningClock := thisLastRunningClock + 1

	s.False(WorkflowHappensAfter(
		thisLastWriteVersion,
		thisLastRunningClock,
		thatLastWriteVersion,
		thatLastRunningClock,
	))
}

func (s *workflowSuite) TestHappensAfter_SameVersion_LatrgerTaskID() {
	thisLastWriteVersion := int64(0)
	thisLastRunningClock := int64(100)
	thatLastWriteVersion := thisLastWriteVersion
	thatLastRunningClock := thisLastRunningClock - 1

	s.True(WorkflowHappensAfter(
		thisLastWriteVersion,
		thisLastRunningClock,
		thatLastWriteVersion,
		thatLastRunningClock,
	))
}

func (s *workflowSuite) TestSuppressWorkflowBy_Error() {
	nDCWorkflow := NewWorkflow(
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		wcache.NoopReleaseFn,
	)

	incomingMockContext := historyi.NewMockWorkflowContext(s.controller)
	incomingMockMutableState := historyi.NewMockMutableState(s.controller)
	incomingNDCWorkflow := NewWorkflow(
		s.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		wcache.NoopReleaseFn,
	)

	// cannot suppress by older workflow
	lastRunningClock := int64(144)
	lastEventVersion := int64(12)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastEventVersion, nil).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		LastRunningClock: lastRunningClock,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	incomingRunID := uuid.NewString()
	incomingLastRunningClock := int64(144)
	incomingLastEventVersion := lastEventVersion - 1
	incomingMockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	incomingMockMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastEventVersion, nil).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		LastRunningClock: incomingLastRunningClock,
	}).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: incomingRunID,
	}).AnyTimes()

	_, err := nDCWorkflow.SuppressBy(incomingNDCWorkflow)
	s.Error(err)
}

func (s *workflowSuite) TestSuppressWorkflowBy_Terminate() {
	randomEventID := int64(2208)
	wtFailedEventID := int64(2)
	lastRunningClock := int64(144)
	lastEventVersion := int64(12)
	s.mockMutableState.EXPECT().GetNextEventID().Return(randomEventID).AnyTimes() // This doesn't matter, GetNextEventID is not used if there is started WT.
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		LastRunningClock: lastRunningClock,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()
	nDCWorkflow := NewWorkflow(
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		wcache.NoopReleaseFn,
	)

	incomingRunID := uuid.NewString()
	incomingLastRunningClock := int64(144)
	incomingLastEventVersion := lastEventVersion + 1
	incomingMockContext := historyi.NewMockWorkflowContext(s.controller)
	incomingMockMutableState := historyi.NewMockMutableState(s.controller)
	incomingNDCWorkflow := NewWorkflow(
		s.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		wcache.NoopReleaseFn,
	)
	incomingMockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	incomingMockMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastEventVersion, nil).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		LastRunningClock: incomingLastRunningClock,
	}).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: incomingRunID,
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastEventVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockMutableState.EXPECT().IsWorkflow().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastEventVersion, true).Return(nil).AnyTimes()
	startedWorkflowTask := &historyi.WorkflowTaskInfo{
		Version:          1234,
		ScheduledEventID: 5678,
		StartedEventID:   9012,
	}
	s.mockMutableState.EXPECT().GetStartedWorkflowTask().Return(startedWorkflowTask)
	s.mockMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		startedWorkflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
		nil,
		consts.IdentityHistoryService,
		nil,
		"",
		"",
		"",
		int64(0),
	).Return(&historypb.HistoryEvent{EventId: wtFailedEventID}, nil)
	s.mockMutableState.EXPECT().FlushBufferedEvents()

	s.mockMutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
		wtFailedEventID, common.FailureReasonWorkflowTerminationDueToVersionConflict, gomock.Any(), consts.IdentityHistoryService, false, nil,
	).Return(&historypb.HistoryEvent{}, nil)

	// if workflow is in zombie or finished state, keep as is
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).Times(2)
	s.mockMutableState.EXPECT().GetCloseVersion().Return(lastEventVersion, nil)
	policy, err := nDCWorkflow.SuppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(historyi.TransactionPolicyPassive, policy)

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(2)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastEventVersion, nil)
	policy, err = nDCWorkflow.SuppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(historyi.TransactionPolicyActive, policy)
}

func (s *workflowSuite) TestSuppressWorkflowBy_Zombiefy() {
	lastRunningClock := int64(144)
	lastEventVersion := int64(12)
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		LastRunningClock: lastRunningClock,
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	executionState := &persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}
	s.mockMutableState.EXPECT().UpdateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING).
		DoAndReturn(func(state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus) (bool, error) {
			executionState.State, executionState.Status = state, status
			return true, nil
		}).AnyTimes()

	nDCWorkflow := NewWorkflow(
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		wcache.NoopReleaseFn,
	)

	incomingRunID := uuid.NewString()
	incomingLastRunningClock := int64(144)
	incomingLastEventVersion := lastEventVersion + 1
	incomingMockContext := historyi.NewMockWorkflowContext(s.controller)
	incomingMockMutableState := historyi.NewMockMutableState(s.controller)
	incomingNDCWorkflow := NewWorkflow(
		s.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		wcache.NoopReleaseFn,
	)
	incomingMockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	incomingMockMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastEventVersion, nil).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		LastRunningClock: incomingLastRunningClock,
	}).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: incomingRunID,
	}).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastEventVersion).Return(cluster.TestAlternativeClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	// if workflow is in zombie or finished state, keep as is
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).Times(2)
	s.mockMutableState.EXPECT().GetCloseVersion().Return(lastEventVersion, nil).AnyTimes()
	policy, err := nDCWorkflow.SuppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(historyi.TransactionPolicyPassive, policy)

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(2)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastEventVersion, nil).AnyTimes()
	policy, err = nDCWorkflow.SuppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(historyi.TransactionPolicyPassive, policy)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, executionState.State)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, executionState.Status)
}
