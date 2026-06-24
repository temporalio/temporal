package ndc

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
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
		common.FailureReasonWorkflowTerminationDueToVersionConflict, gomock.Any(), consts.IdentityHistoryService, false, nil,
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
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, executionState.Status)
}

func (s *workflowSuite) wfNewWorkflow() *WorkflowImpl {
	return NewWorkflow(
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		wcache.NoopReleaseFn,
	)
}

func (s *workflowSuite) TestGetVectorClock_NotRunning_CloseVersion() {
	closeVersion := int64(99)
	lastRunningClock := int64(7)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)
	s.mockMutableState.EXPECT().GetCloseVersion().Return(closeVersion, nil)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		LastRunningClock: lastRunningClock,
	})

	version, clock, err := s.wfNewWorkflow().GetVectorClock()
	s.NoError(err)
	s.Equal(closeVersion, version)
	s.Equal(lastRunningClock, clock)
}

func (s *workflowSuite) TestGetVectorClock_RunningError() {
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(0), errors.New("boom"))
	_, _, err := s.wfNewWorkflow().GetVectorClock()
	s.Error(err)
}

func (s *workflowSuite) TestGetVectorClock_CloseVersionError() {
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)
	s.mockMutableState.EXPECT().GetCloseVersion().Return(int64(0), errors.New("boom"))
	_, _, err := s.wfNewWorkflow().GetVectorClock()
	s.Error(err)
}

func (s *workflowSuite) TestHappensAfter() {
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(10), nil).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		LastRunningClock: 5,
	}).AnyTimes()

	incomingMutableState := historyi.NewMockMutableState(s.controller)
	incomingMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	incomingMutableState.EXPECT().GetLastWriteVersion().Return(int64(8), nil).AnyTimes()
	incomingMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		LastRunningClock: 100,
	}).AnyTimes()
	incoming := NewWorkflow(s.mockClusterMetadata, s.mockContext, incomingMutableState, wcache.NoopReleaseFn)

	after, err := s.wfNewWorkflow().HappensAfter(incoming)
	s.NoError(err)
	s.True(after)
}

func (s *workflowSuite) TestHappensAfter_ThisError() {
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(0), errors.New("boom"))
	_, err := s.wfNewWorkflow().HappensAfter(nil)
	s.Error(err)
}

func (s *workflowSuite) TestHappensAfter_ThatError() {
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(10), nil).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()

	incomingMutableState := historyi.NewMockMutableState(s.controller)
	incomingMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	incomingMutableState.EXPECT().GetLastWriteVersion().Return(int64(0), errors.New("boom"))
	incoming := NewWorkflow(s.mockClusterMetadata, s.mockContext, incomingMutableState, wcache.NoopReleaseFn)

	_, err := s.wfNewWorkflow().HappensAfter(incoming)
	s.Error(err)
}

func (s *workflowSuite) TestRevive_NotZombie() {
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	err := s.wfNewWorkflow().Revive(context.Background(), nil)
	s.NoError(err)
}

func (s *workflowSuite) TestRevive_Zombie_Running() {
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	s.mockMutableState.EXPECT().IsWorkflow().Return(true)
	s.mockMutableState.EXPECT().HadOrHasWorkflowTask().Return(true)
	s.mockMutableState.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).Return(true, nil)

	taskRefresher := workflow.NewMockTaskRefresher(s.controller)
	taskRefresher.EXPECT().Refresh(gomock.Any(), s.mockMutableState, false).Return(nil)

	err := s.wfNewWorkflow().Revive(context.Background(), taskRefresher)
	s.NoError(err)
}

func (s *workflowSuite) TestRevive_Zombie_Created() {
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	s.mockMutableState.EXPECT().IsWorkflow().Return(true)
	s.mockMutableState.EXPECT().HadOrHasWorkflowTask().Return(false)
	s.mockMutableState.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).Return(true, nil)

	taskRefresher := workflow.NewMockTaskRefresher(s.controller)
	taskRefresher.EXPECT().Refresh(gomock.Any(), s.mockMutableState, false).Return(nil)

	err := s.wfNewWorkflow().Revive(context.Background(), taskRefresher)
	s.NoError(err)
}

func (s *workflowSuite) TestRevive_Zombie_UpdateError() {
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	s.mockMutableState.EXPECT().IsWorkflow().Return(false)
	s.mockMutableState.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).Return(false, errors.New("boom"))

	err := s.wfNewWorkflow().Revive(context.Background(), nil)
	s.Error(err)
}

func (s *workflowSuite) TestFlushBufferedEvents_NotWorkflow() {
	s.mockMutableState.EXPECT().IsWorkflow().Return(false)
	s.NoError(s.wfNewWorkflow().FlushBufferedEvents())
}

func (s *workflowSuite) TestFlushBufferedEvents_NotRunning() {
	s.mockMutableState.EXPECT().IsWorkflow().Return(true)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)
	s.NoError(s.wfNewWorkflow().FlushBufferedEvents())
}

func (s *workflowSuite) TestFlushBufferedEvents_NoBufferedEvents() {
	s.mockMutableState.EXPECT().IsWorkflow().Return(true)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false)
	s.NoError(s.wfNewWorkflow().FlushBufferedEvents())
}

func (s *workflowSuite) TestFlushBufferedEvents_WrongCluster() {
	lastWriteVersion := int64(12)
	s.mockMutableState.EXPECT().IsWorkflow().Return(true)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(true)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastWriteVersion).Return(cluster.TestAlternativeClusterName)

	err := s.wfNewWorkflow().FlushBufferedEvents()
	s.Error(err)
}

func (s *workflowSuite) TestFlushBufferedEvents_LastWriteVersionError() {
	s.mockMutableState.EXPECT().IsWorkflow().Return(true)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(true)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(0), errors.New("boom"))
	s.Error(s.wfNewWorkflow().FlushBufferedEvents())
}

func (s *workflowSuite) TestFlushBufferedEvents_Success() {
	lastWriteVersion := int64(12)
	s.mockMutableState.EXPECT().IsWorkflow().Return(true)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(true)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastWriteVersion, true).Return(nil)

	startedWorkflowTask := &historyi.WorkflowTaskInfo{ScheduledEventID: 5}
	s.mockMutableState.EXPECT().GetStartedWorkflowTask().Return(startedWorkflowTask)
	s.mockMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		startedWorkflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
		nil, consts.IdentityHistoryService, nil, "", "", "", int64(0),
	).Return(&historypb.HistoryEvent{}, nil)
	s.mockMutableState.EXPECT().FlushBufferedEvents()
	s.mockMutableState.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
	s.mockMutableState.EXPECT().AddWorkflowTaskScheduledEvent(
		false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	).Return(&historyi.WorkflowTaskInfo{}, nil)

	s.NoError(s.wfNewWorkflow().FlushBufferedEvents())
}

func (s *workflowSuite) TestFlushBufferedEvents_Paused() {
	lastWriteVersion := int64(12)
	s.mockMutableState.EXPECT().IsWorkflow().Return(true)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(true)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastWriteVersion, true).Return(nil)
	// no started workflow task -> failWorkflowTask returns nil, nil
	s.mockMutableState.EXPECT().GetStartedWorkflowTask().Return(nil)
	s.mockMutableState.EXPECT().IsWorkflowExecutionStatusPaused().Return(true)

	s.NoError(s.wfNewWorkflow().FlushBufferedEvents())
}

func (s *workflowSuite) TestFlushBufferedEvents_UpdateCurrentVersionError() {
	lastWriteVersion := int64(12)
	s.mockMutableState.EXPECT().IsWorkflow().Return(true)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(true)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastWriteVersion, true).Return(errors.New("boom"))

	s.Error(s.wfNewWorkflow().FlushBufferedEvents())
}

func (s *workflowSuite) TestFlushBufferedEvents_FailWorkflowTaskError() {
	lastWriteVersion := int64(12)
	s.mockMutableState.EXPECT().IsWorkflow().Return(true)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(true)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastWriteVersion, true).Return(nil)

	startedWorkflowTask := &historyi.WorkflowTaskInfo{ScheduledEventID: 5}
	s.mockMutableState.EXPECT().GetStartedWorkflowTask().Return(startedWorkflowTask)
	s.mockMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		startedWorkflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
		nil, consts.IdentityHistoryService, nil, "", "", "", int64(0),
	).Return(nil, errors.New("boom"))

	s.Error(s.wfNewWorkflow().FlushBufferedEvents())
}

func (s *workflowSuite) TestFlushBufferedEvents_ScheduleError() {
	lastWriteVersion := int64(12)
	s.mockMutableState.EXPECT().IsWorkflow().Return(true)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(true)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastWriteVersion, true).Return(nil)
	s.mockMutableState.EXPECT().GetStartedWorkflowTask().Return(nil)
	s.mockMutableState.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
	s.mockMutableState.EXPECT().AddWorkflowTaskScheduledEvent(
		false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	).Return(nil, errors.New("boom"))

	s.Error(s.wfNewWorkflow().FlushBufferedEvents())
}

func (s *workflowSuite) TestSuppressBy_Terminate_UpdateCurrentVersionError() {
	lastWriteVersion := int64(12)
	incomingLastWriteVersion := lastWriteVersion + 1
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()

	incomingMutableState := historyi.NewMockMutableState(s.controller)
	incomingMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	incomingMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastWriteVersion, nil).AnyTimes()
	incomingMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()
	incoming := NewWorkflow(s.mockClusterMetadata, s.mockContext, incomingMutableState, wcache.NoopReleaseFn)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastWriteVersion, true).Return(errors.New("boom"))

	policy, err := s.wfNewWorkflow().SuppressBy(incoming)
	s.Error(err)
	s.Equal(historyi.TransactionPolicyActive, policy)
}

func (s *workflowSuite) TestSuppressBy_Terminate_FailWorkflowTaskError() {
	lastWriteVersion := int64(12)
	incomingLastWriteVersion := lastWriteVersion + 1
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()

	incomingMutableState := historyi.NewMockMutableState(s.controller)
	incomingMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	incomingMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastWriteVersion, nil).AnyTimes()
	incomingMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()
	incoming := NewWorkflow(s.mockClusterMetadata, s.mockContext, incomingMutableState, wcache.NoopReleaseFn)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastWriteVersion, true).Return(nil)
	s.mockMutableState.EXPECT().IsWorkflow().Return(true)
	startedWorkflowTask := &historyi.WorkflowTaskInfo{ScheduledEventID: 5}
	s.mockMutableState.EXPECT().GetStartedWorkflowTask().Return(startedWorkflowTask)
	s.mockMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		startedWorkflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
		nil, consts.IdentityHistoryService, nil, "", "", "", int64(0),
	).Return(nil, errors.New("boom"))

	policy, err := s.wfNewWorkflow().SuppressBy(incoming)
	s.Error(err)
	s.Equal(historyi.TransactionPolicyActive, policy)
}

func (s *workflowSuite) TestSuppressBy_Chasm_Terminate() {
	lastWriteVersion := int64(12)
	incomingLastWriteVersion := lastWriteVersion + 1
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		LastRunningClock: 5,
	}).AnyTimes()

	incomingMutableState := historyi.NewMockMutableState(s.controller)
	incomingMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	incomingMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastWriteVersion, nil).AnyTimes()
	incomingMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		LastRunningClock: 5,
	}).AnyTimes()
	incoming := NewWorkflow(s.mockClusterMetadata, s.mockContext, incomingMutableState, wcache.NoopReleaseFn)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastWriteVersion, true).Return(nil)
	s.mockMutableState.EXPECT().IsWorkflow().Return(false)

	chasmTree := historyi.NewMockChasmTree(s.controller)
	chasmTree.EXPECT().Terminate(gomock.Any()).DoAndReturn(func(req chasm.TerminateComponentRequest) error {
		s.Equal(consts.IdentityHistoryService, req.Identity)
		return nil
	})
	s.mockMutableState.EXPECT().ChasmTree().Return(chasmTree)

	policy, err := s.wfNewWorkflow().SuppressBy(incoming)
	s.NoError(err)
	s.Equal(historyi.TransactionPolicyActive, policy)
}

func (s *workflowSuite) TestSuppressBy_GetVectorClockError() {
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(0), errors.New("boom"))
	policy, err := s.wfNewWorkflow().SuppressBy(nil)
	s.Error(err)
	s.Equal(historyi.TransactionPolicyActive, policy)
}

func (s *workflowSuite) TestSuppressBy_IncomingGetVectorClockError() {
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(10), nil).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()

	incomingMutableState := historyi.NewMockMutableState(s.controller)
	incomingMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	incomingMutableState.EXPECT().GetLastWriteVersion().Return(int64(0), errors.New("boom"))
	incoming := NewWorkflow(s.mockClusterMetadata, s.mockContext, incomingMutableState, wcache.NoopReleaseFn)

	policy, err := s.wfNewWorkflow().SuppressBy(incoming)
	s.Error(err)
	s.Equal(historyi.TransactionPolicyActive, policy)
	var internalErr *serviceerror.Internal
	s.False(errors.As(err, &internalErr) && err.Error() == "Workflow cannot suppress workflow by older workflow")
}
