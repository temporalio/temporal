package workflow

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	transactionSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockShard          *historyi.MockShardContext
		mockEngine         *historyi.MockEngine
		mockNamespaceCache *namespace.MockRegistry

		logger log.Logger

		transaction *TransactionImpl
	}
)

func TestTransactionSuite(t *testing.T) {
	s := new(transactionSuite)
	suite.Run(t, s)
}

func (s *transactionSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockShard = historyi.NewMockShardContext(s.controller)
	s.mockEngine = historyi.NewMockEngine(s.controller)
	s.mockNamespaceCache = namespace.NewMockRegistry(s.controller)
	s.logger = log.NewTestLogger()

	s.mockShard.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	s.mockShard.EXPECT().GetEngine(gomock.Any()).Return(s.mockEngine, nil).AnyTimes()
	s.mockShard.EXPECT().GetNamespaceRegistry().Return(s.mockNamespaceCache).AnyTimes()
	s.mockShard.EXPECT().GetLogger().Return(s.logger).AnyTimes()
	s.mockShard.EXPECT().ChasmRegistry().Return(chasm.NewRegistry(s.logger)).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	s.transaction = NewTransaction(s.mockShard)
}

func (s *transactionSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *transactionSuite) TestOperationMayApplied() {
	testCases := []struct {
		err        error
		mayApplied bool
	}{
		{err: &persistence.CurrentWorkflowConditionFailedError{}, mayApplied: false},
		{err: &persistence.WorkflowConditionFailedError{}, mayApplied: false},
		{err: &persistence.ConditionFailedError{}, mayApplied: false},
		{err: &persistence.ShardOwnershipLostError{}, mayApplied: false},
		{err: &persistence.InvalidPersistenceRequestError{}, mayApplied: false},
		{err: &persistence.TransactionSizeLimitError{}, mayApplied: false},
		{err: &serviceerror.ResourceExhausted{}, mayApplied: false},
		{err: &serviceerror.NotFound{}, mayApplied: false},
		{err: &serviceerror.NamespaceNotFound{}, mayApplied: false},
		{err: nil, mayApplied: true},
		{err: &persistence.TimeoutError{}, mayApplied: true},
		{err: &serviceerror.Unavailable{}, mayApplied: true},
		{err: errors.New("some unknown error"), mayApplied: true},
	}

	for _, tc := range testCases {
		s.Equal(tc.mayApplied, persistence.OperationPossiblySucceeded(tc.err))
	}
}

func (s *transactionSuite) TestCreateWorkflowExecution_NotifyTaskWhenFailed() {
	timeoutErr := &persistence.TimeoutError{}
	s.True(persistence.OperationPossiblySucceeded(timeoutErr))

	s.mockShard.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, timeoutErr)
	s.setupMockForTaskNotification()

	_, err := s.transaction.CreateWorkflowExecution(
		context.Background(),
		persistence.CreateWorkflowModeBrandNew,
		chasm.WorkflowArchetypeID,
		0,
		&persistence.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId: tests.NamespaceID.String(),
				WorkflowId:  tests.WorkflowID,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId: tests.RunID,
			},
		},
		[]*persistence.WorkflowEvents{},
		true, // isWorkflow
	)
	s.Equal(timeoutErr, err)
}

func (s *transactionSuite) TestUpdateWorkflowExecution_NotifyTaskWhenFailed() {
	timeoutErr := &persistence.TimeoutError{}
	s.True(persistence.OperationPossiblySucceeded(timeoutErr))

	s.mockShard.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, timeoutErr)
	s.setupMockForTaskNotification() // for current workflow mutation
	s.setupMockForTaskNotification() // for new workflow snapshot

	_, _, err := s.transaction.UpdateWorkflowExecution(
		context.Background(),
		persistence.UpdateWorkflowModeUpdateCurrent,
		chasm.WorkflowArchetypeID,
		0,
		&persistence.WorkflowMutation{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId: tests.NamespaceID.String(),
				WorkflowId:  tests.WorkflowID,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId: tests.RunID,
			},
		},
		[]*persistence.WorkflowEvents{},
		new(int64(0)),
		&persistence.WorkflowSnapshot{},
		[]*persistence.WorkflowEvents{},
		true, // isWorkflow
	)
	s.Equal(timeoutErr, err)
}

// closingWorkflowEvents returns events whose last event is a workflow-closing event of the given type.
func closingWorkflowEvents(closeEventType enumspb.EventType) []*persistence.WorkflowEvents {
	return []*persistence.WorkflowEvents{{
		Events: []*historypb.HistoryEvent{
			{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED},
			{EventId: 2, EventType: closeEventType},
		},
	}}
}

// A completion metric is emitted only when the run's closing event is written in this
// transaction, independent of the persisted status or the update mode.
func (s *transactionSuite) TestUpdateWorkflowExecution_CompletionMetrics() {
	metricsHandler := metricstest.NewCaptureHandler()
	s.mockShard.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()
	s.mockShard.EXPECT().GetClusterMetadata().Return(clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, true))).AnyTimes()
	s.mockShard.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()

	s.mockShard.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil).AnyTimes()
	s.mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()

	cases := []struct {
		name       string
		updateMode persistence.UpdateWorkflowMode
		status     enumspb.WorkflowExecutionStatus
		state      enumsspb.WorkflowExecutionState
		events     []*persistence.WorkflowEvents
		metricName string
		expectEmit bool
	}{
		{
			name:       "completed with closing event emits",
			updateMode: persistence.UpdateWorkflowModeUpdateCurrent,
			status:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			state:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			events:     closingWorkflowEvents(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED),
			metricName: metrics.WorkflowSuccessCount.Name(),
			expectEmit: true,
		},
		{
			name:       "failed with closing event emits",
			updateMode: persistence.UpdateWorkflowModeUpdateCurrent,
			status:     enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			state:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			events:     closingWorkflowEvents(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED),
			metricName: metrics.WorkflowFailedCount.Name(),
			expectEmit: true,
		},
		{
			name:       "failed without closing event (reset re-persist) does not emit",
			updateMode: persistence.UpdateWorkflowModeUpdateCurrent,
			status:     enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			state:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			events:     []*persistence.WorkflowEvents{},
			metricName: metrics.WorkflowFailedCount.Name(),
			expectEmit: false,
		},
		{
			name:       "IgnoreCurrent with closing event emits",
			updateMode: persistence.UpdateWorkflowModeIgnoreCurrent,
			status:     enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			state:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			events:     closingWorkflowEvents(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED),
			metricName: metrics.WorkflowFailedCount.Name(),
			expectEmit: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {

			capture := metricsHandler.StartCapture()

			_, _, err := s.transaction.UpdateWorkflowExecution(
				context.Background(),
				tc.updateMode,
				chasm.WorkflowArchetypeID,
				0,
				&persistence.WorkflowMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						NamespaceId:      tests.NamespaceID.String(),
						WorkflowId:       tests.WorkflowID,
						VersionHistories: &historyspb.VersionHistories{},
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId:  tests.RunID,
						Status: tc.status,
						State:  tc.state,
					},
				},
				tc.events,
				nil,
				nil,
				nil,
				true, // isWorkflow
			)
			s.Require().NoError(err)

			completionMetric := capture.Snapshot()[tc.metricName]

			if tc.expectEmit {
				s.Require().Len(completionMetric, 1)
			} else {
				s.Require().Empty(completionMetric)
			}

			metricsHandler.StopCapture(capture)
		})
	}

}

// A reset snapshot with a terminal status emits a completion metric only when its
// closing event is written in this transaction.
func (s *transactionSuite) TestConflictResolveWorkflowExecution_CompletionMetrics() {
	metricsHandler := metricstest.NewCaptureHandler()
	s.mockShard.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()
	s.mockShard.EXPECT().GetClusterMetadata().Return(clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, true))).AnyTimes()
	s.mockShard.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()

	resp := &persistence.ConflictResolveWorkflowExecutionResponse{
		ResetMutableStateStats: persistence.MutableStateStatistics{
			HistoryStatistics: &persistence.HistoryStatistics{},
		},
	}
	s.mockShard.EXPECT().ConflictResolveWorkflowExecution(gomock.Any(), gomock.Any()).Return(resp, nil).AnyTimes()
	s.mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()

	cases := []struct {
		name       string
		events     []*persistence.WorkflowEvents
		expectEmit bool
	}{
		{
			name:       "reset snapshot failed with closing event emits",
			events:     closingWorkflowEvents(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED),
			expectEmit: true,
		},
		{
			name:       "reset snapshot failed without closing event does not emit",
			events:     []*persistence.WorkflowEvents{},
			expectEmit: false,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {

			capture := metricsHandler.StartCapture()

			_, _, _, err := s.transaction.ConflictResolveWorkflowExecution(
				context.Background(),
				persistence.ConflictResolveWorkflowModeUpdateCurrent,
				chasm.WorkflowArchetypeID,
				0,
				&persistence.WorkflowSnapshot{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						NamespaceId:      tests.NamespaceID.String(),
						WorkflowId:       tests.WorkflowID,
						VersionHistories: &historyspb.VersionHistories{},
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId:  tests.RunID,
						Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
						State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					},
				},
				tc.events,
				nil,  // newWorkflowFailoverVersion
				nil,  // newWorkflowSnapshot
				nil,  // newWorkflowEventsSeq
				nil,  // currentWorkflowFailoverVersion
				nil,  // currentWorkflowMutation
				nil,  // currentWorkflowEventsSeq
				true, // isWorkflow
			)
			s.Require().NoError(err)

			completionMetric := capture.Snapshot()[metrics.WorkflowFailedCount.Name()]

			if tc.expectEmit {
				s.Require().Len(completionMetric, 1)
			} else {
				s.Require().Empty(completionMetric)
			}

			metricsHandler.StopCapture(capture)
		})
	}
}

func (s *transactionSuite) TestConflictResolveWorkflowExecution_NotifyTaskWhenFailed() {
	timeoutErr := &persistence.TimeoutError{}
	s.True(persistence.OperationPossiblySucceeded(timeoutErr))

	s.mockShard.EXPECT().ConflictResolveWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, timeoutErr)
	s.setupMockForTaskNotification() // for reset workflow snapshot
	s.setupMockForTaskNotification() // for new workflow snapshot
	s.setupMockForTaskNotification() // for current workflow mutation

	_, _, _, err := s.transaction.ConflictResolveWorkflowExecution(
		context.Background(),
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		chasm.WorkflowArchetypeID,
		0,
		&persistence.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId: tests.NamespaceID.String(),
				WorkflowId:  tests.WorkflowID,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId: tests.RunID,
			},
		},
		[]*persistence.WorkflowEvents{},
		new(int64(0)),
		&persistence.WorkflowSnapshot{},
		[]*persistence.WorkflowEvents{},
		new(int64(0)),
		&persistence.WorkflowMutation{},
		[]*persistence.WorkflowEvents{},
		true, // isWorkflow
	)
	s.Equal(timeoutErr, err)
}

func (s *transactionSuite) TestConflictResolveWorkflowExecution_NotifyChasmExecution() {
	timeoutErr := &persistence.TimeoutError{}
	s.True(persistence.OperationPossiblySucceeded(timeoutErr))

	resetWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId: tests.RunID,
		},
		ChasmNodes: map[string]*persistencespb.ChasmNode{
			"path1": {},
		},
	}
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId: "new-run-id",
		},
		ChasmNodes: map[string]*persistencespb.ChasmNode{
			"path2": {},
		},
	}
	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId: "current-run-id",
		},
		UpsertChasmNodes: map[string]*persistencespb.ChasmNode{
			"path3": {},
		},
	}

	s.mockShard.EXPECT().ConflictResolveWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, timeoutErr)
	s.setupMockForTaskNotification() // for reset workflow snapshot
	s.setupMockForTaskNotification() // for new workflow snapshot
	s.setupMockForTaskNotification() // for current workflow mutation

	// Expect CHASM notifications for all three workflows
	s.mockEngine.EXPECT().NotifyChasmExecution(chasm.ExecutionKey{
		NamespaceID: tests.NamespaceID.String(),
		BusinessID:  tests.WorkflowID,
		RunID:       tests.RunID,
	}, gomock.Any()).Times(1)
	s.mockEngine.EXPECT().NotifyChasmExecution(chasm.ExecutionKey{
		NamespaceID: tests.NamespaceID.String(),
		BusinessID:  tests.WorkflowID,
		RunID:       "new-run-id",
	}, gomock.Any()).Times(1)
	s.mockEngine.EXPECT().NotifyChasmExecution(chasm.ExecutionKey{
		NamespaceID: tests.NamespaceID.String(),
		BusinessID:  tests.WorkflowID,
		RunID:       "current-run-id",
	}, gomock.Any()).Times(1)

	_, _, _, err := s.transaction.ConflictResolveWorkflowExecution(
		context.Background(),
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		chasm.WorkflowArchetypeID,
		0,
		resetWorkflowSnapshot,
		[]*persistence.WorkflowEvents{},
		new(int64(0)),
		newWorkflowSnapshot,
		[]*persistence.WorkflowEvents{},
		new(int64(0)),
		currentWorkflowMutation,
		[]*persistence.WorkflowEvents{},
		true, // isWorkflow
	)
	s.Equal(timeoutErr, err)
}

func (s *transactionSuite) setupMockForTaskNotification() {
	s.mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).Times(1)
}
