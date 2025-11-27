package workflow

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
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
	"go.temporal.io/server/common/util"
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
		util.Ptr(int64(0)),
		&persistence.WorkflowSnapshot{},
		[]*persistence.WorkflowEvents{},
		true, // isWorkflow
	)
	s.Equal(timeoutErr, err)
}

func (s *transactionSuite) TestUpdateWorkflowExecution_CompletionMetrics() {
	metricsHandler := metricstest.NewCaptureHandler()
	s.mockShard.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()
	s.mockShard.EXPECT().GetClusterMetadata().Return(clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, true))).AnyTimes()
	s.mockShard.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()

	s.mockShard.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil).AnyTimes()
	s.mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()

	cases := []struct {
		name                   string
		updateMode             persistence.UpdateWorkflowMode
		expectCompletionMetric bool
	}{
		{
			name:                   "UpdateCurrent",
			updateMode:             persistence.UpdateWorkflowModeUpdateCurrent,
			expectCompletionMetric: true,
		},
		{
			name:                   "BypassCurrent",
			updateMode:             persistence.UpdateWorkflowModeBypassCurrent,
			expectCompletionMetric: true,
		},
		{
			name:                   "IgnoreCurrent",
			updateMode:             persistence.UpdateWorkflowModeIgnoreCurrent,
			expectCompletionMetric: false,
		},
	}

	for _, tc := range cases {
		s.T().Run(tc.name, func(t *testing.T) {

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
						Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
						State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					},
				},
				[]*persistence.WorkflowEvents{},
				nil,
				nil,
				nil,
				true, // isWorkflow
			)
			s.NoError(err)

			snapshot := capture.Snapshot()
			completionMetric := snapshot[metrics.WorkflowSuccessCount.Name()]

			if tc.expectCompletionMetric {
				s.Len(completionMetric, 1)
			} else {
				s.Empty(completionMetric)
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
		util.Ptr(int64(0)),
		&persistence.WorkflowSnapshot{},
		[]*persistence.WorkflowEvents{},
		util.Ptr(int64(0)),
		&persistence.WorkflowMutation{},
		[]*persistence.WorkflowEvents{},
		true, // isWorkflow
	)
	s.Equal(timeoutErr, err)
}

func (s *transactionSuite) setupMockForTaskNotification() {
	s.mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).Times(1)
}
