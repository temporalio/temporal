package ndc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	chasmtests "go.temporal.io/server/chasm/lib/tests"
	testspb "go.temporal.io/server/chasm/lib/tests/gen/testspb/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/limiter"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This suite characterizes the s-aw035 incident at the TransactionManager-to-persistence boundary.
// TransactionManager, Workflow, MutableState and Context are real; currentRecordStore is a mock
// ExecutionManager that applies the same expected-current-run predicate as the SQL and Cassandra
// stores. It does not run the replication processor, a database, or the DLQ pipeline.
//
// Two runs of one business ID. "clock" is LastRunningClock, which is what HappensAfter compares once
// both runs share a failover version. Run1's close having stamped a *higher* clock than run2's
// creation enables the two current-record handoffs. The final promotion verifies that the clean
// current run's identity is carried independently when its persistence mutation is omitted.
//
// Useful breakpoints: transaction_manager_new_workflow.go createAsZombie (a zombie being born),
// suppressCurrentAndCreateAsCurrent (a switch off a running run), and
// transaction_manager_existing_workflow.go suppressCurrentAndUpdateAsCurrent (a switch off a closed
// run). On the final run2 VT 2 path, step from ContextImpl.ConflictResolveWorkflowExecution into
// MutableStateImpl.CloseTransactionAsMutation -> closeTransactionShouldSkipPersistence to watch the
// clean run1 mutation become nil.
//
// Full store fidelity for these shapes belongs in
// common/persistence/tests/execution_mutable_state.go, which runs against real databases.
type (
	chasmCurrentRecordChainSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		mockShard  *shard.ContextTest
		store      *currentRecordStore

		namespaceEntry *namespace.Namespace
		transactionMgr TransactionManager
	}

	// currentRecordStore is an in-test stand-in for the execution store, holding just enough state to
	// make the current-execution-record behave like the real one.
	currentRecordStore struct {
		currentRunID string
		runs         map[string]*persistencespb.WorkflowMutableState

		lastConflictResolve                   *persistence.ConflictResolveWorkflowExecutionRequest
		lastConflictResolveActualCurrentRunID string
		lastConflictResolveExpectedRunID      string
		lastCreateMode                        persistence.CreateWorkflowMode
		lastUpdate                            *persistence.UpdateWorkflowExecutionRequest
	}
)

func TestChasmCurrentRecordChainSuite(t *testing.T) {
	suite.Run(t, new(chasmCurrentRecordChainSuite))
}

func (s *chasmCurrentRecordChainSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	config := tests.NewDynamicConfig()
	config.EnableTransitionHistory = func(string) bool { return true }
	// Without this the mutable state keeps a noop CHASM tree whose archetype is the workflow one, and
	// the persistence skip under test can never trigger.
	config.EnableChasm = func(string) bool { return true }

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{ShardId: 1, RangeId: 1},
		config,
	)
	s.namespaceEntry = tests.GlobalNamespaceEntry

	registry := chasm.NewRegistry(s.mockShard.GetLogger())
	s.Require().NoError(registry.Register(chasmtests.Library))
	s.mockShard.SetChasmRegistry(registry)
	s.Require().NoError(workflow.RegisterStateMachine(s.mockShard.StateMachineRegistry()))

	mockEngine := historyi.NewMockEngine(s.controller)
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().NotifyChasmExecution(gomock.Any(), gomock.Any()).AnyTimes()
	mockEngine.EXPECT().Stop().AnyTimes()
	s.mockShard.SetEngineForTesting(mockEngine)

	s.mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().
		Return(cluster.TestCurrentClusterName).AnyTimes()
	// Both runs were last written by the other cluster, so SuppressBy zombifies rather than terminates.
	s.mockShard.Resource.ClusterMetadata.EXPECT().
		ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).
		Return(cluster.TestAlternativeClusterName).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().
		IsVersionFromSameCluster(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	s.mockShard.Resource.NamespaceCache.EXPECT().
		GetNamespaceByID(s.namespaceEntry.ID()).Return(s.namespaceEntry, nil).AnyTimes()

	s.store = &currentRecordStore{runs: map[string]*persistencespb.WorkflowMutableState{}}
	s.store.wire(s.mockShard.Resource.ExecutionMgr)

	s.transactionMgr = NewTransactionManager(
		s.mockShard,
		wcache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler),
		NewMockEventsReapplier(s.controller),
		s.mockShard.GetLogger(),
		false,
	)
}

func (s *chasmCurrentRecordChainSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

// TestAlternativeRun2VT1CreatedAsZombieThenVT2Promotes covers the alternative ordering where
// run1's close reaches the passive before run2 VT 1.
//
//	                    | run1                  | run2                  | current | outcome
//	--------------------+-----------------------+-----------------------+---------+-------------------------
//	start               | COMPLETED, clock 1369 | does not exist        | run1    | seeded
//	run2 VT 1 arrives   | untouched             | created ZOMBIE, VT 1  | run1    | createAsZombie, no switch
//	                    |                       | clock 1               |         | (1 < 2)
//	run2 VT 2 arrives   | untouched             | RUNNING at VT 2       | run2    | promotion succeeds
//	                    |                       | clock 3               |         | CAS expects run1
//
// Clock values in the table use the last four digits of the incident clocks. In row 3 the decision is
// right (1382 > 1369, so run2 must become current). ExpectedCurrentRunID preserves run1 as the CAS
// predicate even though the clean completed run has no persistence mutation.
func (s *chasmCurrentRecordChainSuite) TestAlternativeRun2VT1CreatedAsZombieThenVT2Promotes() {
	const (
		businessID      = "test-standalone-activity-zombify"
		run1ID          = "00000000-0000-0000-0000-000000000001"
		run2ID          = "00000000-0000-0000-0000-000000000002"
		run1CloseClock  = int64(2)
		run2CreateClock = int64(1)
		run2VT2Clock    = int64(3)
	)
	ctx := context.Background()

	// run1 as the passive cluster already has it: its VT 1..3 applied, closed, and named by the
	// current execution record.
	s.store.seed(
		s.chasmDBState(businessID, run1ID, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, run1CloseClock, 3),
		true,
	)

	// ---- step 1: run2's VT 1 arrives -------------------------------------------------------------
	run2VT1 := s.chasmDBState(businessID, run2ID, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, run2CreateClock, 1)
	err := s.transactionMgr.CreateWorkflow(ctx, chasmtests.ArchetypeID, s.newWorkflow(run2VT1))
	s.Require().NoError(err)

	s.Equal(persistence.CreateWorkflowModeBypassCurrent, s.store.lastCreateMode,
		"the clock inversion should route run2 through createAsZombie")
	s.Equal(run1ID, s.store.currentRunID, "the current record must still name run1")
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, s.store.runs[run2ID].ExecutionState.State,
		"run2 is persisted as a zombie, which is the state the incident dump showed")

	// ---- step 2: run2's VT 2 arrives -------------------------------------------------------------
	// Load the persisted zombie, then apply the source transition just as the state replicator does.
	// Its in-memory state becomes RUNNING while stateInDB remains ZOMBIE, forcing the current-record
	// lookup in dispatchForExistingWorkflow.
	run2Workflow, err := s.transactionMgr.LoadWorkflow(
		ctx,
		s.namespaceEntry.ID(),
		businessID,
		run2ID,
		chasmtests.ArchetypeID,
	)
	s.Require().NoError(err)
	s.applyTransition(run2Workflow, s.chasmDBState(
		businessID,
		run2ID,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		run2VT2Clock,
		2,
	))

	err = s.transactionMgr.UpdateWorkflow(ctx, false, chasmtests.ArchetypeID, run2Workflow, nil)

	s.Require().NoError(err)

	s.Require().NotNil(s.store.lastConflictResolve)
	s.Equal(persistence.ConflictResolveWorkflowModeUpdateCurrent, s.store.lastConflictResolve.Mode)
	s.Equal(run2ID, s.store.lastConflictResolve.ResetWorkflowSnapshot.ExecutionState.RunId)
	s.Nil(s.store.lastConflictResolve.CurrentWorkflowMutation,
		"a clean CHASM transaction should not need an otherwise empty current mutation")
	s.Equal(run1ID, s.store.lastConflictResolve.ExpectedCurrentRunID)
	s.Equal(run1ID, s.store.lastConflictResolveActualCurrentRunID)
	s.Equal(run1ID, s.store.lastConflictResolveExpectedRunID)
	s.Equal(run2ID, s.store.currentRunID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, s.store.runs[run2ID].ExecutionState.State)
	s.Equal(int64(2), s.store.runs[run2ID].ExecutionState.LastUpdateVersionedTransition.TransitionCount)
	s.Equal(run2VT2Clock, s.store.runs[run2ID].ExecutionInfo.LastRunningClock)
}

// TestIncidentRun2VT1ThenRun1VT3ThenRun2VT2VT3Converge covers the observed arrival order.
// All three handoffs exercise clean CHASM current suppression through the real TransactionManager
// and Context paths. Run2 VT 3 then verifies that replication can continue after VT 2 restores the
// correct current record.
//
//	                  | run1                      | run2                      | current | outcome
//	------------------+---------------------------+---------------------------+---------+--------------------------
//	start             | RUNNING, clock 1, VT 2    | does not exist            | run1    | seeded
//	run2 VT 1 arrives | ZOMBIE                    | RUNNING, clock 2, VT 1    | run2    | first handoff succeeds
//	run1 VT 3 arrives | COMPLETED, clock 3        | ZOMBIE                    | run1    | second handoff succeeds
//	run2 VT 2 arrives | unchanged                 | RUNNING, clock 4, VT 2    | run2    | CAS expects run1, succeeds
//	run2 VT 3 arrives | unchanged                 | COMPLETED, VT 3           | run2    | converged
func (s *chasmCurrentRecordChainSuite) TestIncidentRun2VT1ThenRun1VT3ThenRun2VT2VT3Converge() {
	const (
		businessID = "test-standalone-activity-zombify"
		run1ID     = "00000000-0000-0000-0000-000000000001"
		run2ID     = "00000000-0000-0000-0000-000000000002"

		run1RunningClock = int64(1)
		run2CreateClock  = int64(2)
		run1CloseClock   = int64(3)
		run2VT2Clock     = int64(4)
	)
	ctx := context.Background()

	run1VT2 := s.chasmDBState(
		businessID,
		run1ID,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		run1RunningClock,
		2,
	)
	s.store.seed(run1VT2, true)

	// ---- step 1: run2 VT 1 takes current from running run1 ---------------------------------------
	run2VT1 := s.chasmDBState(
		businessID,
		run2ID,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		run2CreateClock,
		1,
	)
	s.Require().NoError(s.transactionMgr.CreateWorkflow(ctx, chasmtests.ArchetypeID, s.newWorkflow(run2VT1)))
	s.Require().NotNil(s.store.lastUpdate)
	s.Equal(persistence.UpdateWorkflowModeUpdateCurrent, s.store.lastUpdate.Mode)
	s.Equal(run1ID, s.store.lastUpdate.UpdateWorkflowMutation.ExecutionState.RunId)
	s.Equal(run2ID, s.store.lastUpdate.UpdateWorkflowMutation.ExecutionInfo.SuccessorRunId)
	s.Require().NotNil(s.store.lastUpdate.NewWorkflowSnapshot)
	s.Equal(run2ID, s.store.lastUpdate.NewWorkflowSnapshot.ExecutionState.RunId)
	s.Equal(run2ID, s.store.currentRunID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, s.store.runs[run1ID].ExecutionState.State)
	s.Equal(run2ID, s.store.runs[run1ID].ExecutionInfo.SuccessorRunId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, s.store.runs[run2ID].ExecutionState.State)

	// ---- step 2: run1 VT 3 takes current back from running run2 ----------------------------------
	run1Workflow, err := s.transactionMgr.LoadWorkflow(
		ctx,
		s.namespaceEntry.ID(),
		businessID,
		run1ID,
		chasmtests.ArchetypeID,
	)
	s.Require().NoError(err)
	s.applyTransition(run1Workflow, s.chasmDBState(
		businessID,
		run1ID,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		run1CloseClock,
		3,
	))
	s.Require().NoError(s.transactionMgr.UpdateWorkflow(ctx, false, chasmtests.ArchetypeID, run1Workflow, nil))
	s.Require().NotNil(s.store.lastConflictResolve)
	s.Equal(run1ID, s.store.lastConflictResolve.ResetWorkflowSnapshot.ExecutionState.RunId)
	s.Require().NotNil(s.store.lastConflictResolve.CurrentWorkflowMutation)
	s.Equal(run2ID, s.store.lastConflictResolve.CurrentWorkflowMutation.ExecutionState.RunId)
	s.Equal(run1ID, s.store.currentRunID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, s.store.runs[run1ID].ExecutionState.State)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, s.store.runs[run2ID].ExecutionState.State)

	// ---- step 3: run2 VT 2 is newer and takes current from clean closed run1 ----------------------
	run2Workflow, err := s.transactionMgr.LoadWorkflow(
		ctx,
		s.namespaceEntry.ID(),
		businessID,
		run2ID,
		chasmtests.ArchetypeID,
	)
	s.Require().NoError(err)
	s.applyTransition(run2Workflow, s.chasmDBState(
		businessID,
		run2ID,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		run2VT2Clock,
		2,
	))
	err = s.transactionMgr.UpdateWorkflow(ctx, false, chasmtests.ArchetypeID, run2Workflow, nil)

	s.Require().NoError(err)
	s.Require().NotNil(s.store.lastConflictResolve)
	s.Nil(s.store.lastConflictResolve.CurrentWorkflowMutation)
	s.Equal(run2ID, s.store.lastConflictResolve.ResetWorkflowSnapshot.ExecutionState.RunId)
	s.Equal(run1ID, s.store.lastConflictResolve.ExpectedCurrentRunID)
	s.Equal(run1ID, s.store.lastConflictResolveActualCurrentRunID)
	s.Equal(run1ID, s.store.lastConflictResolveExpectedRunID)
	s.Equal(run2ID, s.store.currentRunID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, s.store.runs[run1ID].ExecutionState.State)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, s.store.runs[run2ID].ExecutionState.State)
	s.Equal(int64(2), s.store.runs[run2ID].ExecutionState.LastUpdateVersionedTransition.TransitionCount)
	s.Equal(run2VT2Clock, s.store.runs[run2ID].ExecutionInfo.LastRunningClock)

	// ---- step 4: run2 VT 3 applies normally after VT 2 restores it as current -------------------
	run2Workflow, err = s.transactionMgr.LoadWorkflow(
		ctx,
		s.namespaceEntry.ID(),
		businessID,
		run2ID,
		chasmtests.ArchetypeID,
	)
	s.Require().NoError(err)
	s.applyTransition(run2Workflow, s.chasmDBState(
		businessID,
		run2ID,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		run2VT2Clock,
		3,
	))
	s.Require().NoError(s.transactionMgr.UpdateWorkflow(
		ctx,
		false,
		chasmtests.ArchetypeID,
		run2Workflow,
		nil,
	))

	s.Equal(run2ID, s.store.currentRunID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, s.store.runs[run2ID].ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, s.store.runs[run2ID].ExecutionState.Status)
	s.Equal(int64(3), s.store.runs[run2ID].ExecutionState.LastUpdateVersionedTransition.TransitionCount)
}

// TestRunningCurrentSuppressionIsPersistedDuringExistingRunPromotion covers a current handoff where
// the old current run is still running. This isolates persistence-only suppression from the missing
// current-record identity case: run1 must have a non-nil mutation, so that mutation already carries
// the correct current-record CAS run ID.
//
//	                  | run1                   | run2                   | current | outcome
//	------------------+------------------------+------------------------+---------+---------------------------
//	start             | RUNNING, clock 1, VT 1 | ZOMBIE, clock 2, VT 1  | run1    | seeded
//	run2 VT 2 arrives | ZOMBIE, clock 1, VT 1  | RUNNING, clock 3, VT 2 | run2    | promotion succeeds
//
// The handoff must atomically persist run1's cluster-local ZOMBIE state and promote run2 without
// advancing run1's replicated transition or LastRunningClock.
func (s *chasmCurrentRecordChainSuite) TestRunningCurrentSuppressionIsPersistedDuringExistingRunPromotion() {
	const (
		businessID      = "test-standalone-activity-persistence-only-suppression"
		run1ID          = "00000000-0000-0000-0000-000000000001"
		run2ID          = "00000000-0000-0000-0000-000000000002"
		run1Clock       = int64(1)
		run2CreateClock = int64(2)
		run2VT2Clock    = int64(3)
	)
	ctx := context.Background()

	s.store.seed(s.chasmDBState(
		businessID,
		run1ID,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		run1Clock,
		1,
	), true)
	s.store.seed(s.chasmDBState(
		businessID,
		run2ID,
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		run2CreateClock,
		1,
	), false)

	run2Workflow, err := s.transactionMgr.LoadWorkflow(
		ctx,
		s.namespaceEntry.ID(),
		businessID,
		run2ID,
		chasmtests.ArchetypeID,
	)
	s.Require().NoError(err)
	s.applyTransition(run2Workflow, s.chasmDBState(
		businessID,
		run2ID,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		run2VT2Clock,
		2,
	))

	err = s.transactionMgr.UpdateWorkflow(ctx, false, chasmtests.ArchetypeID, run2Workflow, nil)
	s.Require().NoError(err)

	s.Require().NotNil(s.store.lastConflictResolve)
	s.Equal(persistence.ConflictResolveWorkflowModeUpdateCurrent, s.store.lastConflictResolve.Mode)
	s.Require().NotNil(s.store.lastConflictResolve.CurrentWorkflowMutation,
		"suppressing a clean running CHASM execution must not be treated as a persistence no-op")
	currentMutation := s.store.lastConflictResolve.CurrentWorkflowMutation
	s.Equal(run1ID, currentMutation.ExecutionState.RunId)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, currentMutation.ExecutionState.State)
	s.Equal(int64(1), currentMutation.ExecutionState.LastUpdateVersionedTransition.TransitionCount,
		"cluster-local suppression must not advance the replicated transition")
	s.Equal(run1Clock, currentMutation.ExecutionInfo.LastRunningClock,
		"cluster-local suppression must not advance the replication clock")

	s.Equal(run2ID, s.store.currentRunID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, s.store.runs[run1ID].ExecutionState.State)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, s.store.runs[run2ID].ExecutionState.State)
	s.Equal(int64(2), s.store.runs[run2ID].ExecutionState.LastUpdateVersionedTransition.TransitionCount)
}

// newWorkflow wraps a db record in the real MutableStateImpl / ContextImpl / Workflow trio so the
// transaction manager exercises the real vector clock, SuppressBy and Revive logic.
func (s *chasmCurrentRecordChainSuite) newWorkflow(dbState *persistencespb.WorkflowMutableState) Workflow {
	mutableState, err := workflow.NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.MockEventsCache,
		s.mockShard.GetLogger(),
		s.namespaceEntry,
		dbState,
		1,
	)
	s.Require().NoError(err)
	s.False(mutableState.IsWorkflow(), "the CHASM skip only applies to non-workflow archetypes")
	_, err = mutableState.StartTransaction(s.namespaceEntry)
	s.Require().NoError(err)

	wfContext := workflow.NewContext(
		s.mockShard.GetConfig(),
		mutableState.GetWorkflowKey(),
		chasmtests.ArchetypeID,
		log.NewNoopLogger(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		limiter.NewKeyedBytesLimiter(),
	)
	wfContext.MutableState = mutableState

	return NewWorkflow(s.mockShard.GetClusterMetadata(), wfContext, mutableState, func(error) {})
}

func (s *chasmCurrentRecordChainSuite) chasmDBState(
	businessID string,
	runID string,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	lastRunningClock int64,
	transitionCount int64,
) *persistencespb.WorkflowMutableState {
	failoverVersion := s.namespaceEntry.FailoverVersion(businessID)
	lastVersionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: failoverVersion,
		TransitionCount:          transitionCount,
	}
	componentData, err := serialization.Encode(&testspb.TestPayloadStore{}, serialization.WithDeterministicProto3)
	s.Require().NoError(err)

	return &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:       s.namespaceEntry.ID().String(),
			WorkflowId:        businessID,
			LastRunningClock:  lastRunningClock,
			TransitionHistory: []*persistencespb.VersionedTransition{lastVersionedTransition},
			// CHASM executions have no events; an empty current history keeps GetCloseVersion on the
			// transition history path.
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{{}},
			},
			ExecutionStats: &persistencespb.ExecutionStats{},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:                         runID,
			State:                         state,
			Status:                        status,
			StartTime:                     timestamppb.New(s.mockShard.GetTimeSource().Now().Add(-time.Minute)),
			LastUpdateVersionedTransition: lastVersionedTransition,
		},
		ChasmNodes: map[string]*persistencespb.ChasmNode{
			"": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: failoverVersion,
						TransitionCount:          1,
					},
					LastUpdateVersionedTransition: lastVersionedTransition,
					Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
						ComponentAttributes: &persistencespb.ChasmComponentAttributes{
							TypeId: chasmtests.ArchetypeID,
						},
					},
				},
				Data: componentData,
			},
		},
		NextEventId: 1,
	}
}

func (s *chasmCurrentRecordChainSuite) applyTransition(
	wf Workflow,
	dbState *persistencespb.WorkflowMutableState,
) {
	source := proto.Clone(dbState).(*persistencespb.WorkflowMutableState)
	s.Require().NoError(wf.GetMutableState().ApplyMutation(&persistencespb.WorkflowMutableStateMutation{
		ExecutionInfo:     source.ExecutionInfo,
		ExecutionState:    source.ExecutionState,
		UpdatedChasmNodes: source.ChasmNodes,
	}))
}

// applyMutation mirrors how the stores treat a WorkflowMutation: it carries CHASM node upserts and
// deletes rather than the whole tree, so the untouched nodes have to survive the write.
func (s *currentRecordStore) applyMutation(mutation persistence.WorkflowMutation) {
	runID := mutation.ExecutionState.RunId
	stored := &persistencespb.WorkflowMutableState{
		ChasmNodes: map[string]*persistencespb.ChasmNode{},
	}
	if existing, ok := s.runs[runID]; ok {
		stored = proto.Clone(existing).(*persistencespb.WorkflowMutableState)
	}
	if stored.ChasmNodes == nil {
		stored.ChasmNodes = map[string]*persistencespb.ChasmNode{}
	}
	for path, node := range mutation.UpsertChasmNodes {
		stored.ChasmNodes[path] = proto.Clone(node).(*persistencespb.ChasmNode)
	}
	for path := range mutation.DeleteChasmNodes {
		delete(stored.ChasmNodes, path)
	}
	stored.ExecutionInfo = proto.Clone(mutation.ExecutionInfo).(*persistencespb.WorkflowExecutionInfo)
	stored.ExecutionState = proto.Clone(mutation.ExecutionState).(*persistencespb.WorkflowExecutionState)
	stored.NextEventId = mutation.NextEventID
	s.runs[runID] = stored
}

func (s *currentRecordStore) applySnapshot(snapshot persistence.WorkflowSnapshot) {
	stored := &persistencespb.WorkflowMutableState{
		ExecutionInfo:  snapshot.ExecutionInfo,
		ExecutionState: snapshot.ExecutionState,
		ChasmNodes:     snapshot.ChasmNodes,
		NextEventId:    snapshot.NextEventID,
	}
	s.runs[snapshot.ExecutionState.RunId] = proto.Clone(stored).(*persistencespb.WorkflowMutableState)
}

func (s *currentRecordStore) seed(dbState *persistencespb.WorkflowMutableState, isCurrent bool) {
	s.runs[dbState.ExecutionState.RunId] = proto.Clone(dbState).(*persistencespb.WorkflowMutableState)
	if isCurrent {
		s.currentRunID = dbState.ExecutionState.RunId
	}
}

func (s *currentRecordStore) wire(mockExecutionMgr *persistence.MockExecutionManager) {
	mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			_ *persistence.GetCurrentExecutionRequest,
		) (*persistence.GetCurrentExecutionResponse, error) {
			if s.currentRunID == "" {
				return nil, &persistence.CurrentWorkflowConditionFailedError{Msg: "no current record"}
			}
			current := s.runs[s.currentRunID]
			return &persistence.GetCurrentExecutionResponse{
				RunID:  s.currentRunID,
				State:  current.ExecutionState.State,
				Status: current.ExecutionState.Status,
			}, nil
		}).AnyTimes()

	mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			request *persistence.GetWorkflowExecutionRequest,
		) (*persistence.GetWorkflowExecutionResponse, error) {
			state, ok := s.runs[request.RunID]
			if !ok {
				return nil, &persistence.WorkflowConditionFailedError{Msg: "run not found"}
			}
			// A real store deserializes a fresh copy; handing out the stored pointer would let
			// in-memory mutations (SuppressBy, Revive) rewrite the "database" without a write.
			return &persistence.GetWorkflowExecutionResponse{
				State:           proto.Clone(state).(*persistencespb.WorkflowMutableState),
				DBRecordVersion: 1,
			}, nil
		}).AnyTimes()

	mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			request *persistence.CreateWorkflowExecutionRequest,
		) (*persistence.CreateWorkflowExecutionResponse, error) {
			s.lastCreateMode = request.Mode
			snapshot := request.NewWorkflowSnapshot
			s.applySnapshot(snapshot)
			// CreateWorkflowModeBypassCurrent leaves the current record alone; the other modes are the
			// ones that move it.
			if request.Mode != persistence.CreateWorkflowModeBypassCurrent {
				s.currentRunID = snapshot.ExecutionState.RunId
			}
			return &persistence.CreateWorkflowExecutionResponse{}, nil
		}).AnyTimes()

	mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			request *persistence.UpdateWorkflowExecutionRequest,
		) (*persistence.UpdateWorkflowExecutionResponse, error) {
			s.lastUpdate = request
			mutation := request.UpdateWorkflowMutation
			if request.Mode == persistence.UpdateWorkflowModeUpdateCurrent &&
				mutation.ExecutionState.RunId != s.currentRunID {
				return nil, &persistence.CurrentWorkflowConditionFailedError{
					Msg: fmt.Sprintf(
						"UpdateWorkflowExecution encountered runID mismatch: actual run ID: %v, request run ID: %v",
						s.currentRunID, mutation.ExecutionState.RunId,
					),
					RunID: s.currentRunID,
				}
			}
			s.applyMutation(mutation)
			if request.NewWorkflowSnapshot != nil {
				snapshot := request.NewWorkflowSnapshot
				s.applySnapshot(*snapshot)
			}
			if request.Mode == persistence.UpdateWorkflowModeUpdateCurrent {
				if request.NewWorkflowSnapshot != nil {
					s.currentRunID = request.NewWorkflowSnapshot.ExecutionState.RunId
				} else {
					s.currentRunID = mutation.ExecutionState.RunId
				}
			}
			return &persistence.UpdateWorkflowExecutionResponse{
				UpdateMutableStateStats: persistence.MutableStateStatistics{
					HistoryStatistics: &persistence.HistoryStatistics{},
				},
			}, nil
		}).AnyTimes()

	mockExecutionMgr.EXPECT().ConflictResolveWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			request *persistence.ConflictResolveWorkflowExecutionRequest,
		) (*persistence.ConflictResolveWorkflowExecutionResponse, error) {
			s.lastConflictResolve = request
			if request.Mode == persistence.ConflictResolveWorkflowModeUpdateCurrent {
				s.lastConflictResolveActualCurrentRunID = s.currentRunID
				expectedCurrentRunID := request.ExpectedCurrentRunID
				if expectedCurrentRunID == "" {
					expectedCurrentRunID = request.ResetWorkflowSnapshot.ExecutionState.RunId
					if request.CurrentWorkflowMutation != nil {
						expectedCurrentRunID = request.CurrentWorkflowMutation.ExecutionState.RunId
					}
				}
				s.lastConflictResolveExpectedRunID = expectedCurrentRunID
				if expectedCurrentRunID != s.currentRunID {
					return nil, &persistence.CurrentWorkflowConditionFailedError{
						Msg: fmt.Sprintf(
							"ConflictResolveWorkflowExecution encountered runID mismatch: actual run ID: %v, request run ID: %v",
							s.currentRunID, expectedCurrentRunID,
						),
						RunID: s.currentRunID,
					}
				}
				s.currentRunID = request.ResetWorkflowSnapshot.ExecutionState.RunId
			}
			if request.CurrentWorkflowMutation != nil {
				s.applyMutation(*request.CurrentWorkflowMutation)
			}
			reset := request.ResetWorkflowSnapshot
			s.applySnapshot(reset)
			return &persistence.ConflictResolveWorkflowExecutionResponse{
				ResetMutableStateStats: persistence.MutableStateStatistics{
					HistoryStatistics: &persistence.HistoryStatistics{},
				},
			}, nil
		}).AnyTimes()
}
