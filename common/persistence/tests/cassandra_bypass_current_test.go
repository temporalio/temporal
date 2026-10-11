package tests

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
)

// beforeBatchSession inserts another transaction after the bypass-current read and
// before its conditional batch, making that concurrent ordering deterministic.
type beforeBatchSession struct {
	gocql.Session
	beforeBatch func()
}

func (s *beforeBatchSession) MapExecuteBatchCAS(batch *gocql.Batch, previous map[string]any) (bool, gocql.Iter, error) {
	if f := s.beforeBatch; f != nil {
		s.beforeBatch = nil
		f()
	}
	return s.Session.MapExecuteBatchCAS(batch, previous)
}

func TestCassandraBypassCurrentExecution(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	require.NoError(t, err)
	executionStore, err := testData.Factory.NewExecutionStore()
	require.NoError(t, err)
	store := executionStore.(*cassandra.ExecutionStore)
	session := &beforeBatchSession{Session: store.MutableStateStore.Session}
	store.MutableStateStore.Session = session

	var shardID int32
	for _, operation := range []string{"update", "conflict resolve"} {
		for _, transition := range []string{"promote target", "create same run", "create different run", "change shard owner"} {
			t.Run(operation+"/"+transition, func(t *testing.T) {
				s := NewExecutionMutableStateSuite(t, shardStore, executionStore, serialization.NewSerializer(), testData.Logger)
				s.SetT(t)
				s.ShardID = shardID
				s.SetupTest()
				shardID = s.ShardID
				defer s.TearDownTest()

				var current *p.WorkflowSnapshot
				if transition == "promote target" {
					_, current, _ = s.CreateWorkflow(1, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
						enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, 1)
				}
				targetRunID := uuid.NewString()
				target, _ := RandomSnapshot(t, s.NamespaceID, s.WorkflowID, targetRunID, common.FirstEventID, 1,
					enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, 1, nil)
				_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
					ShardID: s.ShardID, RangeID: s.RangeID, Mode: p.CreateWorkflowModeBypassCurrent,
					ArchetypeID: chasm.WorkflowArchetypeID, NewWorkflowSnapshot: *target,
				})
				require.NoError(t, err)

				mutation, _ := RandomMutation(t, s.NamespaceID, s.WorkflowID, targetRunID, target.NextEventID, 1,
					enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, 2, nil)
				reset, _ := RandomSnapshot(t, s.NamespaceID, s.WorkflowID, targetRunID, target.NextEventID, 1,
					enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, 2, nil)
				mutation.Condition = target.NextEventID
				reset.Condition = target.NextEventID

				var promoted *p.WorkflowSnapshot
				var newCurrentRunID string
				interleaved := false
				session.beforeBatch = func() {
					interleaved = true
					switch transition {
					case "promote target":
						promoted, _ = RandomSnapshot(t, s.NamespaceID, s.WorkflowID, targetRunID, common.FirstEventID, 2,
							enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, 2, nil)
						// Promotion can leave next_event_id unchanged; DBRecordVersion must still fence the bypass writer.
						require.Equal(t, target.NextEventID, promoted.NextEventID)
						promoted.Condition = target.NextEventID
						currentMutation, _ := RandomMutation(t, s.NamespaceID, s.WorkflowID, current.ExecutionState.RunId,
							current.NextEventID, 1, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
							enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, 2, nil)
						currentMutation.Condition = current.NextEventID
						_, err := s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
							ShardID: s.ShardID, RangeID: s.RangeID, Mode: p.ConflictResolveWorkflowModeUpdateCurrent,
							ArchetypeID: chasm.WorkflowArchetypeID, ResetWorkflowSnapshot: *promoted,
							CurrentWorkflowMutation: currentMutation,
						})
						require.NoError(t, err)
						newCurrentRunID = targetRunID
					case "create same run", "create different run":
						runID := targetRunID
						if transition == "create different run" {
							runID = uuid.NewString()
						}
						snapshot, _ := RandomSnapshot(t, s.NamespaceID, s.WorkflowID, runID, common.FirstEventID, 2,
							enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, 1, nil)
						_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
							ShardID: s.ShardID, RangeID: s.RangeID, Mode: p.CreateWorkflowModeBrandNew,
							ArchetypeID: chasm.WorkflowArchetypeID, NewWorkflowSnapshot: *snapshot,
						})
						if transition == "create same run" {
							require.ErrorAs(t, err, new(*p.WorkflowConditionFailedError))
						} else {
							require.NoError(t, err)
							newCurrentRunID = runID
						}
					case "change shard owner":
						err := s.ShardManager.UpdateShard(s.Ctx, &p.UpdateShardRequest{
							ShardInfo:       &persistencespb.ShardInfo{ShardId: s.ShardID, RangeId: s.RangeID + 1},
							PreviousRangeID: s.RangeID,
						})
						require.NoError(t, err)
					default:
						require.FailNow(t, "unknown transition", transition)
					}
				}

				if operation == "update" {
					_, err = s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
						ShardID: s.ShardID, RangeID: s.RangeID, Mode: p.UpdateWorkflowModeBypassCurrent,
						ArchetypeID: chasm.WorkflowArchetypeID, UpdateWorkflowMutation: *mutation,
					})
				} else {
					_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
						ShardID: s.ShardID, RangeID: s.RangeID, Mode: p.ConflictResolveWorkflowModeBypassCurrent,
						ArchetypeID: chasm.WorkflowArchetypeID, ResetWorkflowSnapshot: *reset,
					})
				}
				require.True(t, interleaved, "the bypass precheck must reach the conditional batch")
				switch transition {
				case "promote target":
					require.ErrorAs(t, err, new(*p.WorkflowConditionFailedError))
					s.AssertMSEqualWithDB(chasm.WorkflowArchetypeID, promoted)
				case "change shard owner":
					require.ErrorAs(t, err, new(*p.ShardOwnershipLostError))
					s.AssertMSEqualWithDB(chasm.WorkflowArchetypeID, target)
				default:
					require.NoError(t, err)
					if operation == "update" {
						s.AssertMSEqualWithDB(chasm.WorkflowArchetypeID, target, mutation)
					} else {
						s.AssertMSEqualWithDB(chasm.WorkflowArchetypeID, reset)
					}
				}
				currentResponse, err := s.ExecutionManager.GetCurrentExecution(s.Ctx, &p.GetCurrentExecutionRequest{
					ShardID: s.ShardID, NamespaceID: s.NamespaceID, WorkflowID: s.WorkflowID,
					ArchetypeID: chasm.WorkflowArchetypeID,
				})
				if newCurrentRunID == "" {
					require.ErrorAs(t, err, new(*serviceerror.NotFound))
				} else {
					require.NoError(t, err)
					require.Equal(t, newCurrentRunID, currentResponse.RunID)
				}
			})
		}
	}
}
