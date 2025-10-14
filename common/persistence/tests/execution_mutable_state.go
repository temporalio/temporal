package tests

import (
	"context"
	"fmt"
	"maps"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
)

type (
	ExecutionMutableStateSuite struct {
		suite.Suite
		*require.Assertions
		protorequire.ProtoAssertions

		ShardID     int32
		RangeID     int64
		NamespaceID string
		WorkflowID  string
		RunID       string

		ShardManager      p.ShardManager
		ExecutionManager  p.ExecutionManager
		historyBranchUtil p.HistoryBranchUtil
		Logger            log.Logger

		Ctx    context.Context
		Cancel context.CancelFunc
	}
)

func NewExecutionMutableStateSuite(
	t *testing.T,
	shardStore p.ShardStore,
	executionStore p.ExecutionStore,
	serializer serialization.Serializer,
	historyBranchUtil p.HistoryBranchUtil,
	logger log.Logger,
) *ExecutionMutableStateSuite {
	return &ExecutionMutableStateSuite{
		Assertions:      require.New(t),
		ProtoAssertions: protorequire.New(t),
		ShardManager: p.NewShardManager(
			shardStore,
			serializer,
		),
		ExecutionManager: p.NewExecutionManager(
			executionStore,
			serializer,
			nil,
			logger,
			dynamicconfig.GetIntPropertyFn(4*1024*1024),
			dynamicconfig.GetBoolPropertyFn(false),
		),
		historyBranchUtil: historyBranchUtil,
		Logger:            logger,
	}
}

func (s *ExecutionMutableStateSuite) SetupSuite() {
}

func (s *ExecutionMutableStateSuite) TearDownSuite() {
}

func (s *ExecutionMutableStateSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.Ctx, s.Cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)

	s.ShardID++
	resp, err := s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID: s.ShardID,
		InitialShardInfo: &persistencespb.ShardInfo{
			ShardId: s.ShardID,
			RangeId: 1,
		},
	})
	s.NoError(err)
	previousRangeID := resp.ShardInfo.RangeId
	resp.ShardInfo.RangeId++
	err = s.ShardManager.UpdateShard(s.Ctx, &p.UpdateShardRequest{
		ShardInfo:       resp.ShardInfo,
		PreviousRangeID: previousRangeID,
	})
	s.NoError(err)
	s.RangeID = resp.ShardInfo.RangeId

	s.NamespaceID = uuid.New().String()
	s.WorkflowID = uuid.New().String()
	s.RunID = uuid.New().String()
}

func (s *ExecutionMutableStateSuite) TearDownTest() {
	s.Cancel()
}

func (s *ExecutionMutableStateSuite) TestCreate_BrandNew() {
	branchToken, newSnapshot, newEvents := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertHEEqualWithDB(branchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestCreate_BrandNew_CHASM() {
	// CHASM snapshot has no events and empty current version history.
	newSnapshot := s.CreateCHASMSnapshot(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	s.AssertMSEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestCreate_BrandNew_CurrentConflict() {
	lastWriteVersion := rand.Int63()
	branchToken, newSnapshot, newEvents := s.CreateWorkflow(
		lastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	// Remember original execution stats because the CreateWorkflowExecution mutates the stats before failing to persist
	executionStats, ok := proto.Clone(newSnapshot.ExecutionInfo.ExecutionStats).(*persistencespb.ExecutionStats)
	s.True(ok)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBrandNew,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   newEvents,
	})
	if err, ok := err.(*p.CurrentWorkflowConditionFailedError); ok {
		err.Msg = ""
	}
	s.DeepEqual(&p.CurrentWorkflowConditionFailedError{
		Msg:              "",
		RequestIDs:       newSnapshot.ExecutionState.RequestIds,
		RunID:            newSnapshot.ExecutionState.RunId,
		State:            newSnapshot.ExecutionState.State,
		Status:           newSnapshot.ExecutionState.Status,
		LastWriteVersion: lastWriteVersion,
		StartTime:        timestamp.TimeValuePtr(newSnapshot.ExecutionState.StartTime),
	}, err)

	// Restore origin execution stats so GetWorkflowExecution matches with the pre-failed snapshot stats above
	newSnapshot.ExecutionInfo.ExecutionStats = executionStats
	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertHEEqualWithDB(branchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestCreate_Reuse() {
	prevLastWriteVersion := rand.Int63()
	_, prevSnapshot, _ := s.CreateWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	newRunID := uuid.New().String()
	newBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, newRunID, s.historyBranchUtil)
	newSnapshot, newEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		newRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		newBranchToken,
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeUpdateCurrent,

		PreviousRunID:            prevSnapshot.ExecutionState.RunId,
		PreviousLastWriteVersion: prevLastWriteVersion,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   newEvents,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertHEEqualWithDB(newBranchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestCreate_Reuse_CHASM() {
	// CHASM snapshot has no events and empty current version history.
	prevLastWriteVersion := rand.Int63()
	prevSnapshot := s.CreateCHASMSnapshot(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	newRunID := uuid.New().String()
	newSnapshot, newEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		newRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		nil, // CHASM snapshot has no events
	)
	s.Empty(newEvents)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeUpdateCurrent,

		PreviousRunID:            prevSnapshot.ExecutionState.RunId,
		PreviousLastWriteVersion: prevLastWriteVersion,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   newEvents,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestCreate_Reuse_CurrentConflict() {
	prevLastWriteVersion := rand.Int63()
	branchToken, prevSnapshot, prevEvents := s.CreateWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	// Remember original execution stats because the CreateWorkflowExecution mutates the stats before failing to persist
	executionStats, ok := proto.Clone(prevSnapshot.ExecutionInfo.ExecutionStats).(*persistencespb.ExecutionStats)
	s.True(ok)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeUpdateCurrent,

		PreviousRunID:            uuid.New().String(),
		PreviousLastWriteVersion: rand.Int63(),

		NewWorkflowSnapshot: *prevSnapshot,
		NewWorkflowEvents:   prevEvents,
	})
	if err, ok := err.(*p.CurrentWorkflowConditionFailedError); ok {
		err.Msg = ""
	}
	s.DeepEqual(&p.CurrentWorkflowConditionFailedError{
		Msg:              "",
		RequestIDs:       prevSnapshot.ExecutionState.RequestIds,
		RunID:            prevSnapshot.ExecutionState.RunId,
		State:            prevSnapshot.ExecutionState.State,
		Status:           prevSnapshot.ExecutionState.Status,
		LastWriteVersion: prevLastWriteVersion,
		StartTime:        timestamp.TimeValuePtr(prevSnapshot.ExecutionState.StartTime),
	}, err)

	// Restore origin execution stats so GetWorkflowExecution matches with the pre-failed snapshot stats above
	prevSnapshot.ExecutionInfo.ExecutionStats = executionStats
	s.AssertMSEqualWithDB(prevSnapshot)
	s.AssertHEEqualWithDB(branchToken, prevEvents)
}

func (s *ExecutionMutableStateSuite) TestCreate_Zombie() {
	prevLastWriteVersion := rand.Int63()
	s.CreateWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	newRunID := uuid.New().String()
	newBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, newRunID, s.historyBranchUtil)
	newSnapshot, newEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		newRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		newBranchToken,
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   newEvents,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertHEEqualWithDB(newBranchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestCreate_Conflict() {
	lastWriteVersion := rand.Int63()
	_, newSnapshot, newEvents := s.CreateWorkflow(
		lastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeUpdateCurrent,

		PreviousRunID:            newSnapshot.ExecutionState.RunId,
		PreviousLastWriteVersion: lastWriteVersion,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   newEvents,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)
}

func (s *ExecutionMutableStateSuite) TestCreate_ClosedWorkflow_BrandNew() {
	branchToken, newSnapshot, newEvents := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		rand.Int63(),
	)

	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertHEEqualWithDB(branchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestCreate_ClosedWorkflow_Bypass() {
	prevLastWriteVersion := rand.Int63()
	s.CreateWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	newRunID := uuid.New().String()
	newBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, newRunID, s.historyBranchUtil)
	newSnapshot, newEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		newRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		rand.Int63(),
		newBranchToken,
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   newEvents,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertHEEqualWithDB(newBranchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestCreate_ClosedWorkflow_UpdateCurrent() {
	prevLastWriteVersion := rand.Int63()
	_, prevSnapshot, _ := s.CreateWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	newRunID := uuid.New().String()
	newBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, newRunID, s.historyBranchUtil)
	newSnapshot, newEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		newRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
		newBranchToken,
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeUpdateCurrent,

		PreviousRunID:            prevSnapshot.ExecutionState.RunId,
		PreviousLastWriteVersion: prevLastWriteVersion,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   newEvents,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertHEEqualWithDB(newBranchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestUpdate_NotZombie() {
	branchToken, newSnapshot, newEvents := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	currentMutation, currentEvents := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		newSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		newSnapshot.DBRecordVersion+1,
		branchToken,
	)
	_, err := s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   currentEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(newSnapshot, currentMutation)
	s.AssertHEEqualWithDB(branchToken, newEvents, currentEvents)
}

func (s *ExecutionMutableStateSuite) TestUpdate_NotZombie_CHASM() {
	newSnapshot := s.CreateCHASMSnapshot(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	currentMutation, currentEvents := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		newSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		newSnapshot.DBRecordVersion+1,
		nil, // No branch token for CHASM
	)
	_, err := s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   currentEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(newSnapshot, currentMutation)
}

func (s *ExecutionMutableStateSuite) TestUpdate_NotZombie_CurrentConflict() {
	s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	// Mutation for a different run of the same workflowID
	// not related to the workflow created above
	newRunID := uuid.New().String()
	newBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, newRunID, s.historyBranchUtil)
	currentMutation, currentEvents := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		newRunID,
		rand.Int63(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		newBranchToken,
	)
	_, err := s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   currentEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)

	s.AssertMissingFromDB(
		currentMutation.ExecutionInfo.NamespaceId,
		currentMutation.ExecutionInfo.WorkflowId,
		currentMutation.ExecutionState.RunId,
	)
}

func (s *ExecutionMutableStateSuite) TestUpdate_NotZombie_Conflict() {
	branchToken, newSnapshot, newEvents := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	currentMutation, currentEvents := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		newSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		branchToken,
	)
	_, err := s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   currentEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertHEPrefixWithDB(branchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestUpdate_NotZombie_WithNew() {
	branchToken, currentSnapshot, currentEvents := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	updateMutation, updateEvents := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		currentSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		currentSnapshot.DBRecordVersion+1,
		branchToken,
	)

	newRunID := uuid.New().String()
	newBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, newRunID, s.historyBranchUtil)
	newSnapshot, newEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		newRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		newBranchToken,
	)
	_, err := s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,

		UpdateWorkflowMutation: *updateMutation,
		UpdateWorkflowEvents:   updateEvents,

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   newEvents,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(currentSnapshot, updateMutation)
	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertHEEqualWithDB(branchToken, currentEvents, updateEvents)
	s.AssertHEEqualWithDB(newBranchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestUpdate_Zombie() {
	s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	zombieRunID := uuid.New().String()
	zombieBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, zombieRunID, s.historyBranchUtil)
	zombieSnapshot, zombieEvents1 := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		zombieRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		zombieBranchToken,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *zombieSnapshot,
		NewWorkflowEvents:   zombieEvents1,
	})
	s.NoError(err)

	zombieMutation, zombieEvents2 := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		zombieRunID,
		zombieSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		zombieSnapshot.DBRecordVersion+1,
		zombieBranchToken,
	)
	_, err = s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,

		UpdateWorkflowMutation: *zombieMutation,
		UpdateWorkflowEvents:   zombieEvents2,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(zombieSnapshot, zombieMutation)
	s.AssertHEEqualWithDB(zombieBranchToken, zombieEvents1, zombieEvents2)
}

func (s *ExecutionMutableStateSuite) TestUpdate_Zombie_CurrentConflict() {
	branchToken, newSnapshot, newEvents := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	currentMutation, currentEvents := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		newSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		newSnapshot.DBRecordVersion+1,
		branchToken,
	)
	_, err := s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   currentEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)

	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertHEPrefixWithDB(branchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestUpdate_Zombie_Conflict() {
	s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	zombieRunID := uuid.New().String()
	zombieBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, zombieRunID, s.historyBranchUtil)
	zombieSnapshot, zombieEvents1 := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		zombieRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		zombieBranchToken,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *zombieSnapshot,
		NewWorkflowEvents:   zombieEvents1,
	})
	s.NoError(err)

	zombieMutation, zombieEvents2 := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		zombieRunID,
		zombieSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		zombieBranchToken,
	)
	_, err = s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,

		UpdateWorkflowMutation: *zombieMutation,
		UpdateWorkflowEvents:   zombieEvents2,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.AssertMSEqualWithDB(zombieSnapshot)
	s.AssertHEPrefixWithDB(zombieBranchToken, zombieEvents1)
}

func (s *ExecutionMutableStateSuite) TestUpdate_Zombie_WithNew() {
	s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	zombieRunID := uuid.New().String()
	zombieBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, zombieRunID, s.historyBranchUtil)
	zombieSnapshot, zombieEvents1 := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		zombieRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		zombieBranchToken,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *zombieSnapshot,
		NewWorkflowEvents:   zombieEvents1,
	})
	s.NoError(err)

	zombieMutation, zombieEvents2 := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		zombieRunID,
		zombieSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		zombieSnapshot.DBRecordVersion+1,
		zombieBranchToken,
	)
	newRunID := uuid.New().String()
	newBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, newRunID, s.historyBranchUtil)
	newZombieSnapshot, newEvents3 := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		newRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		newBranchToken,
	)
	_, err = s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,

		UpdateWorkflowMutation: *zombieMutation,
		UpdateWorkflowEvents:   zombieEvents2,

		NewWorkflowSnapshot: newZombieSnapshot,
		NewWorkflowEvents:   newEvents3,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(zombieSnapshot, zombieMutation)
	s.AssertMSEqualWithDB(newZombieSnapshot)
	s.AssertHEEqualWithDB(zombieBranchToken, zombieEvents1, zombieEvents2)
	s.AssertHEEqualWithDB(newBranchToken, newEvents3)
}

func (s *ExecutionMutableStateSuite) TestUpdate_ClosedWorkflow_IsCurrent() {
	branchToken, newSnapshot, newEvents := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	// NOTE: no new events for closed workflows
	currentMutation, _ := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		newSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		newSnapshot.DBRecordVersion+1,
		branchToken,
	)
	_, err := s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeIgnoreCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(newSnapshot, currentMutation)
	s.AssertHEEqualWithDB(branchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestUpdate_ClosedWorkflow_IsNonCurrent() {
	nonCurrentLastWriteVersion := rand.Int63()
	nonCurrentBranchToken, nonCurrentSnapshot, nonCurrentEvents := s.CreateWorkflow(
		nonCurrentLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	// make current workflow to a different run
	currentRunID := uuid.New().String()
	currentBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, currentRunID, s.historyBranchUtil)
	currentSnapshot, currentEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		currentRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		currentBranchToken,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeUpdateCurrent,

		PreviousRunID:            nonCurrentSnapshot.ExecutionState.RunId,
		PreviousLastWriteVersion: nonCurrentLastWriteVersion,

		NewWorkflowSnapshot: *currentSnapshot,
		NewWorkflowEvents:   currentEvents,
	})
	s.NoError(err)

	// Update the original closed workflow
	// NOTE: no new events for closed workflows
	nonCurrentMutation, _ := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		nonCurrentSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		nonCurrentSnapshot.DBRecordVersion+1,
		nonCurrentBranchToken,
	)
	_, err = s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeIgnoreCurrent,

		UpdateWorkflowMutation: *nonCurrentMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(nonCurrentSnapshot, nonCurrentMutation)
	s.AssertHEEqualWithDB(nonCurrentBranchToken, nonCurrentEvents)
	s.AssertMSEqualWithDB(currentSnapshot)
	s.AssertHEEqualWithDB(currentBranchToken, currentEvents)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent() {
	branchToken, currentSnapshot, currentEvents1 := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	baseRunID := uuid.New().String()
	baseBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, baseRunID, s.historyBranchUtil)
	baseSnapshot, baseEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		baseBranchToken,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   baseEvents,
	})
	s.NoError(err)

	resetSnapshot, resetEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		baseSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
		baseBranchToken,
	)
	currentMutation, currentEvents2 := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		currentSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
		branchToken,
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   resetEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   currentEvents2,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(resetSnapshot)
	s.AssertMSEqualWithDB(currentSnapshot, currentMutation)
	s.AssertHEEqualWithDB(baseBranchToken, baseEvents, resetEvents)
	s.AssertHEEqualWithDB(branchToken, currentEvents1, currentEvents2)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent_CurrentConflict() {
	_, currentSnapshot, _ := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	baseRunID := uuid.New().String()
	baseBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, baseRunID, s.historyBranchUtil)
	baseSnapshot, baseEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		baseBranchToken,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   baseEvents,
	})
	s.NoError(err)

	resetSnapshot, resetEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		baseSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
		baseBranchToken,
	)
	currentRunID := uuid.New().String()
	currentBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, currentRunID, s.historyBranchUtil)
	currentMutation, currentEvents := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		currentRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		currentBranchToken,
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   resetEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   currentEvents,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)

	s.AssertMSEqualWithDB(baseSnapshot)
	s.AssertMSEqualWithDB(currentSnapshot)
	s.AssertHEPrefixWithDB(baseBranchToken, baseEvents)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent_Conflict_Case1() {
	branchToken, currentSnapshot, _ := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	baseRunID := uuid.New().String()
	baseBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, baseRunID, s.historyBranchUtil)
	baseSnapshot, baseEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		baseBranchToken,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   baseEvents,
	})
	s.NoError(err)

	resetSnapshot, resetEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		baseSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
		baseBranchToken,
	)
	currentMutation, currentEvents2 := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		currentSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		branchToken,
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   resetEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   currentEvents2,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.AssertMSEqualWithDB(baseSnapshot)
	s.AssertMSEqualWithDB(currentSnapshot)
	s.AssertHEPrefixWithDB(baseBranchToken, baseEvents)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent_Conflict_Case2() {
	branchToken, currentSnapshot, _ := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	baseRunID := uuid.New().String()
	baseBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, baseRunID, s.historyBranchUtil)
	baseSnapshot, baseEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		baseBranchToken,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   baseEvents,
	})
	s.NoError(err)

	resetSnapshot, resetEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		baseSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		baseBranchToken,
	)
	currentMutation, currentEvents2 := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		currentSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
		branchToken,
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   resetEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   currentEvents2,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.AssertMSEqualWithDB(baseSnapshot)
	s.AssertMSEqualWithDB(currentSnapshot)
	s.AssertHEPrefixWithDB(baseBranchToken, baseEvents)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent_WithNew() {
	branchToken, currentSnapshot, currentEvents1 := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	baseRunID := uuid.New().String()
	baseBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, baseRunID, s.historyBranchUtil)
	baseSnapshot, baseEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		baseBranchToken,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   baseEvents,
	})
	s.NoError(err)

	resetSnapshot, resetEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		baseSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		baseSnapshot.DBRecordVersion+1,
		baseBranchToken,
	)
	newRunID := uuid.New().String()
	newBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, newRunID, s.historyBranchUtil)
	newSnapshot, newEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		newRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		newBranchToken,
	)
	currentMutation, currentEvents2 := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		newSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
		branchToken,
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   resetEvents,

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   newEvents,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   currentEvents2,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(resetSnapshot)
	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertMSEqualWithDB(currentSnapshot, currentMutation)
	s.AssertHEEqualWithDB(baseBranchToken, baseEvents, resetEvents)
	s.AssertHEEqualWithDB(newBranchToken, newEvents)
	s.AssertHEEqualWithDB(branchToken, currentEvents1, currentEvents2)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent_WithNew_CHASM() {
	currentSnapshot := s.CreateCHASMSnapshot(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	baseRunID := uuid.New().String()
	baseSnapshot, baseEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		nil,
	)
	s.Empty(baseEvents)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   baseEvents,
	})
	s.NoError(err)

	resetSnapshot, resetEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		baseSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		baseSnapshot.DBRecordVersion+1,
		nil,
	)
	s.Empty(resetEvents)

	newRunID := uuid.New().String()
	newSnapshot, newEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		newRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		nil,
	)
	s.Empty(newEvents)

	currentMutation, currentEvents := RandomMutation(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		newSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
		nil,
	)
	s.Empty(currentEvents)

	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   resetEvents,

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   newEvents,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   currentEvents,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(resetSnapshot)
	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertMSEqualWithDB(currentSnapshot, currentMutation)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_ResetCurrent() {
	branchToken, baseSnapshot, baseEvents := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	resetSnapshot, resetEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		baseSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
		branchToken,
	)
	_, err := s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   resetEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(resetSnapshot)
	s.AssertHEEqualWithDB(branchToken, baseEvents, resetEvents)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_ResetCurrent_CurrentConflict() {
	s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	baseRunID := uuid.New().String()
	baseBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, baseRunID, s.historyBranchUtil)
	baseSnapshot, baseEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		baseBranchToken,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   baseEvents,
	})
	s.NoError(err)

	resetSnapshot, resetEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		baseSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
		baseBranchToken,
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   resetEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)

	s.AssertMSEqualWithDB(baseSnapshot)
	s.AssertHEPrefixWithDB(baseBranchToken, baseEvents)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_ResetCurrent_Conflict() {
	branchToken, baseSnapshot, baseEvents := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	resetSnapshot, resetEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		baseSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		branchToken,
	)
	_, err := s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   resetEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.AssertMSEqualWithDB(baseSnapshot)
	s.AssertHEPrefixWithDB(branchToken, baseEvents)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_ResetCurrent_WithNew() {
	branchToken, baseSnapshot, baseEvents := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	resetSnapshot, resetEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		baseSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		baseSnapshot.DBRecordVersion+1,
		branchToken,
	)
	newRunID := uuid.New().String()
	newBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, newRunID, s.historyBranchUtil)
	newSnapshot, newEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		newRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		newBranchToken,
	)
	_, err := s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   resetEvents,

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   newEvents,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(resetSnapshot)
	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertHEEqualWithDB(branchToken, baseEvents, resetEvents)
	s.AssertHEEqualWithDB(newBranchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_Zombie() {
	s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	baseRunID := uuid.New().String()
	baseBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, baseRunID, s.historyBranchUtil)
	baseSnapshot, baseEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		baseBranchToken,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   baseEvents,
	})
	s.NoError(err)

	resetSnapshot, resetEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		baseSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
		baseBranchToken,
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   resetEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(resetSnapshot)
	s.AssertHEEqualWithDB(baseBranchToken, baseEvents, resetEvents)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_Zombie_CurrentConflict() {
	branchToken, baseSnapshot, baseEvents := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	resetSnapshot, resetEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		baseSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
		branchToken,
	)
	_, err := s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   resetEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)

	s.AssertMSEqualWithDB(baseSnapshot)
	s.AssertHEPrefixWithDB(branchToken, baseEvents)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_Zombie_Conflict() {
	s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	baseRunID := uuid.New().String()
	baseBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, baseRunID, s.historyBranchUtil)
	baseSnapshot, baseEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		baseBranchToken,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   baseEvents,
	})
	s.NoError(err)

	resetSnapshot, resetEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		baseSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		baseBranchToken,
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   resetEvents,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.AssertMSEqualWithDB(baseSnapshot)
	s.AssertHEPrefixWithDB(baseBranchToken, baseEvents)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_Zombie_WithNew() {
	s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	baseRunID := uuid.New().String()
	baseBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, baseRunID, s.historyBranchUtil)
	baseSnapshot, baseEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		baseBranchToken,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   baseEvents,
	})
	s.NoError(err)

	resetSnapshot, resetEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		baseRunID,
		baseSnapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		baseSnapshot.DBRecordVersion+1,
		baseBranchToken,
	)
	newRunID := uuid.New().String()
	newBranchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, newRunID, s.historyBranchUtil)
	newSnapshot, newEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		newRunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		newBranchToken,
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   resetEvents,

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   newEvents,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(resetSnapshot)
	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertHEEqualWithDB(baseBranchToken, baseEvents, resetEvents)
	s.AssertHEEqualWithDB(newBranchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestSet_NotExists() {
	branchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, s.RunID, s.historyBranchUtil)
	setSnapshot, _ := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		branchToken,
	)
	_, err := s.ExecutionManager.SetWorkflowExecution(s.Ctx, &p.SetWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,

		SetWorkflowSnapshot: *setSnapshot,
	})
	s.IsType(&p.ConditionFailedError{}, err)

	s.AssertMissingFromDB(s.NamespaceID, s.WorkflowID, s.RunID)
}

func (s *ExecutionMutableStateSuite) TestSet_Conflict() {
	branchToken, snapshot, events := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	setSnapshot, _ := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		snapshot.NextEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		branchToken,
	)
	_, err := s.ExecutionManager.SetWorkflowExecution(s.Ctx, &p.SetWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,

		SetWorkflowSnapshot: *setSnapshot,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.AssertMSEqualWithDB(snapshot)
	s.AssertHEEqualWithDB(branchToken, events)
}

func (s *ExecutionMutableStateSuite) TestSet() {
	branchToken, snapshot, events := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	setSnapshot, _ := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		snapshot.DBRecordVersion+1,
		branchToken,
	)
	_, err := s.ExecutionManager.SetWorkflowExecution(s.Ctx, &p.SetWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,

		SetWorkflowSnapshot: *setSnapshot,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(setSnapshot)
	s.AssertHEEqualWithDB(branchToken, events)
}

func (s *ExecutionMutableStateSuite) TestSet_CHASM() {
	snapshot := s.CreateCHASMSnapshot(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	setSnapshot, _ := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		snapshot.DBRecordVersion+1,
		nil, // CHASM snapshot has no events
	)
	_, err := s.ExecutionManager.SetWorkflowExecution(s.Ctx, &p.SetWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,

		SetWorkflowSnapshot: *setSnapshot,
	})
	s.NoError(err)

	s.AssertMSEqualWithDB(setSnapshot)
}

func (s *ExecutionMutableStateSuite) TestDeleteCurrent_IsCurrent() {
	branchToken, newSnapshot, newEvents := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	err := s.ExecutionManager.DeleteCurrentWorkflowExecution(s.Ctx, &p.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: s.NamespaceID,
		WorkflowID:  s.WorkflowID,
		RunID:       s.RunID,
	})
	s.NoError(err)

	_, err = s.ExecutionManager.GetCurrentExecution(s.Ctx, &p.GetCurrentExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: s.NamespaceID,
		WorkflowID:  s.WorkflowID,
	})
	s.IsType(&serviceerror.NotFound{}, err)
	s.EqualError(err, "workflow not found for ID: "+s.WorkflowID)

	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertHEEqualWithDB(branchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestDeleteCurrent_NotCurrent() {
	branchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, s.RunID, s.historyBranchUtil)
	newSnapshot, newEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		common.FirstEventID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		branchToken,
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   newEvents,
	})
	s.NoError(err)

	err = s.ExecutionManager.DeleteCurrentWorkflowExecution(s.Ctx, &p.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: s.NamespaceID,
		WorkflowID:  s.WorkflowID,
		RunID:       s.RunID,
	})
	s.NoError(err)

	_, err = s.ExecutionManager.GetCurrentExecution(s.Ctx, &p.GetCurrentExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: s.NamespaceID,
		WorkflowID:  s.WorkflowID,
	})
	s.IsType(&serviceerror.NotFound{}, err)
	s.EqualError(err, "workflow not found for ID: "+s.WorkflowID)

	s.AssertMSEqualWithDB(newSnapshot)
	s.AssertHEEqualWithDB(branchToken, newEvents)
}

func (s *ExecutionMutableStateSuite) TestDelete_Exists() {
	branchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, s.RunID, s.historyBranchUtil)
	newSnapshot, newEvents := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
		branchToken,
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   newEvents,
	})
	s.NoError(err)

	err = s.ExecutionManager.DeleteWorkflowExecution(s.Ctx, &p.DeleteWorkflowExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: s.NamespaceID,
		WorkflowID:  s.WorkflowID,
		RunID:       s.RunID,
	})
	s.NoError(err)

	s.AssertMissingFromDB(s.NamespaceID, s.WorkflowID, s.RunID)
}

func (s *ExecutionMutableStateSuite) TestDelete_NotExists() {
	err := s.ExecutionManager.DeleteWorkflowExecution(s.Ctx, &p.DeleteWorkflowExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: s.NamespaceID,
		WorkflowID:  s.WorkflowID,
		RunID:       s.RunID,
	})
	s.NoError(err)

	s.AssertMissingFromDB(s.NamespaceID, s.WorkflowID, s.RunID)
}

func (s *ExecutionMutableStateSuite) CreateWorkflow(
	lastWriteVersion int64,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	dbRecordVersion int64,
) ([]byte, *p.WorkflowSnapshot, []*p.WorkflowEvents) {
	branchToken := RandomBranchToken(s.NamespaceID, s.WorkflowID, s.RunID, s.historyBranchUtil)
	snapshot, events := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		common.FirstEventID,
		lastWriteVersion,
		state,
		status,
		dbRecordVersion,
		branchToken,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBrandNew,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *snapshot,
		NewWorkflowEvents:   events,
	})
	s.NoError(err)
	return branchToken, snapshot, events
}

func (s *ExecutionMutableStateSuite) CreateCHASMSnapshot(
	lastWriteVersion int64,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	dbRecordVersion int64,
) *p.WorkflowSnapshot {
	snapshot, events := RandomSnapshot(
		s.T(),
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		common.FirstEventID,
		lastWriteVersion,
		state,
		status,
		dbRecordVersion,
		nil,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBrandNew,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *snapshot,
		NewWorkflowEvents:   events,
	})
	s.NoError(err)
	return snapshot
}

func (s *ExecutionMutableStateSuite) AssertMissingFromDB(
	namespaceID string,
	workflowID string,
	runID string,
) {
	_, err := s.ExecutionManager.GetWorkflowExecution(s.Ctx, &p.GetWorkflowExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	s.IsType(&serviceerror.NotFound{}, err)
	s.EqualError(err, fmt.Sprintf("workflow execution not found for workflow ID %q and run ID %q", workflowID, runID))
}

func (s *ExecutionMutableStateSuite) AssertHEEqualWithDB(branchToken []byte, events ...[]*p.WorkflowEvents) {
	s.assertHEWithDB(branchToken, events, false)
}

func (s *ExecutionMutableStateSuite) AssertHEPrefixWithDB(branchToken []byte, events ...[]*p.WorkflowEvents) {
	s.assertHEWithDB(branchToken, events, true)
}

func (s *ExecutionMutableStateSuite) assertHEWithDB(
	branchToken []byte,
	eventBatches [][]*p.WorkflowEvents,
	assertPrefix bool,
) {
	var historyEvents []*historypb.HistoryEvent
	for _, eventBatch := range eventBatches {
		for _, event := range eventBatch {
			historyEvents = append(historyEvents, event.Events...)
		}
	}
	pageSize := len(historyEvents)
	if !assertPrefix {
		pageSize++ // plus one to check against extra page
	}
	resp, err := s.ExecutionManager.ReadHistoryBranch(s.Ctx, &p.ReadHistoryBranchRequest{
		ShardID:       s.ShardID,
		BranchToken:   branchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    math.MaxInt64,
		PageSize:      pageSize,
		NextPageToken: nil,
	})
	s.NoError(err)
	if !assertPrefix {
		s.Nil(resp.NextPageToken)
	}
	s.Equal(len(historyEvents), len(resp.HistoryEvents))
	for i, event := range historyEvents {
		s.ProtoEqual(event, resp.HistoryEvents[i])
	}
}

func (s *ExecutionMutableStateSuite) AssertMSEqualWithDB(
	snapshot *p.WorkflowSnapshot,
	mutations ...*p.WorkflowMutation,
) {
	resp, err := s.ExecutionManager.GetWorkflowExecution(s.Ctx, &p.GetWorkflowExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: snapshot.ExecutionInfo.NamespaceId,
		WorkflowID:  snapshot.ExecutionInfo.WorkflowId,
		RunID:       snapshot.ExecutionState.RunId,
	})
	s.NoError(err)

	actualMutableState := resp.State
	actualDBRecordVersion := resp.DBRecordVersion

	expectedMutableState, expectedDBRecordVersion := s.Accumulate(snapshot, mutations...)

	// need to special handling signal request IDs ...
	// since ^ is slice
	s.Equal(
		convert.StringSliceToSet(expectedMutableState.SignalRequestedIds),
		convert.StringSliceToSet(actualMutableState.SignalRequestedIds),
	)
	actualMutableState.SignalRequestedIds = expectedMutableState.SignalRequestedIds

	s.Equal(expectedDBRecordVersion, actualDBRecordVersion)
	s.ProtoEqual(expectedMutableState, actualMutableState)
}

func (s *ExecutionMutableStateSuite) Accumulate(
	snapshot *p.WorkflowSnapshot,
	mutations ...*p.WorkflowMutation,
) (*persistencespb.WorkflowMutableState, int64) {
	mutableState := &persistencespb.WorkflowMutableState{
		ExecutionInfo:       snapshot.ExecutionInfo,
		ExecutionState:      snapshot.ExecutionState,
		NextEventId:         snapshot.NextEventID,
		ActivityInfos:       snapshot.ActivityInfos,
		TimerInfos:          snapshot.TimerInfos,
		ChildExecutionInfos: snapshot.ChildExecutionInfos,
		RequestCancelInfos:  snapshot.RequestCancelInfos,
		SignalInfos:         snapshot.SignalInfos,
		SignalRequestedIds:  convert.StringSetToSlice(snapshot.SignalRequestedIDs),
		ChasmNodes:          snapshot.ChasmNodes,
	}
	dbRecordVersion := snapshot.DBRecordVersion

	for _, mutation := range mutations {
		s.Equal(dbRecordVersion, mutation.DBRecordVersion-1)
		dbRecordVersion = mutation.DBRecordVersion

		mutableState.ExecutionInfo = mutation.ExecutionInfo
		mutableState.ExecutionState = mutation.ExecutionState

		mutableState.NextEventId = mutation.NextEventID

		// activity infos
		maps.Copy(mutableState.ActivityInfos, mutation.UpsertActivityInfos)
		for key := range mutation.DeleteActivityInfos {
			delete(mutableState.ActivityInfos, key)
		}

		// timer infos
		maps.Copy(mutableState.TimerInfos, mutation.UpsertTimerInfos)
		for key := range mutation.DeleteTimerInfos {
			delete(mutableState.TimerInfos, key)
		}

		// child workflow infos
		maps.Copy(mutableState.ChildExecutionInfos, mutation.UpsertChildExecutionInfos)
		for key := range mutation.DeleteChildExecutionInfos {
			delete(mutableState.ChildExecutionInfos, key)
		}

		// request cancel infos
		maps.Copy(mutableState.RequestCancelInfos, mutation.UpsertRequestCancelInfos)
		for key := range mutation.DeleteRequestCancelInfos {
			delete(mutableState.RequestCancelInfos, key)
		}

		// signal infos
		maps.Copy(mutableState.SignalInfos, mutation.UpsertSignalInfos)
		for key := range mutation.DeleteSignalInfos {
			delete(mutableState.SignalInfos, key)
		}

		// signal request IDs
		signalRequestIDs := convert.StringSliceToSet(mutableState.SignalRequestedIds)
		maps.Copy(signalRequestIDs, mutation.UpsertSignalRequestedIDs)
		for key := range mutation.DeleteSignalRequestedIDs {
			delete(signalRequestIDs, key)
		}
		mutableState.SignalRequestedIds = convert.StringSetToSlice(signalRequestIDs)

		// chasm nodes
		maps.Copy(mutableState.ChasmNodes, mutation.UpsertChasmNodes)
		for key := range mutation.DeleteChasmNodes {
			delete(mutableState.ChasmNodes, key)
		}

		// buffered events
		if mutation.ClearBufferedEvents {
			mutableState.BufferedEvents = nil
		} else if mutation.NewBufferedEvents != nil {
			mutableState.BufferedEvents = append(mutableState.BufferedEvents, mutation.NewBufferedEvents...)
		}
	}

	// need to serialize & deserialize to get rid of timezone information ...
	bytes, err := proto.Marshal(mutableState)
	s.NoError(err)
	mutableState = &persistencespb.WorkflowMutableState{}
	err = proto.Unmarshal(bytes, mutableState)
	s.NoError(err)

	// make equal test easier
	if mutableState.ActivityInfos == nil {
		mutableState.ActivityInfos = make(map[int64]*persistencespb.ActivityInfo)
	}
	if mutableState.TimerInfos == nil {
		mutableState.TimerInfos = make(map[string]*persistencespb.TimerInfo)
	}
	if mutableState.ChildExecutionInfos == nil {
		mutableState.ChildExecutionInfos = make(map[int64]*persistencespb.ChildExecutionInfo)
	}
	if mutableState.RequestCancelInfos == nil {
		mutableState.RequestCancelInfos = make(map[int64]*persistencespb.RequestCancelInfo)
	}
	if mutableState.SignalInfos == nil {
		mutableState.SignalInfos = make(map[int64]*persistencespb.SignalInfo)
	}
	if mutableState.SignalRequestedIds == nil {
		mutableState.SignalRequestedIds = make([]string, 0)
	}
	if mutableState.BufferedEvents == nil {
		mutableState.BufferedEvents = make([]*historypb.HistoryEvent, 0)
	}
	if mutableState.ChasmNodes == nil {
		mutableState.ChasmNodes = make(map[string]*persistencespb.ChasmNode)
	}

	return mutableState, dbRecordVersion
}
