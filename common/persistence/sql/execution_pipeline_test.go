package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

type (
	// fakePipelineDB begins its pipelined and sequential transactions from fakes,
	// and counts the history nodes appended outside a transaction.
	fakePipelineDB struct {
		sqlplugin.DB
		pipelined    *fakeUpdateTx
		sequential   *fakeUpdateTx
		historyNodes int
	}

	// fakeUpdateTx answers the calls of a minimal UpdateCurrent update the way a
	// successful update sees them.
	fakeUpdateTx struct {
		sqlplugin.PipelinedTx
		request *p.InternalUpdateWorkflowExecutionRequest

		updateExecutionsErr error
		commitErr           error

		begun        bool
		historyNodes int
		committed    bool
	}
)

func TestUpdateWorkflowExecutionPipelined_Committed(t *testing.T) {
	request := newPipelineTestRequest(false)
	db := newFakePipelineDB(request)

	require.NoError(t, updateWithFakes(db, request))
	require.Equal(t, 1, db.pipelined.historyNodes)
	require.True(t, db.pipelined.committed)
	require.Zero(t, db.historyNodes)
	require.False(t, db.sequential.begun)
}

func TestUpdateWorkflowExecutionPipelined_RejectedRunsSequential(t *testing.T) {
	request := newPipelineTestRequest(false)
	db := newFakePipelineDB(request)
	db.pipelined.commitErr = fmt.Errorf("%w: %w", sqlplugin.ErrPipelineNotWritten, errors.New("division by zero"))

	require.NoError(t, updateWithFakes(db, request))
	require.Equal(t, 1, db.historyNodes)
	require.True(t, db.sequential.committed)
}

func TestUpdateWorkflowExecutionPipelined_UnknownOutcomeIsUnavailable(t *testing.T) {
	request := newPipelineTestRequest(false)
	db := newFakePipelineDB(request)
	db.pipelined.commitErr = errors.New("connection reset")

	err := updateWithFakes(db, request)
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, "UpdateWorkflowExecution: failed to commit transaction: connection reset", unavailable.Message)
	require.Zero(t, db.historyNodes)
	require.False(t, db.sequential.begun)
}

func TestUpdateWorkflowExecutionPipelined_NewBranchRunsSequential(t *testing.T) {
	request := newPipelineTestRequest(true)
	db := newFakePipelineDB(request)

	require.NoError(t, updateWithFakes(db, request))
	require.False(t, db.pipelined.begun)
	require.Equal(t, 1, db.sequential.historyNodes)
	require.True(t, db.sequential.committed)
}

func TestUpdateWorkflowExecutionPipelined_RecordErrorRunsSequential(t *testing.T) {
	request := newPipelineTestRequest(false)
	db := newFakePipelineDB(request)
	db.pipelined.updateExecutionsErr = errors.New("record failed")

	require.NoError(t, updateWithFakes(db, request))
	require.False(t, db.pipelined.committed)
	require.Equal(t, 1, db.historyNodes)
	require.True(t, db.sequential.committed)
}

func newPipelineTestRequest(isNewBranch bool) *p.InternalUpdateWorkflowExecutionRequest {
	return &p.InternalUpdateWorkflowExecutionRequest{
		ShardID:     1,
		RangeID:     5,
		Mode:        p.UpdateWorkflowModeUpdateCurrent,
		ArchetypeID: chasm.WorkflowArchetypeID,
		UpdateWorkflowMutation: p.InternalWorkflowMutation{
			NamespaceID:        primitives.NewUUID().String(),
			WorkflowID:         "workflow-id",
			ExecutionState:     &persistencespb.WorkflowExecutionState{RunId: primitives.NewUUID().String()},
			ExecutionStateBlob: &commonpb.DataBlob{},
			ExecutionInfoBlob:  &commonpb.DataBlob{},
			NextEventID:        11,
			DBRecordVersion:    3,
			Condition:          10,
		},
		UpdateWorkflowNewEvents: []*p.InternalAppendHistoryNodesRequest{{
			IsNewBranch: isNewBranch,
			BranchInfo: &persistencespb.HistoryBranch{
				TreeId:   primitives.NewUUID().String(),
				BranchId: primitives.NewUUID().String(),
			},
			TreeInfo: &commonpb.DataBlob{},
			Node:     p.InternalHistoryNode{NodeID: 10, Events: &commonpb.DataBlob{}},
			ShardID:  1,
		}},
	}
}

func newFakePipelineDB(request *p.InternalUpdateWorkflowExecutionRequest) *fakePipelineDB {
	return &fakePipelineDB{
		pipelined:  &fakeUpdateTx{request: request},
		sequential: &fakeUpdateTx{request: request},
	}
}

// updateWithFakes runs the update on a store whose DB is wrapped in a DbConn,
// as Factory.NewExecutionStore wraps it.
func updateWithFakes(db *fakePipelineDB, request *p.InternalUpdateWorkflowExecutionRequest) error {
	store, err := NewSQLExecutionStore(&DbConn{DB: db}, log.NewNoopLogger(), serialization.NewSerializer())
	if err != nil {
		return err
	}
	return store.UpdateWorkflowExecution(context.Background(), request)
}

func (d *fakePipelineDB) BeginPipelinedTx(context.Context) (sqlplugin.PipelinedTx, error) {
	d.pipelined.begun = true
	return d.pipelined, nil
}

func (d *fakePipelineDB) BeginTx(context.Context) (sqlplugin.Tx, error) {
	d.sequential.begun = true
	return d.sequential, nil
}

func (d *fakePipelineDB) InsertIntoHistoryNode(context.Context, *sqlplugin.HistoryNodeRow) (sql.Result, error) {
	d.historyNodes++
	return driver.RowsAffected(1), nil
}

func (t *fakeUpdateTx) InsertIntoHistoryNode(context.Context, *sqlplugin.HistoryNodeRow) (sql.Result, error) {
	t.historyNodes++
	return driver.RowsAffected(1), nil
}

func (t *fakeUpdateTx) InsertIntoHistoryTree(context.Context, *sqlplugin.HistoryTreeRow) (sql.Result, error) {
	return driver.RowsAffected(1), nil
}

func (t *fakeUpdateTx) ReadLockShards(context.Context, sqlplugin.ShardsFilter) (int64, error) {
	return t.request.RangeID, nil
}

func (t *fakeUpdateTx) ReadLockShardsWithRangeID(context.Context, sqlplugin.ShardsFilter, int64) error {
	return nil
}

func (t *fakeUpdateTx) LockCurrentExecutions(context.Context, sqlplugin.CurrentExecutionsFilter) (*sqlplugin.CurrentExecutionsRow, error) {
	return &sqlplugin.CurrentExecutionsRow{
		RunID: primitives.MustParseUUID(t.request.UpdateWorkflowMutation.ExecutionState.RunId),
	}, nil
}

func (t *fakeUpdateTx) LockCurrentExecutionsWithRunID(context.Context, sqlplugin.CurrentExecutionsFilter) error {
	return nil
}

func (t *fakeUpdateTx) UpdateCurrentExecutions(context.Context, *sqlplugin.CurrentExecutionsRow) (sql.Result, error) {
	return driver.RowsAffected(1), nil
}

func (t *fakeUpdateTx) UpdateExecutionsWithCondition(context.Context, *sqlplugin.ExecutionsUpdate) (sql.Result, error) {
	if t.updateExecutionsErr != nil {
		return nil, t.updateExecutionsErr
	}
	return driver.RowsAffected(1), nil
}

func (t *fakeUpdateTx) Commit() error {
	if t.commitErr != nil {
		return t.commitErr
	}
	t.committed = true
	return nil
}

func (t *fakeUpdateTx) Rollback() error {
	return nil
}
