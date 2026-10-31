package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
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
	"go.temporal.io/server/service/history/tasks"
)

type (
	// fakePipelineDB begins its pipelined and sequential transactions from fakes.
	fakePipelineDB struct {
		sqlplugin.DB
		pipelined  *fakeUpdateTx
		sequential *fakeUpdateTx
	}

	// fakeUpdateTx answers the calls of a minimal UpdateCurrent update the way a
	// successful update sees them. As a pipelined transaction it answers reads with
	// ErrNotFlushed until Flush.
	fakeUpdateTx struct {
		sqlplugin.PipelinedTx
		request   *p.InternalUpdateWorkflowExecutionRequest
		pipelined bool

		updateExecutionsErr error
		flushErr            error
		diverged            bool

		flushed   bool
		committed bool
	}

	// taskOrderTx records the task table each insert goes to.
	taskOrderTx struct {
		sqlplugin.Tx
		inserts []string
	}
)

// A pipelined replay must queue the statements its record pass queued, so
// applyTasks must not follow map iteration order.
func TestApplyTasks_InsertsCategoriesInIDOrder(t *testing.T) {
	task := []p.InternalHistoryTask{{Blob: &commonpb.DataBlob{}}}
	insertTasks := map[tasks.Category][]p.InternalHistoryTask{
		tasks.CategoryVisibility:  task,
		tasks.CategoryReplication: task,
		tasks.CategoryTimer:       task,
		tasks.CategoryTransfer:    task,
	}
	// Map iteration order changes between loops, so repeat to catch one that follows it.
	for range 100 {
		tx := &taskOrderTx{}
		require.NoError(t, applyTasks(context.Background(), tx, 1, insertTasks))
		require.Equal(t, []string{"transfer", "timer", "replication", "visibility"}, tx.inserts)
	}
}

func TestUpdateWorkflowExecutionPipelined_RecordErrorRunsSequential(t *testing.T) {
	request := newPipelineTestRequest()
	db := newFakePipelineDB(request)
	db.pipelined.updateExecutionsErr = errors.New("record failed")

	require.NoError(t, updateWithFakes(db, request))
	require.False(t, db.pipelined.flushed)
	require.True(t, db.sequential.committed)
}

func TestUpdateWorkflowExecutionPipelined_DivergedReplayRunsSequential(t *testing.T) {
	request := newPipelineTestRequest()
	db := newFakePipelineDB(request)
	db.pipelined.diverged = true

	require.NoError(t, updateWithFakes(db, request))
	require.True(t, db.pipelined.flushed)
	require.False(t, db.pipelined.committed)
	require.True(t, db.sequential.committed)
}

func TestUpdateWorkflowExecutionPipelined_FlushErrorIsStartFailure(t *testing.T) {
	request := newPipelineTestRequest()
	db := newFakePipelineDB(request)
	db.pipelined.flushErr = errors.New("connection refused")

	err := updateWithFakes(db, request)
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, "UpdateWorkflowExecution failed. Failed to start transaction. Error: connection refused", unavailable.Message)
	require.False(t, db.sequential.committed)
}

func newPipelineTestRequest() *p.InternalUpdateWorkflowExecutionRequest {
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
	}
}

func newFakePipelineDB(request *p.InternalUpdateWorkflowExecutionRequest) *fakePipelineDB {
	return &fakePipelineDB{
		pipelined:  &fakeUpdateTx{request: request, pipelined: true},
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
	return d.pipelined, nil
}

func (d *fakePipelineDB) BeginTx(context.Context) (sqlplugin.Tx, error) {
	return d.sequential, nil
}

func (t *fakeUpdateTx) recording() bool {
	return t.pipelined && !t.flushed
}

func (t *fakeUpdateTx) ReadLockShards(context.Context, sqlplugin.ShardsFilter) (int64, error) {
	if t.recording() {
		return 0, sqlplugin.ErrNotFlushed
	}
	return t.request.RangeID, nil
}

func (t *fakeUpdateTx) LockCurrentExecutions(context.Context, sqlplugin.CurrentExecutionsFilter) (*sqlplugin.CurrentExecutionsRow, error) {
	if t.recording() {
		return nil, sqlplugin.ErrNotFlushed
	}
	return &sqlplugin.CurrentExecutionsRow{
		RunID: primitives.MustParseUUID(t.request.UpdateWorkflowMutation.ExecutionState.RunId),
	}, nil
}

func (t *fakeUpdateTx) WriteLockExecutions(context.Context, sqlplugin.ExecutionsFilter) (int64, int64, error) {
	if t.recording() {
		return 0, 0, sqlplugin.ErrNotFlushed
	}
	mutation := t.request.UpdateWorkflowMutation
	return mutation.DBRecordVersion - 1, mutation.Condition, nil
}

func (t *fakeUpdateTx) UpdateCurrentExecutions(context.Context, *sqlplugin.CurrentExecutionsRow) (sql.Result, error) {
	return driver.RowsAffected(1), nil
}

func (t *fakeUpdateTx) UpdateExecutions(context.Context, *sqlplugin.ExecutionsRow) (sql.Result, error) {
	if t.updateExecutionsErr != nil {
		return nil, t.updateExecutionsErr
	}
	return driver.RowsAffected(1), nil
}

func (t *fakeUpdateTx) Flush() error {
	t.flushed = true
	return t.flushErr
}

func (t *fakeUpdateTx) Diverged() bool {
	return t.diverged
}

func (t *fakeUpdateTx) Commit() error {
	if t.diverged {
		return errors.New("diverged")
	}
	t.committed = true
	return nil
}

func (t *fakeUpdateTx) Rollback() error {
	return nil
}

func (t *taskOrderTx) InsertIntoTransferTasks(_ context.Context, rows []sqlplugin.TransferTasksRow) (sql.Result, error) {
	t.inserts = append(t.inserts, "transfer")
	return driver.RowsAffected(len(rows)), nil
}

func (t *taskOrderTx) InsertIntoTimerTasks(_ context.Context, rows []sqlplugin.TimerTasksRow) (sql.Result, error) {
	t.inserts = append(t.inserts, "timer")
	return driver.RowsAffected(len(rows)), nil
}

func (t *taskOrderTx) InsertIntoReplicationTasks(_ context.Context, rows []sqlplugin.ReplicationTasksRow) (sql.Result, error) {
	t.inserts = append(t.inserts, "replication")
	return driver.RowsAffected(len(rows)), nil
}

func (t *taskOrderTx) InsertIntoVisibilityTasks(_ context.Context, rows []sqlplugin.VisibilityTasksRow) (sql.Result, error) {
	t.inserts = append(t.inserts, "visibility")
	return driver.RowsAffected(len(rows)), nil
}
