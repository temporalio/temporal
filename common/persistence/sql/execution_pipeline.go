package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

// expectingTx answers the reads and row counts that updateWorkflowExecutionTx
// checks with the values a successful update sees, so a record pass over a
// PipelinedTx queues every statement of the update.
type expectingTx struct {
	sqlplugin.Tx
	rangeID         int64
	runID           primitives.UUID
	dbRecordVersion int64
	condition       int64
}

// updateWorkflowExecutionPipelined runs the UpdateWorkflowExecution transaction
// in one round trip when the plugin supports it. It records the transaction's
// statements, flushes them, then runs the transaction again against the flushed
// results, so today's checks build today's errors. It returns false when the
// caller must run the sequential transaction.
func (m *sqlExecutionStore) updateWorkflowExecutionPipelined(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) (bool, error) {
	pipeliner, ok := m.DB.(sqlplugin.TxPipeliner)
	if !ok || request.Mode != p.UpdateWorkflowModeUpdateCurrent || request.NewWorkflowSnapshot != nil {
		return false, nil
	}

	update := func(tx sqlplugin.Tx) error {
		if err := readLockShard(ctx, tx, request.ShardID, request.RangeID); err != nil {
			return err
		}
		return m.updateWorkflowExecutionTx(ctx, tx, request)
	}

	tx, err := pipeliner.BeginPipelinedTx(ctx)
	if errors.Is(err, sqlplugin.ErrPipelineUnsupported) {
		return false, nil
	}
	if err != nil {
		return true, serviceerror.NewUnavailablef("UpdateWorkflowExecution failed. Failed to start transaction. Error: %v", err)
	}
	mutation := request.UpdateWorkflowMutation
	if err := update(&expectingTx{
		Tx:              tx,
		rangeID:         request.RangeID,
		runID:           primitives.MustParseUUID(mutation.ExecutionState.RunId),
		dbRecordVersion: mutation.DBRecordVersion,
		condition:       mutation.Condition,
	}); err != nil {
		m.logger.Warn("UpdateWorkflowExecution pipeline recording failed", tag.ShardID(request.ShardID), tag.Error(err))
		return false, nil
	}
	if err := tx.Flush(); err != nil {
		if errors.Is(err, sqlplugin.ErrPipelineUnsupported) {
			return false, nil
		}
		return true, serviceerror.NewUnavailablef("UpdateWorkflowExecution failed. Failed to start transaction. Error: %v", err)
	}

	err = m.finishTx(tx, "UpdateWorkflowExecution", update(tx))
	if tx.Diverged() {
		m.logger.Warn("UpdateWorkflowExecution pipeline replay diverged", tag.ShardID(request.ShardID), tag.Error(err))
		return false, nil
	}
	return true, err
}

func (t *expectingTx) ReadLockShards(ctx context.Context, filter sqlplugin.ShardsFilter) (int64, error) {
	if _, err := t.Tx.ReadLockShards(ctx, filter); !errors.Is(err, sqlplugin.ErrNotFlushed) {
		return 0, err
	}
	return t.rangeID, nil
}

func (t *expectingTx) LockCurrentExecutions(ctx context.Context, filter sqlplugin.CurrentExecutionsFilter) (*sqlplugin.CurrentExecutionsRow, error) {
	if _, err := t.Tx.LockCurrentExecutions(ctx, filter); !errors.Is(err, sqlplugin.ErrNotFlushed) {
		return nil, err
	}
	return &sqlplugin.CurrentExecutionsRow{RunID: t.runID}, nil
}

// WriteLockExecutions returns values that pass lockAndCheckExecution whether it
// checks the record version or the next event ID.
func (t *expectingTx) WriteLockExecutions(ctx context.Context, filter sqlplugin.ExecutionsFilter) (int64, int64, error) {
	if _, _, err := t.Tx.WriteLockExecutions(ctx, filter); !errors.Is(err, sqlplugin.ErrNotFlushed) {
		return 0, 0, err
	}
	return t.dbRecordVersion - 1, t.condition, nil
}

func (t *expectingTx) UpdateCurrentExecutions(ctx context.Context, row *sqlplugin.CurrentExecutionsRow) (sql.Result, error) {
	_, err := t.Tx.UpdateCurrentExecutions(ctx, row)
	return queuedRows(err, 1)
}

func (t *expectingTx) UpdateExecutions(ctx context.Context, row *sqlplugin.ExecutionsRow) (sql.Result, error) {
	_, err := t.Tx.UpdateExecutions(ctx, row)
	return queuedRows(err, 1)
}

func (t *expectingTx) InsertIntoHistoryImmediateTasks(ctx context.Context, rows []sqlplugin.HistoryImmediateTasksRow) (sql.Result, error) {
	_, err := t.Tx.InsertIntoHistoryImmediateTasks(ctx, rows)
	return queuedRows(err, len(rows))
}

func (t *expectingTx) InsertIntoHistoryScheduledTasks(ctx context.Context, rows []sqlplugin.HistoryScheduledTasksRow) (sql.Result, error) {
	_, err := t.Tx.InsertIntoHistoryScheduledTasks(ctx, rows)
	return queuedRows(err, len(rows))
}

func (t *expectingTx) InsertIntoTransferTasks(ctx context.Context, rows []sqlplugin.TransferTasksRow) (sql.Result, error) {
	_, err := t.Tx.InsertIntoTransferTasks(ctx, rows)
	return queuedRows(err, len(rows))
}

func (t *expectingTx) InsertIntoTimerTasks(ctx context.Context, rows []sqlplugin.TimerTasksRow) (sql.Result, error) {
	_, err := t.Tx.InsertIntoTimerTasks(ctx, rows)
	return queuedRows(err, len(rows))
}

func (t *expectingTx) InsertIntoReplicationTasks(ctx context.Context, rows []sqlplugin.ReplicationTasksRow) (sql.Result, error) {
	_, err := t.Tx.InsertIntoReplicationTasks(ctx, rows)
	return queuedRows(err, len(rows))
}

func (t *expectingTx) InsertIntoVisibilityTasks(ctx context.Context, rows []sqlplugin.VisibilityTasksRow) (sql.Result, error) {
	_, err := t.Tx.InsertIntoVisibilityTasks(ctx, rows)
	return queuedRows(err, len(rows))
}

// queuedRows reports a queued exec as affecting the given number of rows, or
// returns its error.
func queuedRows(err error, rows int) (sql.Result, error) {
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(rows), nil
}
