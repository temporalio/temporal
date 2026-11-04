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
// checks with the values a successful update sees, and queues the statements
// that fail the transaction otherwise, so a pass over a PipelinedTx queues every
// statement of the update.
type expectingTx struct {
	sqlplugin.PipelinedTx
	runID primitives.UUID
}

// updateWorkflowExecutionPipelined appends the history and runs the
// UpdateWorkflowExecution transaction in one round trip when the plugin
// supports it. It returns false when nothing was written and the caller must
// run the sequential path, which returns today's errors when the database
// rejected the pipelined transaction.
func (m *sqlExecutionStore) updateWorkflowExecutionPipelined(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) (bool, error) {
	pipeliner, ok := m.DB.(sqlplugin.TxPipeliner)
	if !ok || request.Mode != p.UpdateWorkflowModeUpdateCurrent || request.NewWorkflowSnapshot != nil {
		return false, nil
	}
	for _, req := range request.UpdateWorkflowNewEvents {
		// A new branch also inserts its history_tree row in its own transaction.
		if req.IsNewBranch {
			return false, nil
		}
	}

	tx, err := pipeliner.BeginPipelinedTx(ctx)
	if errors.Is(err, sqlplugin.ErrPipelineUnsupported) {
		return false, nil
	}
	if err != nil {
		return true, serviceerror.NewUnavailablef("UpdateWorkflowExecution failed. Failed to start transaction. Error: %v", err)
	}
	if err := m.recordUpdateWorkflowExecution(ctx, tx, request); err != nil {
		m.logger.Warn("UpdateWorkflowExecution pipeline recording failed", tag.ShardID(request.ShardID), tag.Error(err))
		return false, nil
	}

	err = tx.Commit()
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, sqlplugin.ErrPipelineRejected):
		m.logger.Warn("UpdateWorkflowExecution pipeline rejected", tag.ShardID(request.ShardID), tag.Error(err))
		return false, nil
	default:
		return true, convertSQLError("UpdateWorkflowExecution", "failed to commit transaction", err)
	}
}

// recordUpdateWorkflowExecution queues the history nodes and the statements of
// the sequential transaction, in the same order.
func (m *sqlExecutionStore) recordUpdateWorkflowExecution(
	ctx context.Context,
	tx sqlplugin.PipelinedTx,
	request *p.InternalUpdateWorkflowExecutionRequest,
) error {
	for _, req := range request.UpdateWorkflowNewEvents {
		nodeRow, err := historyNodeRow(req)
		if err != nil {
			return err
		}
		if _, err := tx.InsertIntoHistoryNode(ctx, nodeRow); err != nil {
			return err
		}
	}
	if err := tx.ReadLockShardsWithRangeID(ctx, sqlplugin.ShardsFilter{ShardID: request.ShardID}, request.RangeID); err != nil {
		return err
	}
	return m.updateWorkflowExecutionTx(ctx, &expectingTx{
		PipelinedTx: tx,
		runID:       primitives.MustParseUUID(request.UpdateWorkflowMutation.ExecutionState.RunId),
	}, request)
}

func (t *expectingTx) LockCurrentExecutions(ctx context.Context, filter sqlplugin.CurrentExecutionsFilter) (*sqlplugin.CurrentExecutionsRow, error) {
	filter.RunID = t.runID
	if err := t.LockCurrentExecutionsWithRunID(ctx, filter); err != nil {
		return nil, err
	}
	return &sqlplugin.CurrentExecutionsRow{RunID: t.runID}, nil
}

func (t *expectingTx) UpdateExecutionsWithCondition(ctx context.Context, update *sqlplugin.ExecutionsUpdate) (sql.Result, error) {
	_, err := t.PipelinedTx.UpdateExecutionsWithCondition(ctx, update)
	return queuedRows(err, 1)
}

func (t *expectingTx) InsertIntoHistoryImmediateTasks(ctx context.Context, rows []sqlplugin.HistoryImmediateTasksRow) (sql.Result, error) {
	_, err := t.PipelinedTx.InsertIntoHistoryImmediateTasks(ctx, rows)
	return queuedRows(err, len(rows))
}

func (t *expectingTx) InsertIntoHistoryScheduledTasks(ctx context.Context, rows []sqlplugin.HistoryScheduledTasksRow) (sql.Result, error) {
	_, err := t.PipelinedTx.InsertIntoHistoryScheduledTasks(ctx, rows)
	return queuedRows(err, len(rows))
}

func (t *expectingTx) InsertIntoTransferTasks(ctx context.Context, rows []sqlplugin.TransferTasksRow) (sql.Result, error) {
	_, err := t.PipelinedTx.InsertIntoTransferTasks(ctx, rows)
	return queuedRows(err, len(rows))
}

func (t *expectingTx) InsertIntoTimerTasks(ctx context.Context, rows []sqlplugin.TimerTasksRow) (sql.Result, error) {
	_, err := t.PipelinedTx.InsertIntoTimerTasks(ctx, rows)
	return queuedRows(err, len(rows))
}

func (t *expectingTx) InsertIntoReplicationTasks(ctx context.Context, rows []sqlplugin.ReplicationTasksRow) (sql.Result, error) {
	_, err := t.PipelinedTx.InsertIntoReplicationTasks(ctx, rows)
	return queuedRows(err, len(rows))
}

func (t *expectingTx) InsertIntoVisibilityTasks(ctx context.Context, rows []sqlplugin.VisibilityTasksRow) (sql.Result, error) {
	_, err := t.PipelinedTx.InsertIntoVisibilityTasks(ctx, rows)
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
