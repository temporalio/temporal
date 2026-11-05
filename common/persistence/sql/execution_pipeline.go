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
)

// expectingTx answers the row counts that applyWorkflowMutationTx checks with
// the values a successful update sees. The statements it queues fail the
// transaction on any other count.
type expectingTx struct {
	sqlplugin.PipelinedTx
}

// updateWorkflowExecutionPipelined appends the history and runs the
// UpdateWorkflowExecution transaction as one batch when the plugin
// supports it. newEvents are the request's new events and nodeRows their history
// node rows. It returns false, so that the sequential path runs the update, when
// the request or the plugin's connection needs it, when Begin fails after the
// context ended, and when Commit ran none of the statements.
func (m *sqlExecutionStore) updateWorkflowExecutionPipelined(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
	newEvents []*p.InternalAppendHistoryNodesRequest,
	nodeRows []*sqlplugin.HistoryNodeRow,
) (bool, error) {
	pipeliner, ok := m.DB.(sqlplugin.TxPipeliner)
	if !ok || request.NewWorkflowSnapshot != nil {
		return false, nil
	}
	// BypassCurrent checks that the run is not current, which PipelinedTx has no
	// statement for.
	if request.Mode != p.UpdateWorkflowModeUpdateCurrent && request.Mode != p.UpdateWorkflowModeIgnoreCurrent {
		return false, nil
	}
	for _, req := range newEvents {
		// A new branch also inserts its history_tree row in its own transaction.
		if req.IsNewBranch {
			return false, nil
		}
	}

	tx, err := pipeliner.BeginPipelinedTx(ctx)
	switch {
	case errors.Is(err, sqlplugin.ErrPipelineUnsupported):
		return false, nil
	case err != nil && ctx.Err() != nil:
		// The sequential path returns its own error for a context that ended,
		// without connecting. A connect timeout also wraps DeadlineExceeded, but
		// with ctx alive.
		return false, nil
	case err != nil:
		return true, convertSQLError("UpdateWorkflowExecution", "failed to start transaction", err)
	}
	// Commit returns the connection Begin took to the pool, and this rollback
	// returns it on every other exit, a panic while recording included.
	sent := false
	defer func() {
		if !sent {
			if rollBackErr := tx.Rollback(); rollBackErr != nil {
				m.logger.Error("transaction rollback error", tag.Error(rollBackErr))
			}
		}
	}()
	if err := m.recordUpdateWorkflowExecution(ctx, tx, request, nodeRows); err != nil {
		return true, convertTxError("UpdateWorkflowExecution", err)
	}
	sent = true
	err = tx.Commit()
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, sqlplugin.ErrPipelineNotSent):
		return false, nil
	default:
		return true, pipelinedCommitError(request.ShardID, err)
	}
}

// pipelinedCommitError returns the error the sequential path returns for the
// check that failed, and converts any other error as a failed COMMIT is
// converted.
func pipelinedCommitError(shardID int32, err error) error {
	const prefix = "UpdateWorkflowExecution: "
	switch {
	case errors.Is(err, sqlplugin.ErrShardRangeIDMismatch):
		return &p.ShardOwnershipLostError{ShardID: shardID, Msg: prefix + sqlplugin.ErrShardRangeIDMismatch.Error()}
	case errors.Is(err, sqlplugin.ErrCurrentExecutionMissing):
		return serviceerror.NewUnavailable(prefix + sqlplugin.ErrCurrentExecutionMissing.Error())
	case errors.Is(err, sqlplugin.ErrCurrentExecutionRunIDMismatch):
		return &p.CurrentWorkflowConditionFailedError{Msg: prefix + sqlplugin.ErrCurrentExecutionRunIDMismatch.Error()}
	case errors.Is(err, sqlplugin.ErrExecutionMissing):
		return &p.ConditionFailedError{Msg: prefix + sqlplugin.ErrExecutionMissing.Error()}
	case errors.Is(err, sqlplugin.ErrExecutionConditionMismatch):
		return &p.WorkflowConditionFailedError{Msg: prefix + sqlplugin.ErrExecutionConditionMismatch.Error()}
	default:
		return convertSQLError("UpdateWorkflowExecution", "failed to commit transaction", err)
	}
}

// recordUpdateWorkflowExecution queues the history nodes and the statements of
// the sequential transaction, in the same order.
func (m *sqlExecutionStore) recordUpdateWorkflowExecution(
	ctx context.Context,
	tx sqlplugin.PipelinedTx,
	request *p.InternalUpdateWorkflowExecutionRequest,
	nodeRows []*sqlplugin.HistoryNodeRow,
) error {
	for _, nodeRow := range nodeRows {
		// InsertIntoHistoryNode can change the row (Postgres negates its TxnID), and
		// the sequential path appends nodeRows after a Commit that ran none of the
		// statements.
		row := *nodeRow
		if _, err := tx.InsertIntoHistoryNode(ctx, &row); err != nil {
			return err
		}
	}
	if err := readLockShard(ctx, tx, request.ShardID, request.RangeID); err != nil {
		return err
	}
	return m.updateWorkflowExecutionTx(ctx, &expectingTx{PipelinedTx: tx}, request)
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
