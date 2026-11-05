package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"slices"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
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
// UpdateWorkflowExecution transaction in one round trip when the plugin
// supports it. It returns false when the request or the plugin's connection
// needs the sequential path, before anything is sent.
func (m *sqlExecutionStore) updateWorkflowExecutionPipelined(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) (bool, error) {
	pipeliner, ok := m.DB.(sqlplugin.TxPipeliner)
	if !ok || request.NewWorkflowSnapshot != nil {
		return false, nil
	}
	// BypassCurrent checks that the run is not current, which the recorder has no
	// statement for.
	if request.Mode != p.UpdateWorkflowModeUpdateCurrent && request.Mode != p.UpdateWorkflowModeIgnoreCurrent {
		return false, nil
	}
	newEvents := slices.Concat(request.UpdateWorkflowNewEvents, request.NewWorkflowNewEvents)
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
	case len(newEvents) > 0 && (common.IsContextCanceledErr(err) || common.IsContextDeadlineExceededErr(err)):
		// The sequential path appends the history first, and returns this error.
		return true, &p.AppendHistoryTimeoutError{Msg: err.Error()}
	case err != nil:
		return true, convertSQLError("UpdateWorkflowExecution", "failed to start transaction", err)
	}
	// Only Commit and Rollback return the connection Begin took to the pool.
	defer func() {
		if rec := recover(); rec != nil {
			if rollBackErr := tx.Rollback(); rollBackErr != nil {
				m.logger.Error("transaction rollback error", tag.Error(rollBackErr))
			}
			panic(rec) //nolint:forbidigo
		}
	}()
	if err := m.recordUpdateWorkflowExecution(ctx, tx, request); err != nil {
		if rollBackErr := tx.Rollback(); rollBackErr != nil {
			m.logger.Error("transaction rollback error", tag.Error(rollBackErr))
		}
		return true, convertTxError("UpdateWorkflowExecution", err)
	}
	// A context that ended during recording sends nothing at Commit, so with new
	// events return the error Begin returns for it.
	if err := ctx.Err(); err != nil && len(newEvents) > 0 {
		if rollBackErr := tx.Rollback(); rollBackErr != nil {
			m.logger.Error("transaction rollback error", tag.Error(rollBackErr))
		}
		return true, &p.AppendHistoryTimeoutError{Msg: err.Error()}
	}
	if err := tx.Commit(); err != nil {
		return true, pipelinedCommitError(request.ShardID, err)
	}
	return true, nil
}

// pipelinedCommitError returns the error the sequential path returns for the
// check that failed, or Unavailable when the outcome is unknown.
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
) error {
	for _, req := range slices.Concat(request.UpdateWorkflowNewEvents, request.NewWorkflowNewEvents) {
		nodeRow, err := historyNodeRow(req)
		if err != nil {
			return err
		}
		if _, err := tx.InsertIntoHistoryNode(ctx, nodeRow); err != nil {
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
