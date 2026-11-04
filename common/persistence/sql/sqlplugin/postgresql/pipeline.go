package postgresql

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/driver"
)

type (
	// pipeline queues a transaction's statements until Commit sends them.
	pipeline struct {
		ctx        context.Context
		db         *sqlx.DB
		statements []pipelinedStatement
	}

	pipelinedStatement struct {
		sql  string
		args []any
	}

	// pipelinedTx is a transaction whose statements go through a pipeline.
	pipelinedTx struct {
		*db
	}

	// queuedResult is the result of a queued exec.
	queuedResult struct{}
)

var (
	_ sqlplugin.TxPipeliner = (*db)(nil)
	_ sqlplugin.PipelinedTx = pipelinedTx{}
)

// BeginPipelinedTx returns ErrPipelineUnsupported unless the plugin uses pgx,
// since Commit sends the batch through a pgx connection.
func (pdb *db) BeginPipelinedTx(ctx context.Context) (sqlplugin.PipelinedTx, error) {
	if _, ok := pdb.dbDriver.(*driver.PGXDriver); !ok {
		return nil, sqlplugin.ErrPipelineUnsupported
	}
	sqlDB, err := pdb.handle.DB()
	if err != nil {
		return nil, err
	}
	tx := newDB(pdb.dbKind, pdb.dbName, pdb.dbDriver, pdb.handle, nil, pdb.logger)
	tx.pipe = &pipeline{ctx: ctx, db: sqlDB}
	return pipelinedTx{db: tx}, nil
}

func (t pipelinedTx) ReadLockShardsWithRangeID(
	ctx context.Context,
	filter sqlplugin.ShardsFilter,
	rangeID int64,
) error {
	_, err := t.ExecContext(ctx,
		readLockShardWithRangeIDQry,
		filter.ShardID,
		rangeID,
	)
	return err
}

func (t pipelinedTx) LockCurrentExecutionsWithRunID(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) error {
	var err error

	if err := t.assertArchetypeIDSpecified(filter.ArchetypeID); err != nil {
		return err
	}

	if filter.ArchetypeID == chasm.WorkflowArchetypeID {
		_, err = t.ExecContext(ctx,
			lockCurrentExecutionWithRunIDQuery,
			filter.ShardID,
			filter.NamespaceID,
			filter.WorkflowID,
			filter.RunID,
		)
	} else {
		_, err = t.ExecContext(ctx,
			lockCurrentChasmExecutionWithRunIDQuery,
			filter.ShardID,
			filter.NamespaceID,
			filter.WorkflowID,
			filter.ArchetypeID,
			filter.RunID,
		)
	}
	return err
}

// UpdateExecutionsWithCondition fails the transaction when the condition
// matches no row, since a pipeline cannot return the row count.
func (t pipelinedTx) UpdateExecutionsWithCondition(
	ctx context.Context,
	update *sqlplugin.ExecutionsUpdate,
) (sql.Result, error) {
	return t.NamedExecContext(ctx,
		updateExecutionWithConditionOrRejectQuery,
		update,
	)
}

// UpdateCurrentExecutions updates a current execution row only when it differs.
// A sequential transaction decides that from the row it locked, which a
// pipeline cannot read.
func (t pipelinedTx) UpdateCurrentExecutions(
	ctx context.Context,
	row *sqlplugin.CurrentExecutionsRow,
) (sql.Result, error) {
	if err := t.assertArchetypeIDSpecified(row.ArchetypeID); err != nil {
		return nil, err
	}

	if row.ArchetypeID == chasm.WorkflowArchetypeID {
		return t.NamedExecContext(ctx,
			updateCurrentExecutionsIfChangedQuery,
			row,
		)
	}

	return t.NamedExecContext(ctx,
		updateCurrentChasmExecutionsIfChangedQuery,
		row,
	)
}

// Commit sends the queued statements as one batch. Its errors go through
// ConvertError, as BeginTx's do.
func (t pipelinedTx) Commit() error {
	p := t.pipe
	conn, err := p.db.Conn(p.ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", sqlplugin.ErrPipelineNotWritten, t.handle.ConvertError(err))
	}
	defer func() { _ = conn.Close() }()

	var sendErr error
	rawErr := conn.Raw(func(driverConn any) error {
		sendErr = p.send(driverConn.(*stdlib.Conn).Conn())
		if sendErr != nil && !errors.Is(sendErr, sqlplugin.ErrPipelineNotWritten) {
			// The connection can be left mid-batch, so database/sql must close it
			// instead of pooling it.
			return sqldriver.ErrBadConn
		}
		return nil
	})
	if sendErr == nil {
		sendErr = rawErr
	}
	return t.handle.ConvertError(sendErr)
}

// Rollback drops the queued statements, which were never sent.
func (t pipelinedTx) Rollback() error {
	t.pipe.statements = nil
	return nil
}

// send runs the queued statements as one batch. pgx runs a batch without
// transaction control statements as one implicit transaction, which Postgres
// commits after the last statement. A statement error means Postgres skipped
// the rest and rolled the transaction back, so nothing was written. An error
// after every statement succeeded comes from the commit, which may have
// happened.
//
// In simple protocol the batch is one multi-statement query, and Postgres
// reports a commit error as the last statement's result. A trailing statement
// that cannot fail on its own keeps that error apart from the statements'.
func (p *pipeline) send(conn *pgx.Conn) error {
	batch := &pgx.Batch{}
	for _, s := range p.statements {
		batch.Queue(s.sql, s.args...)
	}
	simpleProtocol := conn.Config().DefaultQueryExecMode == pgx.QueryExecModeSimpleProtocol
	if simpleProtocol {
		batch.Queue("SELECT 1")
	}
	results := conn.SendBatch(p.ctx, batch)
	for range p.statements {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				return fmt.Errorf("%w: %w", sqlplugin.ErrPipelineNotWritten, err)
			}
			return err
		}
	}
	if simpleProtocol {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return err
		}
	}
	return results.Close()
}

func (p *pipeline) Rebind(query string) string {
	return p.db.Rebind(query)
}

func (p *pipeline) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	p.statements = append(p.statements, pipelinedStatement{sql: query, args: args})
	return queuedResult{}, nil
}

func (p *pipeline) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	bound, args, err := p.db.BindNamed(query, arg)
	if err != nil {
		return nil, err
	}
	return p.ExecContext(ctx, bound, args...)
}

// GetContext is not answered by pipelined transactions.
func (p *pipeline) GetContext(context.Context, any, string, ...any) error {
	return sqlplugin.ErrNoPipelinedResults
}

// SelectContext is not answered by pipelined transactions.
func (p *pipeline) SelectContext(context.Context, any, string, ...any) error {
	return sqlplugin.ErrNoPipelinedResults
}

// PrepareNamedContext is not used by pipelined transactions.
func (p *pipeline) PrepareNamedContext(context.Context, string) (*sqlx.NamedStmt, error) {
	return nil, sqlplugin.ErrNoPipelinedResults
}

func (queuedResult) LastInsertId() (int64, error) {
	return 0, sqlplugin.ErrNoPipelinedResults
}

func (queuedResult) RowsAffected() (int64, error) {
	return 0, sqlplugin.ErrNoPipelinedResults
}
