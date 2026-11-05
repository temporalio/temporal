package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

// divisionByZeroCode is the code of the error a failed check statement returns.
const divisionByZeroCode = "22012"

// The values of db.pipelining.
const (
	pipeliningUnchecked int32 = iota
	pipeliningSupported
	pipeliningUnsupported
)

type (
	// pipeline queues a transaction's statements until Commit sends them on conn.
	pipeline struct {
		ctx        context.Context
		db         *sqlx.DB
		conn       *sql.Conn
		statements []pipelinedStatement
	}

	pipelinedStatement struct {
		sql  string
		args []any
		// checkErr is the error a check statement stands for.
		checkErr error
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

// BeginPipelinedTx takes the connection Commit sends the batch on from the pool.
// It checks that every connection is a pgx connection, and the first one
// decides the exec mode. Once a check fails, this and every later call return
// ErrPipelineUnsupported, the later ones without taking a connection.
func (pdb *db) BeginPipelinedTx(ctx context.Context) (sqlplugin.PipelinedTx, error) {
	if pdb.pipelining.Load() == pipeliningUnsupported {
		return nil, sqlplugin.ErrPipelineUnsupported
	}
	sqlDB, err := pdb.handle.DB()
	if err != nil {
		return nil, err
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return nil, pdb.handle.ConvertError(err)
	}
	err = conn.Raw(func(driverConn any) error {
		// A custom SQL.Connect can return another driver's connections, also after
		// the handle rebuilds the pool.
		pgxConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			pdb.pipelining.Store(pipeliningUnsupported)
			return sqlplugin.ErrPipelineUnsupported
		}
		if pdb.pipelining.Load() == pipeliningUnchecked {
			// Simple protocol sends the batch as one query, which Postgres logs whole,
			// values included, when a check fails.
			if pgxConn.Conn().Config().DefaultQueryExecMode == pgx.QueryExecModeSimpleProtocol {
				pdb.pipelining.Store(pipeliningUnsupported)
				return sqlplugin.ErrPipelineUnsupported
			}
			pdb.pipelining.Store(pipeliningSupported)
		}
		return nil
	})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	tx := newDB(pdb.dbKind, pdb.dbName, pdb.dbDriver, pdb.handle, nil, pdb.logger)
	tx.pipe = &pipeline{ctx: ctx, db: sqlDB, conn: conn}
	return pipelinedTx{db: tx}, nil
}

func (t pipelinedTx) ReadLockShardsWithRangeID(
	_ context.Context,
	filter sqlplugin.ShardsFilter,
	rangeID int64,
) error {
	t.pipe.check(sqlplugin.ErrShardRangeIDMismatch,
		readLockShardWithRangeIDQry,
		filter.ShardID,
		rangeID,
	)
	return nil
}

func (t pipelinedTx) LockCurrentExecutionsWithRunID(
	_ context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) error {
	if err := t.assertArchetypeIDSpecified(filter.ArchetypeID); err != nil {
		return err
	}

	if filter.ArchetypeID == chasm.WorkflowArchetypeID {
		t.pipe.check(sqlplugin.ErrCurrentExecutionMissing,
			lockCurrentExecutionExistsQuery,
			filter.ShardID,
			filter.NamespaceID,
			filter.WorkflowID,
		)
		t.pipe.check(sqlplugin.ErrCurrentExecutionRunIDMismatch,
			lockCurrentExecutionWithRunIDQuery,
			filter.ShardID,
			filter.NamespaceID,
			filter.WorkflowID,
			filter.RunID,
		)
	} else {
		t.pipe.check(sqlplugin.ErrCurrentExecutionMissing,
			lockCurrentChasmExecutionExistsQuery,
			filter.ShardID,
			filter.NamespaceID,
			filter.WorkflowID,
			filter.ArchetypeID,
		)
		t.pipe.check(sqlplugin.ErrCurrentExecutionRunIDMismatch,
			lockCurrentChasmExecutionWithRunIDQuery,
			filter.ShardID,
			filter.NamespaceID,
			filter.WorkflowID,
			filter.ArchetypeID,
			filter.RunID,
		)
	}
	return nil
}

// UpdateExecutionsWithCondition fails the transaction when the condition
// matches no row, since a pipeline cannot return the row count.
func (t pipelinedTx) UpdateExecutionsWithCondition(
	_ context.Context,
	update *sqlplugin.ExecutionsUpdate,
) (sql.Result, error) {
	query, args, err := t.pipe.db.BindNamed(updateExecutionWithConditionOrRejectQuery, update)
	if err != nil {
		return nil, err
	}
	t.pipe.check(sqlplugin.ErrExecutionMissing,
		writeLockExecutionExistsQuery,
		update.ShardID,
		update.NamespaceID,
		update.WorkflowID,
		update.RunID,
	)
	t.pipe.check(sqlplugin.ErrExecutionConditionMismatch, query, args...)
	return queuedResult{}, nil
}

// UpdateCurrentExecutionsIfChanged updates a current execution row only when it
// differs. A sequential transaction decides that from the row it locked, which a
// pipeline cannot read.
func (t pipelinedTx) UpdateCurrentExecutionsIfChanged(
	ctx context.Context,
	row *sqlplugin.CurrentExecutionsRow,
) error {
	var err error

	if err := t.assertArchetypeIDSpecified(row.ArchetypeID); err != nil {
		return err
	}

	if row.ArchetypeID == chasm.WorkflowArchetypeID {
		_, err = t.NamedExecContext(ctx,
			updateCurrentExecutionsIfChangedQuery,
			row,
		)
	} else {
		_, err = t.NamedExecContext(ctx,
			updateCurrentChasmExecutionsIfChangedQuery,
			row,
		)
	}
	return err
}

// Commit sends the queued statements as one batch and returns the connection to
// the pool. Its errors go through ConvertError, as BeginTx's do, except
// ErrPipelineNotSent: the caller runs the statements another way, and the pool
// discards the connection pgx closed when it next checks it out.
func (t pipelinedTx) Commit() error {
	err := t.pipe.sendOnConn()
	if errors.Is(err, sqlplugin.ErrPipelineNotSent) {
		return err
	}
	return t.handle.ConvertError(err)
}

// Rollback returns the connection to the pool without sending the queued
// statements.
func (t pipelinedTx) Rollback() error {
	return t.pipe.conn.Close()
}

// check queues a statement that fails the transaction when checkErr's check fails.
func (p *pipeline) check(checkErr error, query string, args ...any) {
	p.statements = append(p.statements, pipelinedStatement{sql: query, args: args, checkErr: checkErr})
}

// sendOnConn sends the batch on the connection and returns it to the pool.
func (p *pipeline) sendOnConn() error {
	defer func() { _ = p.conn.Close() }()
	return p.conn.Raw(func(driverConn any) error {
		// BeginPipelinedTx checked that this is a pgx connection.
		return p.send(driverConn.(*stdlib.Conn).Conn())
	})
}

// send runs the queued statements as one batch. pgx runs a batch without
// transaction control statements as one implicit transaction, which Postgres
// commits after the last statement. A check statement fails with a division by
// zero, which send wraps in the statement's checkErr.
func (p *pipeline) send(conn *pgx.Conn) error {
	batch := &pgx.Batch{}
	for _, s := range p.statements {
		batch.Queue(s.sql, s.args...)
	}
	results := conn.SendBatch(p.ctx, batch)
	for _, s := range p.statements {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == divisionByZeroCode && s.checkErr != nil {
				return fmt.Errorf("%w: %w", s.checkErr, err)
			}
			// pgx marks an error as safe to retry when the write that runs the
			// statements sent nothing.
			if pgconn.SafeToRetry(err) {
				return fmt.Errorf("%w: %w", sqlplugin.ErrPipelineNotSent, err)
			}
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
