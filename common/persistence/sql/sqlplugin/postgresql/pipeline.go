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
)

// divisionByZeroCode is the code of the error a failed check statement returns.
const divisionByZeroCode = "22012"

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
// It returns ErrPipelineUnsupported unless that is a pgx connection.
func (pdb *db) BeginPipelinedTx(ctx context.Context) (sqlplugin.PipelinedTx, error) {
	sqlDB, err := pdb.handle.DB()
	if err != nil {
		return nil, err
	}
	if _, ok := sqlDB.Driver().(*stdlib.Driver); !ok {
		return nil, sqlplugin.ErrPipelineUnsupported
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return nil, pdb.handle.ConvertError(err)
	}
	err = conn.Raw(func(driverConn any) error {
		// A custom SQL.Connect can return another driver's connections.
		if _, ok := driverConn.(*stdlib.Conn); !ok {
			return sqlplugin.ErrPipelineUnsupported
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
// the pool. Its errors go through ConvertError, as BeginTx's do.
func (t pipelinedTx) Commit() error {
	return t.handle.ConvertError(t.pipe.sendOnConn())
}

// Rollback drops the queued statements, which were never sent, and returns the
// connection to the pool.
func (t pipelinedTx) Rollback() error {
	t.pipe.statements = nil
	return t.pipe.conn.Close()
}

// check queues a statement that fails the transaction when checkErr's check fails.
func (p *pipeline) check(checkErr error, query string, args ...any) {
	p.statements = append(p.statements, pipelinedStatement{sql: query, args: args, checkErr: checkErr})
}

// sendOnConn sends the batch on the connection and returns it to the pool.
func (p *pipeline) sendOnConn() error {
	defer func() { _ = p.conn.Close() }()

	var err error
	rawErr := p.conn.Raw(func(driverConn any) error {
		var notWritten bool
		// BeginPipelinedTx checked that this is a pgx connection.
		notWritten, err = p.send(driverConn.(*stdlib.Conn).Conn())
		if err != nil && !notWritten {
			// The connection can be left mid-batch, so database/sql must close it
			// instead of pooling it.
			return sqldriver.ErrBadConn
		}
		return nil
	})
	if err == nil {
		err = rawErr
	}
	return err
}

// send runs the queued statements as one batch, and reports whether nothing
// was written next to its error. pgx runs a batch without transaction control
// statements as one implicit transaction, which Postgres commits after the last
// statement. A statement error means Postgres skipped the rest and rolled the
// transaction back, so nothing was written, as with an error pgx marks safe to
// retry. An error after every statement succeeded comes from the commit, which
// may have happened. A check statement fails with a division by zero, which send
// wraps in the statement's checkErr.
//
// In simple protocol the batch is one multi-statement query, and Postgres
// reports a commit error as the last statement's result. A trailing statement
// that cannot fail on its own keeps that error apart from the statements'.
func (p *pipeline) send(conn *pgx.Conn) (notWritten bool, err error) {
	batch := &pgx.Batch{}
	for _, s := range p.statements {
		batch.Queue(s.sql, s.args...)
	}
	batch.Queue("SELECT 1")
	results := conn.SendBatch(p.ctx, batch)
	for _, s := range p.statements {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			pgErr, rejected := errors.AsType[*pgconn.PgError](err)
			notWritten := rejected || pgconn.SafeToRetry(err)
			if rejected && pgErr.Code == divisionByZeroCode && s.checkErr != nil {
				err = fmt.Errorf("%w: %w", s.checkErr, err)
			}
			return notWritten, err
		}
	}
	if _, err := results.Exec(); err != nil {
		_ = results.Close()
		return false, err
	}
	return false, results.Close()
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
