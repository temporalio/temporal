package postgresql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type (
	// pipeline queues a transaction's statements until Flush. After Flush it
	// answers the same calls, in the same order, from the flushed results.
	pipeline struct {
		ctx        context.Context
		db         *sqlx.DB
		statements []pipelinedStatement
		flushed    bool
		next       int
		diverged   bool
		conn       *sql.Conn
		typeMap    *pgtype.Map
	}

	pipelinedStatement struct {
		// query is the text the table method passed, which replay compares.
		query string
		sql   string
		args  []any
		read  bool

		rowsAffected int64
		fields       []pgconn.FieldDescription
		// values is nil when the read returned no row.
		values [][]byte
		err    error
	}

	// pipelinedTx is a transaction whose statements go through a pipeline.
	pipelinedTx struct {
		*db
	}

	// notFlushedResult is the result of an exec queued before Flush.
	notFlushedResult struct{}
)

var (
	_ sqlplugin.TxPipeliner = (*db)(nil)
	_ sqlplugin.PipelinedTx = pipelinedTx{}
)

func (pdb *db) BeginPipelinedTx(ctx context.Context) (sqlplugin.PipelinedTx, error) {
	sqlDB, err := pdb.handle.DB()
	if err != nil {
		return nil, err
	}
	tx := newDB(pdb.dbKind, pdb.dbName, pdb.dbDriver, pdb.handle, nil, pdb.logger)
	tx.pipe = &pipeline{ctx: ctx, db: sqlDB}
	return pipelinedTx{db: tx}, nil
}

// Flush sends BEGIN and the queued statements as one batch and saves each
// statement's result. Its errors go through ConvertError, as BeginTx's do.
func (t pipelinedTx) Flush() error {
	p := t.pipe
	conn, err := p.db.Conn(p.ctx)
	if err != nil {
		return t.handle.ConvertError(err)
	}
	p.conn = conn
	p.flushed = true
	if err := p.run(p.send); err != nil {
		_ = conn.Close()
		return t.handle.ConvertError(err)
	}
	return nil
}

func (t pipelinedTx) Diverged() bool {
	return t.pipe.diverged
}

func (t pipelinedTx) Commit() error {
	p := t.pipe
	if p.diverged || p.next != len(p.statements) {
		p.diverged = true
		_ = t.Rollback()
		return fmt.Errorf("pipelined replay used %d of %d statements", p.next, len(p.statements))
	}
	defer func() { _ = p.conn.Close() }()
	return p.run(func(conn *pgx.Conn) error {
		tag, err := conn.Exec(p.ctx, "commit")
		if err == nil && tag.String() == "ROLLBACK" {
			return pgx.ErrTxCommitRollback
		}
		return err
	})
}

func (t pipelinedTx) Rollback() error {
	p := t.pipe
	defer func() { _ = p.conn.Close() }()
	return p.run(func(conn *pgx.Conn) error {
		_, err := conn.Exec(p.ctx, "rollback")
		return err
	})
}

// run calls f with the pipeline connection's pgx conn.
func (p *pipeline) run(f func(conn *pgx.Conn) error) error {
	var err error
	rawErr := p.conn.Raw(func(driverConn any) error {
		stdlibConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			err = sqlplugin.ErrPipelineUnsupported
			return nil
		}
		err = f(stdlibConn.Conn())
		if err != nil && !errors.Is(err, sqlplugin.ErrPipelineUnsupported) {
			// The connection can be left inside a transaction, so database/sql
			// must close it instead of pooling it.
			return driver.ErrBadConn
		}
		return nil
	})
	if err == nil {
		err = rawErr
	}
	return err
}

// send runs BEGIN and the queued statements as one batch. Postgres skips the
// statements after the first error, and pgx returns that error for each of them.
func (p *pipeline) send(conn *pgx.Conn) error {
	if conn.Config().DefaultQueryExecMode != pgx.QueryExecModeCacheStatement {
		return sqlplugin.ErrPipelineUnsupported
	}
	batch := &pgx.Batch{}
	batch.Queue("begin")
	for _, s := range p.statements {
		batch.Queue(s.sql, s.args...)
	}
	results := conn.SendBatch(p.ctx, batch)
	if _, err := results.Exec(); err != nil {
		_ = results.Close()
		return err
	}
	var statementErr error
	for i := range p.statements {
		s := &p.statements[i]
		if s.read {
			s.fields, s.values, s.err = firstRow(results)
		} else {
			var tag pgconn.CommandTag
			tag, s.err = results.Exec()
			s.rowsAffected = tag.RowsAffected()
		}
		if statementErr == nil {
			statementErr = s.err
		}
	}
	p.typeMap = conn.TypeMap()
	// Close repeats the first statement error, which replay returns already.
	if err := results.Close(); statementErr == nil {
		return err
	}
	return nil
}

// firstRow reads a query result the way database/sql's Row.Scan does: the
// first row counts and the rest of the result must complete without error.
// It copies the row because pgx reuses its buffers.
func firstRow(results pgx.BatchResults) ([]pgconn.FieldDescription, [][]byte, error) {
	rows, err := results.Query()
	if err != nil {
		return nil, nil, err
	}
	// Next closes the rows when it returns false.
	if !rows.Next() {
		return nil, nil, rows.Err()
	}
	fields := slices.Clone(rows.FieldDescriptions())
	values := make([][]byte, len(rows.RawValues()))
	for i, value := range rows.RawValues() {
		values[i] = slices.Clone(value)
	}
	rows.Close()
	return fields, values, rows.Err()
}

// replay returns the flushed result for the next statement. A call that is not
// the statement recorded at that position marks the pipeline diverged.
func (p *pipeline) replay(query string) (*pipelinedStatement, error) {
	if p.diverged || p.next >= len(p.statements) || p.statements[p.next].query != query {
		p.diverged = true
		return nil, fmt.Errorf("pipelined replay diverged at statement %d", p.next)
	}
	s := &p.statements[p.next]
	p.next++
	return s, s.err
}

func (p *pipeline) Rebind(query string) string {
	return p.db.Rebind(query)
}

func (p *pipeline) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	return p.exec(query, query, args)
}

func (p *pipeline) NamedExecContext(_ context.Context, query string, arg any) (sql.Result, error) {
	if p.flushed {
		return p.exec(query, "", nil)
	}
	bound, args, err := p.db.BindNamed(query, arg)
	if err != nil {
		return nil, err
	}
	return p.exec(query, bound, args)
}

func (p *pipeline) exec(query string, bound string, args []any) (sql.Result, error) {
	if !p.flushed {
		p.statements = append(p.statements, pipelinedStatement{query: query, sql: bound, args: args})
		return notFlushedResult{}, nil
	}
	s, err := p.replay(query)
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(s.rowsAffected), nil
}

func (p *pipeline) GetContext(_ context.Context, dest any, query string, args ...any) error {
	if !p.flushed {
		p.statements = append(p.statements, pipelinedStatement{query: query, sql: query, args: args, read: true})
		return sqlplugin.ErrNotFlushed
	}
	s, err := p.replay(query)
	if err != nil {
		return err
	}
	if s.values == nil {
		return sql.ErrNoRows
	}
	return pgx.ScanRow(p.typeMap, s.fields, s.values, p.scanTargets(dest, s.fields)...)
}

// scanTargets returns what sqlx's Get scans into: dest itself for a non-struct
// type, or the struct fields the DB's mapper matches to the column names.
func (p *pipeline) scanTargets(dest any, fields []pgconn.FieldDescription) []any {
	v := reflect.ValueOf(dest).Elem()
	if v.Kind() != reflect.Struct {
		return []any{dest}
	}
	columns := make([]string, len(fields))
	for i, field := range fields {
		columns[i] = field.Name
	}
	targets := make([]any, len(columns))
	for i, traversal := range p.db.Mapper.TraversalsByName(v.Type(), columns) {
		targets[i] = reflectx.FieldByIndexes(v, traversal).Addr().Interface()
	}
	return targets
}

// SelectContext is not used by pipelined transactions.
func (p *pipeline) SelectContext(context.Context, any, string, ...any) error {
	return sqlplugin.ErrNotFlushed
}

// PrepareNamedContext is not used by pipelined transactions.
func (p *pipeline) PrepareNamedContext(context.Context, string) (*sqlx.NamedStmt, error) {
	return nil, sqlplugin.ErrNotFlushed
}

func (notFlushedResult) LastInsertId() (int64, error) {
	return 0, sqlplugin.ErrNotFlushed
}

func (notFlushedResult) RowsAffected() (int64, error) {
	return 0, sqlplugin.ErrNotFlushed
}
