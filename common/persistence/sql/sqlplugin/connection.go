package sqlplugin

import (
	"context"
	"database/sql/driver"
	"errors"
)

type refreshingConn struct {
	driver.Conn
	connector  *RefreshingConnector
	generation uint64
}

// Adding Validator to a driver changes database/sql's canceled-transaction cleanup.
type validatingConn struct {
	*refreshingConn
	discard bool
}

type validatingTx struct {
	driver.Tx
	ctx  context.Context
	conn *validatingConn
}

func (c *refreshingConn) ResetSession(ctx context.Context) error {
	if c.generation != c.connector.generation.Load() {
		return driver.ErrBadConn
	}
	if resetter, ok := c.Conn.(driver.SessionResetter); ok {
		return resetter.ResetSession(ctx)
	}
	return nil
}

func (c *validatingConn) IsValid() bool {
	return !c.discard && c.generation == c.connector.generation.Load() && c.Conn.(driver.Validator).IsValid()
}

func (c *validatingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.refreshingConn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	if _, ok := c.Conn.(driver.SessionResetter); !ok {
		return &validatingTx{Tx: tx, ctx: ctx, conn: c}, nil
	}
	return tx, nil
}

func (tx *validatingTx) Commit() error {
	err := tx.Tx.Commit()
	tx.conn.discard = tx.ctx.Err() != nil
	return err
}

func (tx *validatingTx) Rollback() error {
	err := tx.Tx.Rollback()
	tx.conn.discard = tx.ctx.Err() != nil
	return err
}

func (c *refreshingConn) Ping(ctx context.Context) error {
	if pinger, ok := c.Conn.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

func (c *refreshingConn) CheckNamedValue(value *driver.NamedValue) error {
	if checker, ok := c.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

func (c *refreshingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if preparer, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return preparer.PrepareContext(ctx, query)
	}
	stmt, err := c.Prepare(query)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		_ = stmt.Close()
		return nil, err
	}
	return stmt, nil
}

//nolint:staticcheck // Preserve database/sql's fallback for legacy drivers.
func (c *refreshingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if beginner, ok := c.Conn.(driver.ConnBeginTx); ok {
		return beginner.BeginTx(ctx, opts)
	}
	if opts.Isolation != 0 {
		return nil, errors.New("sql: driver does not support non-default isolation level")
	}
	if opts.ReadOnly {
		return nil, errors.New("sql: driver does not support read-only transactions")
	}
	tx, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

//nolint:staticcheck // Preserve database/sql's fallback for legacy drivers.
func (c *refreshingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if execer, ok := c.Conn.(driver.ExecerContext); ok {
		return execer.ExecContext(ctx, query, args)
	}
	execer, ok := c.Conn.(driver.Execer)
	if !ok {
		return nil, driver.ErrSkip
	}
	values, err := positionalValues(args)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return execer.Exec(query, values)
}

//nolint:staticcheck // Preserve database/sql's fallback for legacy drivers.
func (c *refreshingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if queryer, ok := c.Conn.(driver.QueryerContext); ok {
		return queryer.QueryContext(ctx, query, args)
	}
	queryer, ok := c.Conn.(driver.Queryer)
	if !ok {
		return nil, driver.ErrSkip
	}
	values, err := positionalValues(args)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return queryer.Query(query, values)
}

func positionalValues(args []driver.NamedValue) ([]driver.Value, error) {
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		if arg.Name != "" {
			return nil, errors.New("sql: driver does not support named parameters")
		}
		values[i] = arg.Value
	}
	return values, nil
}
