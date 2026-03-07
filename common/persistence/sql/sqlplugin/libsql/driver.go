package libsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"regexp"
	"strings"

	_ "github.com/tursodatabase/go-libsql"
)

const (
	goSQLDriverName = "libsql_temporal"
	innerDriverName = "libsql"

	sqlTableExistsPattern  = `(?i)table .* already exists`
	sqliteConstraintUnique = "error code = 2067" // SQLITE_CONSTRAINT_UNIQUE
	sqliteConstraintPK     = "error code = 1555" // SQLITE_CONSTRAINT_PRIMARYKEY
)

// wrappedDriver wraps go-libsql's driver to return connections that implement
// ResetSession() and IsValid(), preventing the SQL library from closing the
// connection when a transaction context is canceled.
type wrappedDriver struct {
	inner driver.Driver
}

func (d *wrappedDriver) Open(name string) (driver.Conn, error) {
	c, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &conn{inner: c}, nil
}

// conn wraps a go-libsql connection. We can't embed driver.Conn because
// go-libsql's conn also implements ExecerContext, QueryerContext, etc. and
// embedding would shadow those from database/sql's type assertions.
type conn struct {
	inner driver.Conn
}

func (c *conn) Prepare(query string) (driver.Stmt, error) { return c.inner.Prepare(query) }
func (c *conn) Close() error                              { return c.inner.Close() }
func (c *conn) Begin() (driver.Tx, error)                 { return c.inner.Begin() }

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if bt, ok := c.inner.(driver.ConnBeginTx); ok {
		return bt.BeginTx(ctx, opts)
	}
	return c.inner.Begin()
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if pc, ok := c.inner.(driver.ConnPrepareContext); ok {
		return pc.PrepareContext(ctx, query)
	}
	return c.inner.Prepare(query)
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if ec, ok := c.inner.(driver.ExecerContext); ok {
		return ec.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if qc, ok := c.inner.(driver.QueryerContext); ok {
		return qc.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

// ResetSession does nothing. We only have one connection to the db. It should always be valid.
// Otherwise we would have already lost the database.
func (c *conn) ResetSession(_ context.Context) error { return nil }

// IsValid always returns true. Same reasoning as ResetSession.
func (c *conn) IsValid() bool { return true }

func init() {
	// go-libsql registers "libsql" in its own init(); grab the driver and wrap it.
	db, err := sql.Open(innerDriverName, ":memory:")
	if err != nil {
		panic("libsql plugin: " + err.Error())
	}
	inner := db.Driver()
	_ = db.Close()
	sql.Register(goSQLDriverName, &wrappedDriver{inner: inner})
}

var sqlTableExistsRegex = regexp.MustCompile(sqlTableExistsPattern)

// go-libsql doesn't expose typed errors, but does include the extended
// error code in the message (e.g. "error code = 2067: UNIQUE constraint failed").
func (*db) IsDupEntryError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, sqliteConstraintUnique) || strings.Contains(msg, sqliteConstraintPK) {
		return true
	}
	// fallback for forward compat if error format changes
	lower := strings.ToLower(msg)
	return strings.Contains(lower, "unique constraint failed") ||
		strings.Contains(lower, "primary key constraint failed")
}

func isTableExistsError(err error) bool {
	if err == nil {
		return false
	}
	return sqlTableExistsRegex.MatchString(err.Error())
}
