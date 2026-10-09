package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/ncruces/go-sqlite3"
	sqlitedriver "github.com/ncruces/go-sqlite3/driver"
	"github.com/ncruces/go-sqlite3/ext/fts5"
	_ "github.com/ncruces/go-sqlite3/vfs/memdb" // registers the in-memory VFS
)

const (
	goSQLDriverName = "sqlite"
	// The driver's "sqlite" format drops fractional seconds.
	driverTimeFormat = "2006-01-02 15:04:05.999999999-07:00"
)

var sqlTableExistsRegex = regexp.MustCompile("SQL logic error: table .* already exists$")

func init() {
	sql.Register(goSQLDriverName, &sqliteDriver{})
}

type sqliteDriver struct{}

func (d *sqliteDriver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (*sqliteDriver) OpenConnector(name string) (driver.Connector, error) {
	connector, err := (&sqlitedriver.SQLite{}).OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return &sqliteConnector{Connector: connector}, nil
}

type sqliteConnector struct {
	driver.Connector
}

func (c *sqliteConnector) Driver() driver.Driver {
	return &sqliteDriver{}
}

func (c *sqliteConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	native, ok := conn.(sqlitedriver.Conn)
	if !ok {
		return nil, errors.Join(fmt.Errorf("unexpected SQLite connection type %T", conn), conn.Close())
	}
	for _, option := range []sqlite3.DBConfig{sqlite3.DBCONFIG_DQS_DDL, sqlite3.DBCONFIG_DQS_DML} {
		if _, err := native.Raw().Config(option, true); err != nil {
			return nil, errors.Join(err, conn.Close())
		}
	}
	if err := fts5.Register(native.Raw()); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	return &sqliteConn{Conn: native}, nil
}

type sqliteConn struct {
	sqlitedriver.Conn
	closed bool
}

func (c *sqliteConn) Close() error {
	err := c.Conn.Close()
	c.closed = true
	return err
}

// database/sql needs both interfaces to reuse the connection after a cancelled transaction.
func (c *sqliteConn) ResetSession(context.Context) error {
	if !c.IsValid() {
		return driver.ErrBadConn
	}
	return nil
}

func (c *sqliteConn) IsValid() bool {
	return !c.closed && c.Raw().GetAutocommit()
}

func (c *sqliteConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *sqliteConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.Conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	native, ok := stmt.(sqliteStatement)
	if !ok {
		return nil, errors.Join(fmt.Errorf("unexpected SQLite statement type %T", stmt), stmt.Close())
	}
	return &sqliteStmt{sqliteStatement: native}, nil
}

func (c *sqliteConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if exec, ok := c.Conn.(driver.ExecerContext); ok {
		return exec.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

type sqliteStatement interface {
	driver.Stmt
	driver.StmtExecContext
	driver.StmtQueryContext
	driver.NamedValueChecker
}

type sqliteStmt struct {
	sqliteStatement
}

func (s *sqliteStmt) CheckNamedValue(arg *driver.NamedValue) error {
	if err := s.sqliteStatement.CheckNamedValue(arg); err != nil {
		if !errors.Is(err, driver.ErrSkip) {
			return err
		}
		value, err := driver.DefaultParameterConverter.ConvertValue(arg.Value)
		if err != nil {
			return err
		}
		arg.Value = value
	}
	// Preserve the existing storage layout for timestamp comparisons with older databases.
	if timestamp, ok := arg.Value.(time.Time); ok {
		arg.Value = timestamp.Format(driverTimeFormat)
	}
	return nil
}

func (*db) IsDupEntryError(err error) bool {
	return errors.Is(err, sqlite3.CONSTRAINT_PRIMARYKEY) || errors.Is(err, sqlite3.CONSTRAINT_UNIQUE)
}

func isTableExistsError(err error) bool {
	if sqlErr, ok := errors.AsType[*sqlite3.Error](err); ok {
		return sqlErr.Code() == sqlite3.ERROR && sqlTableExistsRegex.MatchString(sqlErr.Error())
	}
	return false
}
