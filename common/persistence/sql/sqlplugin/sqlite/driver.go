package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"regexp"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	goSQLDriverName       = "sqlite_temporal"
	sqlConstraintCodes    = sqlite3.SQLITE_CONSTRAINT | sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY | sqlite3.SQLITE_CONSTRAINT_UNIQUE
	sqlTableExistsPattern = "SQL logic error: table .* already exists \\(1\\)"
)

// Driver implements a SQL driver that returns a custom connection.
// The custom connection provides ResetSession() and IsValid() methods,
// preventing the SQL library from closing the connection when a transaction context is canceled.
type Driver struct {
	sqlite.Driver
}

func newDriver() driver.Driver {
	return &Driver{sqlite.Driver{}}
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	c, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	return &conn{c}, nil
}

type conn struct {
	driver.Conn
}

// ResetSession does nothing. If the connection is not valid for some reason, we must have lost the database already.
// And there is nothing we can do to create a new connection. Because of this it is safe to return valid in all cases.
// We can let the next database operation fail if the connection is not valid.
func (c *conn) ResetSession(_ context.Context) error {
	return nil
}

// IsValid always returns true. We only have one connection to the sqlite db. It should always be valid. Otherwise,
// we would have lost the database.
func (c *conn) IsValid() bool {
	return true
}

func init() {
	sql.Register(goSQLDriverName, newDriver())
}

var sqlTableExistsRegex = regexp.MustCompile(sqlTableExistsPattern)

func (*db) IsDupEntryError(err error) bool {
	var sqlErr *sqlite.Error
	if errors.As(err, &sqlErr) {
		return sqlErr.Code()&sqlConstraintCodes != 0
	}

	return false
}

func isTableExistsError(err error) bool {
	var sqlErr *sqlite.Error
	if errors.As(err, &sqlErr) {
		return sqlTableExistsRegex.MatchString(sqlErr.Error())
	}

	return false
}
