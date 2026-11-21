package sqlite

import (
	"errors"
	"regexp"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	goSQLDriverName       = "sqlite"
	sqlTableExistsPattern = "SQL logic error: table .* already exists \\(1\\)"
)

var sqlTableExistsRegex = regexp.MustCompile(sqlTableExistsPattern)

func (*db) IsDupEntryError(err error) bool {
	sqlErr, ok := errors.AsType[*sqlite.Error](err)
	if !ok {
		return false
	}
	// modernc enables extended result codes. A duplicate row is a primary-key
	// or unique violation. Other constraint failures use a different extended
	// code, and a bare SQLITE_CONSTRAINT is not a duplicate either.
	switch sqlErr.Code() {
	case sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY, sqlite3.SQLITE_CONSTRAINT_UNIQUE:
		return true
	default:
		return false
	}
}

func isTableExistsError(err error) bool {
	if sqlErr, ok := errors.AsType[*sqlite.Error](err); ok {
		return sqlTableExistsRegex.MatchString(sqlErr.Error())
	}

	return false
}
