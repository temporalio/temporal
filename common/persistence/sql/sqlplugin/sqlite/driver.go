//go:build !wasip1

package sqlite

import (
	"errors"
	"net/url"
	"regexp"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	goSQLDriverName       = "sqlite"
	sqlConstraintCodes    = sqlite3.SQLITE_CONSTRAINT | sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY | sqlite3.SQLITE_CONSTRAINT_UNIQUE
	sqlTableExistsPattern = "SQL logic error: table .* already exists \\(1\\)"
)

var sqlTableExistsRegex = regexp.MustCompile(sqlTableExistsPattern)

func driverDSN(databaseName string, _ url.Values) string {
	return databaseName
}

func driverTimeParameter() (key, value string) {
	return "_time_format", "sqlite"
}

func (*db) IsDupEntryError(err error) bool {
	if sqlErr, ok := errors.AsType[*sqlite.Error](err); ok {
		return sqlErr.Code()&sqlConstraintCodes != 0
	}

	return false
}

func isTableExistsError(err error) bool {
	if sqlErr, ok := errors.AsType[*sqlite.Error](err); ok {
		return sqlTableExistsRegex.MatchString(sqlErr.Error())
	}

	return false
}
