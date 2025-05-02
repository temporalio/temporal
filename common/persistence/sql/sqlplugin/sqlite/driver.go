package sqlite

import (
	"errors"
	"regexp"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	goSqlDriverName       = "sqlite"
	sqlConstraintCodes    = sqlite3.SQLITE_CONSTRAINT | sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY | sqlite3.SQLITE_CONSTRAINT_UNIQUE
	sqlTableExistsPattern = "SQL logic error: table .* already exists \\(1\\)"
)

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
