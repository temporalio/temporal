package sqlite

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	testSchemaCreateTablePattern   = regexp.MustCompile(`(?i)^\s*create\s+(?:temp(?:orary)?\s+)?table\b`)
	testSchemaVarcharColumnPattern = regexp.MustCompile(`(?im)(?:^|[,(])\s*("?[a-z_][a-z0-9_]*"?)\s+varchar\s*\(\s*(\d+)\s*\)`)
)

// RewriteSchemaStatements makes SQLite schemas catch values that would
// fail against SQL databases that enforce VARCHAR(n), such as Postgres and MySQL.
func (mdb *db) RewriteSchemaStatements(statements []string) []string {
	rewritten := make([]string, len(statements))
	for i, stmt := range statements {
		rewritten[i] = addSchemaVarcharLengthChecks(stmt)
	}
	return rewritten
}

func addSchemaVarcharLengthChecks(stmt string) string {
	// Only CREATE TABLE statements can accept generated table constraints.
	if !testSchemaCreateTablePattern.MatchString(stmt) {
		return stmt
	}

	varcharColumns := testSchemaVarcharColumnPattern.FindAllStringSubmatch(stmt, -1)
	if len(varcharColumns) == 0 {
		return stmt
	}

	// Append table-level CHECK constraints instead of editing column definitions.
	trimmedStmt := strings.TrimSpace(stmt)
	trimmedStmt = strings.TrimSuffix(trimmedStmt, ";")
	trimmedStmt = strings.TrimSpace(trimmedStmt)
	tableConstraintOffset := strings.LastIndex(trimmedStmt, ")")
	if tableConstraintOffset < 0 {
		return stmt
	}

	var constraints strings.Builder
	for _, column := range varcharColumns {
		fmt.Fprintf(&constraints, ",\n\tCHECK(length(%s) <= %s)", column[1], column[2])
	}

	return stmt[:tableConstraintOffset] + constraints.String() + stmt[tableConstraintOffset:]
}
