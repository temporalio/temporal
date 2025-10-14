package sql

import (
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

type (
	// unsafeSQLString don't escape the string value; unlike sqlparser.SQLVal.
	// This is used for building string known to be safe.
	unsafeSQLString struct {
		sqlparser.Expr
		Val string
	}

	colName struct {
		sqlparser.Expr
		Name string
	}

	saColName struct {
		sqlparser.Expr
		dbColName *colName
		alias     string
		fieldName string
		valueType enumspb.IndexedValueType
	}
)

const (
	coalesceFuncName = "coalesce"
)

var _ sqlparser.Expr = (*unsafeSQLString)(nil)
var _ sqlparser.Expr = (*colName)(nil)
var _ sqlparser.Expr = (*saColName)(nil)

var (
	maxDatetimeValue = getMaxDatetimeValue()

	closeTimeSaColName = newSAColName(
		searchattribute.GetSqlDbColName(searchattribute.CloseTime),
		searchattribute.CloseTime,
		searchattribute.CloseTime,
		enumspb.INDEXED_VALUE_TYPE_DATETIME,
	)
)

func (node *unsafeSQLString) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("'%s'", node.Val)
}

func (node *colName) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("%s", node.Name)
}

func (node *saColName) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("%v", node.dbColName)
}

func newUnsafeSQLString(val string) *unsafeSQLString {
	return &unsafeSQLString{Val: val}
}

func newColName(name string) *colName {
	return &colName{Name: name}
}

func newSAColName(
	dbColName string,
	alias string,
	fieldName string,
	valueType enumspb.IndexedValueType,
) *saColName {
	return &saColName{
		dbColName: newColName(dbColName),
		alias:     alias,
		fieldName: fieldName,
		valueType: valueType,
	}
}

func newFuncExpr(name string, exprs ...sqlparser.Expr) *sqlparser.FuncExpr {
	args := make([]sqlparser.SelectExpr, len(exprs))
	for i := range exprs {
		args[i] = &sqlparser.AliasedExpr{Expr: exprs[i]}
	}
	return &sqlparser.FuncExpr{
		Name:  sqlparser.NewColIdent(name),
		Exprs: args,
	}
}

func addPrefix(prefix string, fields []string) []string {
	out := make([]string, len(fields))
	for i, field := range fields {
		out[i] = prefix + field
	}
	return out
}

func getMaxDatetimeValue() time.Time {
	t, _ := time.Parse(time.RFC3339, "9999-12-31T23:59:59Z")
	return t
}

// formatComparisonExprStringForError formats comparison expression after
// custom search attribute was mapped to field name to show alias name for
// better user experience.
func formatComparisonExprStringForError(expr sqlparser.ComparisonExpr) string {
	if colNameExpr, ok := expr.Left.(*saColName); ok {
		expr.Left = newColName(colNameExpr.alias)
	}
	return sqlparser.String(&expr)
}

// Simple tokenizer by spaces. It's a temporary solution as it doesn't cover tokenizer used by
// PostgreSQL or SQLite.
func tokenizeTextQueryString(s string) []string {
	nonEmptyTokens := make([]string, 0, strings.Count(s, " ")+1)
	for token := range strings.SplitSeq(s, " ") {
		if token != "" {
			nonEmptyTokens = append(nonEmptyTokens, token)
		}
	}
	return nonEmptyTokens
}

func getUnsafeStringTupleValues(valTuple sqlparser.ValTuple) ([]string, error) {
	values := make([]string, len(valTuple))
	for i, val := range valTuple {
		switch v := val.(type) {
		case *unsafeSQLString:
			values[i] = v.Val
		default:
			return nil, query.NewConverterError(
				"%s: unexpected value type in tuple (expected string, got %v)",
				query.InvalidExpressionErrMessage,
				sqlparser.String(v),
			)
		}
	}
	return values, nil
}
