package query

import (
	"strconv"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
)

type (
	// UnsafeSQLString don't escape the string value; unlike sqlparser.SQLVal.
	// This is used for building string known to be safe.
	UnsafeSQLString struct {
		sqlparser.Expr
		Val string
	}

	ColName struct {
		sqlparser.Expr
		Name string
	}

	SAColName struct {
		sqlparser.Expr
		Alias     string
		FieldName string
		ValueType enumspb.IndexedValueType
	}
)

const (
	coalesceFuncName = "coalesce"
)

var _ sqlparser.Expr = (*UnsafeSQLString)(nil)
var _ sqlparser.Expr = (*ColName)(nil)
var _ sqlparser.Expr = (*SAColName)(nil)

var (
	NamespaceIDSaColName = NewSAColName(
		searchattribute.NamespaceID,
		searchattribute.NamespaceID,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	NamespaceDivisionSaColName = NewSAColName(
		searchattribute.TemporalNamespaceDivision,
		searchattribute.TemporalNamespaceDivision,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	CloseTimeSaColName = NewSAColName(
		searchattribute.CloseTime,
		searchattribute.CloseTime,
		enumspb.INDEXED_VALUE_TYPE_DATETIME,
	)
)

func (node *UnsafeSQLString) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("'%s'", node.Val)
}

func (node *ColName) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("%s", node.Name)
}

func (node *SAColName) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("%s", searchattribute.GetSqlDbColName(node.FieldName))
}

func NewUnsafeSQLString(val string) *UnsafeSQLString {
	return &UnsafeSQLString{Val: val}
}

func NewColName(name string) *ColName {
	return &ColName{Name: name}
}

func NewSAColName(alias string, fieldName string, valueType enumspb.IndexedValueType) *SAColName {
	return &SAColName{
		Alias:     alias,
		FieldName: fieldName,
		ValueType: valueType,
	}
}

func NewFuncExpr(name string, exprs ...sqlparser.Expr) *sqlparser.FuncExpr {
	args := make([]sqlparser.SelectExpr, len(exprs))
	for i := range exprs {
		args[i] = &sqlparser.AliasedExpr{Expr: exprs[i]}
	}
	return &sqlparser.FuncExpr{
		Name:  sqlparser.NewColIdent(name),
		Exprs: args,
	}
}

// TokenizeTextQueryString tokenizes the string by spaces
// It's a temporary solution as it doesn't cover tokenizer used by PostgreSQL or SQLite.
func TokenizeTextQueryString(s string) []string {
	tokens := strings.Split(s, " ")
	nonEmptyTokens := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token != "" {
			nonEmptyTokens = append(nonEmptyTokens, token)
		}
	}
	return nonEmptyTokens
}

func GetUnsafeStringTupleValues(valTuple sqlparser.ValTuple) ([]string, error) {
	values := make([]string, len(valTuple))
	for i, val := range valTuple {
		switch v := val.(type) {
		case *UnsafeSQLString:
			values[i] = v.Val
		default:
			return nil, NewConverterError(
				"%s: unexpected value type in tuple (expected string, got %v)",
				InvalidExpressionErrMessage,
				sqlparser.String(v),
			)
		}
	}
	return values, nil
}

func ParseExecutionDurationStr(durationStr string) (time.Duration, error) {
	if durationNanos, err := strconv.ParseInt(durationStr, 10, 64); err == nil {
		return time.Duration(durationNanos), nil
	}

	// To support durations passed as golang durations such as "300ms", "-1.5h" or "2h45m".
	// Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
	// Custom timestamp.ParseDuration also supports "d" as additional unit for days.
	if duration, err := timestamp.ParseDuration(durationStr); err == nil {
		return duration, nil
	}

	// To support "hh:mm:ss" durations.
	return timestamp.ParseHHMMSSDuration(durationStr)
}

func ReduceExprs(
	reduceFunc func(left, right sqlparser.Expr) sqlparser.Expr,
	exprs ...sqlparser.Expr,
) sqlparser.Expr {
	if len(exprs) == 0 {
		return nil
	}
	for n := len(exprs); n > 1; n = (n + 1) / 2 {
		for i := 0; i < n; i += 2 {
			if i+1 < n {
				exprs[i/2] = reduceFunc(exprs[i], exprs[i+1])
			} else {
				exprs[i/2] = exprs[i]
			}
		}
	}
	return exprs[0]
}
