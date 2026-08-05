package mssql

import (
	"fmt"
	"strings"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

const (
	// coalesceCloseTimeColName is the name of a PERSISTED computed column
	// defined by the mssql visibility schema as
	// COALESCE(close_time, CONVERT(DATETIME2(6), '9999-12-31 23:59:59')).
	// Unlike the mysql/postgresql plugins, which inline the COALESCE
	// expression into every statement, SQL Server needs a persisted column
	// so the (coalesce_close_time, start_time, run_id) sort order can be
	// backed by an index. The schema definition of this column must stay in
	// sync with the pagination/ordering logic in this file.
	coalesceCloseTimeColName = "coalesce_close_time"

	// openJSONValueColName is the name of the value column returned by
	// OPENJSON with the default schema (columns: key, value, type).
	openJSONValueColName = "value"
)

type (
	// openJSONExistsExpr builds an EXISTS predicate over the elements of a
	// JSON array column (used for KeywordList search attributes):
	//
	//	exists (select 1 from openjson(<JSONDoc>) where <Filter>)
	openJSONExistsExpr struct {
		sqlparser.Expr
		JSONDoc sqlparser.Expr
		Filter  sqlparser.Expr
	}

	// containsExpr builds a SQL Server full-text search predicate (used for
	// Text search attributes):
	//
	//	contains(<Column>, <Value>)
	containsExpr struct {
		sqlparser.Expr
		Column sqlparser.Expr
		Value  sqlparser.Expr
	}
)

var _ sqlparser.Expr = (*openJSONExistsExpr)(nil)
var _ sqlparser.Expr = (*containsExpr)(nil)

func (node *openJSONExistsExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("exists (select 1 from openjson(%v) where %v)", node.JSONDoc, node.Filter)
}

func (node *containsExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("contains(%v, %v)", node.Column, node.Value)
}

type queryConverter struct{}

var _ sqlplugin.VisibilityQueryConverter = (*queryConverter)(nil)

func (c *queryConverter) GetDatetimeFormat() string {
	return "2006-01-02 15:04:05.999999"
}

// ConvertComparisonExpr implements the optional convertComparisonExprIf hook
// of the store-side SQLQueryConverter. T-SQL has no true/false literals and
// the mssql schema stores Bool search attributes as BIT, so boolean values
// (including elements of IN tuples) are rewritten to 1/0 before the default
// comparison expression is built.
func (c *queryConverter) ConvertComparisonExpr(
	operator string,
	col *query.SAColumn,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	return &sqlparser.ComparisonExpr{
		Operator: operator,
		Left:     col,
		Right:    rewriteBoolLiterals(value),
	}, nil
}

func rewriteBoolLiterals(value sqlparser.Expr) sqlparser.Expr {
	switch v := value.(type) {
	case sqlparser.BoolVal:
		if v {
			return sqlparser.NewIntVal([]byte("1"))
		}
		return sqlparser.NewIntVal([]byte("0"))
	case sqlparser.ValTuple:
		for i := range v {
			v[i] = rewriteBoolLiterals(v[i])
		}
		return v
	}
	return value
}

// getCoalesceCloseTimeExpr returns a reference to the coalesce_close_time
// PERSISTED computed column instead of inlining a COALESCE expression.
// See the comment on coalesceCloseTimeColName: the mssql visibility schema
// must keep that column definition in sync with this converter.
func (c *queryConverter) getCoalesceCloseTimeExpr() sqlparser.Expr {
	return query.NewColName(coalesceCloseTimeColName)
}

func (c *queryConverter) ConvertKeywordListComparisonExpr(
	operator string,
	col *query.SAColumn,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	var negate bool
	var filter sqlparser.Expr
	switch operator {
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		// build the following expression:
		// `exists (select 1 from openjson({col}) where value = {value})`
		filter = &sqlparser.ComparisonExpr{
			Operator: sqlparser.EqualStr,
			Left:     query.NewColName(openJSONValueColName),
			Right:    value,
		}
		negate = operator == sqlparser.NotEqualStr
	case sqlparser.InStr, sqlparser.NotInStr:
		// build the following expression:
		// `exists (select 1 from openjson({col}) where value in ({values...}))`
		valTuple, isValTuple := value.(sqlparser.ValTuple)
		if !isValTuple {
			return nil, query.NewConverterError(
				"%s: unexpected value type (expected tuple of strings, got %s)",
				query.InvalidExpressionErrMessage,
				sqlparser.String(value),
			)
		}
		filter = &sqlparser.ComparisonExpr{
			Operator: sqlparser.InStr,
			Left:     query.NewColName(openJSONValueColName),
			Right:    valTuple,
		}
		negate = operator == sqlparser.NotInStr
	default:
		// this should never happen since isSupportedKeywordListOperator should already fail
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type",
			query.InvalidExpressionErrMessage,
			operator,
		)
	}

	var newExpr sqlparser.Expr = &openJSONExistsExpr{
		JSONDoc: col,
		Filter:  filter,
	}
	if negate {
		newExpr = &sqlparser.NotExpr{Expr: &sqlparser.ParenExpr{Expr: newExpr}}
	}
	return newExpr, nil
}

func (c *queryConverter) ConvertTextComparisonExpr(
	operator string,
	col *query.SAColumn,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	valueExpr, ok := value.(*query.UnsafeSQLString)
	if !ok {
		return nil, query.NewConverterError(
			"%s: unexpected value type (expected string, got %s)",
			query.InvalidExpressionErrMessage,
			sqlparser.String(value),
		)
	}
	tokens := query.TokenizeTextQueryString(valueExpr.Val)
	if len(tokens) == 0 {
		return nil, query.NewConverterError(
			"%s: unexpected value for Text type search attribute (no tokens found)",
			query.InvalidExpressionErrMessage,
		)
	}
	// build the following expression (requires a full-text index on {col}):
	// `contains({col}, '"tok1" OR "tok2" OR ..."tokN"')`
	valueExpr.Val = `"` + strings.Join(tokens, `" OR "`) + `"`
	var newExpr sqlparser.Expr = &containsExpr{
		Column: col,
		Value:  valueExpr,
	}
	if operator == sqlparser.NotEqualStr {
		newExpr = &sqlparser.NotExpr{Expr: newExpr}
	}
	return newExpr, nil
}

func (c *queryConverter) BuildSelectStmt(
	queryParams *query.QueryParams[sqlparser.Expr],
	pageSize int,
	token *sqlplugin.VisibilityPageToken,
) (string, []any) {
	var whereClauses []string
	// T-SQL has no LIMIT clause; pagination uses `SELECT TOP (?)`. The TOP
	// placeholder precedes every WHERE placeholder in the statement, so the
	// page size argument must come FIRST in the args slice.
	queryArgs := []any{pageSize}

	if queryParams.QueryExpr != nil {
		if queryString := sqlparser.String(queryParams.QueryExpr); queryString != "" {
			whereClauses = append(whereClauses, queryString)
		}
	}

	if token != nil {
		whereClauses = append(
			whereClauses,
			fmt.Sprintf(
				"((%s = ? AND %s = ? AND %s > ?) OR (%s = ? AND %s < ?) OR %s < ?)",
				sqlparser.String(c.getCoalesceCloseTimeExpr()),
				sadefs.GetSqlDbColName(sadefs.StartTime),
				sadefs.GetSqlDbColName(sadefs.RunID),
				sqlparser.String(c.getCoalesceCloseTimeExpr()),
				sadefs.GetSqlDbColName(sadefs.StartTime),
				sqlparser.String(c.getCoalesceCloseTimeExpr()),
			),
		)
		queryArgs = append(
			queryArgs,
			token.CloseTime,
			token.StartTime,
			token.RunID,
			token.CloseTime,
			token.StartTime,
			token.CloseTime,
		)
	}

	whereString := ""
	if len(whereClauses) > 0 {
		whereString = " WHERE " + strings.Join(whereClauses, " AND ")
	}

	stmt := fmt.Sprintf(
		`SELECT TOP (?) %s FROM executions_visibility%s ORDER BY %s DESC, %s DESC, %s`,
		strings.Join(sqlplugin.DbFields, ", "),
		whereString,
		sqlparser.String(c.getCoalesceCloseTimeExpr()),
		sadefs.GetSqlDbColName(sadefs.StartTime),
		sadefs.GetSqlDbColName(sadefs.RunID),
	)

	return stmt, queryArgs
}

func (c *queryConverter) BuildCountStmt(
	queryParams *query.QueryParams[sqlparser.Expr],
) (string, []any) {
	whereString := ""
	if queryParams.QueryExpr != nil {
		whereString = sqlparser.String(queryParams.QueryExpr)
		if whereString != "" {
			whereString = " WHERE " + whereString
		}
	}

	groupBy := make([]string, 0, len(queryParams.GroupBy)+1)
	for _, field := range queryParams.GroupBy {
		groupBy = append(groupBy, sadefs.GetSqlDbColName(field.FieldName))
	}

	groupByClause := ""
	if len(queryParams.GroupBy) > 0 {
		groupByClause = fmt.Sprintf(" GROUP BY %s", strings.Join(groupBy, ", "))
	}

	return fmt.Sprintf(
		"SELECT %s FROM executions_visibility%s%s",
		strings.Join(append(groupBy, "COUNT(*)"), ", "),
		whereString,
		groupByClause,
	), nil
}
