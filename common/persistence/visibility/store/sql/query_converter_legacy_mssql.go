package sql

import (
	"fmt"
	"strings"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

type (
	// openJsonExistsExpr is an expression that renders to:
	//
	//	exists (select 1 from openjson({JSONExpr}) where value {Operator} {ValueExpr})
	//
	// It is used to check whether a KeywordList type search attribute (stored
	// as a JSON array in SQL Server) contains a given value (Operator "=" with
	// a single value) or any of the given values (Operator "in" with a value
	// tuple).
	openJsonExistsExpr struct {
		sqlparser.Expr
		JSONExpr  sqlparser.Expr
		Operator  string
		ValueExpr sqlparser.Expr
	}

	// containsExpr is an expression that renders to the SQL Server full-text
	// search predicate:
	//
	//	contains({Column}, {SearchExpr})
	containsExpr struct {
		sqlparser.Expr
		Column     sqlparser.Expr
		SearchExpr sqlparser.Expr
	}

	mssqlQueryConverter struct{}
)

const (
	// coalesceCloseTimeColName is the name of a persisted computed column in
	// the executions_visibility table defined as
	// COALESCE(close_time, '9999-12-31 23:59:59').
	coalesceCloseTimeColName = "coalesce_close_time"
)

var _ sqlparser.Expr = (*openJsonExistsExpr)(nil)
var _ sqlparser.Expr = (*containsExpr)(nil)

var _ pluginQueryConverterLegacy = (*mssqlQueryConverter)(nil)
var _ pluginBoolValueConverterLegacy = (*mssqlQueryConverter)(nil)

func (node *openJsonExistsExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf(
		"exists (select 1 from openjson(%v) where value %s %v)",
		node.JSONExpr,
		node.Operator,
		node.ValueExpr,
	)
}

func (node *containsExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("contains(%v, %v)", node.Column, node.SearchExpr)
}

func newMSSQLQueryConverter(
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
	queryString string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) *QueryConverterLegacy {
	return newQueryConverterInternal(
		&mssqlQueryConverter{},
		namespaceName,
		namespaceID,
		saTypeMap,
		saMapper,
		queryString,
		chasmMapper,
		archetypeID,
	)
}

func (c *mssqlQueryConverter) getDatetimeFormat() string {
	return "2006-01-02 15:04:05.999999"
}

// convertBoolValueExpr rewrites boolean literals to 1/0: T-SQL has no
// true/false literals and the mssql schema stores Bool search attributes
// as BIT columns.
func (c *mssqlQueryConverter) convertBoolValueExpr(v sqlparser.BoolVal) sqlparser.Expr {
	if v {
		return sqlparser.NewIntVal([]byte("1"))
	}
	return sqlparser.NewIntVal([]byte("0"))
}

func (c *mssqlQueryConverter) getCoalesceCloseTimeExpr() sqlparser.Expr {
	return newColName(coalesceCloseTimeColName)
}

func (c *mssqlQueryConverter) convertKeywordListComparisonExpr(
	expr *sqlparser.ComparisonExpr,
) (sqlparser.Expr, error) {
	if !isSupportedKeywordListOperator(expr.Operator) {
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type search attribute in `%s`",
			query.InvalidExpressionErrMessage,
			expr.Operator,
			formatComparisonExprStringForError(*expr),
		)
	}

	var negate bool
	var newExpr sqlparser.Expr
	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		newExpr = &openJsonExistsExpr{
			JSONExpr:  expr.Left,
			Operator:  sqlparser.EqualStr,
			ValueExpr: expr.Right,
		}
		negate = expr.Operator == sqlparser.NotEqualStr
	case sqlparser.InStr, sqlparser.NotInStr:
		valTupleExpr, isValTuple := expr.Right.(sqlparser.ValTuple)
		if !isValTuple {
			return nil, query.NewConverterError(
				"%s: unexpected value type (expected tuple of strings, got %s)",
				query.InvalidExpressionErrMessage,
				sqlparser.String(expr.Right),
			)
		}
		newExpr = &openJsonExistsExpr{
			JSONExpr:  expr.Left,
			Operator:  sqlparser.InStr,
			ValueExpr: valTupleExpr,
		}
		negate = expr.Operator == sqlparser.NotInStr
	default:
		// this should never happen since isSupportedKeywordListOperator should already fail
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type search attribute in `%s`",
			query.InvalidExpressionErrMessage,
			expr.Operator,
			formatComparisonExprStringForError(*expr),
		)
	}

	if negate {
		newExpr = &sqlparser.NotExpr{Expr: newExpr}
	}
	return newExpr, nil
}

func (c *mssqlQueryConverter) convertTextComparisonExpr(
	expr *sqlparser.ComparisonExpr,
) (sqlparser.Expr, error) {
	if !isSupportedTextOperator(expr.Operator) {
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for Text type search attribute in `%s`",
			query.InvalidExpressionErrMessage,
			expr.Operator,
			formatComparisonExprStringForError(*expr),
		)
	}
	valueExpr, ok := expr.Right.(*unsafeSQLString)
	if !ok {
		return nil, query.NewConverterError(
			"%s: unexpected value type (expected string, got %s)",
			query.InvalidExpressionErrMessage,
			sqlparser.String(expr.Right),
		)
	}
	tokens := tokenizeTextQueryString(valueExpr.Val)
	if len(tokens) == 0 {
		return nil, query.NewConverterError(
			"%s: unexpected value for Text type search attribute (no tokens found in %s)",
			query.InvalidExpressionErrMessage,
			sqlparser.String(expr.Right),
		)
	}
	// Build the full-text search condition: `"tok1" OR "tok2" OR ...`.
	valueExpr.Val = fmt.Sprintf(`"%s"`, strings.Join(tokens, `" OR "`))
	var newExpr sqlparser.Expr = &containsExpr{
		Column:     expr.Left,
		SearchExpr: valueExpr,
	}
	if expr.Operator == sqlparser.NotEqualStr {
		newExpr = &sqlparser.NotExpr{Expr: newExpr}
	}
	return newExpr, nil
}

func (c *mssqlQueryConverter) buildSelectStmt(
	namespaceID namespace.ID,
	queryString string,
	pageSize int,
	token *pageTokenLegacy,
) (string, []any) {
	var whereClauses []string
	var queryArgs []any

	// The TOP (?) placeholder appears before the WHERE clause placeholders in
	// the query string, so pageSize must be the first query argument.
	queryArgs = append(queryArgs, pageSize)

	whereClauses = append(
		whereClauses,
		fmt.Sprintf("%s = ?", sadefs.GetSqlDbColName(sadefs.NamespaceID)),
	)
	queryArgs = append(queryArgs, namespaceID.String())

	if len(queryString) > 0 {
		whereClauses = append(whereClauses, queryString)
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

	return fmt.Sprintf(
		`SELECT TOP (?) %s
		FROM executions_visibility
		WHERE %s
		ORDER BY %s DESC, %s DESC, %s`,
		strings.Join(sqlplugin.DbFields, ", "),
		strings.Join(whereClauses, " AND "),
		sqlparser.String(c.getCoalesceCloseTimeExpr()),
		sadefs.GetSqlDbColName(sadefs.StartTime),
		sadefs.GetSqlDbColName(sadefs.RunID),
	), queryArgs
}

func (c *mssqlQueryConverter) buildCountStmt(
	namespaceID namespace.ID,
	queryString string,
	groupBy []string,
) (string, []any) {
	var whereClauses []string
	var queryArgs []any

	whereClauses = append(
		whereClauses,
		fmt.Sprintf("(%s = ?)", sadefs.GetSqlDbColName(sadefs.NamespaceID)),
	)
	queryArgs = append(queryArgs, namespaceID.String())

	if len(queryString) > 0 {
		whereClauses = append(whereClauses, queryString)
	}

	groupByClause := ""
	if len(groupBy) > 0 {
		groupByClause = fmt.Sprintf("GROUP BY %s", strings.Join(groupBy, ", "))
	}

	return fmt.Sprintf(
		"SELECT %s FROM executions_visibility WHERE %s %s",
		strings.Join(append(groupBy, "COUNT(*)"), ", "),
		strings.Join(whereClauses, " AND "),
		groupByClause,
	), queryArgs
}
