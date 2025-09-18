package sql

import (
	"fmt"
	"strings"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

type (
	pgCastExpr struct {
		sqlparser.Expr
		Value sqlparser.Expr
		Type  *sqlparser.ConvertType
	}

	pgQueryConverter struct{}
)

const (
	jsonBuildArrayFuncName = "jsonb_build_array"
	jsonContainsOp         = "@>"
	ftsMatchOp             = "@@"
)

var (
	convertTypeTSQuery = &sqlparser.ConvertType{Type: "tsquery"}
)

var _ sqlparser.Expr = (*pgCastExpr)(nil)
var _ pluginQueryConverterLegacy = (*pgQueryConverter)(nil)

func (node *pgCastExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("%v::%v", node.Value, node.Type)
}

func newPostgreSQLQueryConverter(
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
	queryString string,
) *QueryConverterLegacy {
	return newQueryConverterInternal(
		&pgQueryConverter{},
		namespaceName,
		namespaceID,
		saTypeMap,
		saMapper,
		queryString,
	)
}

func (c *pgQueryConverter) getDatetimeFormat() string {
	return "2006-01-02 15:04:05.999999"
}

func (c *pgQueryConverter) getCoalesceCloseTimeExpr() sqlparser.Expr {
	return newFuncExpr(
		coalesceFuncName,
		closeTimeSaColName,
		newUnsafeSQLString(maxDatetimeValue.Format(c.getDatetimeFormat())),
	)
}

func (c *pgQueryConverter) convertKeywordListComparisonExpr(
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

	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		newExpr := c.newJsonContainsExpr(expr.Left, expr.Right)
		if expr.Operator == sqlparser.NotEqualStr {
			newExpr = &sqlparser.NotExpr{Expr: newExpr}
		}
		return newExpr, nil
	case sqlparser.InStr, sqlparser.NotInStr:
		valTupleExpr, isValTuple := expr.Right.(sqlparser.ValTuple)
		if !isValTuple {
			return nil, query.NewConverterError(
				"%s: unexpected value type (expected tuple of strings, got %s)",
				query.InvalidExpressionErrMessage,
				sqlparser.String(expr.Right),
			)
		}
		var newExpr sqlparser.Expr = &sqlparser.ParenExpr{
			Expr: c.convertInExpr(expr.Left, valTupleExpr),
		}
		if expr.Operator == sqlparser.NotInStr {
			newExpr = &sqlparser.NotExpr{Expr: newExpr}
		}
		return newExpr, nil
	default:
		// this should never happen since isSupportedKeywordListOperator should already fail
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type search attribute in `%s`",
			query.InvalidExpressionErrMessage,
			expr.Operator,
			formatComparisonExprStringForError(*expr),
		)
	}
}

func (c *pgQueryConverter) convertInExpr(
	leftExpr sqlparser.Expr,
	values sqlparser.ValTuple,
) sqlparser.Expr {
	exprs := make([]sqlparser.Expr, len(values))
	for i, value := range values {
		exprs[i] = c.newJsonContainsExpr(leftExpr, value)
	}
	for len(exprs) > 1 {
		k := 0
		for i := 0; i < len(exprs); i += 2 {
			if i+1 < len(exprs) {
				exprs[k] = &sqlparser.OrExpr{
					Left:  exprs[i],
					Right: exprs[i+1],
				}
			} else {
				exprs[k] = exprs[i]
			}
			k++
		}
		exprs = exprs[:k]
	}
	return exprs[0]
}

func (c *pgQueryConverter) convertTextComparisonExpr(
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
	valueExpr.Val = strings.Join(tokens, " | ")
	var newExpr sqlparser.Expr = &sqlparser.ComparisonExpr{
		Operator: ftsMatchOp,
		Left:     expr.Left,
		Right: &pgCastExpr{
			Value: expr.Right,
			Type:  convertTypeTSQuery,
		},
	}
	if expr.Operator == sqlparser.NotEqualStr {
		newExpr = &sqlparser.NotExpr{Expr: newExpr}
	}
	return newExpr, nil
}

func (c *pgQueryConverter) newJsonContainsExpr(
	jsonExpr sqlparser.Expr,
	valueExpr sqlparser.Expr,
) sqlparser.Expr {
	return &sqlparser.ComparisonExpr{
		Operator: jsonContainsOp,
		Left:     jsonExpr,
		Right:    newFuncExpr(jsonBuildArrayFuncName, valueExpr),
	}
}

func (c *pgQueryConverter) buildSelectStmt(
	namespaceID namespace.ID,
	queryString string,
	pageSize int,
	token *pageTokenLegacy,
) (string, []any) {
	var whereClauses []string
	var queryArgs []any

	whereClauses = append(
		whereClauses,
		fmt.Sprintf("%s = ?", searchattribute.GetSqlDbColName(searchattribute.NamespaceID)),
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
				searchattribute.GetSqlDbColName(searchattribute.StartTime),
				searchattribute.GetSqlDbColName(searchattribute.RunID),
				sqlparser.String(c.getCoalesceCloseTimeExpr()),
				searchattribute.GetSqlDbColName(searchattribute.StartTime),
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

	queryArgs = append(queryArgs, pageSize)

	return fmt.Sprintf(
		`SELECT %s
		FROM executions_visibility
		WHERE %s
		ORDER BY %s DESC, %s DESC, %s
		LIMIT ?`,
		strings.Join(sqlplugin.DbFields, ", "),
		strings.Join(whereClauses, " AND "),
		sqlparser.String(c.getCoalesceCloseTimeExpr()),
		searchattribute.GetSqlDbColName(searchattribute.StartTime),
		searchattribute.GetSqlDbColName(searchattribute.RunID),
	), queryArgs
}

func (c *pgQueryConverter) buildCountStmt(
	namespaceID namespace.ID,
	queryString string,
	groupBy []string,
) (string, []any) {
	var whereClauses []string
	var queryArgs []any

	whereClauses = append(
		whereClauses,
		fmt.Sprintf("(%s = ?)", searchattribute.GetSqlDbColName(searchattribute.NamespaceID)),
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
