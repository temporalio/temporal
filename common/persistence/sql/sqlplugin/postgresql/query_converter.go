package postgresql

import (
	"fmt"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

var maxDatetime = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

const (
	jsonBuildArrayFuncName = "jsonb_build_array"
	plaintoTSQueryFuncName = "plainto_tsquery"
	jsonContainsOp         = "@>"
	ftsMatchOp             = "@@"

	// textSearchConfig is the PostgreSQL text search configuration used to
	// build both the stored Text search attribute tsvector columns (see the
	// visibility schema) and the tsquery on the read path. It must match on
	// both sides so queries keep matching the stored values. "simple" only
	// folds case and splits on non-word characters (no stemming or stopword
	// removal), which matches the token-based semantics used by the MySQL and
	// SQLite visibility stores.
	textSearchConfig = "simple"
)

// pgTextSearchQueryExpr renders a tsquery that matches any of the given
// sub-queries, combined with the tsquery OR operator (||). Each sub-query is
// built with plainto_tsquery instead of a raw ::tsquery cast, so characters
// that are significant in tsquery syntax (most notably ':') don't cause a
// "syntax error in tsquery".
type pgTextSearchQueryExpr struct {
	sqlparser.Expr
	queries []sqlparser.Expr
}

var _ sqlparser.Expr = (*pgTextSearchQueryExpr)(nil)

func (node *pgTextSearchQueryExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("(")
	for i, q := range node.queries {
		if i > 0 {
			buf.Myprintf(" || ")
		}
		buf.Myprintf("%v", q)
	}
	buf.Myprintf(")")
}

type queryConverter struct{}

var _ sqlplugin.VisibilityQueryConverter = (*queryConverter)(nil)

func (c *queryConverter) GetDatetimeFormat() string {
	return "2006-01-02 15:04:05.999999"
}

func (c *queryConverter) GetCoalesceCloseTimeExpr() sqlparser.Expr {
	return query.NewFuncExpr(
		"coalesce",
		query.CloseTimeSAColumn,
		query.NewUnsafeSQLString(maxDatetime.Format(c.GetDatetimeFormat())),
	)
}

func (c *queryConverter) ConvertKeywordListComparisonExpr(
	operator string,
	col *query.SAColumn,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	switch operator {
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		newExpr := newJSONContainsExpr(col, value)
		if operator == sqlparser.NotEqualStr {
			newExpr = &sqlparser.NotExpr{Expr: newExpr}
		}
		return newExpr, nil
	case sqlparser.InStr, sqlparser.NotInStr:
		valTupleExpr, isValTuple := value.(sqlparser.ValTuple)
		if !isValTuple {
			return nil, query.NewConverterError(
				"%s: unexpected value type (expected tuple of strings, got %s)",
				query.InvalidExpressionErrMessage,
				sqlparser.String(value),
			)
		}
		var newExpr sqlparser.Expr = &sqlparser.ParenExpr{
			Expr: c.convertInExpr(col, valTupleExpr),
		}
		if operator == sqlparser.NotInStr {
			newExpr = &sqlparser.NotExpr{Expr: newExpr}
		}
		return newExpr, nil
	default:
		// this should never happen since isSupportedKeywordListOperator should already fail
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type",
			query.InvalidExpressionErrMessage,
			operator,
		)
	}
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
	queries := make([]sqlparser.Expr, len(tokens))
	for i, token := range tokens {
		queries[i] = query.NewFuncExpr(
			plaintoTSQueryFuncName,
			query.NewUnsafeSQLString(textSearchConfig),
			query.NewUnsafeSQLString(token),
		)
	}
	var newExpr sqlparser.Expr = &sqlparser.ComparisonExpr{
		Operator: ftsMatchOp,
		Left:     col,
		Right:    &pgTextSearchQueryExpr{queries: queries},
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
	var queryArgs []any

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
				sqlparser.String(c.GetCoalesceCloseTimeExpr()),
				sadefs.GetSqlDbColName(sadefs.StartTime),
				sadefs.GetSqlDbColName(sadefs.RunID),
				sqlparser.String(c.GetCoalesceCloseTimeExpr()),
				sadefs.GetSqlDbColName(sadefs.StartTime),
				sqlparser.String(c.GetCoalesceCloseTimeExpr()),
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
		`SELECT %s FROM executions_visibility%s ORDER BY %s DESC, %s DESC, %s LIMIT ?`,
		strings.Join(sqlplugin.DbFields, ", "),
		whereString,
		sqlparser.String(c.GetCoalesceCloseTimeExpr()),
		sadefs.GetSqlDbColName(sadefs.StartTime),
		sadefs.GetSqlDbColName(sadefs.RunID),
	)
	queryArgs = append(queryArgs, pageSize)

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

func (c *queryConverter) convertInExpr(
	leftExpr sqlparser.Expr,
	values sqlparser.ValTuple,
) sqlparser.Expr {
	exprs := make([]sqlparser.Expr, len(values))
	for i, value := range values {
		exprs[i] = newJSONContainsExpr(leftExpr, value)
	}
	return query.ReduceExprs(
		func(left, right sqlparser.Expr) sqlparser.Expr {
			if left == nil {
				return right
			}
			if right == nil {
				return left
			}
			return &sqlparser.OrExpr{
				Left:  left,
				Right: right,
			}
		},
		exprs...,
	)
}

func newJSONContainsExpr(
	jsonExpr sqlparser.Expr,
	valueExpr sqlparser.Expr,
) sqlparser.Expr {
	return &sqlparser.ComparisonExpr{
		Operator: jsonContainsOp,
		Left:     jsonExpr,
		Right:    query.NewFuncExpr(jsonBuildArrayFuncName, valueExpr),
	}
}
