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
	// pgTextSearchQueryExpr renders a tsquery that matches any of the given
	// sub-queries, combined with the tsquery OR operator (||). Each sub-query
	// is built with plainto_tsquery instead of a raw ::tsquery cast, so
	// characters that are significant in tsquery syntax (most notably ':')
	// don't cause a "syntax error in tsquery".
	pgTextSearchQueryExpr struct {
		sqlparser.Expr
		queries []sqlparser.Expr
	}

	pgQueryConverter struct{}
)

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

var _ sqlparser.Expr = (*pgTextSearchQueryExpr)(nil)
var _ pluginQueryConverterLegacy = (*pgQueryConverter)(nil)

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

func newPostgreSQLQueryConverter(
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
	queryString string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) *QueryConverterLegacy {
	return newQueryConverterInternal(
		&pgQueryConverter{},
		namespaceName,
		namespaceID,
		saTypeMap,
		saMapper,
		queryString,
		chasmMapper,
		archetypeID,
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
	queries := make([]sqlparser.Expr, len(tokens))
	for i, token := range tokens {
		queries[i] = query.NewFuncExpr(
			plaintoTSQueryFuncName,
			newUnsafeSQLString(textSearchConfig),
			newUnsafeSQLString(token),
		)
	}
	var newExpr sqlparser.Expr = &sqlparser.ComparisonExpr{
		Operator: ftsMatchOp,
		Left:     expr.Left,
		Right:    &pgTextSearchQueryExpr{queries: queries},
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
		sadefs.GetSqlDbColName(sadefs.StartTime),
		sadefs.GetSqlDbColName(sadefs.RunID),
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
