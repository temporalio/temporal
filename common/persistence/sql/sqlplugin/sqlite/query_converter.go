package sqlite

import (
	"fmt"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

var maxDatetime = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

const (
	keywordListTypeFtsTableName = "executions_visibility_fts_keyword_list"
	textTypeFtsTableName        = "executions_visibility_fts_text"
)

type queryConverter struct{}

var _ sqlplugin.VisibilityQueryConverter = (*queryConverter)(nil)

func (c *queryConverter) GetDatetimeFormat() string {
	return "2006-01-02 15:04:05.999999-07:00"
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
	var ftsQuery string
	switch operator {
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		valueExpr, ok := value.(*query.UnsafeSQLString)
		if !ok {
			return nil, query.NewConverterError(
				"%s: unexpected value type (expected string, got %s)",
				query.InvalidExpressionErrMessage,
				sqlparser.String(value),
			)
		}
		ftsQuery = buildFtsQueryString(col.FieldName, valueExpr.Val)

	case sqlparser.InStr, sqlparser.NotInStr:
		valTupleExpr, isValTuple := value.(sqlparser.ValTuple)
		if !isValTuple {
			return nil, query.NewConverterError(
				"%s: unexpected value type (expected tuple of strings, got %s)",
				query.InvalidExpressionErrMessage,
				sqlparser.String(value),
			)
		}
		values, err := query.GetUnsafeStringTupleValues(valTupleExpr)
		if err != nil {
			return nil, err
		}
		ftsQuery = buildFtsQueryString(col.FieldName, values...)

	default:
		// this should never happen since isSupportedKeywordListOperator should already fail
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type",
			query.InvalidExpressionErrMessage,
			operator,
		)
	}

	var oper string
	switch operator {
	case sqlparser.EqualStr, sqlparser.InStr:
		oper = sqlparser.InStr
	case sqlparser.NotEqualStr, sqlparser.NotInStr:
		oper = sqlparser.NotInStr
	default:
		// this should never happen since isSupportedKeywordListOperator should already fail
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type",
			query.InvalidExpressionErrMessage,
			operator,
		)
	}

	newExpr := sqlparser.ComparisonExpr{
		Operator: oper,
		Left:     query.NewColName("rowid"),
		Right: &sqlparser.Subquery{
			Select: buildFtsSelectStmt(keywordListTypeFtsTableName, ftsQuery),
		},
	}
	return &newExpr, nil
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

	var oper string
	switch operator {
	case sqlparser.EqualStr:
		oper = sqlparser.InStr
	case sqlparser.NotEqualStr:
		oper = sqlparser.NotInStr
	default:
		// this should never happen since isSupportedTextOperator should already fail
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for Text type",
			query.InvalidExpressionErrMessage,
			operator,
		)
	}

	ftsQuery := buildFtsQueryString(col.FieldName, tokens...)
	newExpr := sqlparser.ComparisonExpr{
		Operator: oper,
		Left:     query.NewColName("rowid"),
		Right: &sqlparser.Subquery{
			Select: buildFtsSelectStmt(textTypeFtsTableName, ftsQuery),
		},
	}
	return &newExpr, nil
}

func (c *queryConverter) BuildSelectStmt(
	queryParams *query.QueryParams[sqlparser.Expr],
	pageSize int,
	token *sqlplugin.VisibilityPageToken,
) (string, []any) {
	var whereClauses []string
	var queryArgs []any

	queryString := sqlparser.String(queryParams.QueryExpr)
	whereClauses = append(whereClauses, queryString)

	if token != nil {
		whereClauses = append(
			whereClauses,
			fmt.Sprintf(
				"((%s = ? AND %s = ? AND %s > ?) OR (%s = ? AND %s < ?) OR %s < ?)",
				sqlparser.String(c.GetCoalesceCloseTimeExpr()),
				searchattribute.GetSqlDbColName(searchattribute.StartTime),
				searchattribute.GetSqlDbColName(searchattribute.RunID),
				sqlparser.String(c.GetCoalesceCloseTimeExpr()),
				searchattribute.GetSqlDbColName(searchattribute.StartTime),
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

	stmt := fmt.Sprintf(
		`SELECT %s
		FROM executions_visibility
		WHERE %s
		ORDER BY %s DESC, %s DESC, %s
		LIMIT ?`,
		strings.Join(sqlplugin.DbFields, ", "),
		strings.Join(whereClauses, " AND "),
		sqlparser.String(c.GetCoalesceCloseTimeExpr()),
		searchattribute.GetSqlDbColName(searchattribute.StartTime),
		searchattribute.GetSqlDbColName(searchattribute.RunID),
	)
	queryArgs = append(queryArgs, pageSize)

	return stmt, queryArgs
}

func (c *queryConverter) BuildCountStmt(
	queryParams *query.QueryParams[sqlparser.Expr],
) (string, []any) {
	groupBy := make([]string, 0, len(queryParams.GroupBy)+1)
	for _, field := range queryParams.GroupBy {
		groupBy = append(groupBy, searchattribute.GetSqlDbColName(field.FieldName))
	}

	groupByClause := ""
	if len(queryParams.GroupBy) > 0 {
		groupByClause = fmt.Sprintf("GROUP BY %s", strings.Join(groupBy, ", "))
	}

	return fmt.Sprintf(
		"SELECT %s FROM executions_visibility WHERE %s %s",
		strings.Join(append(groupBy, "COUNT(*)"), ", "),
		sqlparser.String(queryParams.QueryExpr),
		groupByClause,
	), nil
}

// buildFtsSelectStmt builds the following statement for querying FTS:
//
//	SELECT rowid FROM tableName WHERE tableName = '%s'
func buildFtsSelectStmt(
	tableName string,
	queryString string,
) sqlparser.SelectStatement {
	return &sqlparser.Select{
		SelectExprs: sqlparser.SelectExprs{
			&sqlparser.AliasedExpr{
				Expr: query.NewColName("rowid"),
			},
		},
		From: sqlparser.TableExprs{
			&sqlparser.AliasedTableExpr{
				Expr: &sqlparser.TableName{
					Name: sqlparser.NewTableIdent(tableName),
				},
			},
		},
		Where: sqlparser.NewWhere(
			sqlparser.WhereStr,
			&sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     query.NewColName(tableName),
				Right:    query.NewUnsafeSQLString(queryString),
			},
		),
	}
}

func buildFtsQueryString(fieldName string, values ...string) string {
	// FTS query format: 'colName : ("token1" OR "token2" OR ...)'
	colName := searchattribute.GetSqlDbColName(fieldName)
	return fmt.Sprintf(`%s : ("%s")`, colName, strings.Join(values, `" OR "`))
}
