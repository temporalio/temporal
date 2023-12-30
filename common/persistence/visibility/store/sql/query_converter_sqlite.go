// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	sqliteQueryConverter struct{}
)

var _ pluginQueryConverter = (*sqliteQueryConverter)(nil)

const (
	keywordListTypeFtsTableName = "executions_visibility_fts_keyword_list"
	textTypeFtsTableName        = "executions_visibility_fts_text"
)

func newSqliteQueryConverter(
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
	queryString string,
) *QueryConverter {
	return newQueryConverterInternal(
		&sqliteQueryConverter{},
		namespaceName,
		namespaceID,
		saTypeMap,
		saMapper,
		queryString,
	)
}

func (c *sqliteQueryConverter) getDatetimeFormat() string {
	return "2006-01-02 15:04:05.999999-07:00"
}

func (c *sqliteQueryConverter) getCoalesceCloseTimeExpr() sqlparser.Expr {
	return newFuncExpr(
		coalesceFuncName,
		closeTimeSaColName,
		newUnsafeSQLString(maxDatetimeValue.Format(c.getDatetimeFormat())),
	)
}

func (c *sqliteQueryConverter) convertKeywordListComparisonExpr(
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

	saColNameExpr, isSAColNameExpr := expr.Left.(*saColName)
	if !isSAColNameExpr {
		return nil, query.NewConverterError(
			"%s: must be a search attribute column name but was %T",
			query.InvalidExpressionErrMessage,
			expr.Left,
		)
	}

	var ftsQuery string
	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		valueExpr, ok := expr.Right.(*unsafeSQLString)
		if !ok {
			return nil, query.NewConverterError(
				"%s: unexpected value type (expected string, got %s)",
				query.InvalidExpressionErrMessage,
				sqlparser.String(expr.Right),
			)
		}
		ftsQuery = buildFtsQueryString(saColNameExpr.dbColName.Name, valueExpr.Val)

	case sqlparser.InStr, sqlparser.NotInStr:
		valTupleExpr, isValTuple := expr.Right.(sqlparser.ValTuple)
		if !isValTuple {
			return nil, query.NewConverterError(
				"%s: unexpected value type (expected tuple of strings, got %s)",
				query.InvalidExpressionErrMessage,
				sqlparser.String(expr.Right),
			)
		}
		values, err := getUnsafeStringTupleValues(valTupleExpr)
		if err != nil {
			return nil, err
		}
		ftsQuery = buildFtsQueryString(saColNameExpr.dbColName.Name, values...)

	default:
		// this should never happen since isSupportedKeywordListOperator should already fail
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type search attribute in `%s`",
			query.InvalidExpressionErrMessage,
			expr.Operator,
			formatComparisonExprStringForError(*expr),
		)
	}

	var oper string
	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.InStr:
		oper = sqlparser.InStr
	case sqlparser.NotEqualStr, sqlparser.NotInStr:
		oper = sqlparser.NotInStr
	default:
		// this should never happen since isSupportedKeywordListOperator should already fail
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type search attribute in `%s`",
			query.InvalidExpressionErrMessage,
			expr.Operator,
			formatComparisonExprStringForError(*expr),
		)
	}

	newExpr := sqlparser.ComparisonExpr{
		Operator: oper,
		Left:     newColName("rowid"),
		Right: &sqlparser.Subquery{
			Select: c.buildFtsSelectStmt(keywordListTypeFtsTableName, ftsQuery),
		},
	}
	return &newExpr, nil
}

func (c *sqliteQueryConverter) convertTextComparisonExpr(
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

	saColNameExpr, isSAColNameExpr := expr.Left.(*saColName)
	if !isSAColNameExpr {
		return nil, query.NewConverterError(
			"%s: must be a search attribute column name but was %T",
			query.InvalidExpressionErrMessage,
			expr.Left,
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

	var oper string
	switch expr.Operator {
	case sqlparser.EqualStr:
		oper = sqlparser.InStr
	case sqlparser.NotEqualStr:
		oper = sqlparser.NotInStr
	default:
		// this should never happen since isSupportedTextOperator should already fail
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for Text type search attribute in `%s`",
			query.InvalidExpressionErrMessage,
			expr.Operator,
			formatComparisonExprStringForError(*expr),
		)
	}

	ftsQuery := buildFtsQueryString(saColNameExpr.dbColName.Name, tokens...)
	newExpr := sqlparser.ComparisonExpr{
		Operator: oper,
		Left:     newColName("rowid"),
		Right: &sqlparser.Subquery{
			Select: c.buildFtsSelectStmt(textTypeFtsTableName, ftsQuery),
		},
	}
	return &newExpr, nil
}

func (c *sqliteQueryConverter) buildSelectStmt(
	namespaceID namespace.ID,
	queryString string,
	pageSize int,
	token *pageToken,
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

// buildFtsSelectStmt builds the following statement for querying FTS:
//
//	SELECT rowid FROM tableName WHERE tableName = '%s'
func (c *sqliteQueryConverter) buildFtsSelectStmt(
	tableName string,
	queryString string,
) sqlparser.SelectStatement {
	return &sqlparser.Select{
		SelectExprs: sqlparser.SelectExprs{
			&sqlparser.AliasedExpr{
				Expr: newColName("rowid"),
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
				Left:     newColName(tableName),
				Right:    newUnsafeSQLString(queryString),
			},
		),
	}
}

func (c *sqliteQueryConverter) buildCountStmt(
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

func buildFtsQueryString(colname string, values ...string) string {
	// FTS query format: 'colname : ("token1" OR "token2" OR ...)'
	return fmt.Sprintf(`%s : ("%s")`, colname, strings.Join(values, `" OR "`))
}
