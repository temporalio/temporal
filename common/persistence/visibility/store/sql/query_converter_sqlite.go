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

	"github.com/xwb1989/sqlparser"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/manager"
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
	request *manager.ListWorkflowExecutionsRequestV2,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
) *QueryConverter {
	return newQueryConverterInternal(
		&sqliteQueryConverter{},
		request,
		saTypeMap,
		saMapper,
	)
}

func (c *sqliteQueryConverter) getDatetimeFormat() string {
	return "2006-01-02 15:04:05.999999-07:00"
}

//nolint:revive // cyclomatic complexity 17 (> 15)
func (c *sqliteQueryConverter) convertKeywordListComparisonExpr(
	expr *sqlparser.ComparisonExpr,
) (sqlparser.Expr, error) {
	if !isSupportedKeywordListOperator(expr.Operator) {
		return nil, query.NewConverterError("invalid query")
	}

	colName, isColName := expr.Left.(*colName)
	if !isColName {
		return nil, query.NewConverterError(
			"%s: must be a column name but was %T",
			query.InvalidExpressionErrMessage,
			expr.Left,
		)
	}
	colNameStr := sqlparser.String(colName)

	var ftsQuery string
	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		value, err := query.ParseSqlValue(sqlparser.String(expr.Right))
		if err != nil {
			return nil, err
		}
		switch v := value.(type) {
		case string:
			ftsQuery = fmt.Sprintf(`%s:"%s"`, colNameStr, v)
		default:
			return nil, query.NewConverterError(
				"%s: unexpected value type %T",
				query.InvalidExpressionErrMessage,
				expr,
			)
		}

	case sqlparser.InStr, sqlparser.NotInStr:
		valTupleExpr, isValTuple := expr.Right.(sqlparser.ValTuple)
		if !isValTuple {
			return nil, query.NewConverterError(
				"%s: unexpected value type %T",
				query.InvalidExpressionErrMessage,
				expr,
			)
		}
		var values []string
		for _, valExpr := range valTupleExpr {
			value, err := query.ParseSqlValue(sqlparser.String(valExpr))
			if err != nil {
				return nil, err
			}
			switch v := value.(type) {
			case string:
				values = append(values, fmt.Sprintf(`"%s"`, v))
			default:
				return nil, query.NewConverterError(
					"%s: unexpected value type %T",
					query.InvalidExpressionErrMessage,
					expr,
				)
			}
		}
		ftsQuery = fmt.Sprintf("%s:(%s)", colNameStr, strings.Join(values, " OR "))

	default:
		// this should never happen since isSupportedKeywordListOperator should already fail
		return nil, query.NewConverterError("invalid query")
	}

	var oper string
	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.InStr:
		oper = sqlparser.InStr
	case sqlparser.NotEqualStr, sqlparser.NotInStr:
		oper = sqlparser.NotInStr
	default:
		// this should never happen since isSupportedKeywordListOperator should already fail
		return nil, query.NewConverterError("invalid query")
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
		return nil, query.NewConverterError("invalid query")
	}

	colName, isColName := expr.Left.(*colName)
	if !isColName {
		return nil, query.NewConverterError(
			"%s: must be a column name but was %T",
			query.InvalidExpressionErrMessage,
			expr.Left,
		)
	}
	colNameStr := sqlparser.String(colName)

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
	ftsQuery := fmt.Sprintf("%s:(%s)", colNameStr, strings.Join(tokens, " OR "))

	var oper string
	switch expr.Operator {
	case sqlparser.EqualStr:
		oper = sqlparser.InStr
	case sqlparser.NotEqualStr:
		oper = sqlparser.NotInStr
	default:
		// this should never happen since isSupportedTextOperator should already fail
		return nil, query.NewConverterError("invalid query")
	}

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
	whereClauses := make([]string, 0, 3)
	queryArgs := make([]any, 0, 8)

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
				sqlparser.String(getCoalesceCloseTimeExpr(c.getDatetimeFormat())),
				searchattribute.GetSqlDbColName(searchattribute.StartTime),
				searchattribute.GetSqlDbColName(searchattribute.RunID),
				sqlparser.String(getCoalesceCloseTimeExpr(c.getDatetimeFormat())),
				searchattribute.GetSqlDbColName(searchattribute.StartTime),
				sqlparser.String(getCoalesceCloseTimeExpr(c.getDatetimeFormat())),
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
		sqlparser.String(getCoalesceCloseTimeExpr(c.getDatetimeFormat())),
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
