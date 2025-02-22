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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

type (
	tidbCastExpr struct {
		sqlparser.Expr
		Value sqlparser.Expr
		Type  *sqlparser.ConvertType
	}

	tidbMemberOfExpr struct {
		sqlparser.Expr
		Value   sqlparser.Expr
		JSONArr sqlparser.Expr
	}

	tidbJsonOverlapsExpr struct {
		sqlparser.Expr
		JSONDoc1 sqlparser.Expr
		JSONDoc2 sqlparser.Expr
	}

	tidbQueryConverter struct{}
)

var _ sqlparser.Expr = (*tidbCastExpr)(nil)
var _ sqlparser.Expr = (*tidbMemberOfExpr)(nil)
var _ sqlparser.Expr = (*tidbJsonOverlapsExpr)(nil)

var _ pluginQueryConverter = (*tidbQueryConverter)(nil)

func (node *tidbCastExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("cast(%v as %v)", node.Value, node.Type)
}

func (node *tidbMemberOfExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("%v member of (%v)", node.Value, node.JSONArr)
}

func (node *tidbJsonOverlapsExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("json_overlaps(%v, %v)", node.JSONDoc1, node.JSONDoc2)
}

func newTiDBQueryConverter(
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
	queryString string,
) *QueryConverter {
	return newQueryConverterInternal(
		&tidbQueryConverter{},
		namespaceName,
		namespaceID,
		saTypeMap,
		saMapper,
		queryString,
	)
}

func (c *tidbQueryConverter) getDatetimeFormat() string {
	return "2006-01-02 15:04:05.999999"
}

func (c *tidbQueryConverter) getCoalesceCloseTimeExpr() sqlparser.Expr {
	return newFuncExpr(
		ifFuncName,
		newFuncExpr(
			isNullFuncName,
			closeTimeSaColName,
		),
		&tidbCastExpr{
			Value: newUnsafeSQLString(maxDatetimeValue.Format(c.getDatetimeFormat())),
			Type:  convertTypeDatetime,
		},
		closeTimeSaColName,
	)
}

func (c *tidbQueryConverter) convertKeywordListComparisonExpr(
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
		newExpr = &tidbMemberOfExpr{
			Value:   expr.Right,
			JSONArr: expr.Left,
		}
		negate = expr.Operator == sqlparser.NotEqualStr
	case sqlparser.InStr, sqlparser.NotInStr:
		var err error
		newExpr, err = c.convertToJsonOverlapsExpr(expr)
		if err != nil {
			return nil, err
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

func (c *tidbQueryConverter) convertToJsonOverlapsExpr(
	expr *sqlparser.ComparisonExpr,
) (*tidbJsonOverlapsExpr, error) {
	valTuple, isValTuple := expr.Right.(sqlparser.ValTuple)
	if !isValTuple {
		return nil, query.NewConverterError(
			"%s: unexpected value type (expected tuple of strings, got %s)",
			query.InvalidExpressionErrMessage,
			sqlparser.String(expr.Right),
		)
	}
	values, err := getUnsafeStringTupleValues(valTuple)
	if err != nil {
		return nil, err
	}
	jsonValue, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	return &tidbJsonOverlapsExpr{
		JSONDoc1: expr.Left,
		JSONDoc2: &tidbCastExpr{
			Value: newUnsafeSQLString(string(jsonValue)),
			Type:  convertTypeJSON,
		},
	}, nil
}

func (c *tidbQueryConverter) convertTextComparisonExpr(
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

	// TiDB doesn't support FULLTEXT index, but it supports TiFlash,
	// which is a column-based storage, let's use `{expr.Left} like '%{expr.Right}%'`
	operator := sqlparser.LikeStr
	if expr.Operator == sqlparser.NotEqualStr {
		operator = sqlparser.NotLikeStr
	}

	var newExpr sqlparser.Expr = &sqlparser.ComparisonExpr{
		Operator: operator,
		Left:     expr.Left,
		Right:    sqlparser.NewStrVal([]byte("%" + sqlparser.String(expr.Right) + "%")),
	}

	return newExpr, nil
}

func (c *tidbQueryConverter) buildSelectStmt(
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
		FROM executions_visibility ev
		LEFT JOIN custom_search_attributes
		USING (%s, %s)
		WHERE %s
		ORDER BY %s DESC, %s DESC, %s
		LIMIT ?`,
		strings.Join(addPrefix("ev.", sqlplugin.DbFields), ", "),
		searchattribute.GetSqlDbColName(searchattribute.NamespaceID),
		searchattribute.GetSqlDbColName(searchattribute.RunID),
		strings.Join(whereClauses, " AND "),
		sqlparser.String(c.getCoalesceCloseTimeExpr()),
		searchattribute.GetSqlDbColName(searchattribute.StartTime),
		searchattribute.GetSqlDbColName(searchattribute.RunID),
	), queryArgs
}

func (c *tidbQueryConverter) buildCountStmt(
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
		`SELECT %s
		FROM executions_visibility ev
		LEFT JOIN custom_search_attributes
		USING (%s, %s)
		WHERE %s
		%s`,
		strings.Join(append(groupBy, "COUNT(*)"), ", "),
		searchattribute.GetSqlDbColName(searchattribute.NamespaceID),
		searchattribute.GetSqlDbColName(searchattribute.RunID),
		strings.Join(whereClauses, " AND "),
		groupByClause,
	), queryArgs
}
