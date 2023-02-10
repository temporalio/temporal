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

	"github.com/xwb1989/sqlparser"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

type (
	castExpr struct {
		sqlparser.Expr
		Value sqlparser.Expr
		Type  *sqlparser.ConvertType
	}

	memberOfExpr struct {
		sqlparser.Expr
		Value   sqlparser.Expr
		JSONArr sqlparser.Expr
	}

	jsonOverlapsExpr struct {
		sqlparser.Expr
		JSONDoc1 sqlparser.Expr
		JSONDoc2 sqlparser.Expr
	}

	mysqlQueryConverter struct{}
)

var (
	convertTypeDatetime = &sqlparser.ConvertType{Type: "datetime"}
	convertTypeJSON     = &sqlparser.ConvertType{Type: "json"}
)

var _ sqlparser.Expr = (*castExpr)(nil)
var _ sqlparser.Expr = (*memberOfExpr)(nil)
var _ sqlparser.Expr = (*jsonOverlapsExpr)(nil)

var _ pluginQueryConverter = (*mysqlQueryConverter)(nil)

func (node *castExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("cast(%v as %v)", node.Value, node.Type)
}

func (node *memberOfExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("%v member of (%v)", node.Value, node.JSONArr)
}

func (node *jsonOverlapsExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("json_overlaps(%v, %v)", node.JSONDoc1, node.JSONDoc2)
}

func newMySQLQueryConverter(
	request *manager.ListWorkflowExecutionsRequestV2,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
) *QueryConverter {
	return newQueryConverterInternal(
		&mysqlQueryConverter{},
		request,
		saTypeMap,
		saMapper,
	)
}

func (c *mysqlQueryConverter) getDatetimeFormat() string {
	return "2006-01-02 15:04:05.999999"
}

func (c *mysqlQueryConverter) getCoalesceCloseTimeExpr() sqlparser.Expr {
	return newFuncExpr(
		coalesceFuncName,
		newColName(searchattribute.GetSqlDbColName(searchattribute.CloseTime)),
		&castExpr{
			Value: newUnsafeSQLString(maxDatetimeValue.Format(c.getDatetimeFormat())),
			Type:  convertTypeDatetime,
		},
	)
}

func (c *mysqlQueryConverter) convertKeywordListComparisonExpr(
	expr *sqlparser.ComparisonExpr,
) (sqlparser.Expr, error) {
	if !isSupportedKeywordListOperator(expr.Operator) {
		return nil, query.NewConverterError("invalid query")
	}

	switch expr.Operator {
	case sqlparser.EqualStr:
		return &memberOfExpr{
			Value:   expr.Right,
			JSONArr: expr.Left,
		}, nil
	case sqlparser.NotEqualStr:
		return &sqlparser.NotExpr{
			Expr: &memberOfExpr{
				Value:   expr.Right,
				JSONArr: expr.Left,
			},
		}, nil
	case sqlparser.InStr:
		return c.convertToJsonOverlapsExpr(expr)
	case sqlparser.NotInStr:
		jsonOverlapsExpr, err := c.convertToJsonOverlapsExpr(expr)
		if err != nil {
			return nil, err
		}
		return &sqlparser.NotExpr{Expr: jsonOverlapsExpr}, nil
	default:
		// this should never happen since isSupportedKeywordListOperator should already fail
		return nil, query.NewConverterError("invalid query")
	}
}

func (c *mysqlQueryConverter) convertToJsonOverlapsExpr(
	expr *sqlparser.ComparisonExpr,
) (*jsonOverlapsExpr, error) {
	valTuple, isValTuple := expr.Right.(sqlparser.ValTuple)
	if !isValTuple {
		return nil, query.NewConverterError("invalid query")
	}
	values := make([]any, len(valTuple))
	for i, val := range valTuple {
		value, err := query.ParseSqlValue(sqlparser.String(val))
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	jsonValue, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	return &jsonOverlapsExpr{
		JSONDoc1: expr.Left,
		JSONDoc2: &castExpr{
			Value: newUnsafeSQLString(string(jsonValue)),
			Type:  convertTypeJSON,
		},
	}, nil
}

func (c *mysqlQueryConverter) convertTextComparisonExpr(
	expr *sqlparser.ComparisonExpr,
) (sqlparser.Expr, error) {
	if !isSupportedTextOperator(expr.Operator) {
		return nil, query.NewConverterError("invalid query")
	}
	// build the following expression:
	// `match ({expr.Left}) against ({expr.Right} in natural language mode)`
	var newExpr sqlparser.Expr = &sqlparser.MatchExpr{
		Columns: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: expr.Left}},
		Expr:    expr.Right,
		Option:  sqlparser.NaturalLanguageModeStr,
	}
	if expr.Operator == sqlparser.NotEqualStr {
		newExpr = &sqlparser.NotExpr{Expr: newExpr}
	}
	return newExpr, nil
}

func (c *mysqlQueryConverter) buildSelectStmt(
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
