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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xwb1989/sqlparser"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

type (
	pluginQueryConverter interface {
		convertKeywordListComparisonExpr(expr *sqlparser.ComparisonExpr) (sqlparser.Expr, error)

		convertTextComparisonExpr(expr *sqlparser.ComparisonExpr) (sqlparser.Expr, error)

		buildSelectStmt(
			namespaceID namespace.ID,
			queryString string,
			pageSize int,
			token *pageToken,
		) (string, []any)

		getDatetimeFormat() string
	}

	QueryConverter struct {
		pluginQueryConverter
		request   *manager.ListWorkflowExecutionsRequestV2
		saTypeMap searchattribute.NameTypeMap
		saMapper  searchattribute.Mapper
	}
)

var (
	// strings.Replacer takes a sequence of old to new replacements
	escapeCharMap = []string{
		"'", "''",
		`"`, `\\"`,
		"\b", "\\b",
		"\n", "\\n",
		"\r", "\\r",
		"\t", "\\t",
		"\\", "\\\\",
	}

	supportedComparisonOperators = []string{
		sqlparser.EqualStr,
		sqlparser.NotEqualStr,
		sqlparser.LessThanStr,
		sqlparser.GreaterThanStr,
		sqlparser.LessEqualStr,
		sqlparser.GreaterEqualStr,
		sqlparser.InStr,
		sqlparser.NotInStr,
	}

	supportedKeyworkListOperators = []string{
		sqlparser.EqualStr,
		sqlparser.NotEqualStr,
		sqlparser.InStr,
		sqlparser.NotInStr,
	}

	supportedTextOperators = []string{
		sqlparser.EqualStr,
		sqlparser.NotEqualStr,
	}

	supportedTypesRangeCond = []enumspb.IndexedValueType{
		enumspb.INDEXED_VALUE_TYPE_DATETIME,
		enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		enumspb.INDEXED_VALUE_TYPE_INT,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}
)

func newQueryConverterInternal(
	pqc pluginQueryConverter,
	request *manager.ListWorkflowExecutionsRequestV2,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
) *QueryConverter {
	return &QueryConverter{
		pluginQueryConverter: pqc,
		request:              request,
		saTypeMap:            saTypeMap,
		saMapper:             saMapper,
	}
}

func (c *QueryConverter) BuildSelectStmt() (*sqlplugin.VisibilitySelectFilter, error) {
	token, err := deserializePageToken(c.request.NextPageToken)
	if err != nil {
		return nil, err
	}
	queryString, err := c.convertWhereString(c.request.Query)
	if err != nil {
		return nil, err
	}
	queryString, queryArgs := c.buildSelectStmt(
		c.request.NamespaceID,
		queryString,
		c.request.PageSize,
		token,
	)
	return &sqlplugin.VisibilitySelectFilter{Query: queryString, QueryArgs: queryArgs}, nil
}

func (c *QueryConverter) convertWhereString(queryString string) (string, error) {
	where := strings.TrimSpace(queryString)
	if where != "" && !strings.HasPrefix(strings.ToLower(where), "order by") {
		where = "where " + where
	}
	// sqlparser can't parse just WHERE clause but instead accepts only valid SQL statement.
	sql := "select * from table1 " + where
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return "", err
	}

	selectStmt, _ := stmt.(*sqlparser.Select)
	err = c.convertSelectStmt(selectStmt)
	if err != nil {
		return "", err
	}

	result := ""
	if selectStmt.Where != nil {
		result = sqlparser.String(selectStmt.Where.Expr)
	}
	return result, nil
}

func (c *QueryConverter) convertSelectStmt(sel *sqlparser.Select) error {
	if sel.GroupBy != nil {
		return query.NewConverterError("%s: 'group by' clause", query.NotSupportedErrMessage)
	}

	if sel.OrderBy != nil {
		return query.NewConverterError("%s: 'order by' clause", query.NotSupportedErrMessage)
	}

	if sel.Limit != nil {
		return query.NewConverterError("%s: 'limit' clause", query.NotSupportedErrMessage)
	}

	if sel.Where != nil {
		return c.convertWhereExpr(&sel.Where.Expr)
	}

	return nil
}

func (c *QueryConverter) convertWhereExpr(expr *sqlparser.Expr) error {
	if expr == nil || *expr == nil {
		return errors.New("cannot be nil")
	}

	switch e := (*expr).(type) {
	case *sqlparser.ParenExpr:
		return c.convertWhereExpr(&e.Expr)
	case *sqlparser.NotExpr:
		return c.convertWhereExpr(&e.Expr)
	case *sqlparser.AndExpr:
		return c.convertAndExpr(expr)
	case *sqlparser.OrExpr:
		return c.convertOrExpr(expr)
	case *sqlparser.ComparisonExpr:
		return c.convertComparisonExpr(expr)
	case *sqlparser.RangeCond:
		return c.convertRangeCond(expr)
	case *sqlparser.IsExpr:
		return c.convertIsExpr(expr)
	case *sqlparser.FuncExpr:
		return query.NewConverterError("%s: function expression", query.NotSupportedErrMessage)
	case *sqlparser.ColName:
		return query.NewConverterError("incomplete expression")
	default:
		return query.NewConverterError("%s: expression of type %T", query.NotSupportedErrMessage, e)
	}
}

func (c *QueryConverter) convertAndExpr(exprRef *sqlparser.Expr) error {
	expr, ok := (*exprRef).(*sqlparser.AndExpr)
	if !ok {
		return query.NewConverterError("%v is not an 'and' expression", sqlparser.String(*exprRef))
	}
	err := c.convertWhereExpr(&expr.Left)
	if err != nil {
		return err
	}
	return c.convertWhereExpr(&expr.Right)
}

func (c *QueryConverter) convertOrExpr(exprRef *sqlparser.Expr) error {
	expr, ok := (*exprRef).(*sqlparser.OrExpr)
	if !ok {
		return query.NewConverterError("%v is not an 'or' expression", sqlparser.String(*exprRef))
	}
	err := c.convertWhereExpr(&expr.Left)
	if err != nil {
		return err
	}
	return c.convertWhereExpr(&expr.Right)
}

func (c *QueryConverter) convertComparisonExpr(exprRef *sqlparser.Expr) error {
	expr, ok := (*exprRef).(*sqlparser.ComparisonExpr)
	if !ok {
		return query.NewConverterError("%v is not a comparison expression", sqlparser.String(*exprRef))
	}

	saName, saFieldName, err := c.convertColName(&expr.Left)
	if err != nil {
		return err
	}
	saType, err := c.saTypeMap.GetType(saFieldName)
	if err != nil {
		return query.NewConverterError(
			"%s: column name '%s' is not a valid search attribute",
			query.InvalidExpressionErrMessage,
			saName,
		)
	}

	if !isSupportedComparisonOperator(expr.Operator) {
		return query.NewConverterError(
			"%s: invalid operator '%s'",
			query.InvalidExpressionErrMessage,
			expr.Operator,
		)
	}

	err = c.convertValueExpr(&expr.Right, saName, saType)
	if err != nil {
		return err
	}
	switch saType {
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
		newExpr, err := c.convertKeywordListComparisonExpr(expr)
		if err != nil {
			return err
		}
		*exprRef = newExpr
	case enumspb.INDEXED_VALUE_TYPE_TEXT:
		newExpr, err := c.convertTextComparisonExpr(expr)
		if err != nil {
			return err
		}
		*exprRef = newExpr
	}
	return nil
}

func (c *QueryConverter) convertRangeCond(exprRef *sqlparser.Expr) error {
	expr, ok := (*exprRef).(*sqlparser.RangeCond)
	if !ok {
		return query.NewConverterError(
			"%v is not a range condition expression",
			sqlparser.String(*exprRef),
		)
	}
	saName, saFieldName, err := c.convertColName(&expr.Left)
	if err != nil {
		return err
	}
	saType, err := c.saTypeMap.GetType(saFieldName)
	if err != nil {
		return query.NewConverterError(
			"%s: column name '%s' is not a valid search attribute",
			query.InvalidExpressionErrMessage,
			saName,
		)
	}
	if !isSupportedTypeRangeCond(saType) {
		return query.NewConverterError(
			"%s: cannot do range condition on search attribute '%s' of type %s",
			query.InvalidExpressionErrMessage,
			saName,
			saType.String(),
		)
	}
	err = c.convertValueExpr(&expr.From, saName, saType)
	if err != nil {
		return err
	}
	err = c.convertValueExpr(&expr.To, saName, saType)
	if err != nil {
		return err
	}
	return nil
}

func (c *QueryConverter) convertColName(
	exprRef *sqlparser.Expr,
) (saAlias string, saFieldName string, retError error) {
	expr, ok := (*exprRef).(*sqlparser.ColName)
	if !ok {
		return "", "", query.NewConverterError(
			"%s: must be a column name but was %T",
			query.InvalidExpressionErrMessage,
			*exprRef,
		)
	}
	saAlias = strings.ReplaceAll(sqlparser.String(expr), "`", "")
	saFieldName = saAlias
	if searchattribute.IsMappable(saAlias) {
		var err error
		saFieldName, err = c.saMapper.GetFieldName(saAlias, c.request.Namespace.String())
		if err != nil {
			return "", "", err
		}
	}
	var newExpr sqlparser.Expr = newColName(searchattribute.GetSqlDbColName(saFieldName))
	if saAlias == searchattribute.CloseTime {
		newExpr = getCoalesceCloseTimeExpr(c.getDatetimeFormat())
	}
	*exprRef = newExpr
	return saAlias, saFieldName, nil
}

func (c *QueryConverter) convertValueExpr(
	exprRef *sqlparser.Expr,
	saName string,
	saType enumspb.IndexedValueType,
) error {
	expr := *exprRef
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		value, err := c.parseSQLVal(e, saName, saType)
		if err != nil {
			return err
		}
		switch v := value.(type) {
		case string:
			// escape strings for safety
			replacer := strings.NewReplacer(escapeCharMap...)
			*exprRef = newUnsafeSQLString(replacer.Replace(v))
		case int64:
			*exprRef = sqlparser.NewIntVal([]byte(strconv.FormatInt(v, 10)))
		case float64:
			*exprRef = sqlparser.NewFloatVal([]byte(strconv.FormatFloat(v, 'f', -1, 64)))
		default:
			// this should never happen: query.ParseSqlValue returns one of the types above
			panic(fmt.Sprintf("Unexpected value type: %T", v))
		}
		return nil
	case sqlparser.BoolVal:
		// no-op: no validation needed
		return nil
	case sqlparser.ValTuple:
		// This is "in (1,2,3)" case.
		for _, subExpr := range e {
			err := c.convertValueExpr(&subExpr, saName, saType)
			if err != nil {
				return err
			}
		}
		return nil
	case *sqlparser.GroupConcatExpr:
		return query.NewConverterError("%s: 'group_concat'", query.NotSupportedErrMessage)
	case *sqlparser.FuncExpr:
		return query.NewConverterError("%s: nested func", query.NotSupportedErrMessage)
	case *sqlparser.ColName:
		return query.NewConverterError(
			"%s: column name on the right side of comparison expression (did you forget to quote '%s'?)",
			query.NotSupportedErrMessage,
			sqlparser.String(expr),
		)
	default:
		return query.NewConverterError(
			"%s: unexpected value type %T",
			query.InvalidExpressionErrMessage,
			expr,
		)
	}
}

// parseSQLVal handles values for specific search attributes.
// For datetime, converts to UTC.
// For execution status, converts string to enum value.
func (c *QueryConverter) parseSQLVal(
	expr *sqlparser.SQLVal,
	saName string,
	saType enumspb.IndexedValueType,
) (any, error) {
	// Using expr.Val instead of sqlparser.String(expr) because the latter escapes chars using MySQL
	// conventions which is incompatible with SQLite.
	var sqlValue string
	switch expr.Type {
	case sqlparser.StrVal:
		sqlValue = fmt.Sprintf(`'%s'`, expr.Val)
	default:
		sqlValue = string(expr.Val)
	}
	value, err := query.ParseSqlValue(sqlValue)
	if err != nil {
		return nil, err
	}

	if saType == enumspb.INDEXED_VALUE_TYPE_DATETIME {
		var tm time.Time
		switch v := value.(type) {
		case int64:
			tm = time.Unix(0, v)
		case string:
			var err error
			tm, err = time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", err
			}
		default:
			return "", query.NewConverterError(
				"%s: unexpected value type %T for search attribute %s",
				query.InvalidExpressionErrMessage,
				v,
				saName,
			)
		}
		return tm.UTC().Format(c.getDatetimeFormat()), nil
	}

	if saName == searchattribute.ExecutionStatus {
		var status int64
		switch v := value.(type) {
		case int64:
			status = v
		case string:
			code, ok := enumspb.WorkflowExecutionStatus_value[v]
			if !ok {
				return nil, query.NewConverterError(
					"%s: invalid execution status value '%s'",
					query.InvalidExpressionErrMessage,
					v,
				)
			}
			status = int64(code)
		default:
			return "", query.NewConverterError(
				"%s: unexpected value type %T for search attribute %s",
				query.InvalidExpressionErrMessage,
				v,
				saName,
			)
		}
		return status, nil
	}

	return value, nil
}

func (c *QueryConverter) convertIsExpr(exprRef *sqlparser.Expr) error {
	expr, ok := (*exprRef).(*sqlparser.IsExpr)
	if !ok {
		return query.NewConverterError("%v is not an 'IS' expression", sqlparser.String(*exprRef))
	}
	saName, saFieldName, err := c.convertColName(&expr.Expr)
	if err != nil {
		return err
	}
	_, err = c.saTypeMap.GetType(saFieldName)
	if err != nil {
		return query.NewConverterError(
			"%s: column name '%s' is not a valid search attribute",
			query.InvalidExpressionErrMessage,
			saName,
		)
	}
	switch expr.Operator {
	case sqlparser.IsNullStr, sqlparser.IsNotNullStr:
		// no-op
	default:
		return query.NewConverterError(
			"%s: 'IS' operator can only be used with 'NULL' or 'NOT NULL'",
			query.InvalidExpressionErrMessage,
		)
	}
	return nil
}

func isSupportedOperator(supportedOperators []string, operator string) bool {
	for _, op := range supportedOperators {
		if operator == op {
			return true
		}
	}
	return false
}

func isSupportedComparisonOperator(operator string) bool {
	return isSupportedOperator(supportedComparisonOperators, operator)
}

func isSupportedKeywordListOperator(operator string) bool {
	return isSupportedOperator(supportedKeyworkListOperators, operator)
}

func isSupportedTextOperator(operator string) bool {
	return isSupportedOperator(supportedTextOperators, operator)
}

func isSupportedTypeRangeCond(saType enumspb.IndexedValueType) bool {
	for _, tp := range supportedTypesRangeCond {
		if saType == tp {
			return true
		}
	}
	return false
}
