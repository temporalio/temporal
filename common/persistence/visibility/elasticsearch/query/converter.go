// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Copyright (c) 2017 Xargin
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

// Package query is inspired and partially copied from by github.com/cch123/elasticsql.
package query

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/olivere/elastic/v7"
	"github.com/xwb1989/sqlparser"
)

type (
	Converter struct {
		fnInterceptor FieldNameInterceptor
		fvInterceptor FieldValuesInterceptor
	}

	missingCheck struct{}
)

func NewConverter(fnInterceptor FieldNameInterceptor, fvInterceptor FieldValuesInterceptor) *Converter {
	cvt := &Converter{
		fnInterceptor: fnInterceptor,
		fvInterceptor: fvInterceptor,
	}

	if cvt.fnInterceptor == nil {
		cvt.fnInterceptor = &nopFieldNameInterceptor{}
	}
	if cvt.fvInterceptor == nil {
		cvt.fvInterceptor = &nopFieldValuesInterceptor{}
	}

	return cvt
}

// ConvertWhereOrderBy transforms WHERE SQL statement to Elasticsearch query.
// It also supports ORDER BY clause.
func (c *Converter) ConvertWhereOrderBy(whereOrderBy string) (*elastic.BoolQuery, []*elastic.FieldSort, error) {
	whereOrderBy = strings.TrimSpace(whereOrderBy)

	if whereOrderBy != "" && !strings.HasPrefix(strings.ToLower(whereOrderBy), "order by ") {
		whereOrderBy = "where " + whereOrderBy
	}
	// sqlparser can't parse just WHERE clause but instead accepts only valid SQL statement.
	sql := fmt.Sprintf("select * from table1 %s", whereOrderBy)
	return c.convertSql(sql)
}

// convertSql transforms SQL to Elasticsearch query.
func (c *Converter) convertSql(sql string) (*elastic.BoolQuery, []*elastic.FieldSort, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, nil, NewConverterError(fmt.Sprintf("%s: %v", malformedSqlQueryErrMessage, err))
	}

	selectStmt, isSelect := stmt.(*sqlparser.Select)
	if !isSelect {
		return nil, nil, NewConverterError(fmt.Sprintf("%s: statement must be 'select' not %T", notSupportedErrMessage, stmt))
	}

	return c.convertSelect(selectStmt)
}

func (c *Converter) convertSelect(sel *sqlparser.Select) (*elastic.BoolQuery, []*elastic.FieldSort, error) {
	if sel.GroupBy != nil {
		return nil, nil, NewConverterError(fmt.Sprintf("%s: 'group by' clause", notSupportedErrMessage))
	}

	if sel.Limit != nil {
		return nil, nil, NewConverterError(fmt.Sprintf("%s: 'limit' clause", notSupportedErrMessage))
	}

	var query *elastic.BoolQuery
	if sel.Where != nil {
		q, err := c.convertWhere(sel.Where.Expr)
		if err != nil {
			return nil, nil, NewConverterError(fmt.Sprintf("unable to convert filter expression: %s", err))
		}
		// Result must be BoolQuery.
		var isBoolQuery bool
		if query, isBoolQuery = q.(*elastic.BoolQuery); !isBoolQuery {
			query = elastic.NewBoolQuery().Filter(q)
		}
	}

	var fieldSorts []*elastic.FieldSort
	for _, orderByExpr := range sel.OrderBy {
		colName, err := c.convertColName(orderByExpr.Expr, FieldNameSorter)
		if err != nil {
			return nil, nil, NewConverterError(fmt.Sprintf("unable to convert 'order by' column name: %s", err))
		}
		fieldSort := elastic.NewFieldSort(colName)
		if orderByExpr.Direction == sqlparser.DescScr {
			fieldSort = fieldSort.Desc()
		}
		fieldSorts = append(fieldSorts, fieldSort)
	}

	return query, fieldSorts, nil
}

func (c *Converter) convertWhere(expr sqlparser.Expr) (elastic.Query, error) {
	if expr == nil {
		return nil, errors.New("cannot be nil")
	}

	switch e := (expr).(type) {
	case *sqlparser.AndExpr:
		return c.convertAndExpr(e)
	case *sqlparser.OrExpr:
		return c.convertOrExpr(e)
	case *sqlparser.ComparisonExpr:
		return c.convertComparisonExpr(e)
	case *sqlparser.RangeCond:
		return c.convertRangeCondExpr(e)
	case *sqlparser.ParenExpr:
		return c.convertWhere(e.Expr)
	case *sqlparser.IsExpr:
		return c.convertIsExpr(e)
	case *sqlparser.NotExpr:
		return nil, NewConverterError(fmt.Sprintf("%s: 'not' expression", notSupportedErrMessage))
	case *sqlparser.FuncExpr:
		return nil, NewConverterError(fmt.Sprintf("%s: function expression", notSupportedErrMessage))
	default:
		return nil, NewConverterError(fmt.Sprintf("%s: expression of type %T", notSupportedErrMessage, expr))
	}
}

func (c *Converter) convertAndExpr(expr *sqlparser.AndExpr) (elastic.Query, error) {
	leftExpr := expr.Left
	rightExpr := expr.Right
	leftQuery, err := c.convertWhere(leftExpr)
	if err != nil {
		return nil, err
	}
	rightQuery, err := c.convertWhere(rightExpr)
	if err != nil {
		return nil, err
	}

	// If left or right is a BoolQuery built from AndExpr then reuse it w/o creating new BoolQuery.
	lqBool, isLQBool := leftQuery.(*elastic.BoolQuery)
	_, isLEAnd := leftExpr.(*sqlparser.AndExpr)
	if isLQBool && isLEAnd {
		return lqBool.Filter(rightQuery), nil
	}

	rqBool, isRQBool := rightQuery.(*elastic.BoolQuery)
	_, isREAnd := rightExpr.(*sqlparser.AndExpr)
	if isRQBool && isREAnd {
		return rqBool.Filter(leftQuery), nil
	}

	return elastic.NewBoolQuery().Filter(leftQuery, rightQuery), nil
}

func (c *Converter) convertOrExpr(expr *sqlparser.OrExpr) (elastic.Query, error) {
	leftExpr := expr.Left
	rightExpr := expr.Right
	leftQuery, err := c.convertWhere(leftExpr)
	if err != nil {
		return nil, err
	}
	rightQuery, err := c.convertWhere(rightExpr)
	if err != nil {
		return nil, err
	}

	// If left or right is a BoolQuery built from OrExpr then reuse it w/o creating new BoolQuery.
	lqBool, isLQBool := leftQuery.(*elastic.BoolQuery)
	_, isLEOr := leftExpr.(*sqlparser.OrExpr)
	if isLQBool && isLEOr {
		return lqBool.Should(rightQuery), nil
	}

	rqBool, isRQBool := rightQuery.(*elastic.BoolQuery)
	_, isREOr := rightExpr.(*sqlparser.OrExpr)
	if isRQBool && isREOr {
		return rqBool.Should(leftQuery), nil
	}

	return elastic.NewBoolQuery().Should(leftQuery, rightQuery), nil
}

func (c *Converter) convertRangeCondExpr(expr *sqlparser.RangeCond) (elastic.Query, error) {
	colName, err := c.convertColName(expr.Left, FieldNameFilter)
	if err != nil {
		return nil, NewConverterError(fmt.Sprintf("unable to convert left part of 'between' expression: %s", err))
	}

	fromValue, err := c.parseSqlValue(sqlparser.String(expr.From))
	if err != nil {
		return nil, err
	}
	toValue, err := c.parseSqlValue(sqlparser.String(expr.To))
	if err != nil {
		return nil, err
	}

	values, err := c.fvInterceptor.Values(colName, fromValue, toValue)
	if err != nil {
		return nil, NewConverterError(fmt.Sprintf("unable to convert values of 'between' expression: %s", err))
	}
	fromValue = values[0]
	toValue = values[1]

	var query elastic.Query
	switch expr.Operator {
	case "between":
		query = elastic.NewRangeQuery(colName).Gte(fromValue).Lte(toValue)
	case "not between":
		query = elastic.NewBoolQuery().MustNot(elastic.NewRangeQuery(colName).Gte(fromValue).Lte(toValue))
	default:
		return nil, NewConverterError(fmt.Sprintf("%s: range condition operator must be 'between' or 'not between'", invalidExpressionErrMessage))
	}
	return query, nil
}

func (c *Converter) convertIsExpr(expr *sqlparser.IsExpr) (elastic.Query, error) {
	colName, err := c.convertColName(expr.Expr, FieldNameFilter)
	if err != nil {
		return nil, NewConverterError(fmt.Sprintf("unable to convert left part of 'is' expression: %s", err))
	}

	var query elastic.Query
	switch expr.Operator {
	case "is null":
		query = elastic.NewBoolQuery().MustNot(elastic.NewExistsQuery(colName))
	case "is not null":
		query = elastic.NewExistsQuery(colName)
	default:
		return nil, NewConverterError(fmt.Sprintf("%s: 'is' operator can be used with 'null' and 'not null' only", invalidExpressionErrMessage))
	}

	return query, nil
}

func (c *Converter) convertComparisonExpr(expr *sqlparser.ComparisonExpr) (elastic.Query, error) {
	colName, err := c.convertColName(expr.Left, FieldNameFilter)
	if err != nil {
		return nil, NewConverterError(fmt.Sprintf("unable to convert left part of comparison expression: %s", err))
	}

	colValue, err := c.convertComparisonExprValue(expr.Right)
	if err != nil {
		return nil, NewConverterError(fmt.Sprintf("unable to convert right part of comparison expression: %s", err))
	}

	if expr.Operator == "like" || expr.Operator == "not like" {
		colValue, err = c.cleanLikeValue(colValue)
		if err != nil {
			return nil, err
		}
	}

	colValues, isArray := colValue.([]interface{})
	// colValue should be an array only for "in (1,2,3)" queries.
	if !isArray {
		colValues = []interface{}{colValue}
	}

	colValues, err = c.fvInterceptor.Values(colName, colValues...)
	if err != nil {
		return nil, NewConverterError(fmt.Sprintf("unable to convert values of comparison expression: %s", err))
	}

	var query elastic.Query
	switch expr.Operator {
	case ">=":
		query = elastic.NewRangeQuery(colName).Gte(colValues[0])
	case "<=":
		query = elastic.NewRangeQuery(colName).Lte(colValues[0])
	case ">":
		query = elastic.NewRangeQuery(colName).Gt(colValues[0])
	case "<":
		query = elastic.NewRangeQuery(colName).Lt(colValues[0])
	case "=":
		if _, isMissingCheck := colValues[0].(missingCheck); isMissingCheck {
			query = elastic.NewBoolQuery().MustNot(elastic.NewExistsQuery(colName))
		} else {
			// Not elastic.NewTermQuery to support String custom search attributes.
			query = elastic.NewMatchPhraseQuery(colName, colValues[0])
		}
	case "!=":
		if _, isMissingCheck := colValues[0].(missingCheck); isMissingCheck {
			query = elastic.NewExistsQuery(colName)
		} else {
			// Not elastic.NewTermQuery to support String custom search attributes.
			query = elastic.NewBoolQuery().MustNot(elastic.NewMatchPhraseQuery(colName, colValues[0]))
		}
	case "in":
		query = elastic.NewTermsQuery(colName, colValues...)
	case "not in":
		query = elastic.NewBoolQuery().MustNot(elastic.NewTermsQuery(colName, colValues...))
	case "like":
		query = elastic.NewMatchPhraseQuery(colName, colValues[0])
	case "not like":
		query = elastic.NewBoolQuery().MustNot(elastic.NewMatchPhraseQuery(colName, colValues[0]))
	}

	return query, nil
}

func (c *Converter) convertComparisonExprValue(expr sqlparser.Expr) (interface{}, error) {
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		v, err := c.parseSqlValue(sqlparser.String(e))
		if err != nil {
			return nil, err
		}
		return v, nil
	case sqlparser.BoolVal:
		return bool(e), nil
	case sqlparser.ValTuple:
		// This is "in (1,2,3)" case.
		exprs := []sqlparser.Expr(e)
		var result []interface{}
		for _, expr := range exprs {
			v, err := c.convertComparisonExprValue(expr)
			if err != nil {
				return nil, err
			}
			result = append(result, v)
		}
		return result, nil
	case *sqlparser.GroupConcatExpr:
		return nil, NewConverterError(fmt.Sprintf("%s: 'group_concat'", notSupportedErrMessage))
	case *sqlparser.FuncExpr:
		return nil, NewConverterError(fmt.Sprintf("%s: nested func", notSupportedErrMessage))
	case *sqlparser.ColName:
		if sqlparser.String(expr) == "missing" {
			return missingCheck{}, nil
		}
		return nil, NewConverterError(fmt.Sprintf("%s: column name on the right side of comparison expression", notSupportedErrMessage))
	default:
		return nil, NewConverterError(fmt.Sprintf("%s: unexpected value type %T", invalidExpressionErrMessage, expr))
	}
}

func (c *Converter) cleanLikeValue(colValue interface{}) (string, error) {
	colValueStr, isString := colValue.(string)
	if !isString {
		return "", NewConverterError(fmt.Sprintf("%s: 'like' operator value must be a string but was %T", invalidExpressionErrMessage, colValue))
	}
	return strings.ReplaceAll(colValueStr, "%", ""), nil
}

func (c *Converter) parseSqlValue(sqlValue string) (interface{}, error) {
	if sqlValue == "" {
		return "", nil
	}

	if sqlValue[0] == '\'' && sqlValue[len(sqlValue)-1] == '\'' {
		strValue := strings.Trim(sqlValue, "'")
		return strValue, nil
	}

	// Unquoted value must be a number. Try int64 first.
	if intValue, err := strconv.ParseInt(sqlValue, 10, 64); err == nil {
		return intValue, nil
	}

	// Then float64.
	if floatValue, err := strconv.ParseFloat(sqlValue, 64); err == nil {
		return floatValue, nil
	}

	return nil, NewConverterError(fmt.Sprintf("%s: unable to parse %s", invalidExpressionErrMessage, sqlValue))
}

func (c *Converter) convertColName(colNameExpr sqlparser.Expr, usage FieldNameUsage) (string, error) {
	colName, isColName := colNameExpr.(*sqlparser.ColName)
	if !isColName {
		return "", NewConverterError(fmt.Sprintf("%s: must be a column name but was %T", invalidExpressionErrMessage, colNameExpr))
	}

	colNameStr := sqlparser.String(colName)
	colNameStr = strings.ReplaceAll(colNameStr, "`", "")
	return c.fnInterceptor.Name(colNameStr, usage)
}
