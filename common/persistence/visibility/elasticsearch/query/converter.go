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

func (c *Converter) ConvertWhereOrderBy(whereOrderBy string) (elastic.Query, []elastic.Sorter, error) {
	whereOrderBy = strings.Trim(whereOrderBy, " ")
	if whereOrderBy != "" && !strings.HasPrefix(whereOrderBy, "order by ") {
		whereOrderBy = "where " + whereOrderBy
	}
	sql := fmt.Sprintf("select * from table1 %s", whereOrderBy)
	return c.convertSql(sql)
}

// convertSql transforms SQL to Elasticsearch query.
func (c *Converter) convertSql(sql string) (elastic.Query, []elastic.Sorter, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", MalformedSqlQueryErr, err)
	}

	selectStmt, isSelect := stmt.(*sqlparser.Select)
	if !isSelect {
		return nil, nil, fmt.Errorf("%w: statement must be 'select' not %T", NotSupportedErr, stmt)
	}

	return c.convertSelect(selectStmt)
}

func (c *Converter) convertSelect(sel *sqlparser.Select) (elastic.Query, []elastic.Sorter, error) {
	var (
		query elastic.Query
		err   error
	)

	if sel.GroupBy != nil {
		return nil, nil, fmt.Errorf("%w: 'group by' clause", NotSupportedErr)
	}

	if sel.Where != nil {
		query, err = c.convertSelectWhere(sel.Where.Expr)
		if err != nil {
			return nil, nil, err
		}
		// Result must be BoolQuery.
		if _, isBoolQuery := query.(*elastic.BoolQuery); !isBoolQuery {
			query = elastic.NewBoolQuery().Filter(query)
		}
	} else {
		query = elastic.NewBoolQuery().Filter(elastic.NewMatchAllQuery())
	}

	var sorter []elastic.Sorter
	for _, orderByExpr := range sel.OrderBy {
		colName, err := c.convertColName(orderByExpr.Expr)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to convert 'order by' column name: %w", err)
		}
		sortField := elastic.NewFieldSort(colName)
		if orderByExpr.Direction == sqlparser.DescScr {
			sortField = sortField.Desc()
		}
		sorter = append(sorter, sortField)
	}

	return query, sorter, nil
}

func (c *Converter) convertSelectWhere(expr sqlparser.Expr) (elastic.Query, error) {
	if expr == nil {
		return nil, errors.New("'where' expression cannot be nil")
	}

	switch e := (expr).(type) {
	case *sqlparser.AndExpr:
		return c.convertSelectWhereAndExpr(e)
	case *sqlparser.OrExpr:
		return c.convertSelectWhereOrExpr(e)
	case *sqlparser.ComparisonExpr:
		return c.convertSelectWhereComparisonExpr(e)
	case *sqlparser.RangeCond:
		return c.convertSelectWhereRangeCondExpr(e)
	case *sqlparser.ParenExpr:
		return c.convertSelectWhere(e.Expr)
	case *sqlparser.IsExpr:
		return c.convertSelectIsExpr(e)
	case *sqlparser.NotExpr:
		return nil, fmt.Errorf("%w: 'not' expression", NotSupportedErr)
	case *sqlparser.FuncExpr:
		return nil, fmt.Errorf("%w: function expression", NotSupportedErr)
	default:
		return nil, fmt.Errorf("%w: expression of type %T", NotSupportedErr, expr)
	}
}

func (c *Converter) convertSelectWhereAndExpr(expr *sqlparser.AndExpr) (elastic.Query, error) {
	leftExpr := expr.Left
	rightExpr := expr.Right
	leftQuery, err := c.convertSelectWhere(leftExpr)
	if err != nil {
		return nil, err
	}
	rightQuery, err := c.convertSelectWhere(rightExpr)
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

func (c *Converter) convertSelectWhereOrExpr(expr *sqlparser.OrExpr) (elastic.Query, error) {
	leftExpr := expr.Left
	rightExpr := expr.Right
	leftQuery, err := c.convertSelectWhere(leftExpr)
	if err != nil {
		return nil, err
	}
	rightQuery, err := c.convertSelectWhere(rightExpr)
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

func (c *Converter) convertSelectWhereRangeCondExpr(expr *sqlparser.RangeCond) (elastic.Query, error) {
	colName, err := c.convertColName(expr.Left)
	if err != nil {
		return nil, fmt.Errorf("unable to convert left part of 'between' expression: %w", err)
	}

	fromValue, err := c.parseSqlValue(sqlparser.String(expr.From))
	if err != nil {
		return nil, err
	}
	toValue, err := c.parseSqlValue(sqlparser.String(expr.To))
	if err != nil {
		return nil, err
	}

	var query elastic.Query
	switch expr.Operator {
	case "between":
		query = elastic.NewRangeQuery(colName).Gte(fromValue).Lte(toValue)
	case "not between":
		query = elastic.NewBoolQuery().MustNot(elastic.NewRangeQuery(colName).Gte(fromValue).Lte(toValue))
	default:
		return nil, fmt.Errorf("%w: range condition operator must be 'between' or 'not between'", InvalidExpressionErr)
	}
	return query, nil
}

func (c *Converter) convertSelectIsExpr(expr *sqlparser.IsExpr) (elastic.Query, error) {
	colName, err := c.convertColName(expr.Expr)
	if err != nil {
		return nil, fmt.Errorf("unable to convert left part of 'is' expression: %w", err)
	}

	var query elastic.Query
	switch expr.Operator {
	case "is null":
		query = elastic.NewBoolQuery().MustNot(elastic.NewExistsQuery(colName))
	case "is not null":
		query = elastic.NewExistsQuery(colName)
	default:
		return nil, fmt.Errorf("%w: 'is' operator can be used with 'null' and 'not null' only", InvalidExpressionErr)
	}

	return query, nil
}

func (c *Converter) convertSelectWhereComparisonExpr(expr *sqlparser.ComparisonExpr) (elastic.Query, error) {
	colName, err := c.convertColName(expr.Left)
	if err != nil {
		return nil, fmt.Errorf("unable to convert left part of comparison expression: %w", err)
	}

	colValue, missingCheck, err := c.convertComparisonExprValue(expr.Right)
	if err != nil {
		return nil, err
	}

	var query elastic.Query
	switch expr.Operator {
	case ">=":
		query = elastic.NewRangeQuery(colName).Gte(colValue)
	case "<=":
		query = elastic.NewRangeQuery(colName).Lte(colValue)
	case "=":
		// field is missing
		if missingCheck {
			query = elastic.NewBoolQuery().MustNot(elastic.NewExistsQuery(colName))
		} else {
			query = elastic.NewMatchPhraseQuery(colName, colValue)
		}
	case ">":
		query = elastic.NewRangeQuery(colName).Gt(colValue)
	case "<":
		query = elastic.NewRangeQuery(colName).Lt(colValue)
	case "!=":
		if missingCheck {
			query = elastic.NewExistsQuery(colName)
		} else {
			query = elastic.NewBoolQuery().MustNot(elastic.NewMatchPhraseQuery(colName, colValue))
		}
	case "in":
		colValueStr, isString := colValue.(string)
		if !isString {
			return nil, fmt.Errorf("%w: 'in' operator value must be a string but was %T", InvalidExpressionErr, colValue)
		}
		// colValueStr is a string like ('1', '2', '3').
		parsedValues, err := c.parseSqlRange(colValueStr)
		if err != nil {
			return nil, err
		}
		query = elastic.NewTermsQuery(colName, parsedValues...)
	case "not in":
		colValueStr, isString := colValue.(string)
		if !isString {
			return nil, fmt.Errorf("%w: 'not in' operator value must be a string but was %T", InvalidExpressionErr, colValue)
		}
		// colValue is a string like ('1', '2', '3').
		parsedValues, err := c.parseSqlRange(colValueStr)
		if err != nil {
			return nil, err
		}
		query = elastic.NewBoolQuery().MustNot(elastic.NewTermsQuery(colName, parsedValues...))
	case "like":
		colValueStr, isString := colValue.(string)
		if !isString {
			return nil, fmt.Errorf("%w: 'like' operator value must be a string but was %T", InvalidExpressionErr, colValue)
		}
		colValue = strings.Replace(colValueStr, "%", "", -1)
		query = elastic.NewMatchPhraseQuery(colName, colValue)
	case "not like":
		colValueStr, isString := colValue.(string)
		if !isString {
			return nil, fmt.Errorf("%w: 'not like' operator value must be a string but was %T", InvalidExpressionErr, colValue)
		}
		colValue = strings.Replace(colValueStr, "%", "", -1)
		query = elastic.NewBoolQuery().MustNot(elastic.NewMatchPhraseQuery(colName, colValue))
	}

	return query, nil
}

func (c *Converter) convertComparisonExprValue(expr sqlparser.Expr) (interface{}, bool, error) {
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		sqlValue, err := c.parseSqlValue(sqlparser.String(e))
		return sqlValue, false, err
	case *sqlparser.GroupConcatExpr:
		return "", false, fmt.Errorf("%w: 'group_concat'", NotSupportedErr)
	case *sqlparser.FuncExpr:
		return "", false, fmt.Errorf("%w: nested func", NotSupportedErr)
	case *sqlparser.ColName:
		if sqlparser.String(expr) == "missing" {
			return "", true, nil
		}
		return "", false, fmt.Errorf("%w: column name on the right side of comparison expression", NotSupportedErr)
	case sqlparser.ValTuple:
		return sqlparser.String(expr), false, nil
	default:
		// cannot reach here
		return "", false, fmt.Errorf("%w: unexpected value type %T", InvalidExpressionErr, expr)
	}
}

// parseSqlRange parses strings like "('1', '2', '3')" which comes from sql parser.
func (c *Converter) parseSqlRange(sqlRange string) ([]interface{}, error) {
	sqlRange = strings.Trim(sqlRange, "()")
	var values []interface{}
	for _, v := range strings.Split(sqlRange, ", ") {
		parsedValue, err := c.parseSqlValue(v)
		if err != nil {
			return nil, err
		}
		values = append(values, parsedValue)
	}
	return values, nil
}

func (c *Converter) parseSqlValue(sqlValue string) (interface{}, error) {
	if sqlValue == "" {
		return "", nil
	}

	if sqlValue[0] == '\'' && sqlValue[len(sqlValue)-1] == '\'' {
		return strings.Trim(sqlValue, "'"), nil
	}

	floatValue, err := strconv.ParseFloat(sqlValue, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to parse %s to float64: %v", InvalidExpressionErr, sqlValue, err)
	}

	return floatValue, nil
}

func (c *Converter) convertColName(colNameExpr sqlparser.Expr) (string, error) {
	colName, isColName := colNameExpr.(*sqlparser.ColName)
	if !isColName {
		return "", fmt.Errorf("%w: must be a column name but was %T", InvalidExpressionErr, colNameExpr)
	}

	colNameStr := sqlparser.String(colName)
	colNameStr = strings.Replace(colNameStr, "`", "", -1)
	return c.fnInterceptor.Name(colNameStr)
}
