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
	ExprConverter interface {
		Convert(expr sqlparser.Expr) (elastic.Query, error)
	}

	Converter struct {
		fnInterceptor  FieldNameInterceptor
		whereConverter ExprConverter
	}

	WhereConverter struct {
		And            ExprConverter
		Or             ExprConverter
		RangeCond      ExprConverter
		ComparisonExpr ExprConverter
		Is             ExprConverter
	}

	andConverter struct {
		where ExprConverter
	}

	orConverter struct {
		where ExprConverter
	}

	rangeCondConverter struct {
		fnInterceptor       FieldNameInterceptor
		fvInterceptor       FieldValuesInterceptor
		notBetweenSupported bool
	}

	comparisonExprConverter struct {
		fnInterceptor    FieldNameInterceptor
		fvInterceptor    FieldValuesInterceptor
		allowedOperators map[string]struct{}
	}

	isConverter struct {
		fnInterceptor FieldNameInterceptor
	}

	notSupportedExprConverter struct{}
)

func NewConverter(fnInterceptor FieldNameInterceptor, whereConverter ExprConverter) *Converter {
	if fnInterceptor == nil {
		fnInterceptor = &NopFieldNameInterceptor{}
	}
	return &Converter{
		fnInterceptor:  fnInterceptor,
		whereConverter: whereConverter,
	}
}

func NewWhereConverter(
	and ExprConverter,
	or ExprConverter,
	rangeCond ExprConverter,
	comparisonExpr ExprConverter,
	is ExprConverter) ExprConverter {
	if and == nil {
		and = &notSupportedExprConverter{}
	}

	if or == nil {
		or = &notSupportedExprConverter{}
	}

	if rangeCond == nil {
		rangeCond = &notSupportedExprConverter{}
	}

	if comparisonExpr == nil {
		comparisonExpr = &notSupportedExprConverter{}
	}

	if is == nil {
		is = &notSupportedExprConverter{}
	}

	return &WhereConverter{
		And:            and,
		Or:             or,
		RangeCond:      rangeCond,
		ComparisonExpr: comparisonExpr,
		Is:             is,
	}
}

func NewAndConverter(whereConverter ExprConverter) ExprConverter {
	return &andConverter{
		where: whereConverter,
	}
}

func NewOrConverter(whereConverter ExprConverter) ExprConverter {
	return &orConverter{
		where: whereConverter,
	}
}

func NewRangeCondConverter(
	fnInterceptor FieldNameInterceptor,
	fvInterceptor FieldValuesInterceptor,
	notBetweenSupported bool,
) ExprConverter {
	if fnInterceptor == nil {
		fnInterceptor = &NopFieldNameInterceptor{}
	}
	if fvInterceptor == nil {
		fvInterceptor = &NopFieldValuesInterceptor{}
	}
	return &rangeCondConverter{
		fnInterceptor:       fnInterceptor,
		fvInterceptor:       fvInterceptor,
		notBetweenSupported: notBetweenSupported,
	}
}

func NewComparisonExprConverter(
	fnInterceptor FieldNameInterceptor,
	fvInterceptor FieldValuesInterceptor,
	allowedOperators map[string]struct{},
) ExprConverter {
	if fnInterceptor == nil {
		fnInterceptor = &NopFieldNameInterceptor{}
	}
	if fvInterceptor == nil {
		fvInterceptor = &NopFieldValuesInterceptor{}
	}
	return &comparisonExprConverter{
		fnInterceptor:    fnInterceptor,
		fvInterceptor:    fvInterceptor,
		allowedOperators: allowedOperators,
	}
}

func NewIsConverter(fnInterceptor FieldNameInterceptor) ExprConverter {
	return &isConverter{
		fnInterceptor: fnInterceptor,
	}
}

func NewNotSupportedExprConverter() ExprConverter {
	return &notSupportedExprConverter{}
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
	return c.ConvertSql(sql)
}

// ConvertSql transforms SQL to Elasticsearch query.
func (c *Converter) ConvertSql(sql string) (*elastic.BoolQuery, []*elastic.FieldSort, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, nil, NewConverterError("%s: %v", MalformedSqlQueryErrMessage, err)
	}

	selectStmt, isSelect := stmt.(*sqlparser.Select)
	if !isSelect {
		return nil, nil, NewConverterError("%s: statement must be 'select' not %T", NotSupportedErrMessage, stmt)
	}

	return c.convertSelect(selectStmt)
}

func (c *Converter) convertSelect(sel *sqlparser.Select) (*elastic.BoolQuery, []*elastic.FieldSort, error) {
	if sel.GroupBy != nil {
		return nil, nil, NewConverterError("%s: 'group by' clause", NotSupportedErrMessage)
	}

	if sel.Limit != nil {
		return nil, nil, NewConverterError("%s: 'limit' clause", NotSupportedErrMessage)
	}

	var query *elastic.BoolQuery
	if sel.Where != nil {
		q, err := c.whereConverter.Convert(sel.Where.Expr)
		if err != nil {
			return nil, nil, wrapConverterError("unable to convert filter expression", err)
		}
		// Result must be BoolQuery.
		var isBoolQuery bool
		if query, isBoolQuery = q.(*elastic.BoolQuery); !isBoolQuery {
			query = elastic.NewBoolQuery().Filter(q)
		}
	}

	var fieldSorts []*elastic.FieldSort
	for _, orderByExpr := range sel.OrderBy {
		colName, err := convertColName(c.fnInterceptor, orderByExpr.Expr, FieldNameSorter)
		if err != nil {
			return nil, nil, wrapConverterError("unable to convert 'order by' column name", err)
		}
		fieldSort := elastic.NewFieldSort(colName)
		if orderByExpr.Direction == sqlparser.DescScr {
			fieldSort = fieldSort.Desc()
		}
		fieldSorts = append(fieldSorts, fieldSort)
	}

	return query, fieldSorts, nil
}

func (w *WhereConverter) Convert(expr sqlparser.Expr) (elastic.Query, error) {
	if expr == nil {
		return nil, errors.New("cannot be nil")
	}

	switch e := (expr).(type) {
	case *sqlparser.AndExpr:
		return w.And.Convert(e)
	case *sqlparser.OrExpr:
		return w.Or.Convert(e)
	case *sqlparser.ComparisonExpr:
		return w.ComparisonExpr.Convert(e)
	case *sqlparser.RangeCond:
		return w.RangeCond.Convert(e)
	case *sqlparser.ParenExpr:
		return w.Convert(e.Expr)
	case *sqlparser.IsExpr:
		return w.Is.Convert(e)
	case *sqlparser.NotExpr:
		return nil, NewConverterError("%s: 'not' expression", NotSupportedErrMessage)
	case *sqlparser.FuncExpr:
		return nil, NewConverterError("%s: function expression", NotSupportedErrMessage)
	case *sqlparser.ColName:
		return nil, NewConverterError("incomplete expression")
	default:
		return nil, NewConverterError("%s: expression of type %T", NotSupportedErrMessage, expr)
	}
}

func (a *andConverter) Convert(expr sqlparser.Expr) (elastic.Query, error) {
	andExpr, ok := expr.(*sqlparser.AndExpr)
	if !ok {
		return nil, NewConverterError("%v is not an 'and' expression", sqlparser.String(expr))
	}

	leftExpr := andExpr.Left
	rightExpr := andExpr.Right
	leftQuery, err := a.where.Convert(leftExpr)
	if err != nil {
		return nil, err
	}
	rightQuery, err := a.where.Convert(rightExpr)
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

func (o *orConverter) Convert(expr sqlparser.Expr) (elastic.Query, error) {
	orExpr, ok := expr.(*sqlparser.OrExpr)
	if !ok {
		return nil, NewConverterError("%v is not an 'or' expression", sqlparser.String(expr))
	}

	leftExpr := orExpr.Left
	rightExpr := orExpr.Right
	leftQuery, err := o.where.Convert(leftExpr)
	if err != nil {
		return nil, err
	}
	rightQuery, err := o.where.Convert(rightExpr)
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

func (r *rangeCondConverter) Convert(expr sqlparser.Expr) (elastic.Query, error) {
	rangeCond, ok := expr.(*sqlparser.RangeCond)
	if !ok {
		return nil, NewConverterError("%v is not a range condition", sqlparser.String(expr))
	}

	colName, err := convertColName(r.fnInterceptor, rangeCond.Left, FieldNameFilter)
	if err != nil {
		return nil, wrapConverterError("unable to convert left part of 'between' expression", err)
	}

	fromValue, err := parseSqlValue(sqlparser.String(rangeCond.From))
	if err != nil {
		return nil, err
	}
	toValue, err := parseSqlValue(sqlparser.String(rangeCond.To))
	if err != nil {
		return nil, err
	}

	values, err := r.fvInterceptor.Values(colName, fromValue, toValue)
	if err != nil {
		return nil, wrapConverterError("unable to convert values of 'between' expression", err)
	}
	fromValue = values[0]
	toValue = values[1]

	var query elastic.Query
	switch rangeCond.Operator {
	case "between":
		query = elastic.NewRangeQuery(colName).Gte(fromValue).Lte(toValue)
	case "not between":
		if !r.notBetweenSupported {
			return nil, NewConverterError("%s: 'not between' expression", NotSupportedErrMessage)
		}
		query = elastic.NewBoolQuery().MustNot(elastic.NewRangeQuery(colName).Gte(fromValue).Lte(toValue))
	default:
		return nil, NewConverterError("%s: range condition operator must be 'between' or 'not between'", InvalidExpressionErrMessage)
	}
	return query, nil
}

func (i *isConverter) Convert(expr sqlparser.Expr) (elastic.Query, error) {
	isExpr, ok := expr.(*sqlparser.IsExpr)
	if !ok {
		return nil, NewConverterError("%v is not an 'is' expression", sqlparser.String(expr))
	}

	colName, err := convertColName(i.fnInterceptor, isExpr.Expr, FieldNameFilter)
	if err != nil {
		return nil, wrapConverterError("unable to convert left part of 'is' expression", err)
	}

	var query elastic.Query
	switch isExpr.Operator {
	case "is null":
		query = elastic.NewBoolQuery().MustNot(elastic.NewExistsQuery(colName))
	case "is not null":
		query = elastic.NewExistsQuery(colName)
	default:
		return nil, NewConverterError("%s: 'is' operator can be used with 'null' and 'not null' only", InvalidExpressionErrMessage)
	}

	return query, nil
}

func (c *comparisonExprConverter) Convert(expr sqlparser.Expr) (elastic.Query, error) {
	comparisonExpr, ok := expr.(*sqlparser.ComparisonExpr)
	if !ok {
		return nil, NewConverterError("%v is not a comparison expression", sqlparser.String(expr))
	}

	colName, err := convertColName(c.fnInterceptor, comparisonExpr.Left, FieldNameFilter)
	if err != nil {
		return nil, wrapConverterError("unable to convert left part of comparison expression", err)
	}

	colValue, err := convertComparisonExprValue(comparisonExpr.Right)
	if err != nil {
		return nil, wrapConverterError("unable to convert right part of comparison expression", err)
	}

	if comparisonExpr.Operator == "like" || comparisonExpr.Operator == "not like" {
		colValue, err = cleanLikeValue(colValue)
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
		return nil, wrapConverterError("unable to convert values of comparison expression", err)
	}

	if _, ok := c.allowedOperators[comparisonExpr.Operator]; !ok {
		return nil, NewConverterError("operator '%v' not allowed in comparison expression", comparisonExpr.Operator)
	}

	var query elastic.Query
	switch comparisonExpr.Operator {
	case ">=":
		query = elastic.NewRangeQuery(colName).Gte(colValues[0])
	case "<=":
		query = elastic.NewRangeQuery(colName).Lte(colValues[0])
	case ">":
		query = elastic.NewRangeQuery(colName).Gt(colValues[0])
	case "<":
		query = elastic.NewRangeQuery(colName).Lt(colValues[0])
	case "=", "like": // The only difference is that "%" is removed for LIKE queries.
		// Not elastic.NewTermQuery to support partial word match for String custom search attributes.
		query = elastic.NewMatchQuery(colName, colValues[0])
	case "!=", "not like":
		// Not elastic.NewTermQuery to support partial word match for String custom search attributes.
		query = elastic.NewBoolQuery().MustNot(elastic.NewMatchQuery(colName, colValues[0]))
	case "in":
		query = elastic.NewTermsQuery(colName, colValues...)
	case "not in":
		query = elastic.NewBoolQuery().MustNot(elastic.NewTermsQuery(colName, colValues...))
	}

	return query, nil
}

func convertComparisonExprValue(expr sqlparser.Expr) (interface{}, error) {
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		v, err := parseSqlValue(sqlparser.String(e))
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
			v, err := convertComparisonExprValue(expr)
			if err != nil {
				return nil, err
			}
			result = append(result, v)
		}
		return result, nil
	case *sqlparser.GroupConcatExpr:
		return nil, NewConverterError("%s: 'group_concat'", NotSupportedErrMessage)
	case *sqlparser.FuncExpr:
		return nil, NewConverterError("%s: nested func", NotSupportedErrMessage)
	case *sqlparser.ColName:
		return nil, NewConverterError("%s: column name on the right side of comparison expression", NotSupportedErrMessage)
	default:
		return nil, NewConverterError("%s: unexpected value type %T", InvalidExpressionErrMessage, expr)
	}
}

func cleanLikeValue(colValue interface{}) (string, error) {
	colValueStr, isString := colValue.(string)
	if !isString {
		return "", NewConverterError("%s: 'like' operator value must be a string but was %T", InvalidExpressionErrMessage, colValue)
	}
	return strings.ReplaceAll(colValueStr, "%", ""), nil
}

func (n *notSupportedExprConverter) Convert(expr sqlparser.Expr) (elastic.Query, error) {
	return nil, NewConverterError("%s: expression of type %T", NotSupportedErrMessage, expr)
}

func parseSqlValue(sqlValue string) (interface{}, error) {
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

	return nil, NewConverterError("%s: unable to parse %s", InvalidExpressionErrMessage, sqlValue)
}

func convertColName(fnInterceptor FieldNameInterceptor, colNameExpr sqlparser.Expr, usage FieldNameUsage) (string, error) {
	colName, isColName := colNameExpr.(*sqlparser.ColName)
	if !isColName {
		return "", NewConverterError("%s: must be a column name but was %T", InvalidExpressionErrMessage, colNameExpr)
	}

	colNameStr := sqlparser.String(colName)
	colNameStr = strings.ReplaceAll(colNameStr, "`", "")
	return fnInterceptor.Name(colNameStr, usage)
}
