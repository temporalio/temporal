// Package query is inspired and partially copied from by github.com/cch123/elasticsql.
package query

import (
	"errors"
	"fmt"
	"strings"

	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/sqlquery"
)

type (
	ExprConverter interface {
		Convert(expr sqlparser.Expr) (client.Query, error)
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
		saNameType       searchattribute.NameTypeMap
	}

	isConverter struct {
		fnInterceptor FieldNameInterceptor
	}

	notSupportedExprConverter struct{}

	QueryParams struct {
		Query   client.Query
		Sorter  []client.Sorter
		GroupBy []string
	}
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
	saNameType searchattribute.NameTypeMap,
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
		saNameType:       saNameType,
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
func (c *Converter) ConvertWhereOrderBy(whereOrderBy string) (*QueryParams, error) {
	whereOrderBy = strings.TrimSpace(whereOrderBy)

	if whereOrderBy != "" &&
		!strings.HasPrefix(strings.ToLower(whereOrderBy), "order by ") &&
		!strings.HasPrefix(strings.ToLower(whereOrderBy), "group by ") {
		whereOrderBy = "where " + whereOrderBy
	}
	// sqlparser can't parse just WHERE clause but instead accepts only valid SQL statement.
	sql := fmt.Sprintf("select * from table1 %s", whereOrderBy)
	return c.ConvertSql(sql)
}

// ConvertSql transforms SQL to Elasticsearch query.
func (c *Converter) ConvertSql(sql string) (*QueryParams, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, NewConverterError("%s: %v", MalformedSqlQueryErrMessage, err)
	}

	selectStmt, isSelect := stmt.(*sqlparser.Select)
	if !isSelect {
		return nil, NewConverterError("%s: statement must be 'select' not %T", NotSupportedErrMessage, stmt)
	}

	return c.convertSelect(selectStmt)
}

func (c *Converter) convertSelect(sel *sqlparser.Select) (*QueryParams, error) {
	if sel.Limit != nil {
		return nil, NewConverterError("%s: 'limit' clause", NotSupportedErrMessage)
	}

	queryParams := &QueryParams{}
	if sel.Where != nil {
		query, err := c.whereConverter.Convert(sel.Where.Expr)
		if err != nil {
			return nil, wrapConverterError("unable to convert filter expression", err)
		}
		// Result must be BoolQuery. Since we use our interface-based approach,
		// we'll wrap non-bool queries in a bool filter query.
		if boolQuery, ok := query.(*client.V8BoolQuery); ok {
			queryParams.Query = boolQuery
		} else {
			queryParams.Query = client.NewV8BoolQuery().Filter(query)
		}
	}

	if len(sel.GroupBy) > 1 {
		return nil, NewConverterError("%s: 'group by' clause supports only a single field", NotSupportedErrMessage)
	}
	for _, groupByExpr := range sel.GroupBy {
		_, colName, err := convertColName(c.fnInterceptor, groupByExpr, FieldNameGroupBy)
		if err != nil {
			return nil, wrapConverterError("unable to convert 'group by' column name", err)
		}
		queryParams.GroupBy = append(queryParams.GroupBy, colName)
	}

	for _, orderByExpr := range sel.OrderBy {
		_, colName, err := convertColName(c.fnInterceptor, orderByExpr.Expr, FieldNameSorter)
		if err != nil {
			return nil, wrapConverterError("unable to convert 'order by' column name", err)
		}
		fieldSort := client.NewV8FieldSort(colName)
		if orderByExpr.Direction == sqlparser.DescScr {
			fieldSort = fieldSort.Desc()
		} else {
			fieldSort = fieldSort.Asc()
		}
		queryParams.Sorter = append(queryParams.Sorter, fieldSort)
	}

	if len(queryParams.GroupBy) > 0 && len(queryParams.Sorter) > 0 {
		return nil, NewConverterError(
			"%s: 'order by' clause is not supported with 'group by' clause",
			NotSupportedErrMessage,
		)
	}

	return queryParams, nil
}

func (w *WhereConverter) Convert(expr sqlparser.Expr) (client.Query, error) {
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

func (a *andConverter) Convert(expr sqlparser.Expr) (client.Query, error) {
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

	// For simplicity with our interface-based approach,
	// we'll always create a new BoolQuery with filters
	return client.NewV8BoolQuery().Filter(leftQuery, rightQuery), nil
}

func (o *orConverter) Convert(expr sqlparser.Expr) (client.Query, error) {
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

	// For simplicity with our interface-based approach,
	// we'll always create a new BoolQuery with should clauses
	return client.NewV8BoolQuery().Should(leftQuery, rightQuery), nil
}

func (r *rangeCondConverter) Convert(expr sqlparser.Expr) (client.Query, error) {
	rangeCond, ok := expr.(*sqlparser.RangeCond)
	if !ok {
		return nil, NewConverterError("%v is not a range condition", sqlparser.String(expr))
	}

	alias, colName, err := convertColName(r.fnInterceptor, rangeCond.Left, FieldNameFilter)
	if err != nil {
		return nil, wrapConverterError("unable to convert left part of 'between' expression", err)
	}

	fromValue, err := sqlquery.ParseValue(sqlparser.String(rangeCond.From))
	if err != nil {
		return nil, err
	}
	toValue, err := sqlquery.ParseValue(sqlparser.String(rangeCond.To))
	if err != nil {
		return nil, err
	}

	values, err := r.fvInterceptor.Values(alias, colName, fromValue, toValue)
	if err != nil {
		return nil, wrapConverterError("unable to convert values of 'between' expression", err)
	}
	fromValue = values[0]
	toValue = values[1]

	var query client.Query
	switch rangeCond.Operator {
	case "between":
		query = client.NewV8RangeQuery(colName).Gte(fromValue).Lte(toValue)
	case "not between":
		if !r.notBetweenSupported {
			return nil, NewConverterError("%s: 'not between' expression", NotSupportedErrMessage)
		}
		query = client.NewV8BoolQuery().MustNot(client.NewV8RangeQuery(colName).Gte(fromValue).Lte(toValue))
	default:
		return nil, NewConverterError("%s: range condition operator must be 'between' or 'not between'", InvalidExpressionErrMessage)
	}
	return query, nil
}

func (i *isConverter) Convert(expr sqlparser.Expr) (client.Query, error) {
	isExpr, ok := expr.(*sqlparser.IsExpr)
	if !ok {
		return nil, NewConverterError("%v is not an 'is' expression", sqlparser.String(expr))
	}

	_, colName, err := convertColName(i.fnInterceptor, isExpr.Expr, FieldNameFilter)
	if err != nil {
		return nil, wrapConverterError("unable to convert left part of 'is' expression", err)
	}

	var query client.Query
	switch isExpr.Operator {
	case "is null":
		query = client.NewV8BoolQuery().MustNot(client.NewV8ExistsQuery(colName))
	case "is not null":
		query = client.NewV8ExistsQuery(colName)
	default:
		return nil, NewConverterError("%s: 'is' operator can be used with 'null' and 'not null' only", InvalidExpressionErrMessage)
	}

	return query, nil
}

func (c *comparisonExprConverter) Convert(expr sqlparser.Expr) (client.Query, error) {
	comparisonExpr, ok := expr.(*sqlparser.ComparisonExpr)
	if !ok {
		return nil, NewConverterError("%v is not a comparison expression", sqlparser.String(expr))
	}

	alias, colName, err := convertColName(c.fnInterceptor, comparisonExpr.Left, FieldNameFilter)
	if err != nil {
		return nil, wrapConverterError(
			fmt.Sprintf("unable to convert left side of %q", sqlparser.String(expr)),
			err,
		)
	}

	colValue, err := convertComparisonExprValue(comparisonExpr.Right)
	if err != nil {
		return nil, wrapConverterError(
			fmt.Sprintf("unable to convert right side of %q", sqlparser.String(expr)),
			err,
		)
	}

	colValues, isArray := colValue.([]interface{})
	// colValue should be an array only for "in (1,2,3)" queries.
	if !isArray {
		colValues = []interface{}{colValue}
	}

	colValues, err = c.fvInterceptor.Values(alias, colName, colValues...)
	if err != nil {
		return nil, wrapConverterError("unable to convert values of comparison expression", err)
	}

	if _, ok := c.allowedOperators[comparisonExpr.Operator]; !ok {
		return nil, NewConverterError("operator '%v' not allowed in comparison expression", comparisonExpr.Operator)
	}

	tp, err := c.saNameType.GetType(colName)
	if err != nil {
		return nil, err
	}

	var query client.Query
	switch comparisonExpr.Operator {
	case sqlparser.GreaterEqualStr:
		query = client.NewV8RangeQuery(colName).Gte(colValues[0])
	case sqlparser.LessEqualStr:
		query = client.NewV8RangeQuery(colName).Lte(colValues[0])
	case sqlparser.GreaterThanStr:
		query = client.NewV8RangeQuery(colName).Gt(colValues[0])
	case sqlparser.LessThanStr:
		query = client.NewV8RangeQuery(colName).Lt(colValues[0])
	case sqlparser.EqualStr:
		// Not client.NewV8TermQuery to support partial word match for String custom search attributes.
		if tp == enumspb.INDEXED_VALUE_TYPE_KEYWORD || tp == enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST {
			query = client.NewV8TermQuery(colName, colValues[0])
		} else {
			query = client.NewV8MatchQuery(colName, colValues[0])
		}
	case sqlparser.NotEqualStr:
		// Not client.NewV8TermQuery to support partial word match for String custom search attributes.
		if tp == enumspb.INDEXED_VALUE_TYPE_KEYWORD || tp == enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST {
			query = client.NewV8BoolQuery().MustNot(client.NewV8TermQuery(colName, colValues[0]))
		} else {
			query = client.NewV8BoolQuery().MustNot(client.NewV8MatchQuery(colName, colValues[0]))
		}
	case sqlparser.InStr:
		query = client.NewV8TermsQuery(colName, colValues...)
	case sqlparser.NotInStr:
		query = client.NewV8BoolQuery().MustNot(client.NewV8TermsQuery(colName, colValues...))
	case sqlparser.StartsWithStr:
		v, ok := colValues[0].(string)
		if !ok {
			return nil, NewConverterError("right-hand side of '%v' must be a string", comparisonExpr.Operator)
		}
		query = client.NewV8PrefixQuery(colName, v)
	case sqlparser.NotStartsWithStr:
		v, ok := colValues[0].(string)
		if !ok {
			return nil, NewConverterError("right-hand side of '%v' must be a string", comparisonExpr.Operator)
		}
		query = client.NewV8BoolQuery().MustNot(client.NewV8PrefixQuery(colName, v))
	}

	return query, nil
}

// convertComparisonExprValue returns a string, int64, float64, bool or
// a slice with each value of one of those types.
func convertComparisonExprValue(expr sqlparser.Expr) (interface{}, error) {
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		v, err := sqlquery.ParseValue(sqlparser.String(e))
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
		return nil, NewConverterError(
			"%s: column name on the right side of comparison expression (did you forget to quote %q?)",
			NotSupportedErrMessage,
			sqlparser.String(expr),
		)
	default:
		return nil, NewConverterError("%s: unexpected value type %T", InvalidExpressionErrMessage, expr)
	}
}

func (n *notSupportedExprConverter) Convert(expr sqlparser.Expr) (client.Query, error) {
	return nil, NewConverterError("%s: expression of type %T", NotSupportedErrMessage, expr)
}

func convertColName(fnInterceptor FieldNameInterceptor, colNameExpr sqlparser.Expr, usage FieldNameUsage) (alias string, fieldName string, err error) {
	colName, isColName := colNameExpr.(*sqlparser.ColName)
	if !isColName {
		return "", "", NewConverterError("%s: must be a column name but was %T", InvalidExpressionErrMessage, colNameExpr)
	}

	colNameStr := sqlparser.String(colName)
	colNameStr = strings.ReplaceAll(colNameStr, "`", "")
	fieldName, err = fnInterceptor.Name(colNameStr, usage)
	if err != nil {
		return "", "", err
	}

	return colNameStr, fieldName, nil
}
