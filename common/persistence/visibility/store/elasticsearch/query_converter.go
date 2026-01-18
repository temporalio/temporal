package elasticsearch

import (
	"strings"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

type queryConverter struct{}

var _ query.StoreQueryConverter[elastic.Query] = (*queryConverter)(nil)

func (c *queryConverter) GetDatetimeFormat() string {
	return time.RFC3339Nano
}

func (c *queryConverter) BuildParenExpr(expr elastic.Query) (elastic.Query, error) {
	if expr == nil {
		return nil, nil
	}
	return expr, nil
}

func (c *queryConverter) BuildNotExpr(expr elastic.Query) (elastic.Query, error) {
	if expr == nil {
		return nil, nil
	}
	if bq, ok := expr.(*boolQuery); ok && len(bq.shouldClauses) > 0 {
		// !(a || b) == !a && !b
		ret := newBoolQuery()
		ret.mustNotClauses = bq.shouldClauses
		return ret, nil
	}
	return newBoolQuery().MustNot(expr), nil
}

func (c *queryConverter) BuildAndExpr(exprs ...elastic.Query) (elastic.Query, error) {
	var reusableBoolQuery *boolQuery
	validExprs := make([]elastic.Query, 0, len(exprs))
	for _, e := range exprs {
		if e == nil {
			continue
		}
		if bq, ok := e.(*boolQuery); !ok || len(bq.filterClauses)+len(bq.mustNotClauses) == 0 {
			validExprs = append(validExprs, e)
		} else if reusableBoolQuery == nil {
			reusableBoolQuery = bq
		} else {
			reusableBoolQuery.Filter(bq.filterClauses...).MustNot(bq.mustNotClauses...)
		}
	}
	if reusableBoolQuery != nil {
		reusableBoolQuery.Filter(validExprs...)
		return reusableBoolQuery, nil
	}
	if len(validExprs) == 0 {
		return nil, nil
	}
	if len(validExprs) == 1 {
		return validExprs[0], nil
	}
	return newBoolQuery().Filter(validExprs...), nil
}

func (c *queryConverter) BuildOrExpr(exprs ...elastic.Query) (elastic.Query, error) {
	var reusableBoolQuery *boolQuery
	validExprs := make([]elastic.Query, 0, len(exprs))
	for _, e := range exprs {
		if e == nil {
			continue
		}
		if bq, ok := e.(*boolQuery); !ok || len(bq.shouldClauses) == 0 {
			validExprs = append(validExprs, e)
		} else if reusableBoolQuery == nil {
			reusableBoolQuery = bq
		} else {
			reusableBoolQuery.Should(bq.shouldClauses...)
		}
	}
	if reusableBoolQuery != nil {
		reusableBoolQuery.Should(validExprs...)
		return reusableBoolQuery, nil
	}
	if len(validExprs) == 0 {
		return nil, nil
	}
	if len(validExprs) == 1 {
		return validExprs[0], nil
	}
	return newBoolQuery().Should(validExprs...).MinimumNumberShouldMatch(1), nil
}

func (c *queryConverter) ConvertComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (elastic.Query, error) {
	var res elastic.Query
	negate := false
	colName := col.FieldName
	switch operator {
	case sqlparser.GreaterEqualStr:
		res = elastic.NewRangeQuery(colName).Gte(value)
	case sqlparser.LessEqualStr:
		res = elastic.NewRangeQuery(colName).Lte(value)
	case sqlparser.GreaterThanStr:
		res = elastic.NewRangeQuery(colName).Gt(value)
	case sqlparser.LessThanStr:
		res = elastic.NewRangeQuery(colName).Lt(value)
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		res = elastic.NewTermQuery(colName, value)
		negate = operator == sqlparser.NotEqualStr
	case sqlparser.InStr, sqlparser.NotInStr:
		res = elastic.NewTermsQuery(colName, value.([]any)...)
		negate = operator == sqlparser.NotInStr
	default:
		return nil, query.NewOperatorNotSupportedError(col.Alias, col.ValueType, operator)
	}

	if negate {
		res, _ = c.BuildNotExpr(res)
	}
	return res, nil
}

func (c *queryConverter) ConvertKeywordComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (elastic.Query, error) {
	colName := col.FieldName
	switch operator {
	case sqlparser.StartsWithStr, sqlparser.NotStartsWithStr:
		v, ok := value.(string)
		if !ok {
			return nil, query.NewConverterError(
				"%s: right-hand side of operator '%s' must be a string",
				query.InvalidExpressionErrMessage,
				strings.ToUpper(operator),
			)
		}
		var res elastic.Query = elastic.NewPrefixQuery(colName, v)
		if operator == sqlparser.NotStartsWithStr {
			res, _ = c.BuildNotExpr(res)
		}
		return res, nil
	default:
		return c.ConvertComparisonExpr(operator, col, value)
	}
}

func (c *queryConverter) ConvertKeywordListComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (elastic.Query, error) {
	return c.ConvertKeywordComparisonExpr(operator, col, value)
}

func (c *queryConverter) ConvertTextComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (elastic.Query, error) {
	colName := col.FieldName
	switch operator {
	case sqlparser.EqualStr:
		return elastic.NewMatchQuery(colName, value), nil
	case sqlparser.NotEqualStr:
		return newBoolQuery().MustNot(elastic.NewMatchQuery(colName, value)), nil
	default:
		return nil, query.NewOperatorNotSupportedError(col.Alias, col.ValueType, operator)
	}
}

func (c *queryConverter) ConvertRangeExpr(
	operator string,
	col *query.SAColumn,
	from, to any,
) (elastic.Query, error) {
	colName := col.FieldName
	switch operator {
	case sqlparser.BetweenStr:
		return elastic.NewRangeQuery(colName).Gte(from).Lte(to), nil
	case sqlparser.NotBetweenStr:
		return newBoolQuery().MustNot(elastic.NewRangeQuery(colName).Gte(from).Lte(to)), nil
	default:
		// This should be impossible since the query parser only calls this function with one of those
		// operators strings.
		return nil, query.NewConverterError(
			"%s: unexpected operator '%s' for range condition",
			query.MalformedSqlQueryErrMessage,
			strings.ToUpper(operator),
		)
	}
}

func (c *queryConverter) ConvertIsExpr(
	operator string,
	col *query.SAColumn,
) (elastic.Query, error) {
	colName := col.FieldName
	switch operator {
	case sqlparser.IsNullStr:
		return newBoolQuery().MustNot(elastic.NewExistsQuery(colName)), nil
	case sqlparser.IsNotNullStr:
		return elastic.NewExistsQuery(colName), nil
	default:
		// This should be impossible since the query parser only calls this function with one of those
		// operators strings.
		return nil, query.NewConverterError(
			"%s: 'IS' operator can only be used as 'IS NULL' or 'IS NOT NULL'",
			query.InvalidExpressionErrMessage,
		)
	}
}
