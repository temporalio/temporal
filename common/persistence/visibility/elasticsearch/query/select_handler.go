package query

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/olivere/elastic/v7"
	"github.com/xwb1989/sqlparser"
)

func handleSelect(sel *sqlparser.Select) (elastic.Query, []elastic.Sorter, error) {
	var (
		query elastic.Query
		err   error
	)

	if sel.Where != nil {
		query, err = handleSelectWhere(sel.Where.Expr)
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
		sortField := elastic.NewFieldSort(sanitizeColName(sqlparser.String(orderByExpr.Expr)))
		if orderByExpr.Direction == sqlparser.DescScr {
			sortField = sortField.Desc()
		}
		sorter = append(sorter, sortField)
	}

	return query, sorter, nil
}

func handleSelectWhere(expr sqlparser.Expr) (elastic.Query, error) {
	if expr == nil {
		return nil, errors.New("expression cannot be nil")
	}

	switch e := (expr).(type) {
	case *sqlparser.AndExpr:
		return handleSelectWhereAndExpr(e)
	case *sqlparser.OrExpr:
		return handleSelectWhereOrExpr(e)
	case *sqlparser.ComparisonExpr:
		return handleSelectWhereComparisonExpr(e)
	case *sqlparser.RangeCond:
		return handleSelectWhereRangeCondExpr(e)
	case *sqlparser.ParenExpr:
		return handleSelectWhere(e.Expr)
	case *sqlparser.IsExpr:
		return nil, errors.New("'is' expression is not supported")
	case *sqlparser.NotExpr:
		return nil, errors.New("'not' expression is not supported")
	default:
		return nil, errors.New("function is not supported")
	}
}

func handleSelectWhereAndExpr(expr *sqlparser.AndExpr) (elastic.Query, error) {
	leftExpr := expr.Left
	rightExpr := expr.Right
	leftQuery, err := handleSelectWhere(leftExpr)
	if err != nil {
		return nil, err
	}
	rightQuery, err := handleSelectWhere(rightExpr)
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

func handleSelectWhereOrExpr(expr *sqlparser.OrExpr) (elastic.Query, error) {
	leftExpr := expr.Left
	rightExpr := expr.Right
	leftQuery, err := handleSelectWhere(leftExpr)
	if err != nil {
		return nil, err
	}
	rightQuery, err := handleSelectWhere(rightExpr)
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

func handleSelectWhereRangeCondExpr(e *sqlparser.RangeCond) (elastic.Query, error) {
	// between a and b
	colName, ok := e.Left.(*sqlparser.ColName)
	if !ok {
		return nil, errors.New("range column name is missing")
	}

	colNameStr := sanitizeColName(sqlparser.String(colName))
	fromValue, err := parseSqlValue(sqlparser.String(e.From))
	if err != nil {
		return nil, err
	}
	toValue, err := parseSqlValue(sqlparser.String(e.To))
	if err != nil {
		return nil, err
	}
	return elastic.NewRangeQuery(colNameStr).Gte(fromValue).Lte(toValue), nil
}

func handleSelectWhereComparisonExpr(expr *sqlparser.ComparisonExpr) (elastic.Query, error) {
	colName, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return nil, errors.New("invalid comparison expression, the left must be a column name")
	}

	colNameStr := sanitizeColName(sqlparser.String(colName))
	colValue, missingCheck, err := parseComparisonExprValue(expr.Right)
	if err != nil {
		return nil, err
	}

	var query elastic.Query
	switch expr.Operator {
	case ">=":
		query = elastic.NewRangeQuery(colNameStr).Gte(colValue)
	case "<=":
		query = elastic.NewRangeQuery(colNameStr).Lte(colValue)
	case "=":
		// field is missing
		if missingCheck {
			query = elastic.NewBoolQuery().MustNot(elastic.NewExistsQuery(colNameStr))
		} else {
			query = elastic.NewMatchPhraseQuery(colNameStr, colValue)
		}
	case ">":
		query = elastic.NewRangeQuery(colNameStr).Gt(colValue)
	case "<":
		query = elastic.NewRangeQuery(colNameStr).Lt(colValue)
	case "!=":
		if missingCheck {
			query = elastic.NewExistsQuery(colNameStr)
		} else {
			query = elastic.NewBoolQuery().MustNot(elastic.NewMatchPhraseQuery(colNameStr, colValue))
		}
	case "in":
		colValueStr, isString := colValue.(string)
		if !isString {
			return nil, fmt.Errorf("'in' operator value must be a string but was %T", colValue)
		}
		// colValueStr is a string like ('1', '2', '3').
		parsedValues, err := parseSqlRange(colValueStr)
		if err != nil {
			return nil, err
		}
		query = elastic.NewTermsQuery(colNameStr, parsedValues...)
	case "not in":
		colValueStr, isString := colValue.(string)
		if !isString {
			return nil, fmt.Errorf("'not in' operator value must be a string but was %T", colValue)
		}
		// colValue is a string like ('1', '2', '3').
		parsedValues, err := parseSqlRange(colValueStr)
		if err != nil {
			return nil, err
		}
		query = elastic.NewBoolQuery().MustNot(elastic.NewTermsQuery(colNameStr, parsedValues...))
	case "like":
		colValueStr, isString := colValue.(string)
		if !isString {
			return nil, fmt.Errorf("'like' operator value must be a string but was %T", colValue)
		}
		colValue = strings.Replace(colValueStr, `%`, ``, -1)
		query = elastic.NewMatchPhraseQuery(colNameStr, colValue)
	case "not like":
		colValueStr, isString := colValue.(string)
		if !isString {
			return nil, fmt.Errorf("'not like' operator value must be a string but was %T", colValue)
		}
		colValue = strings.Replace(colValueStr, `%`, ``, -1)
		query = elastic.NewBoolQuery().MustNot(elastic.NewMatchPhraseQuery(colNameStr, colValue))
	}

	return query, nil
}

func parseComparisonExprValue(expr sqlparser.Expr) (interface{}, bool, error) {
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		sqlValue, err := parseSqlValue(sqlparser.String(e))
		return sqlValue, false, err
	case *sqlparser.GroupConcatExpr:
		return "", false, errors.New("group_concat not supported")
	case *sqlparser.FuncExpr:
		return "", false, errors.New("nested func not supported")
	case *sqlparser.ColName:
		if sqlparser.String(expr) == "missing" {
			return "", true, nil
		}
		return "", false, errors.New("column name on the right side of compare operator is not supported")
	case sqlparser.ValTuple:
		return sqlparser.String(expr), false, nil
	default:
		// cannot reach here
		return "", false, errors.New("unexpected type")
	}
}

func sanitizeColName(str string) string {
	return strings.Replace(str, "`", "", -1)
}

// parseSqlRange parses strings like "('1', '2', '3')" which comes from sql parser.
func parseSqlRange(sqlRange string) ([]interface{}, error) {
	sqlRange = strings.Trim(sqlRange, "()")
	var values []interface{}
	for _, v := range strings.Split(sqlRange, ", ") {
		parsedValue, err := parseSqlValue(v)
		if err != nil {
			return nil, err
		}
		values = append(values, parsedValue)
	}
	return values, nil
}

func parseSqlValue(sqlValue string) (interface{}, error) {
	if sqlValue == "" {
		return "", nil
	}

	if sqlValue[0] == '\'' && sqlValue[len(sqlValue)-1] == '\'' {
		return strings.Trim(sqlValue, "'"), nil
	}

	floatValue, err := strconv.ParseFloat(sqlValue, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse %s to float64: %w", sqlValue, err)
	}

	return floatValue, nil
}
