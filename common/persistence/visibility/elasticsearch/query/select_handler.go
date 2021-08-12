package query

import (
	"errors"
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
		// between a and b
		colName, ok := e.Left.(*sqlparser.ColName)
		if !ok {
			return nil, errors.New("range column name is missing")
		}

		colNameStr := sanitizeColName(sqlparser.String(colName))
		fromStr := sanitizeColValue(sqlparser.String(e.From))
		toStr := sanitizeColValue(sqlparser.String(e.To))

		return elastic.NewRangeQuery(colNameStr).Gte(fromStr).Lte(toStr), nil
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

func handleSelectWhereComparisonExpr(expr *sqlparser.ComparisonExpr) (elastic.Query, error) {
	colName, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return nil, errors.New("invalid comparison expression, the left must be a column name")
	}

	colNameStr := sqlparser.String(colName)
	colNameStr = sanitizeColName(colNameStr)
	rightStr, missingCheck, err := buildComparisonExprRightStr(expr.Right)
	if err != nil {
		return nil, err
	}

	var query elastic.Query
	switch expr.Operator {
	case ">=":
		query = elastic.NewRangeQuery(colNameStr).Gte(rightStr)
	case "<=":
		query = elastic.NewRangeQuery(colNameStr).Lte(rightStr)
	case "=":
		// field is missing
		if missingCheck {
			query = elastic.NewBoolQuery().MustNot(elastic.NewExistsQuery(colNameStr))
		} else {
			query = elastic.NewMatchPhraseQuery(colNameStr, rightStr)
		}
	case ">":
		query = elastic.NewRangeQuery(colNameStr).Gt(rightStr)
	case "<":
		query = elastic.NewRangeQuery(colNameStr).Lt(rightStr)
	case "!=":
		if missingCheck {
			query = elastic.NewExistsQuery(colNameStr)
		} else {
			query = elastic.NewBoolQuery().MustNot(elastic.NewMatchPhraseQuery(colNameStr, rightStr))
		}
	case "in":
		// the default valTuple is ('1', '2', '3') like
		// so need to drop the () and replace ' to "
		rightStr = strings.Replace(rightStr, `'`, `"`, -1)
		rightStr = strings.Trim(rightStr, "()")
		rightStrs := strings.Split(rightStr, ",")
		for i := range rightStrs {
			rightStrs[i] = strings.Trim(rightStrs[i], " ")
		}
		query = elastic.NewTermQuery(colNameStr, rightStrs)
	case "like":
		rightStr = strings.Replace(rightStr, `%`, ``, -1)
		query = elastic.NewMatchPhraseQuery(colNameStr, rightStr)
	case "not like":
		rightStr = strings.Replace(rightStr, `%`, ``, -1)
		query = elastic.NewBoolQuery().MustNot(elastic.NewMatchPhraseQuery(colNameStr, rightStr))
	case "not in":
		// the default valTuple is ('1', '2', '3') like
		// so need to drop the () and replace ' to "
		rightStr = strings.Replace(rightStr, `'`, `"`, -1)
		rightStr = strings.Trim(rightStr, "()")
		rightStrs := strings.Split(rightStr, ",")
		for i := range rightStrs {
			rightStrs[i] = strings.Trim(rightStrs[i], " ")
		}
		query = elastic.NewBoolQuery().MustNot(elastic.NewTermQuery(colNameStr, rightStrs))
	}

	return query, nil
}

func buildComparisonExprRightStr(expr sqlparser.Expr) (string, bool, error) {
	switch expr.(type) {
	case *sqlparser.SQLVal:
		rightStr := sanitizeColValue(sqlparser.String(expr))
		return rightStr, false, nil
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
func sanitizeColValue(str string) string {
	return strings.Trim(str, `'`)
}
