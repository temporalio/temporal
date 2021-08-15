package query

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/olivere/elastic/v7"
	"github.com/xwb1989/sqlparser"
)

func ConvertWhereOrderBy(whereOrderBy string) (elastic.Query, []elastic.Sorter, error) {
	whereOrderBy = strings.Trim(whereOrderBy, " ")
	if whereOrderBy != "" && !strings.HasPrefix(whereOrderBy, "order by ") {
		whereOrderBy = "where " + whereOrderBy
	}
	sql := fmt.Sprintf("select * from table1 %s", whereOrderBy)
	return convertSql(sql)
}

// convertSql transforms sql to Elasticsearch query.
func convertSql(sql string) (elastic.Query, []elastic.Sorter, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", MalformedSqlQueryErr, err)
	}

	selectStmt, isSelect := stmt.(*sqlparser.Select)
	if !isSelect {
		return nil, nil, fmt.Errorf("%w: statement must be 'select' not %T", NotSupportedErr, stmt)
	}

	return convertSelect(selectStmt)
}

func convertSelect(sel *sqlparser.Select) (elastic.Query, []elastic.Sorter, error) {
	var (
		query elastic.Query
		err   error
	)

	if sel.GroupBy != nil {
		return nil, nil, fmt.Errorf("%w: 'group by' clause", NotSupportedErr)
	}

	if sel.Where != nil {
		query, err = convertSelectWhere(sel.Where.Expr)
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

func convertSelectWhere(expr sqlparser.Expr) (elastic.Query, error) {
	if expr == nil {
		return nil, errors.New("'where' expression cannot be nil")
	}

	switch e := (expr).(type) {
	case *sqlparser.AndExpr:
		return convertSelectWhereAndExpr(e)
	case *sqlparser.OrExpr:
		return convertSelectWhereOrExpr(e)
	case *sqlparser.ComparisonExpr:
		return convertSelectWhereComparisonExpr(e)
	case *sqlparser.RangeCond:
		return convertSelectWhereRangeCondExpr(e)
	case *sqlparser.ParenExpr:
		return convertSelectWhere(e.Expr)
	case *sqlparser.IsExpr:
		return convertSelectIsExpr(e)
	case *sqlparser.NotExpr:
		return nil, fmt.Errorf("%w: 'not' expression", NotSupportedErr)
	case *sqlparser.FuncExpr:
		return nil, fmt.Errorf("%w: function expression", NotSupportedErr)
	default:
		return nil, fmt.Errorf("%w: expression of type %T", NotSupportedErr, expr)
	}
}

func convertSelectWhereAndExpr(expr *sqlparser.AndExpr) (elastic.Query, error) {
	leftExpr := expr.Left
	rightExpr := expr.Right
	leftQuery, err := convertSelectWhere(leftExpr)
	if err != nil {
		return nil, err
	}
	rightQuery, err := convertSelectWhere(rightExpr)
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

func convertSelectWhereOrExpr(expr *sqlparser.OrExpr) (elastic.Query, error) {
	leftExpr := expr.Left
	rightExpr := expr.Right
	leftQuery, err := convertSelectWhere(leftExpr)
	if err != nil {
		return nil, err
	}
	rightQuery, err := convertSelectWhere(rightExpr)
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

func convertSelectWhereRangeCondExpr(expr *sqlparser.RangeCond) (elastic.Query, error) {
	colName, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("%w: left part of 'range condition' expression must be a column name", InvalidExpressionErr)
	}

	colNameStr := sanitizeColName(sqlparser.String(colName))
	fromValue, err := parseSqlValue(sqlparser.String(expr.From))
	if err != nil {
		return nil, err
	}
	toValue, err := parseSqlValue(sqlparser.String(expr.To))
	if err != nil {
		return nil, err
	}

	var query elastic.Query
	switch expr.Operator {
	case "between":
		query = elastic.NewRangeQuery(colNameStr).Gte(fromValue).Lte(toValue)
	case "not between":
		query = elastic.NewBoolQuery().MustNot(elastic.NewRangeQuery(colNameStr).Gte(fromValue).Lte(toValue))
	default:
		return nil, fmt.Errorf("%w: range operator must be 'between' or 'not between'", InvalidExpressionErr)
	}
	return query, nil
}

func convertSelectIsExpr(expr *sqlparser.IsExpr) (elastic.Query, error) {
	colName, ok := expr.Expr.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("%w: left part of 'is' expression must be a column name", InvalidExpressionErr)
	}

	colNameStr := sanitizeColName(sqlparser.String(colName))
	var query elastic.Query
	switch expr.Operator {
	case "is null":
		query = elastic.NewBoolQuery().MustNot(elastic.NewExistsQuery(colNameStr))
	case "is not null":
		query = elastic.NewExistsQuery(colNameStr)
	default:
		return nil, fmt.Errorf("%w: 'is' operator can be used with 'null' and 'not null' only", InvalidExpressionErr)
	}

	return query, nil
}

func convertSelectWhereComparisonExpr(expr *sqlparser.ComparisonExpr) (elastic.Query, error) {
	colName, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("%w: left part of comparison expression must be a column name", InvalidExpressionErr)
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
			return nil, fmt.Errorf("%w: 'in' operator value must be a string but was %T", InvalidExpressionErr, colValue)
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
			return nil, fmt.Errorf("%w: 'not in' operator value must be a string but was %T", InvalidExpressionErr, colValue)
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
			return nil, fmt.Errorf("%w: 'like' operator value must be a string but was %T", InvalidExpressionErr, colValue)
		}
		colValue = strings.Replace(colValueStr, "%", "", -1)
		query = elastic.NewMatchPhraseQuery(colNameStr, colValue)
	case "not like":
		colValueStr, isString := colValue.(string)
		if !isString {
			return nil, fmt.Errorf("%w: 'not like' operator value must be a string but was %T", InvalidExpressionErr, colValue)
		}
		colValue = strings.Replace(colValueStr, "%", "", -1)
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
		return nil, fmt.Errorf("%w: unable to parse %s to float64: %v", InvalidExpressionErr, sqlValue, err)
	}

	return floatValue, nil
}

func sanitizeColName(str string) string {
	return strings.Replace(str, "`", "", -1)
}
