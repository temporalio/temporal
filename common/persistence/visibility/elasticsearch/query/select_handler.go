package query

import (
	"errors"
	"fmt"
	"strings"

	"github.com/olivere/elastic/v7"
	"github.com/xwb1989/sqlparser"
)

func sanitizeColName(str string) string {
	return strings.Replace(str, "`", "", -1)
}
func sanitizeColValue(str string) string {
	return strings.Trim(str, `'`)
}

func handleSelect(sel *sqlparser.Select) (elastic.Query, []elastic.Sorter, error) {
	var (
		rootParent sqlparser.Expr
		query      elastic.Query
		err        error
	)

	if sel.Where != nil {
		// top level node pass in an empty interface
		// to tell the children this is root
		// is there any better way?
		query, err = handleSelectWhere(sel.Where.Expr, true, rootParent)
		if err != nil {
			return nil, nil, err
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

func handleSelectWhere(expr sqlparser.Expr, topLevel bool, parent sqlparser.Expr) (elastic.Query, error) {
	if expr == nil {
		return nil, errors.New("expression cannot be nil")
	}

	switch e := (expr).(type) {
	case *sqlparser.AndExpr:
		return handleSelectWhereAndExpr(e, topLevel, parent)
	case *sqlparser.OrExpr:
		return handleSelectWhereOrExpr(e, topLevel, parent)
	case *sqlparser.ComparisonExpr:
		return handleSelectWhereComparisonExpr(e, topLevel, parent)
	case *sqlparser.RangeCond:
		// between a and b
		colName, ok := e.Left.(*sqlparser.ColName)

		if !ok {
			return nil, errors.New("range column name is missing")
		}

		colNameStr := sanitizeColName(sqlparser.String(colName))
		fromStr := sanitizeColValue(sqlparser.String(e.From))
		toStr := sanitizeColValue(sqlparser.String(e.To))

		rangeQuery := elastic.NewRangeQuery(colNameStr).Gte(fromStr).Lte(toStr)
		return rangeQuery, nil
	case *sqlparser.ParenExpr:
		boolExpr := e.Expr
		return handleSelectWhere(boolExpr, topLevel, parent)
	case *sqlparser.IsExpr:
		return nil, errors.New("'is' expression is not supported")
	case *sqlparser.NotExpr:
		return nil, errors.New("'not' expression is not supported")
	default:
		return nil, errors.New("function is not supported")
	}
}

func handleSelectWhereAndExpr(expr *sqlparser.AndExpr, topLevel bool, parent sqlparser.Expr) (elastic.Query, error) {
	leftExpr := expr.Left
	rightExpr := expr.Right
	leftQuery, err := handleSelectWhere(leftExpr, false, expr)
	if err != nil {
		return nil, err
	}
	rightQuery, err := handleSelectWhere(rightExpr, false, expr)
	if err != nil {
		return nil, err
	}

	// not toplevel
	// if the parent node is also and, then the result can be merged

	var resultStr string
	if leftStr == "" || rightStr == "" {
		resultStr = leftStr + rightStr
	} else {
		resultStr = leftStr + `,` + rightStr
	}

	if _, ok := (*parent).(*sqlparser.AndExpr); ok {
		return resultStr, nil
	}
	return fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr), nil

	elastic.NewBoolQuery().

	query := elastic.NewBoolQuery().Filter(leftQuery, rightQuery)
	return query, nil
}

func handleSelectWhereOrExpr(expr *sqlparser.OrExpr, topLevel bool, parent sqlparser.Expr) (elastic.Query, error) {
	leftExpr := expr.Left
	rightExpr := expr.Right
	leftQuery, err := handleSelectWhere(leftExpr, false, expr)
	if err != nil {
		return "", err
	}
	rightQuery, err := handleSelectWhere(rightExpr, false, expr)
	if err != nil {
		return "", err
	}

	query := elastic.NewBoolQuery().Filter().Should(leftQuery, rightQuery)
	return query, nil

	var resultStr string
	if leftStr == "" || rightStr == "" {
		resultStr = leftStr + rightStr
	} else {
		resultStr = leftStr + `,` + rightStr
	}

	// not toplevel
	// if the parent node is also or node, then merge the query param
	if _, ok := (*parent).(*sqlparser.OrExpr); ok {
		return resultStr, nil
	}

	return fmt.Sprintf(`{"bool" : {"should" : [%v]}}`, resultStr), nil
}

func handleSelectWhereComparisonExpr(expr *sqlparser.ComparisonExpr, topLevel bool, parent sqlparser.Expr) (elastic.Query, error) {
	colName, ok := expr.Left.(*sqlparser.ColName)

	if !ok {
		return nil, errors.New("elasticsql: invalid comparison expression, the left must be a column name")
	}

	colNameStr := sqlparser.String(colName)
	colNameStr = strings.Replace(colNameStr, "`", "", -1)
	rightStr, missingCheck, err := buildComparisonExprRightStr(expr.Right)
	if err != nil {
		return nil, err
	}

	resultStr := ""

	switch expr.Operator {
	case ">=":
		resultStr = fmt.Sprintf(`{"range" : {"%v" : {"from" : "%v"}}}`, colNameStr, rightStr)
	case "<=":
		resultStr = fmt.Sprintf(`{"range" : {"%v" : {"to" : "%v"}}}`, colNameStr, rightStr)
	case "=":
		// field is missing
		if missingCheck {
			resultStr = fmt.Sprintf(`{"missing":{"field":"%v"}}`, colNameStr)
		} else {
			resultStr = fmt.Sprintf(`{"match_phrase" : {"%v" : {"query" : "%v"}}}`, colNameStr, rightStr)
		}
	case ">":
		resultStr = fmt.Sprintf(`{"range" : {"%v" : {"gt" : "%v"}}}`, colNameStr, rightStr)
	case "<":
		resultStr = fmt.Sprintf(`{"range" : {"%v" : {"lt" : "%v"}}}`, colNameStr, rightStr)
	case "!=":
		if missingCheck {
			resultStr = fmt.Sprintf(`{"bool" : {"must_not" : [{"missing":{"field":"%v"}}]}}`, colNameStr)
		} else {
			resultStr = fmt.Sprintf(`{"bool" : {"must_not" : [{"match_phrase" : {"%v" : {"query" : "%v"}}}]}}`, colNameStr, rightStr)
		}
	case "in":
		// the default valTuple is ('1', '2', '3') like
		// so need to drop the () and replace ' to "
		rightStr = strings.Replace(rightStr, `'`, `"`, -1)
		rightStr = strings.Trim(rightStr, "(")
		rightStr = strings.Trim(rightStr, ")")
		resultStr = fmt.Sprintf(`{"terms" : {"%v" : [%v]}}`, colNameStr, rightStr)
	case "like":
		rightStr = strings.Replace(rightStr, `%`, ``, -1)
		resultStr = fmt.Sprintf(`{"match_phrase" : {"%v" : {"query" : "%v"}}}`, colNameStr, rightStr)
	case "not like":
		rightStr = strings.Replace(rightStr, `%`, ``, -1)
		resultStr = fmt.Sprintf(`{"bool" : {"must_not" : {"match_phrase" : {"%v" : {"query" : "%v"}}}}}`, colNameStr, rightStr)
	case "not in":
		// the default valTuple is ('1', '2', '3') like
		// so need to drop the () and replace ' to "
		rightStr = strings.Replace(rightStr, `'`, `"`, -1)
		rightStr = strings.Trim(rightStr, "(")
		rightStr = strings.Trim(rightStr, ")")
		resultStr = fmt.Sprintf(`{"bool" : {"must_not" : {"terms" : {"%v" : [%v]}}}}`, colNameStr, rightStr)
	}

	// the root node need to have bool and must
	if topLevel {
		resultStr = fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr)
	}

	return resultStr, nil
}

func buildComparisonExprRightStr(expr sqlparser.Expr) (string, bool, error) {
	var rightStr string
	var err error
	var missingCheck = false
	switch expr.(type) {
	case *sqlparser.SQLVal:
		rightStr = sqlparser.String(expr)
		rightStr = strings.Trim(rightStr, `'`)
	case *sqlparser.GroupConcatExpr:
		return "", missingCheck, errors.New("elasticsql: group_concat not supported")
	case *sqlparser.FuncExpr:
		// parse nested
		funcExpr := expr.(*sqlparser.FuncExpr)
		rightStr, err = buildNestedFuncStrValue(funcExpr)
		if err != nil {
			return "", missingCheck, err
		}
	case *sqlparser.ColName:
		if sqlparser.String(expr) == "missing" {
			missingCheck = true
			return "", missingCheck, nil
		}

		return "", missingCheck, errors.New("elasticsql: column name on the right side of compare operator is not supported")
	case sqlparser.ValTuple:
		rightStr = sqlparser.String(expr)
	default:
		// cannot reach here
	}
	return rightStr, missingCheck, err
}

func buildNestedFuncStrValue(nestedFunc *sqlparser.FuncExpr) (string, error) {
	return "", errors.New("elasticsql: unsupported function" + nestedFunc.Name.String())
}
