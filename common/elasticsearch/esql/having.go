// Copyright (c) 2017 Uber Technologies, Inc.
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

package esql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) getAggHaving(having *sqlparser.Where) (string, []string, []string, []string, map[string]int, error) {
	var aggNameSlice, aggTargetSlice, aggTagSlice []string
	aggTagSet := make(map[string]int)
	var script string
	var err error
	if having != nil {
		script, err = e.convertHavingExpr(having.Expr, &aggNameSlice, &aggTargetSlice, &aggTagSlice, aggTagSet)
		if err != nil {
			return "", nil, nil, nil, nil, err
		}
	}
	return script, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet, nil
}

func (e *ESql) convertHavingExpr(expr sqlparser.Expr, aggNameSlice *[]string, aggTargetSlice *[]string,
	aggTagSlice *[]string, aggTagSet map[string]int) (string, error) {

	switch expr.(type) {
	case *sqlparser.ComparisonExpr:
		return e.convertHavingComparisionExpr(expr, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet)
	case *sqlparser.AndExpr:
		return e.convertHavingAndExpr(expr, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet)
	case *sqlparser.OrExpr:
		return e.convertHavingOrExpr(expr, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet)
	case *sqlparser.NotExpr:
		return e.convertHavingNotExpr(expr, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet)
	case *sqlparser.ParenExpr:
		return e.convertHavingParenExpr(expr, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet)
	case *sqlparser.RangeCond:
		return e.convertHavingBetweenExpr(expr, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet)
	default:
		err := fmt.Errorf(`esql: %T expression in HAVING no supported`, expr)
		return "", err
	}
}

func (e *ESql) convertHavingBetweenExpr(expr sqlparser.Expr, aggNameSlice *[]string, aggTargetSlice *[]string,
	aggTagSlice *[]string, aggTagSet map[string]int) (string, error) {

	rangeCond := expr.(*sqlparser.RangeCond)
	lhs := rangeCond.Left
	from, to := rangeCond.From, rangeCond.To
	var expr1 sqlparser.Expr = &sqlparser.ComparisonExpr{Left: lhs, Right: from, Operator: ">="}
	var expr2 sqlparser.Expr = &sqlparser.ComparisonExpr{Left: lhs, Right: to, Operator: "<="}
	var expr3 sqlparser.Expr = &sqlparser.AndExpr{Left: expr1, Right: expr2}

	script, err := e.convertHavingAndExpr(expr3, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet)
	if err != nil {
		return "", err
	}
	// here parenthesis is to deal with the case when an not(!) operator out side
	// if no parenthesis, NOT xxx BETWEEN a and b -> !xxx > a && xxx < b
	script = fmt.Sprintf(`(%v)`, script)
	return script, nil
}

func (e *ESql) convertHavingAndExpr(expr sqlparser.Expr, aggNameSlice *[]string, aggTargetSlice *[]string,
	aggTagSlice *[]string, aggTagSet map[string]int) (string, error) {

	andExpr := expr.(*sqlparser.AndExpr)
	leftExpr := andExpr.Left
	rightExpr := andExpr.Right
	scriptLeft, err := e.convertHavingExpr(leftExpr, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet)
	if err != nil {
		return "", err
	}
	scriptRight, err := e.convertHavingExpr(rightExpr, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`%v && %v`, scriptLeft, scriptRight), nil
}

func (e *ESql) convertHavingOrExpr(expr sqlparser.Expr, aggNameSlice *[]string, aggTargetSlice *[]string,
	aggTagSlice *[]string, aggTagSet map[string]int) (string, error) {

	orExpr := expr.(*sqlparser.OrExpr)
	leftExpr := orExpr.Left
	rightExpr := orExpr.Right
	scriptLeft, err := e.convertHavingExpr(leftExpr, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet)
	if err != nil {
		return "", err
	}
	scriptRight, err := e.convertHavingExpr(rightExpr, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`%v || %v`, scriptLeft, scriptRight), nil
}

func (e *ESql) convertHavingParenExpr(expr sqlparser.Expr, aggNameSlice *[]string, aggTargetSlice *[]string,
	aggTagSlice *[]string, aggTagSet map[string]int) (string, error) {

	parenExpr := expr.(*sqlparser.ParenExpr)
	script, err := e.convertHavingExpr(parenExpr.Expr, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`(%v)`, script), nil
}

func (e *ESql) convertHavingNotExpr(expr sqlparser.Expr, aggNameSlice *[]string, aggTargetSlice *[]string,
	aggTagSlice *[]string, aggTagSet map[string]int) (string, error) {

	notExpr := expr.(*sqlparser.NotExpr)
	script, err := e.convertHavingExpr(notExpr.Expr, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`!%v`, script), nil
}

func (e *ESql) convertHavingComparisionExpr(expr sqlparser.Expr, aggNameSlice *[]string, aggTargetSlice *[]string,
	aggTagSlice *[]string, aggTagSet map[string]int) (string, error) {

	comparisonExpr := expr.(*sqlparser.ComparisonExpr)
	var funcExprs []*sqlparser.FuncExpr

	if _, exist := op2PainlessOp[comparisonExpr.Operator]; !exist {
		err := fmt.Errorf(`esql: %s operator not supported in having comparison clause`, comparisonExpr.Operator)
		return "", err
	}
	// convert SQL operator format to equivalent painless operator
	op := op2PainlessOp[comparisonExpr.Operator]

	// lhs
	leftFuncExpr, ok := comparisonExpr.Left.(*sqlparser.FuncExpr)
	if !ok {
		err := fmt.Errorf("esql: found %v in HAVING which is not aggregation function", sqlparser.String(comparisonExpr.Left))
		return "", err
	}
	funcExprs = append(funcExprs, leftFuncExpr)

	// rhs, can be a value or an aggregation function
	var rhsStr, script string
	switch comparisonExpr.Right.(type) {
	case *sqlparser.SQLVal:
		rhsStr = sqlparser.String(comparisonExpr.Right)
		rhsStr = strings.Trim(rhsStr, `'`)
	case *sqlparser.FuncExpr:
		rightFuncExpr := comparisonExpr.Right.(*sqlparser.FuncExpr)
		funcExprs = append(funcExprs, rightFuncExpr)
	default:
		err := fmt.Errorf("esql: %T in HAVING rhs not supported", comparisonExpr.Right)
		return "", err
	}

	for _, funcExpr := range funcExprs {
		aggNameStr := strings.ToLower(funcExpr.Name.String())
		aggTargetStr := sqlparser.String(funcExpr.Exprs)
		aggTargetStr = strings.Trim(aggTargetStr, "`")
		aggTargetStr, err := e.keyProcess(aggTargetStr)
		if err != nil {
			return "", err
		}

		var aggTagStr string
		switch aggNameStr {
		case "count":
			if aggTargetStr == "*" {
				aggTagStr = "_count"
			} else if funcExpr.Distinct {
				aggTagStr = aggNameStr + "_distinct_" + aggTargetStr
				aggNameStr = "cardinality"
			} else {
				aggTagStr = aggNameStr + "_" + aggTargetStr
				aggNameStr = "value_count"
			}
		case "avg", "sum", "min", "max":
			if funcExpr.Distinct {
				err := fmt.Errorf(`esql: HAVING: aggregation function %v w/ DISTINCT not supported`, aggNameStr)
				return "", err
			}
			aggTagStr = aggNameStr + "_" + aggTargetStr
		default:
			err := fmt.Errorf(`esql: HAVING: aggregation function %v not supported`, aggNameStr)
			return "", err
		}
		aggTagStr = strings.Replace(aggTagStr, ".", "_", -1)
		aggTagSet[aggTagStr] = len(*aggNameSlice)
		*aggNameSlice = append(*aggNameSlice, aggNameStr)
		*aggTargetSlice = append(*aggTargetSlice, aggTargetStr)
		*aggTagSlice = append(*aggTagSlice, aggTagStr)
	}

	n := len(*aggTagSlice)
	if rhsStr == "" {
		script = fmt.Sprintf(`params.%v %v params.%v`, (*aggTagSlice)[n-2], op, (*aggTagSlice)[n-1])
	} else {
		script = fmt.Sprintf(`params.%v %v %v`, (*aggTagSlice)[n-1], op, rhsStr)
	}
	return script, nil
}
