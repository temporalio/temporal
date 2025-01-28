// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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

package matcher

import (
	"fmt"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/workflow"
)

// Supported Fields
const (
	workflowIDColName              = searchattribute.WorkflowID
	workflowTypeNameColName        = searchattribute.WorkflowType
	workflowStartTimeColName       = searchattribute.StartTime
	workflowExecutionStatusColName = searchattribute.ExecutionStatus
)

type MutableStateMatchEvaluator struct {
	ms workflow.MutableState
}

func newMutableStateMatchEvaluator(ms workflow.MutableState) *MutableStateMatchEvaluator {
	return &MutableStateMatchEvaluator{ms: ms}
}

func (m *MutableStateMatchEvaluator) Evaluate(query string) (bool, error) {
	query, err := prepareQuery(query)
	if err != nil {
		return false, err
	}

	whereCause, err := getWhereCause(query)
	if err != nil {
		return false, err
	}

	return m.evaluateExpression(whereCause)
}

func (m *MutableStateMatchEvaluator) evaluateExpression(expr sqlparser.Expr) (bool, error) {

	if expr == nil {
		return false, NewMatcherError("input expression cannot be nil")
	}

	switch e := (expr).(type) {
	case *sqlparser.AndExpr:
		return m.evaluateAnd(e)
	case *sqlparser.OrExpr:
		return m.evaluateOr(e)
	case *sqlparser.ComparisonExpr:
		return m.evaluateComparison(e)
	case *sqlparser.RangeCond:
		return m.evaluateRange(e)
	case *sqlparser.ParenExpr:
		return m.evaluateExpression(e.Expr)
	case *sqlparser.IsExpr:
		return false, NewMatcherError("%s: 'is' expression", notSupportedErrMessage)
	case *sqlparser.NotExpr:
		return false, NewMatcherError("%s: 'not' expression", notSupportedErrMessage)
	case *sqlparser.FuncExpr:
		return false, NewMatcherError("%s: function expression", notSupportedErrMessage)
	case *sqlparser.ColName:
		return false, NewMatcherError("incomplete expression")
	default:
		return false, NewMatcherError("%s: expression of type %T", notSupportedErrMessage, expr)
	}
}
func (m *MutableStateMatchEvaluator) evaluateAnd(expr *sqlparser.AndExpr) (bool, error) {
	if expr == nil {
		return false, NewMatcherError("And expression input expression cannot be nil")
	}
	if leftResult, err := m.evaluateExpression(expr.Left); err != nil || !leftResult {
		return leftResult, err
	}

	// if left is true, then right must be evaluated
	return m.evaluateExpression(expr.Right)
}

func (m *MutableStateMatchEvaluator) evaluateOr(expr *sqlparser.OrExpr) (bool, error) {
	if expr == nil {
		return false, NewMatcherError("Or expression input expression cannot be nil")
	}
	if leftResult, err := m.evaluateExpression(expr.Left); err != nil || leftResult {
		return leftResult, err
	}
	// if left is false, then right must be evaluated
	return m.evaluateExpression(expr.Right)
}

func (m *MutableStateMatchEvaluator) evaluateComparison(expr *sqlparser.ComparisonExpr) (bool, error) {
	if expr == nil {
		return false, NewMatcherError("ComparisonExpr input expression cannot be nil")
	}

	colName, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return false, fmt.Errorf("invalid filter name: %s", sqlparser.String(expr.Left))
	}
	colNameStr := sqlparser.String(colName)
	valExpr, ok := expr.Right.(*sqlparser.SQLVal)
	if !ok {
		return false, fmt.Errorf("invalid value: %s", sqlparser.String(expr.Right))
	}
	valStr := sqlparser.String(valExpr)

	switch colNameStr {
	case workflowTypeNameColName:
		val, err := util.ExtractStringValue(valStr)
		if err != nil {
			return false, err
		}
		return m.compareWorkflowType(val, expr.Operator)
	case workflowIDColName:
		val, err := util.ExtractStringValue(valStr)
		if err != nil {
			return false, err
		}
		return m.compareWorkflowID(val, expr.Operator)
	case workflowExecutionStatusColName:
		val, err := util.ExtractStringValue(valStr)
		if err != nil {
			return false, err
		}
		return m.compareWorkflowStatus(val, expr.Operator)
	case workflowStartTimeColName:
		return m.compareStartTime(valStr, expr.Operator)

	default:
		return false, fmt.Errorf("unknown or unsupported search attribute name: %s", colNameStr)
	}
}

func (m *MutableStateMatchEvaluator) evaluateRange(expr *sqlparser.RangeCond) (bool, error) {
	if expr == nil {
		return false, NewMatcherError("RangeCond input expression cannot be nil")
	}
	colName, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return false, fmt.Errorf("unknown or unsupported search attribute name: %s", sqlparser.String(expr.Left))
	}
	colNameStr := sqlparser.String(colName)

	switch colNameStr {
	case workflowStartTimeColName:
		fromValue, err := util.ConvertToTime(sqlparser.String(expr.From))
		if err != nil {
			return false, err
		}
		toValue, err := util.ConvertToTime(sqlparser.String(expr.To))
		if err != nil {
			return false, err
		}

		switch expr.Operator {
		case sqlparser.BetweenStr:
			return m.compareStartTimeBetween(fromValue, toValue)
		case sqlparser.NotBetweenStr:
			result, err := m.compareStartTimeBetween(fromValue, toValue)
			return !result, err
		default:
			// should never happen
			return false, NewMatcherError("%s: range condition operator must be 'between' or 'not between'. Got %s",
				invalidExpressionErrMessage, expr.Operator)
		}

	default:
		return false, fmt.Errorf("unknown or unsupported search attribute name: %s", colNameStr)
	}
}

func (m *MutableStateMatchEvaluator) compareWorkflowType(workflowType string, operation string) (bool, error) {
	existingWorkflowType := m.ms.GetExecutionInfo().WorkflowTypeName
	return m.compareString(existingWorkflowType, workflowType, operation, workflowTypeNameColName)
}

func (m *MutableStateMatchEvaluator) compareWorkflowID(workflowID string, operation string) (bool, error) {
	existingWorkflowId := m.ms.GetExecutionInfo().WorkflowId
	return m.compareString(workflowID, existingWorkflowId, operation, workflowIDColName)
}

func (m *MutableStateMatchEvaluator) compareString(inStr string, expectedStr string, operation string, fieldName string) (bool, error) {
	if len(inStr) == 0 {
		return false, NewMatcherError("%s cannot be empty", fieldName)
	}
	switch operation {
	case sqlparser.EqualStr:
		return expectedStr == inStr, nil
	case sqlparser.NotEqualStr:
		return expectedStr != inStr, nil
	case sqlparser.StartsWithStr:
		return strings.HasPrefix(expectedStr, inStr), nil
	case sqlparser.NotStartsWithStr:
		return !strings.HasPrefix(expectedStr, inStr), nil
	default:
		return false, NewMatcherError("%s: operation %s is not supported for %s filter", invalidExpressionErrMessage, operation, fieldName)
	}
}

func (m *MutableStateMatchEvaluator) compareWorkflowStatus(status string, operation string) (bool, error) {
	if len(status) == 0 {
		return false, NewMatcherError("workflow status cannot be empty")
	}
	msStatus := m.ms.GetExecutionState().Status.String()
	switch operation {
	case sqlparser.EqualStr:
		return msStatus == status, nil
	case sqlparser.NotEqualStr:
		return msStatus != status, nil
	default:
		return false, NewMatcherError("%s: operation %s is not supported for execution status filter", invalidExpressionErrMessage, operation)
	}
}

func (m *MutableStateMatchEvaluator) compareStartTime(val string, operation string) (bool, error) {
	expectedTime, err := util.ConvertToTime(val)
	if err != nil {
		return false, err
	}
	startTime := m.ms.GetExecutionState().StartTime.AsTime()
	switch operation {
	case sqlparser.GreaterEqualStr:
		return startTime.Compare(expectedTime) >= 0, nil
	case sqlparser.LessEqualStr:
		return startTime.Compare(expectedTime) <= 0, nil
	case sqlparser.GreaterThanStr:
		return startTime.After(expectedTime), nil
	case sqlparser.LessThanStr:
		return startTime.Before(expectedTime), nil
	case sqlparser.EqualStr:
		return startTime == expectedTime, nil
	case sqlparser.NotEqualStr:
		return startTime != expectedTime, nil
	default:
		return false, NewMatcherError("%s: operation %s is not supported for StartTime", invalidExpressionErrMessage, operation)
	}
}

func (m *MutableStateMatchEvaluator) compareStartTimeBetween(fromTime time.Time, toTime time.Time) (bool, error) {
	startTime := m.ms.GetExecutionState().StartTime.AsTime()
	lc := startTime.Compare(fromTime)
	rc := startTime.Compare(toTime)
	return lc >= 0 && rc <= 0, nil
}

func getWhereCause(query string) (sqlparser.Expr, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, NewMatcherError("%s: %v", malformedSqlQueryErrMessage, err)
	}

	selectStmt, isSelect := stmt.(*sqlparser.Select)
	if !isSelect {
		return nil, NewMatcherError("%s: statement must be 'select' not %T", notSupportedErrMessage, stmt)
	}

	if selectStmt.Limit != nil {
		return nil, NewMatcherError("%s: 'limit' clause", notSupportedErrMessage)
	}

	if selectStmt.Where == nil {
		return nil, NewMatcherError("%s: 'where' clause is missing", notSupportedErrMessage)
	}

	return selectStmt.Where.Expr, nil
}

func prepareQuery(query string) (string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return "", nil
	}

	if strings.HasPrefix(strings.ToLower(query), "where ") ||
		strings.HasPrefix(strings.ToLower(query), "select ") {
		return "", fmt.Errorf("invalid filter: %s", query)
	}

	// sqlparser can't parse just WHERE clause but instead accepts only valid SQL statement.
	query = fmt.Sprintf("select * from table1 where %s", query)

	return query, nil
}
