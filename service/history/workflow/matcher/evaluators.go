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
	"go.temporal.io/server/service/history/workflow"
)

// Supported Fields
const (
	WorkflowID              = searchattribute.WorkflowId
	WorkflowTypeName        = searchattribute.WorkflowType
	WorkflowStartTime       = searchattribute.StartTime
	WorkflowExecutionStatus = searchattribute.ExecutionStatus
)

type (
	ExprEvaluator interface {
		Evaluate(expr sqlparser.Expr) (bool, error)
	}

	MutableStateMatchEvaluator struct {
		ms workflow.MutableState
	}
)

func newMutableStateMatchEvaluator(ms workflow.MutableState) *MutableStateMatchEvaluator {
	return &MutableStateMatchEvaluator{ms: ms}
}

func (m *MutableStateMatchEvaluator) Evaluate(expr sqlparser.Expr) (bool, error) {
	return m.evaluate(expr)
}

func (m *MutableStateMatchEvaluator) evaluate(expr sqlparser.Expr) (bool, error) {

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
		return m.evaluate(e.Expr)
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
func (m *MutableStateMatchEvaluator) evaluateAnd(expr sqlparser.Expr) (bool, error) {
	andExpr, ok := expr.(*sqlparser.AndExpr)
	if !ok {
		return false, NewMatcherError("%v is not an 'and' expression", sqlparser.String(expr))
	}

	leftResult, err := m.evaluate(andExpr.Left)
	if err != nil {
		return false, err
	}
	if leftResult == false {
		return leftResult, nil
	}
	// if left is true, then right must be evaluated
	return m.evaluate(andExpr.Right)
}
func (m *MutableStateMatchEvaluator) evaluateOr(expr sqlparser.Expr) (bool, error) {
	orExpr, ok := expr.(*sqlparser.OrExpr)
	if !ok {
		return false, NewMatcherError("%v is not an 'or' expression", sqlparser.String(expr))
	}

	if leftResult, err := m.evaluate(andExpr.Left); err != nil || !leftResult {
		return leftResult, err
	}
	// if left is false, then right must be evaluated
	return m.evaluate(andExpr.Right)
}

func (m *MutableStateMatchEvaluator) evaluateComparison(expr sqlparser.Expr) (bool, error) {
	compExpr, ok := expr.(*sqlparser.ComparisonExpr)
	if !ok {
		return false, NewMatcherError("%v is not an 'comparison' expression", sqlparser.String(expr))
	}

	colName, ok := compExpr.Left.(*sqlparser.ColName)
	if !ok {
		return false, fmt.Errorf("invalid filter name: %s", sqlparser.String(compExpr.Left))
	}
	colNameStr := sqlparser.String(colName)
	op := compExpr.Operator
	valExpr, ok := compExpr.Right.(*sqlparser.SQLVal)
	if !ok {
		return false, fmt.Errorf("invalid value: %s", sqlparser.String(compExpr.Right))
	}
	valStr := sqlparser.String(valExpr)

	switch colNameStr {
	case WorkflowTypeName:
		val, err := extractStringValue(valStr)
		if err != nil {
			return false, err
		}
		return m.compareWorkflowType(val, compExpr.Operator)
	case WorkflowID:
		val, err := extractStringValue(valStr)
		if err != nil {
			return false, err
		}
		return m.compareWorkflowID(val, compExpr.Operator)
	case WorkflowExecutionStatus:
		val, err := extractStringValue(valStr)
		if err != nil {
			return false, err
		}
		return m.compareWorkflowStatus(val, op)
	case WorkflowStartTime:
		return m.compareStartTime(valStr, op)

	default:
		return false, fmt.Errorf("unknown filter name: %s", colNameStr)
	}
}

func (m *MutableStateMatchEvaluator) evaluateRange(expr sqlparser.Expr) (bool, error) {
	rangeCond, ok := expr.(*sqlparser.RangeCond)
	if !ok {
		return false, NewMatcherError("%v is not an 'comparison' expression", sqlparser.String(expr))
	}

	colName, ok := rangeCond.Left.(*sqlparser.ColName)
	if !ok {
		return false, fmt.Errorf("invalid filter name: %s", sqlparser.String(rangeCond.Left))
	}
	colNameStr := sqlparser.String(colName)

	switch colNameStr {
	case WorkflowStartTime:
		fromValue, err := convertToTime(sqlparser.String(rangeCond.From))
		if err != nil {
			return false, err
		}
		toValue, err := convertToTime(sqlparser.String(rangeCond.To))
		if err != nil {
			return false, err
		}

		switch rangeCond.Operator {
		case "between":
			return m.compareStartTimeBetween(fromValue, toValue)
		case "not between":
			result, err := m.compareStartTimeBetween(fromValue, toValue)
			return !result, err
		default:
			return false, NewMatcherError("%s: range condition operator must be 'between' or 'not between'", invalidExpressionErrMessage)
		}

	default:
		return false, fmt.Errorf("unknown filter name: %s", colNameStr)
	}
}

func (m *MutableStateMatchEvaluator) compareWorkflowType(workflowType string, operation string) (bool, error) {
	existingWorkflowType := m.ms.GetExecutionInfo().WorkflowTypeName
	return m.compareWorkflowString(existingWorkflowType, workflowType, operation, WorkflowTypeName)
}

func (m *MutableStateMatchEvaluator) compareWorkflowID(workflowID string, operation string) (bool, error) {
	existingWorkflowId := m.ms.GetExecutionInfo().WorkflowId
	return m.compareWorkflowString(workflowID, existingWorkflowId, operation, WorkflowID)
}

func (m *MutableStateMatchEvaluator) compareString(inStr string, expectedStr string, operation string, fieldName string) (bool, error) {
	if len(inStr) == 0 {
		return false, NewMatcherError("%s cannot be empty", fieldName)
	}
	switch operation {
	case sqlparser.EqualStr:
		return strings.EqualFold(expectedStr, inStr), nil
	case sqlparser.NotEqualStr:
		return !strings.EqualFold(expectedStr, inStr), nil
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
		return strings.EqualFold(msStatus, status), nil
	case sqlparser.NotEqualStr:
		return !strings.EqualFold(msStatus, status), nil
	default:
		return false, NewMatcherError("%s: operation %s is not supported for execution status filter", invalidExpressionErrMessage, operation)
	}
}

func (m *MutableStateMatchEvaluator) compareStartTime(val string, operation string) (bool, error) {
	expectedTime, err := convertToTime(val)
	if err != nil {
		return false, err
	}
	startTime := m.ms.GetExecutionState().StartTime.AsTime()
	switch operation {
	case sqlparser.GreaterEqualStr:
		return startTime.Compare(expectedTime) >= 0, nil
	case sqlparser.LessEqualStr:
		if startTime == expectedTime {
			return true, nil
		}
		return startTime.Before(expectedTime), nil
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
