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
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/sqlquery"
	"go.temporal.io/server/service/history/workflow"
)

// Supported Fields
const (
	activityIDColName              = "ActivityId"
	activityTypeNameColName        = "ActivityType"
	activityStatusColName          = "ActivityStatus"
	activityAttemptsColName        = "Attempts"
	activityBackoffIntervalColName = "BackoffInterval"
	activityLastFailureColName     = "LastFailure"
	activityTaskQueueColName       = "TaskQueue"
	activityStartedTime            = "StartedTime"
)

type ActivityMatchEvaluator struct {
	ai *persistencespb.ActivityInfo
}

func newActivityMatchEvaluator(ai *persistencespb.ActivityInfo) *ActivityMatchEvaluator {
	return &ActivityMatchEvaluator{ai: ai}
}

func (m *ActivityMatchEvaluator) Evaluate(query string) (bool, error) {
	if m.ai == nil {
		return false, NewMatcherError("activity info is nil")
	}
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

func (m *ActivityMatchEvaluator) evaluateExpression(expr sqlparser.Expr) (bool, error) {

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
	case *sqlparser.ParenExpr:
		return m.evaluateExpression(e.Expr)
	case *sqlparser.RangeCond:
		return m.evaluateRange(e)
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

func (m *ActivityMatchEvaluator) evaluateAnd(expr *sqlparser.AndExpr) (bool, error) {
	if expr == nil {
		return false, NewMatcherError("And expression input expression cannot be nil")
	}
	if leftResult, err := m.evaluateExpression(expr.Left); err != nil || !leftResult {
		return leftResult, err
	}

	// if left is true, then right must be evaluated
	return m.evaluateExpression(expr.Right)
}

func (m *ActivityMatchEvaluator) evaluateOr(expr *sqlparser.OrExpr) (bool, error) {
	if expr == nil {
		return false, NewMatcherError("Or expression input expression cannot be nil")
	}
	if leftResult, err := m.evaluateExpression(expr.Left); err != nil || leftResult {
		return leftResult, err
	}
	// if left is false, then right must be evaluated
	return m.evaluateExpression(expr.Right)
}

func (m *ActivityMatchEvaluator) evaluateComparison(expr *sqlparser.ComparisonExpr) (bool, error) {
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
	case activityTypeNameColName:
		val, err := sqlquery.ExtractStringValue(valStr)
		if err != nil {
			return false, err
		}
		return m.compareActivityType(val, expr.Operator)
	case activityIDColName:
		val, err := sqlquery.ExtractStringValue(valStr)
		if err != nil {
			return false, err
		}
		return m.compareActivityId(val, expr.Operator)
	case activityStatusColName:
		val, err := sqlquery.ExtractStringValue(valStr)
		if err != nil {
			return false, err
		}
		return m.compareActivityStatus(val, expr.Operator)
	case activityAttemptsColName:
		val, err := sqlquery.ExtractIntValue(valStr)
		if err != nil {
			return false, err
		}
		return m.compareActivityAttempts(val, expr.Operator)
	case activityBackoffIntervalColName:
		val, err := sqlquery.ExtractIntValue(valStr)
		if err != nil {
			return false, err
		}
		return m.compareBackoffInterval(val, expr.Operator)
	case activityLastFailureColName:
		val, err := sqlquery.ExtractStringValue(valStr)
		if err != nil {
			return false, err
		}
		return m.compareLastFailure(val, expr.Operator)
	case activityTaskQueueColName:
		val, err := sqlquery.ExtractStringValue(valStr)
		if err != nil {
			return false, err
		}
		return m.compareActivityTaskQueue(val, expr.Operator)
	case activityStartedTime:
		return m.compareStartTime(valStr, expr.Operator)
	default:
		return false, fmt.Errorf("unknown or unsupported search attribute name: %s", colNameStr)
	}
}

func (m *ActivityMatchEvaluator) evaluateRange(expr *sqlparser.RangeCond) (bool, error) {
	if expr == nil {
		return false, NewMatcherError("RangeCond input expression cannot be nil")
	}
	colName, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return false, fmt.Errorf("unknown or unsupported search attribute name: %s", sqlparser.String(expr.Left))
	}
	colNameStr := sqlparser.String(colName)

	switch colNameStr {
	case activityStartedTime:
		fromValue, err := sqlquery.ConvertToTime(sqlparser.String(expr.From))
		if err != nil {
			return false, err
		}
		toValue, err := sqlquery.ConvertToTime(sqlparser.String(expr.To))
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

func (m *ActivityMatchEvaluator) compareActivityType(activityType string, operation string) (bool, error) {
	existingActivityType := m.ai.GetActivityType().GetName()
	return compareQueryString(activityType, existingActivityType, operation, activityTypeNameColName)
}

func (m *ActivityMatchEvaluator) compareActivityId(activityId string, operation string) (bool, error) {
	existingActivityId := m.ai.GetActivityId()
	return compareQueryString(activityId, existingActivityId, operation, workflowIDColName)
}

func (m *ActivityMatchEvaluator) compareActivityStatus(status string, operator string) (bool, error) {
	if m.ai.Paused {
		return compareQueryString(status, "Paused", operator, activityStatusColName)
	}
	switch workflow.GetActivityState(m.ai) { //nolint:exhaustive
	case enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED:
		return compareQueryString(status, "Cancelled", operator, activityStatusColName)
	case enumspb.PENDING_ACTIVITY_STATE_STARTED:
		return compareQueryString(status, "Running", operator, activityStatusColName)
	case enumspb.PENDING_ACTIVITY_STATE_SCHEDULED:
		return compareQueryString(status, "Scheduled", operator, activityStatusColName)
	default:
		return false, NewMatcherError("unknown activity status: %s", status)
	}
}

func (m *ActivityMatchEvaluator) compareActivityTaskQueue(taskQueue string, operator string) (bool, error) {
	existingTaskQueue := m.ai.TaskQueue
	return compareQueryString(taskQueue, existingTaskQueue, operator, activityTaskQueueColName)
}

func (m *ActivityMatchEvaluator) compareActivityAttempts(attempts int, operator string) (bool, error) {
	existingAttempts := m.ai.Attempt
	return compareQueryInt(attempts, int(existingAttempts), operator, activityAttemptsColName)
}

func (m *ActivityMatchEvaluator) compareBackoffInterval(intervalInSec int, operator string) (bool, error) {
	if m.ai.Attempt < 2 {
		// no backoff interval for first attempt
		return false, nil
	}

	backoffIntervalSec := int(m.ai.ScheduledTime.AsTime().Sub(m.ai.LastAttemptCompleteTime.AsTime()).Seconds())
	return compareQueryInt(intervalInSec, backoffIntervalSec, operator, activityBackoffIntervalColName)
}

func (m *ActivityMatchEvaluator) compareLastFailure(val string, operator string) (bool, error) {
	if m.ai.RetryLastFailure == nil || m.ai.RetryLastFailure.Message == "" {
		return false, nil
	}
	if operator != "contains" {
		return false, NewMatcherError("unsupported operator for LastFailure column: %s", operator)
	}
	return strings.Contains(m.ai.RetryLastFailure.Message, val), nil
}

func (m *ActivityMatchEvaluator) compareStartTime(val string, operation string) (bool, error) {
	expectedTime, err := sqlquery.ConvertToTime(val)
	if err != nil {
		return false, err
	}
	startTime := m.ai.GetStartedTime().AsTime()
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
		return false, NewMatcherError("%s: operation %s is not supported for activity StartTime", invalidExpressionErrMessage, operation)
	}
}

func (m *ActivityMatchEvaluator) compareStartTimeBetween(fromTime time.Time, toTime time.Time) (bool, error) {
	if m.ai.GetStartedTime() == nil {
		return false, nil
	}
	startTime := m.ai.GetStartedTime().AsTime()
	return startTime.Compare(fromTime) >= 0 && startTime.Compare(toTime) <= 0, nil
}
