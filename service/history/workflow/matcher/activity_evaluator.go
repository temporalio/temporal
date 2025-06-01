package matcher

import (
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/sqlquery"
)

// Supported Fields
const (
	activityIDColName              = "ActivityId"
	activityTypeNameColName        = "ActivityType"
	activityStateColName           = "ActivityState"
	activityAttemptsColName        = "Attempts"
	activityBackoffIntervalColName = "BackoffInterval"
	activityLastFailureColName     = "LastFailure"
	activityTaskQueueColName       = "TaskQueue"
	activityStartedTime            = "StartedTime"
)

type activityMatchEvaluator struct {
	ai *persistencespb.ActivityInfo
}

func newActivityMatchEvaluator(ai *persistencespb.ActivityInfo) *activityMatchEvaluator {
	return &activityMatchEvaluator{ai: ai}
}

func (m *activityMatchEvaluator) Evaluate(query string) (bool, error) {
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

func (m *activityMatchEvaluator) evaluateExpression(expr sqlparser.Expr) (bool, error) {

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

func (m *activityMatchEvaluator) evaluateAnd(expr *sqlparser.AndExpr) (bool, error) {
	if expr == nil {
		return false, NewMatcherError("And expression input expression cannot be nil")
	}
	if leftResult, err := m.evaluateExpression(expr.Left); err != nil || !leftResult {
		return leftResult, err
	}

	// if left is true, then right must be evaluated
	return m.evaluateExpression(expr.Right)
}

func (m *activityMatchEvaluator) evaluateOr(expr *sqlparser.OrExpr) (bool, error) {
	if expr == nil {
		return false, NewMatcherError("Or expression input expression cannot be nil")
	}
	if leftResult, err := m.evaluateExpression(expr.Left); err != nil || leftResult {
		return leftResult, err
	}
	// if left is false, then right must be evaluated
	return m.evaluateExpression(expr.Right)
}

func (m *activityMatchEvaluator) evaluateComparison(expr *sqlparser.ComparisonExpr) (bool, error) {
	if expr == nil {
		return false, NewMatcherError("ComparisonExpr input expression cannot be nil")
	}

	colName, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return false, NewMatcherError("invalid filter name: %s", sqlparser.String(expr.Left))
	}
	colNameStr := sqlparser.String(colName)
	valExpr, ok := expr.Right.(*sqlparser.SQLVal)
	if !ok {
		return false, NewMatcherError("invalid value: %s", sqlparser.String(expr.Right))
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
	case activityStateColName:
		val, err := sqlquery.ExtractStringValue(valStr)
		if err != nil {
			return false, err
		}
		return m.compareActivityState(val, expr.Operator)
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
		return false, NewMatcherError("unknown or unsupported activity search field: %s", colNameStr)
	}
}

func (m *activityMatchEvaluator) evaluateRange(expr *sqlparser.RangeCond) (bool, error) {
	if expr == nil {
		return false, NewMatcherError("RangeCond input expression cannot be nil")
	}
	colName, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return false, NewMatcherError("unknown or unsupported search field name: %s", sqlparser.String(expr.Left))
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
		return false, NewMatcherError("unknown or unsupported activity search field: %s", colNameStr)
	}
}

func (m *activityMatchEvaluator) compareActivityType(activityType string, operation string) (bool, error) {
	existingActivityType := m.ai.GetActivityType().GetName()
	return compareQueryString(activityType, existingActivityType, operation, activityTypeNameColName)
}

func (m *activityMatchEvaluator) compareActivityId(activityId string, operation string) (bool, error) {
	existingActivityId := m.ai.GetActivityId()
	return compareQueryString(activityId, existingActivityId, operation, workflowIDColName)
}

func (m *activityMatchEvaluator) compareActivityState(status string, operator string) (bool, error) {
	if m.ai.Paused {
		return compareQueryString(status, "Paused", operator, activityStateColName)
	}

	switch activityState := getActivityState(m.ai); activityState { //nolint:exhaustive
	case enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED:
		return compareQueryString(status, activityState.String(), operator, activityStateColName)
	case enumspb.PENDING_ACTIVITY_STATE_STARTED:
		return compareQueryString(status, activityState.String(), operator, activityStateColName)
	case enumspb.PENDING_ACTIVITY_STATE_SCHEDULED:
		return compareQueryString(status, activityState.String(), operator, activityStateColName)
	default:
		return false, NewMatcherError("unknown or unsupported activity status: %s", status)
	}
}

func (m *activityMatchEvaluator) compareActivityTaskQueue(taskQueue string, operator string) (bool, error) {
	existingTaskQueue := m.ai.TaskQueue
	return compareQueryString(taskQueue, existingTaskQueue, operator, activityTaskQueueColName)
}

func (m *activityMatchEvaluator) compareActivityAttempts(attempts int, operator string) (bool, error) {
	existingAttempts := m.ai.Attempt
	return compareQueryInt(attempts, int(existingAttempts), operator, activityAttemptsColName)
}

func (m *activityMatchEvaluator) compareBackoffInterval(intervalInSec int, operator string) (bool, error) {
	if m.ai.Attempt < 2 {
		// no backoff interval for first attempt
		return false, nil
	}

	backoffIntervalSec := int(m.ai.ScheduledTime.AsTime().Sub(m.ai.LastAttemptCompleteTime.AsTime()).Seconds())
	return compareQueryInt(intervalInSec, backoffIntervalSec, operator, activityBackoffIntervalColName)
}

func (m *activityMatchEvaluator) compareLastFailure(val string, operator string) (bool, error) {
	if m.ai.RetryLastFailure == nil || m.ai.RetryLastFailure.Message == "" {
		return false, nil
	}
	if operator != "contains" {
		return false, NewMatcherError("unsupported operator for LastFailure column: %s", operator)
	}
	return strings.Contains(m.ai.RetryLastFailure.Message, val), nil
}

func (m *activityMatchEvaluator) compareStartTime(val string, operation string) (bool, error) {
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

func (m *activityMatchEvaluator) compareStartTimeBetween(fromTime time.Time, toTime time.Time) (bool, error) {
	if m.ai.GetStartedTime() == nil {
		return false, nil
	}
	startTime := m.ai.GetStartedTime().AsTime()
	return startTime.Compare(fromTime) >= 0 && startTime.Compare(toTime) <= 0, nil
}

func getActivityState(ai *persistencespb.ActivityInfo) enumspb.PendingActivityState {
	activityState := enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
	if ai.CancelRequested {
		activityState = enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
	}
	if ai.StartedEventId != common.EmptyEventID {
		activityState = enumspb.PENDING_ACTIVITY_STATE_STARTED
	}
	return activityState
}
