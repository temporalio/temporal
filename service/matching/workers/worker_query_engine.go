package workers

import (
	"fmt"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/api/serviceerror"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/sqlquery"
)

const (
	workerInstanceKeyColName    = "WorkerInstanceKey"
	workerIdentityColName       = "WorkerIdentity"
	workerHostNameColName       = "HostName"
	workerTaskQueueColName      = "TaskQueue"
	workerDeploymentNameColName = "DeploymentName"
	workerBuildIDColName        = "BuildId"
	workerSdkNameColName        = "SdkName"
	workerSdkVersionColName     = "SdkVersion"
	workerStartTimeColName      = "StartTime"
	workerHeartbeatTimeColName  = "HeartbeatTime"
	workerStatusColName         = "WorkerStatus"
	// "Status" is a SQL reserved word, so the parser lowercases and backtick-quotes it.
	// After stripping backticks we get "status".
	workerStatusColNameAlias = "status"
)

const (
	malformedSqlQueryErrMessage = "malformed query"
	notSupportedErrMessage      = "operation is not supported"
	invalidExpressionErrMessage = "invalid expression"
)

/*
FilterWorkers filters the list of per-namespace worker heartbeats against the provided query.
The query should be a valid SQL query without WHERE clause.

Query is used to filter workers based on worker heartbeat info.
The following worker attributes are supported as part of the query:
* WorkerInstanceKey
* WorkerIdentity
* HostName
* TaskQueue
* DeploymentName
* BuildId
* SdkName
* SdkVersion
* StartTime
* HeartbeatTime
* WorkerStatus (or Status)
Currently metrics are not supported as a part of ListWorkers query.

Field names are case-sensitive.

The query can have multiple conditions combined with AND/OR.
The query can have conditions on multiple fields.
Date time fields should be in RFC3339 format.
Example query:

	"TaskQueue = 'my_task_queue' AND HeartbeatTime < '2023-10-27T10:30:00Z' "

Different fields can support different operators.
  - string fields (e.g., WorkerIdentity, HostName, TaskQueue, DeploymentName, BuildId, SdkName, SdkVersion):
		=, !=, starts_with, not starts_with, IS NULL, IS NOT NULL
  - time fields (e.g., StartTime, HeartbeatTime):
		 =, !=, >, >=, <, <=, between, IS NULL, IS NOT NULL
  - metric fields (e.g., total_sticky_cache_hit):
		=, !=, >, >=, <, <=

For string fields, IS NULL matches workers where the field is empty, and IS NOT NULL matches
workers where the field is non-empty. For time fields, IS NULL matches workers where the
timestamp is not set.

Returns the list of workers for which the query matches the worker heartbeat, or an error,
Errors are:
 - the query is invalid.
 - the query is not supported.
 - the provided namespace doesn't exist.
*/

func newWorkerQueryEngine(nsID string, query string) (*workerQueryEngine, error) {
	engine := &workerQueryEngine{
		nsID:  nsID,
		query: query,
	}

	err := engine.validateQuery()
	if err != nil {
		return nil, err
	}
	return engine, nil
}

type WorkerHeartbeatPropertyFunc func(*workerpb.WorkerHeartbeat) string

var (
	propertyMapFuncs = map[string]WorkerHeartbeatPropertyFunc{
		workerInstanceKeyColName: func(hb *workerpb.WorkerHeartbeat) string {
			return hb.WorkerInstanceKey
		},
		workerIdentityColName: func(hb *workerpb.WorkerHeartbeat) string {
			return hb.WorkerIdentity
		},
		workerHostNameColName: func(hb *workerpb.WorkerHeartbeat) string {
			if hb.HostInfo == nil {
				return ""
			}
			return hb.HostInfo.HostName
		},
		workerTaskQueueColName: func(hb *workerpb.WorkerHeartbeat) string {
			return hb.TaskQueue
		},
		workerDeploymentNameColName: func(hb *workerpb.WorkerHeartbeat) string {
			if hb.DeploymentVersion == nil {
				return ""
			}
			return hb.DeploymentVersion.DeploymentName
		},
		workerBuildIDColName: func(hb *workerpb.WorkerHeartbeat) string {
			if hb.DeploymentVersion == nil {
				return ""
			}
			return hb.DeploymentVersion.BuildId
		},
		workerSdkNameColName: func(hb *workerpb.WorkerHeartbeat) string {
			return hb.SdkName
		},
		workerSdkVersionColName: func(hb *workerpb.WorkerHeartbeat) string {
			return hb.SdkVersion
		},
		workerStatusColName: func(hb *workerpb.WorkerHeartbeat) string {
			return hb.Status.String()
		},
		workerStatusColNameAlias: func(hb *workerpb.WorkerHeartbeat) string {
			return hb.Status.String()
		},
	}
)

type workerQueryEngine struct {
	nsID                  string // Namespace ID
	query                 string
	parsedWhereExpression sqlparser.Expr
	currentWorker         *workerpb.WorkerHeartbeat // Current worker heartbeat being evaluated
}

func (w *workerQueryEngine) EvaluateWorker(hb *workerpb.WorkerHeartbeat) (bool, error) {

	w.currentWorker = hb
	return w.evaluateExpression(w.parsedWhereExpression)
}

func (w *workerQueryEngine) validateQuery() error {
	query, err := prepareQuery(w.query)
	if err != nil {
		return err
	}

	w.parsedWhereExpression, err = getWhereCause(query)
	if err != nil {
		return err
	}
	return nil
}

func getWhereCause(query string) (sqlparser.Expr, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, serviceerror.NewInvalidArgumentf("%s: %v", malformedSqlQueryErrMessage, err)
	}

	selectStmt, isSelect := stmt.(*sqlparser.Select)
	if !isSelect {
		return nil, serviceerror.NewInvalidArgumentf("%s: statement must be 'select' not %T", notSupportedErrMessage, stmt)
	}

	if selectStmt.Limit != nil {
		return nil, serviceerror.NewInvalidArgumentf("%s: 'limit' clause", notSupportedErrMessage)
	}

	if selectStmt.Where == nil {
		return nil, serviceerror.NewInvalidArgumentf("%s: 'where' clause is missing", notSupportedErrMessage)
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

func (w *workerQueryEngine) evaluateExpression(expr sqlparser.Expr) (bool, error) {

	if expr == nil {
		return false, serviceerror.NewInvalidArgumentf("input expression cannot be nil")
	}

	switch e := (expr).(type) {
	case *sqlparser.AndExpr:
		return w.evaluateAnd(e)
	case *sqlparser.OrExpr:
		return w.evaluateOr(e)
	case *sqlparser.ComparisonExpr:
		return w.evaluateComparison(e)
	case *sqlparser.ParenExpr:
		return w.evaluateExpression(e.Expr)
	case *sqlparser.RangeCond:
		return w.evaluateRange(e)
	case *sqlparser.IsExpr:
		return w.evaluateIsExpr(e)
	case *sqlparser.NotExpr:
		return false, serviceerror.NewInvalidArgumentf("%s: 'not' expression", notSupportedErrMessage)
	case *sqlparser.FuncExpr:
		return false, serviceerror.NewInvalidArgumentf("%s: function expression", notSupportedErrMessage)
	case *sqlparser.ColName:
		return false, serviceerror.NewInvalidArgumentf("incomplete expression")
	default:
		return false, serviceerror.NewInvalidArgumentf("%s: expression of type %T", notSupportedErrMessage, expr)
	}
}

func (w *workerQueryEngine) evaluateAnd(expr *sqlparser.AndExpr) (bool, error) {
	if expr == nil {
		return false, serviceerror.NewInvalidArgumentf("And expression input expression cannot be nil")
	}
	if leftResult, err := w.evaluateExpression(expr.Left); err != nil || !leftResult {
		return leftResult, err
	}

	// if left is true, then right must be evaluated
	return w.evaluateExpression(expr.Right)
}

func (w *workerQueryEngine) evaluateOr(expr *sqlparser.OrExpr) (bool, error) {
	if expr == nil {
		return false, serviceerror.NewInvalidArgumentf("Or expression input expression cannot be nil")
	}
	if leftResult, err := w.evaluateExpression(expr.Left); err != nil || leftResult {
		return leftResult, err
	}
	// if left is false, then right must be evaluated
	return w.evaluateExpression(expr.Right)
}

func (w *workerQueryEngine) evaluateIsExpr(expr *sqlparser.IsExpr) (bool, error) {
	if expr == nil {
		return false, serviceerror.NewInvalidArgumentf("IsExpr input expression cannot be nil")
	}

	colNameExpr, ok := expr.Expr.(*sqlparser.ColName)
	if !ok {
		return false, serviceerror.NewInvalidArgumentf("invalid filter name: %s", sqlparser.String(expr.Expr))
	}
	colName := strings.ReplaceAll(sqlparser.String(colNameExpr), "`", "")

	if expr.Operator != sqlparser.IsNullStr && expr.Operator != sqlparser.IsNotNullStr {
		return false, serviceerror.NewInvalidArgumentf(
			"%s: 'is' operator %q is not supported; only IS NULL and IS NOT NULL are supported",
			notSupportedErrMessage, expr.Operator)
	}

	isNull := expr.Operator == sqlparser.IsNullStr

	if propertyFunc, ok := propertyMapFuncs[colName]; ok {
		isEmpty := propertyFunc(w.currentWorker) == ""
		return isEmpty == isNull, nil
	}

	switch colName {
	case workerStartTimeColName, workerHeartbeatTimeColName:
		timeValue, err := w.getTimeValue(colName)
		if err != nil {
			return false, err
		}
		return timeValue.IsZero() == isNull, nil
	default:
		return false, serviceerror.NewInvalidArgumentf("unknown or unsupported worker heartbeat search field: %s", colName)
	}
}

func (w *workerQueryEngine) evaluateComparison(expr *sqlparser.ComparisonExpr) (bool, error) {
	if expr == nil {
		return false, serviceerror.NewInvalidArgumentf("ComparisonExpr input expression cannot be nil")
	}

	colNameExpr, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return false, serviceerror.NewInvalidArgumentf("invalid filter name: %s", sqlparser.String(expr.Left))
	}
	// Strip backticks added by the SQL parser for reserved words (e.g., "Status" → "`status`" → "status").
	colName := strings.ReplaceAll(sqlparser.String(colNameExpr), "`", "")
	valExpr, ok := expr.Right.(*sqlparser.SQLVal)
	if !ok {
		return false, serviceerror.NewInvalidArgumentf("invalid value: %s", sqlparser.String(expr.Right))
	}
	valStr := sqlparser.String(valExpr)

	// First check if the column name is a valid property function.
	if propertyFunc, ok := propertyMapFuncs[colName]; ok {
		val, err := sqlquery.ExtractStringValue(valStr)
		if err != nil {
			return false, serviceerror.NewInvalidArgumentf("invalid value for %s: %v", colName, err)
		}
		existingVal := propertyFunc(w.currentWorker)
		return compareQueryString(val, existingVal, expr.Operator, colName)
	}

	// If not, then check if the column name is a valid time column.
	switch colName {
	case workerStartTimeColName:
		expectedTime, err := sqlquery.ConvertToTime(valStr)
		if err != nil {
			return false, serviceerror.NewInvalidArgumentf("invalid value for %s: %v", colName, err)
		}
		receivedTime := w.currentWorker.GetStartTime().AsTime()
		return w.compareTime(receivedTime, expectedTime, expr.Operator)
	case workerHeartbeatTimeColName:
		expectedTime, err := sqlquery.ConvertToTime(valStr)
		if err != nil {
			return false, serviceerror.NewInvalidArgumentf("invalid value for %s: %v", colName, err)
		}
		receivedTime := w.currentWorker.GetHeartbeatTime().AsTime()
		return w.compareTime(receivedTime, expectedTime, expr.Operator)
	default:
		return false, serviceerror.NewInvalidArgumentf("unknown or unsupported worker heartbeat search field: %s", colName)
	}
}

func (w *workerQueryEngine) evaluateRange(expr *sqlparser.RangeCond) (bool, error) {
	if expr == nil {
		return false, serviceerror.NewInvalidArgumentf("RangeCond input expression cannot be nil")
	}
	colName, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return false, serviceerror.NewInvalidArgumentf("unknown or unsupported column name: %s", sqlparser.String(expr.Left))
	}
	// Strip backticks added by the SQL parser for reserved words (e.g., "Status" → "`status`" → "status").
	colNameStr := strings.ReplaceAll(sqlparser.String(colName), "`", "")

	switch colNameStr {
	case workerStartTimeColName, workerHeartbeatTimeColName:
		fromValue, err := sqlquery.ConvertToTime(sqlparser.String(expr.From))
		if err != nil {
			return false, err
		}
		toValue, err := sqlquery.ConvertToTime(sqlparser.String(expr.To))
		if err != nil {
			return false, err
		}

		timeValue, err := w.getTimeValue(colNameStr)
		if err != nil {
			return false, err
		}
		if timeValue.IsZero() {
			// If the time value is zero, it means the time was not provided with heartbeat.
			// In this case, we return false for the range condition.
			return false, nil
		}

		switch expr.Operator {
		case sqlparser.BetweenStr:
			return w.compareTimeBetween(fromValue, toValue, timeValue), nil
		case sqlparser.NotBetweenStr:
			result := w.compareTimeBetween(fromValue, toValue, timeValue)
			return !result, nil
		default:
			return false, serviceerror.NewInvalidArgumentf("%s: range condition operator must be 'between' or 'not between'. Got %s",
				invalidExpressionErrMessage, expr.Operator)
		}

	default:
		return false, serviceerror.NewInvalidArgumentf("unknown or unsupported column name: %s", colNameStr)
	}
}

func compareQueryString(inStr string, expectedStr string, operation string, colName string) (bool, error) {
	if len(inStr) == 0 {
		return false, serviceerror.NewInvalidArgumentf("%s cannot be empty", colName)
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
		return false, serviceerror.NewInvalidArgumentf("%s: operation %s is not supported for %s column", invalidExpressionErrMessage, operation, colName)
	}
}

func (w *workerQueryEngine) getTimeValue(colName string) (time.Time, error) {
	var zeroTime time.Time
	switch colName {
	case workerStartTimeColName:
		if w.currentWorker.GetStartTime() == nil {
			return zeroTime, nil
		}
		return w.currentWorker.GetStartTime().AsTime(), nil
	case workerHeartbeatTimeColName:
		if w.currentWorker.GetHeartbeatTime() == nil {
			return zeroTime, nil
		}
		return w.currentWorker.GetHeartbeatTime().AsTime(), nil

	default:
		return zeroTime, serviceerror.NewInvalidArgumentf("unknown or unsupported column name: %s", colName)
	}
}

func (w *workerQueryEngine) compareTimeBetween(
	fromTime time.Time, toTime time.Time, timeValue time.Time,
) bool {
	return timeValue.Compare(fromTime) >= 0 && timeValue.Compare(toTime) <= 0
}

func (w *workerQueryEngine) compareTime(receivedTime time.Time, expectedTime time.Time, operation string) (bool, error) {
	switch operation {
	case sqlparser.GreaterEqualStr:
		return receivedTime.Compare(expectedTime) >= 0, nil
	case sqlparser.LessEqualStr:
		return receivedTime.Compare(expectedTime) <= 0, nil
	case sqlparser.GreaterThanStr:
		return receivedTime.After(expectedTime), nil
	case sqlparser.LessThanStr:
		return receivedTime.Before(expectedTime), nil
	case sqlparser.EqualStr:
		return receivedTime.Equal(expectedTime), nil
	case sqlparser.NotEqualStr:
		return !receivedTime.Equal(expectedTime), nil
	default:
		return false, serviceerror.NewInvalidArgumentf("%s: operation %s is not supported", invalidExpressionErrMessage, operation)
	}
}
