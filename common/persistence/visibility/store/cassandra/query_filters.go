package cassandra

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xwb1989/sqlparser"
	"go.temporal.io/api/enums/v1"
)

type QueryFilters struct {
	HasWorkflowIDFilter bool
	WorkflowID string
	HasWorkflowTypeFilter bool
	WorkflowType string
	HasExecutionStatusFilter bool
	ExecutionStatus enums.WorkflowExecutionStatus
	HasStartTimeRangeFilter bool
	StartTimeFrom time.Time
	StartTimeTo time.Time
}

func newQueryFilters(whereOrderBy string) (*QueryFilters, error) {
	whereOrderBy = strings.TrimSpace(whereOrderBy)

	if whereOrderBy != "" && !strings.HasPrefix(strings.ToLower(whereOrderBy), "order by ") {
		whereOrderBy = "where " + whereOrderBy
	}
	// sqlparser can't parse just WHERE clause but instead accepts only valid SQL statement.
	queryStr := fmt.Sprintf("select * from table1 %s", whereOrderBy)

	stmt, err := sqlparser.Parse(queryStr)
	if err != nil {
		return nil, fmt.Errorf("invalid query %v", queryStr)
	}

	selectStmt := stmt.(*sqlparser.Select)

	if selectStmt.OrderBy != nil {
		return nil, errors.New("order by not supported for Cassandra visibility")
	}

	queryFilter := &QueryFilters{
		StartTimeFrom: time.Unix(0, 0),
		StartTimeTo: time.Now().Add(24*time.Hour),
	}

	if selectStmt.Where == nil {
		return queryFilter, nil
	}

	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch (node).(type) {
		case *sqlparser.ComparisonExpr:
			comparisonExpr := node.(*sqlparser.ComparisonExpr)
			colName, isColName := comparisonExpr.Left.(*sqlparser.ColName)
			if !isColName {
				return false, fmt.Errorf("invalid query string")
			}

			if comparisonExpr.Operator != sqlparser.EqualStr {
				return false, fmt.Errorf("comparison operator %v not supported for Cassandra visibility", comparisonExpr.Operator)
			}

			colNameStr := sqlparser.String(colName.Name)
			switch colNameStr {
			case "WorkflowID":
				queryFilter.HasWorkflowIDFilter = true
				queryFilter.WorkflowID = strings.Trim(sqlparser.String(comparisonExpr.Right), "'")
			case "WorkflowType":
				queryFilter.HasWorkflowTypeFilter = true
				queryFilter.WorkflowType = strings.Trim(sqlparser.String(comparisonExpr.Right),"'")
			case "ExecutionStatus":
				queryFilter.HasExecutionStatusFilter = true
				statusStr := strings.Trim(sqlparser.String(comparisonExpr.Right),"'")
				queryFilter.ExecutionStatus = enums.WorkflowExecutionStatus(enums.WorkflowExecutionStatus_value[statusStr])
			default:
				return false, fmt.Errorf("filter %v not supported for Cassandra visibility", colNameStr)
			}

		case *sqlparser.RangeCond:
			rangeCond := node.(*sqlparser.RangeCond)

			colName, isColName := rangeCond.Left.(*sqlparser.ColName)
			if !isColName {
				return false, fmt.Errorf("invalid query string")
			}

			colNameStr := sqlparser.String(colName)
			if colNameStr != "StartTime" {
				return false, fmt.Errorf("range query on %v not supported for Cassandra visibility", colNameStr)
			}

			if rangeCond.Operator != "between" {
				return false, fmt.Errorf("operator %v not supported for range query", rangeCond.Operator)
			}

			fromStr := strings.ReplaceAll(sqlparser.String(rangeCond.From), "'", "")
			queryFilter.StartTimeFrom, err = time.Parse(time.RFC3339Nano, fromStr)
			if err != nil {
				return false, errors.New("invalid range start timestamp for StartTime")
			}

			toStr := strings.ReplaceAll(sqlparser.String(rangeCond.To), "'", "")
			queryFilter.StartTimeTo, err = time.Parse(time.RFC3339Nano, toStr)
			if err != nil {
				return false, errors.New("invalid range end timestamp for StartTime")
			}

			queryFilter.HasStartTimeRangeFilter = true
			return true, nil
		case *sqlparser.OrExpr:
			return false, errors.New("or operator not supported for Cassandra visibility")
		case *sqlparser.ParenExpr:
			return false, errors.New("paren operator not supported for Cassandra visibility")
		case *sqlparser.IsExpr:
			return false, errors.New("is operator not supported for Cassandra visibility")
		case *sqlparser.NotExpr:
			return false, errors.New("not operator not supported for Cassandra visibility")
		case *sqlparser.FuncExpr:
			return false, errors.New("func operator not supported for Cassandra visibility")
		default:
			return true, nil
		}

		return true, nil
	}, selectStmt.Where.Expr)

	if err != nil {
		return nil, err
	}

	if queryFilter.HasWorkflowIDFilter && queryFilter.HasWorkflowTypeFilter ||
		queryFilter.HasWorkflowIDFilter && queryFilter.HasExecutionStatusFilter ||
		queryFilter.HasWorkflowTypeFilter && queryFilter.HasExecutionStatusFilter {
		return nil, errors.New("AND condition for WorkflowID, WorkflowType and ExecutionStatus not supported for Cassandra visibility")
	}

	return queryFilter, nil
}
