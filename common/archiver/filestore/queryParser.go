// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source queryParser.go -destination queryParser_mock.go -mock_names Interface=MockQueryParser

package filestore

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xwb1989/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/util"
)

type (
	// QueryParser parses a limited SQL where clause into a struct
	QueryParser interface {
		Parse(query string) (*parsedQuery, error)
	}

	queryParser struct{}

	parsedQuery struct {
		earliestCloseTime time.Time
		latestCloseTime   time.Time
		workflowID        *string
		runID             *string
		workflowTypeName  *string
		status            *enumspb.WorkflowExecutionStatus
		emptyResult       bool
	}
)

// All allowed fields for filtering
const (
	WorkflowID   = "WorkflowId"
	RunID        = "RunId"
	WorkflowType = "WorkflowType"
	CloseTime    = "CloseTime"
	// Field name can't be just "Status" because it is reserved keyword in MySQL parser.
	ExecutionStatus = "ExecutionStatus"
)

const (
	queryTemplate = "select * from dummy where %s"

	defaultDateTimeFormat = time.RFC3339
)

// NewQueryParser creates a new query parser for filestore
func NewQueryParser() QueryParser {
	return &queryParser{}
}

func (p *queryParser) Parse(query string) (*parsedQuery, error) {
	stmt, err := sqlparser.Parse(fmt.Sprintf(queryTemplate, query))
	if err != nil {
		return nil, err
	}
	whereExpr := stmt.(*sqlparser.Select).Where.Expr
	parsedQuery := &parsedQuery{
		earliestCloseTime: time.Time{},
		latestCloseTime:   time.Now().UTC(),
	}
	if err := p.convertWhereExpr(whereExpr, parsedQuery); err != nil {
		return nil, err
	}
	return parsedQuery, nil
}

func (p *queryParser) convertWhereExpr(expr sqlparser.Expr, parsedQuery *parsedQuery) error {
	if expr == nil {
		return errors.New("where expression is nil")
	}

	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		return p.convertComparisonExpr(expr, parsedQuery)
	case *sqlparser.AndExpr:
		return p.convertAndExpr(expr, parsedQuery)
	case *sqlparser.ParenExpr:
		return p.convertParenExpr(expr, parsedQuery)
	default:
		return errors.New("only comparison and \"and\" expression is supported")
	}
}

func (p *queryParser) convertParenExpr(parenExpr *sqlparser.ParenExpr, parsedQuery *parsedQuery) error {
	return p.convertWhereExpr(parenExpr.Expr, parsedQuery)
}

func (p *queryParser) convertAndExpr(andExpr *sqlparser.AndExpr, parsedQuery *parsedQuery) error {
	if err := p.convertWhereExpr(andExpr.Left, parsedQuery); err != nil {
		return err
	}
	return p.convertWhereExpr(andExpr.Right, parsedQuery)
}

func (p *queryParser) convertComparisonExpr(compExpr *sqlparser.ComparisonExpr, parsedQuery *parsedQuery) error {
	colName, ok := compExpr.Left.(*sqlparser.ColName)
	if !ok {
		return fmt.Errorf("invalid filter name: %s", sqlparser.String(compExpr.Left))
	}
	colNameStr := sqlparser.String(colName)
	op := compExpr.Operator
	valExpr, ok := compExpr.Right.(*sqlparser.SQLVal)
	if !ok {
		return fmt.Errorf("invalid value: %s", sqlparser.String(compExpr.Right))
	}
	valStr := sqlparser.String(valExpr)

	switch colNameStr {
	case WorkflowID:
		val, err := extractStringValue(valStr)
		if err != nil {
			return err
		}
		if op != "=" {
			return fmt.Errorf("only operation = is support for %s", WorkflowID)
		}
		if parsedQuery.workflowID != nil && *parsedQuery.workflowID != val {
			parsedQuery.emptyResult = true
			return nil
		}
		parsedQuery.workflowID = convert.StringPtr(val)
	case RunID:
		val, err := extractStringValue(valStr)
		if err != nil {
			return err
		}
		if op != "=" {
			return fmt.Errorf("only operation = is support for %s", RunID)
		}
		if parsedQuery.runID != nil && *parsedQuery.runID != val {
			parsedQuery.emptyResult = true
			return nil
		}
		parsedQuery.runID = convert.StringPtr(val)
	case WorkflowType:
		val, err := extractStringValue(valStr)
		if err != nil {
			return err
		}
		if op != "=" {
			return fmt.Errorf("only operation = is support for %s", WorkflowType)
		}
		if parsedQuery.workflowTypeName != nil && *parsedQuery.workflowTypeName != val {
			parsedQuery.emptyResult = true
			return nil
		}
		parsedQuery.workflowTypeName = convert.StringPtr(val)
	case ExecutionStatus:
		val, err := extractStringValue(valStr)
		if err != nil {
			// if failed to extract string value, it means user input close status as a number
			val = valStr
		}
		if op != "=" {
			return fmt.Errorf("only operation = is support for %s", ExecutionStatus)
		}
		status, err := convertStatusStr(val)
		if err != nil {
			return err
		}
		if parsedQuery.status != nil && *parsedQuery.status != status {
			parsedQuery.emptyResult = true
			return nil
		}
		parsedQuery.status = &status
	case CloseTime:
		timestamp, err := convertToTime(valStr)
		if err != nil {
			return err
		}
		return p.convertCloseTime(timestamp, op, parsedQuery)
	default:
		return fmt.Errorf("unknown filter name: %s", colNameStr)
	}

	return nil
}

func (p *queryParser) convertCloseTime(timestamp time.Time, op string, parsedQuery *parsedQuery) error {
	switch op {
	case "=":
		if err := p.convertCloseTime(timestamp, ">=", parsedQuery); err != nil {
			return err
		}
		if err := p.convertCloseTime(timestamp, "<=", parsedQuery); err != nil {
			return err
		}
	case "<":
		parsedQuery.latestCloseTime = util.MinTime(parsedQuery.latestCloseTime, timestamp.Add(-1*time.Nanosecond))
	case "<=":
		parsedQuery.latestCloseTime = util.MinTime(parsedQuery.latestCloseTime, timestamp)
	case ">":
		parsedQuery.earliestCloseTime = util.MaxTime(parsedQuery.earliestCloseTime, timestamp.Add(1*time.Nanosecond))
	case ">=":
		parsedQuery.earliestCloseTime = util.MaxTime(parsedQuery.earliestCloseTime, timestamp)
	default:
		return fmt.Errorf("operator %s is not supported for close time", op)
	}
	return nil
}

func convertToTime(timeStr string) (time.Time, error) {
	ts, err := strconv.ParseInt(timeStr, 10, 64)
	if err == nil {
		return timestamp.UnixOrZeroTime(ts), nil
	}
	timestampStr, err := extractStringValue(timeStr)
	if err != nil {
		return time.Time{}, err
	}
	parsedTime, err := time.Parse(defaultDateTimeFormat, timestampStr)
	if err != nil {
		return time.Time{}, err
	}
	return parsedTime, nil
}

func convertStatusStr(statusStr string) (enumspb.WorkflowExecutionStatus, error) {
	statusStr = strings.ToLower(strings.TrimSpace(statusStr))
	switch statusStr {
	case "completed", convert.Int32ToString(int32(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)):
		return enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, nil
	case "failed", convert.Int32ToString(int32(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED)):
		return enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, nil
	case "canceled", convert.Int32ToString(int32(enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED)):
		return enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, nil
	case "terminated", convert.Int32ToString(int32(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED)):
		return enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, nil
	case "continuedasnew", "continued_as_new", convert.Int32ToString(int32(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW)):
		return enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, nil
	case "timedout", "timed_out", convert.Int32ToString(int32(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT)):
		return enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, nil
	default:
		return 0, fmt.Errorf("unknown workflow close status: %s", statusStr)
	}
}

func extractStringValue(s string) (string, error) {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1], nil
	}
	return "", fmt.Errorf("value %s is not a string value", s)
}
