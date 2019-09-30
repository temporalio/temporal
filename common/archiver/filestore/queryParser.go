// Copyright (c) 2019 Uber Technologies, Inc.
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

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/xwb1989/sqlparser"
)

type (
	// QueryParser parses a limited SQL where clause into a struct
	QueryParser interface {
		Parse(query string) (*parsedQuery, error)
	}

	queryParser struct{}

	parsedQuery struct {
		earliestCloseTime int64
		latestCloseTime   int64
		workflowID        *string
		runID             *string
		workflowTypeName  *string
		closeStatus       *shared.WorkflowExecutionCloseStatus
		emptyResult       bool
	}
)

// All allowed fields for filtering
const (
	WorkflowID   = "WorkflowID"
	RunID        = "RunID"
	WorkflowType = "WorkflowType"
	CloseTime    = "CloseTime"
	CloseStatus  = "CloseStatus"
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
		earliestCloseTime: 0,
		latestCloseTime:   time.Now().UnixNano(),
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

	switch expr.(type) {
	case *sqlparser.ComparisonExpr:
		return p.convertComparisonExpr(expr.(*sqlparser.ComparisonExpr), parsedQuery)
	case *sqlparser.AndExpr:
		return p.convertAndExpr(expr.(*sqlparser.AndExpr), parsedQuery)
	case *sqlparser.ParenExpr:
		return p.convertParenExpr(expr.(*sqlparser.ParenExpr), parsedQuery)
	default:
		return errors.New("only comparsion and \"and\" expression is supported")
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
		parsedQuery.workflowID = common.StringPtr(val)
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
		parsedQuery.runID = common.StringPtr(val)
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
		parsedQuery.workflowTypeName = common.StringPtr(val)
	case CloseStatus:
		val, err := extractStringValue(valStr)
		if err != nil {
			return err
		}
		if op != "=" {
			return fmt.Errorf("only operation = is support for %s", CloseStatus)
		}
		status, err := convertStatusStr(val)
		if err != nil {
			return err
		}
		if parsedQuery.closeStatus != nil && *parsedQuery.closeStatus != status {
			parsedQuery.emptyResult = true
			return nil
		}
		parsedQuery.closeStatus = status.Ptr()
	case CloseTime:
		timestamp, err := convertToTimestamp(valStr)
		if err != nil {
			return err
		}
		return p.convertCloseTime(timestamp, op, parsedQuery)
	default:
		return fmt.Errorf("unknown filter name: %s", colNameStr)
	}

	return nil
}

func (p *queryParser) convertCloseTime(timestamp int64, op string, parsedQuery *parsedQuery) error {
	switch op {
	case "=":
		if err := p.convertCloseTime(timestamp, ">=", parsedQuery); err != nil {
			return err
		}
		if err := p.convertCloseTime(timestamp, "<=", parsedQuery); err != nil {
			return err
		}
	case "<":
		parsedQuery.latestCloseTime = common.MinInt64(parsedQuery.latestCloseTime, timestamp-1)
	case "<=":
		parsedQuery.latestCloseTime = common.MinInt64(parsedQuery.latestCloseTime, timestamp)
	case ">":
		parsedQuery.earliestCloseTime = common.MaxInt64(parsedQuery.earliestCloseTime, timestamp+1)
	case ">=":
		parsedQuery.earliestCloseTime = common.MaxInt64(parsedQuery.earliestCloseTime, timestamp)
	default:
		return fmt.Errorf("operator %s is not supported for close time", op)
	}
	return nil
}

func convertToTimestamp(timeStr string) (int64, error) {
	timestamp, err := strconv.ParseInt(timeStr, 10, 64)
	if err == nil {
		return timestamp, nil
	}
	timestampStr, err := extractStringValue(timeStr)
	if err != nil {
		return 0, err
	}
	parsedTime, err := time.Parse(defaultDateTimeFormat, timestampStr)
	if err != nil {
		return 0, err
	}
	return parsedTime.UnixNano(), nil
}

func convertStatusStr(statusStr string) (shared.WorkflowExecutionCloseStatus, error) {
	statusStr = strings.ToLower(statusStr)
	switch statusStr {
	case "completed":
		return shared.WorkflowExecutionCloseStatusCompleted, nil
	case "failed":
		return shared.WorkflowExecutionCloseStatusFailed, nil
	case "canceled":
		return shared.WorkflowExecutionCloseStatusCanceled, nil
	case "continuedasnew":
		return shared.WorkflowExecutionCloseStatusContinuedAsNew, nil
	case "timedout":
		return shared.WorkflowExecutionCloseStatusTimedOut, nil
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
