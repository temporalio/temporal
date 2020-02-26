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

package s3store

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/xwb1989/sqlparser"

	"github.com/uber/cadence/common"
)

type (
	// QueryParser parses a limited SQL where clause into a struct
	QueryParser interface {
		Parse(query string) (*parsedQuery, error)
	}

	queryParser struct{}

	parsedQuery struct {
		workflowTypeName *string
		workflowID       *string
		startTime        *int64
		closeTime        *int64
		searchPrecision  *string
	}
)

// All allowed fields for filtering
const (
	WorkflowTypeName = "WorkflowTypeName"
	WorkflowID       = "WorkflowID"
	StartTime        = "StartTime"
	CloseTime        = "CloseTime"
	SearchPrecision  = "SearchPrecision"
)

// Precision specific values
const (
	PrecisionDay    = "Day"
	PrecisionHour   = "Hour"
	PrecisionMinute = "Minute"
	PrecisionSecond = "Second"
)
const (
	queryTemplate         = "select * from dummy where %s"
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
	parsedQuery := &parsedQuery{}
	if err := p.convertWhereExpr(whereExpr, parsedQuery); err != nil {
		return nil, err
	}
	if parsedQuery.workflowID == nil && parsedQuery.workflowTypeName == nil {
		return nil, errors.New("WorkflowID or WorkflowTypeName is required in query")
	}
	if parsedQuery.workflowID != nil && parsedQuery.workflowTypeName != nil {
		return nil, errors.New("only one of WorkflowID or WorkflowTypeName can be specified in a query")
	}
	if parsedQuery.closeTime != nil && parsedQuery.startTime != nil {
		return nil, errors.New("only one of StartTime or CloseTime can be specified in a query")
	}
	if (parsedQuery.closeTime != nil || parsedQuery.startTime != nil) && parsedQuery.searchPrecision == nil {
		return nil, errors.New("SearchPrecision is required when searching for a StartTime or CloseTime")
	}

	if parsedQuery.closeTime == nil && parsedQuery.startTime == nil && parsedQuery.searchPrecision != nil {
		return nil, errors.New("SearchPrecision requires a StartTime or CloseTime")
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
	case WorkflowTypeName:
		val, err := extractStringValue(valStr)
		if err != nil {
			return err
		}
		if op != "=" {
			return fmt.Errorf("only operation = is support for %s", WorkflowTypeName)
		}
		if parsedQuery.workflowTypeName != nil {
			return fmt.Errorf("can not query %s multiple times", WorkflowTypeName)
		}
		parsedQuery.workflowTypeName = common.StringPtr(val)
	case WorkflowID:
		val, err := extractStringValue(valStr)
		if err != nil {
			return err
		}
		if op != "=" {
			return fmt.Errorf("only operation = is support for %s", WorkflowID)
		}
		if parsedQuery.workflowID != nil {
			return fmt.Errorf("can not query %s multiple times", WorkflowID)
		}
		parsedQuery.workflowID = common.StringPtr(val)
	case CloseTime:
		timestamp, err := convertToTimestamp(valStr)
		if err != nil {
			return err
		}
		if op != "=" {
			return fmt.Errorf("only operation = is support for %s", CloseTime)
		}
		parsedQuery.closeTime = &timestamp
	case StartTime:
		timestamp, err := convertToTimestamp(valStr)
		if err != nil {
			return err
		}
		if op != "=" {
			return fmt.Errorf("only operation = is support for %s", CloseTime)
		}
		parsedQuery.startTime = &timestamp
	case SearchPrecision:
		val, err := extractStringValue(valStr)
		if err != nil {
			return err
		}
		if op != "=" {
			return fmt.Errorf("only operation = is support for %s", SearchPrecision)
		}
		if parsedQuery.searchPrecision != nil && *parsedQuery.searchPrecision != val {
			return fmt.Errorf("only one expression is allowed for %s", SearchPrecision)
		}
		switch val {
		case PrecisionDay:
		case PrecisionHour:
		case PrecisionMinute:
		case PrecisionSecond:
		default:
			return fmt.Errorf("invalid value for %s: %s", SearchPrecision, val)
		}
		parsedQuery.searchPrecision = common.StringPtr(val)

	default:
		return fmt.Errorf("unknown filter name: %s", colNameStr)
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

func extractStringValue(s string) (string, error) {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1], nil
	}
	return "", fmt.Errorf("value %s is not a string value", s)
}
