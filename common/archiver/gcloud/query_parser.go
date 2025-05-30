//go:generate mockgen -package $GOPACKAGE -source query_parser.go -destination query_parser_mock.go -mock_names Interface=MockQueryParser

package gcloud

import (
	"errors"
	"fmt"
	"time"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/sqlquery"
	"go.temporal.io/server/common/util"
)

type (
	// QueryParser parses a limited SQL where clause into a struct
	QueryParser interface {
		Parse(query string) (*parsedQuery, error)
	}

	queryParser struct{}

	parsedQuery struct {
		workflowID      *string
		workflowType    *string
		startTime       time.Time
		closeTime       time.Time
		searchPrecision *string
		runID           *string
		emptyResult     bool
	}
)

// All allowed fields for filtering
const (
	WorkflowID      = "WorkflowId"
	RunID           = "RunId"
	WorkflowType    = "WorkflowType"
	CloseTime       = "CloseTime"
	StartTime       = "StartTime"
	SearchPrecision = "SearchPrecision"
)

// Precision specific values
const (
	PrecisionDay    = "Day"
	PrecisionHour   = "Hour"
	PrecisionMinute = "Minute"
	PrecisionSecond = "Second"
)

// NewQueryParser creates a new query parser for filestore
func NewQueryParser() QueryParser {
	return &queryParser{}
}

func (p *queryParser) Parse(query string) (*parsedQuery, error) {
	stmt, err := sqlparser.Parse(fmt.Sprintf(sqlquery.QueryTemplate, query))
	if err != nil {
		return nil, err
	}
	whereExpr := stmt.(*sqlparser.Select).Where.Expr
	parsedQuery := &parsedQuery{}
	if err := p.convertWhereExpr(whereExpr, parsedQuery); err != nil {
		return nil, err
	}

	if (parsedQuery.closeTime.IsZero() && parsedQuery.startTime.IsZero()) || (!parsedQuery.closeTime.IsZero() && !parsedQuery.startTime.IsZero()) {
		return nil, errors.New("requires a StartTime or CloseTime")
	}

	if parsedQuery.searchPrecision == nil {
		return nil, errors.New("SearchPrecision is required when searching for a StartTime or CloseTime")
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
		val, err := sqlquery.ExtractStringValue(valStr)
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
		parsedQuery.workflowID = util.Ptr(val)
	case RunID:
		val, err := sqlquery.ExtractStringValue(valStr)
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
		parsedQuery.runID = util.Ptr(val)
	case CloseTime:
		closeTime, err := sqlquery.ConvertToTime(valStr)
		if err != nil {
			return err
		}
		if op != "=" {
			return fmt.Errorf("only operation = is support for %s", CloseTime)
		}
		parsedQuery.closeTime = closeTime

	case StartTime:
		startTime, err := sqlquery.ConvertToTime(valStr)
		if err != nil {
			return err
		}
		if op != "=" {
			return fmt.Errorf("only operation = is support for %s", CloseTime)
		}
		parsedQuery.startTime = startTime
	case WorkflowType:
		val, err := sqlquery.ExtractStringValue(valStr)
		if err != nil {
			return err
		}
		if op != "=" {
			return fmt.Errorf("only operation = is support for %s", WorkflowType)
		}
		if parsedQuery.workflowType != nil && *parsedQuery.workflowType != val {
			parsedQuery.emptyResult = true
			return nil
		}
		parsedQuery.workflowType = util.Ptr(val)
	case SearchPrecision:
		val, err := sqlquery.ExtractStringValue(valStr)
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
		parsedQuery.searchPrecision = util.Ptr(val)
	default:
		return fmt.Errorf("unknown filter name: %s", colNameStr)
	}

	return nil
}
