// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to qvom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, qvETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package validator

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

// VisibilityQueryValidator for sql query validation
type VisibilityQueryValidator struct {
	validSearchAttributes dynamicconfig.MapPropertyFn
}

// NewQueryValidator create VisibilityQueryValidator
func NewQueryValidator(validSearchAttributes dynamicconfig.MapPropertyFn) *VisibilityQueryValidator {
	return &VisibilityQueryValidator{
		validSearchAttributes: validSearchAttributes,
	}
}

// ValidateListRequestForQuery validate that search attributes in listRequest query is legal,
// and add prefix for custom keys
func (qv *VisibilityQueryValidator) ValidateListRequestForQuery(listRequest *workflow.ListWorkflowExecutionsRequest) error {
	whereClause := listRequest.GetQuery()
	newQuery, err := qv.validateListOrCountRequestForQuery(whereClause)
	if err != nil {
		return err
	}
	listRequest.Query = common.StringPtr(newQuery)
	return nil
}

// ValidateCountRequestForQuery validate that search attributes in countRequest query is legal,
// and add prefix for custom keys
func (qv *VisibilityQueryValidator) ValidateCountRequestForQuery(countRequest *workflow.CountWorkflowExecutionsRequest) error {
	whereClause := countRequest.GetQuery()
	newQuery, err := qv.validateListOrCountRequestForQuery(whereClause)
	if err != nil {
		return err
	}
	countRequest.Query = common.StringPtr(newQuery)
	return nil
}

// validateListOrCountRequestForQuery valid sql for visibility API
// it also adds attr prefix for customized fields
func (qv *VisibilityQueryValidator) validateListOrCountRequestForQuery(whereClause string) (string, error) {
	if len(whereClause) != 0 {
		// Build a placeholder query that allows us to easily parse the contents of the where clause.
		// IMPORTANT: This query is never executed, it is just used to parse and validate whereClause
		var placeholderQuery string
		whereClause := strings.TrimSpace(whereClause)
		// #nosec
		if common.IsJustOrderByClause(whereClause) { // just order by
			placeholderQuery = fmt.Sprintf("SELECT * FROM dummy %s", whereClause)
		} else {
			placeholderQuery = fmt.Sprintf("SELECT * FROM dummy WHERE %s", whereClause)
		}

		stmt, err := sqlparser.Parse(placeholderQuery)
		if err != nil {
			return "", &workflow.BadRequestError{Message: "Invalid query."}
		}

		sel, ok := stmt.(*sqlparser.Select)
		if !ok {
			return "", &workflow.BadRequestError{Message: "Invalid select query."}
		}
		buf := sqlparser.NewTrackedBuffer(nil)
		// validate where expr
		if sel.Where != nil {
			err = qv.validateWhereExpr(sel.Where.Expr)
			if err != nil {
				return "", &workflow.BadRequestError{Message: err.Error()}
			}
			sel.Where.Expr.Format(buf)
		}
		// validate order by
		err = qv.validateOrderByExpr(sel.OrderBy)
		if err != nil {
			return "", &workflow.BadRequestError{Message: err.Error()}
		}
		sel.OrderBy.Format(buf)

		return buf.String(), nil
	}
	return whereClause, nil
}

func (qv *VisibilityQueryValidator) validateWhereExpr(expr sqlparser.Expr) error {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *sqlparser.AndExpr, *sqlparser.OrExpr:
		return qv.validateAndOrExpr(expr)
	case *sqlparser.ComparisonExpr:
		return qv.validateComparisonExpr(expr)
	case *sqlparser.RangeCond:
		return qv.validateRangeExpr(expr)
	case *sqlparser.ParenExpr:
		return qv.validateWhereExpr(expr.Expr)
	default:
		return errors.New("invalid where clause")
	}

}

func (qv *VisibilityQueryValidator) validateAndOrExpr(expr sqlparser.Expr) error {
	var leftExpr sqlparser.Expr
	var rightExpr sqlparser.Expr

	switch expr := expr.(type) {
	case *sqlparser.AndExpr:
		leftExpr = expr.Left
		rightExpr = expr.Right
	case *sqlparser.OrExpr:
		leftExpr = expr.Left
		rightExpr = expr.Right
	}

	if err := qv.validateWhereExpr(leftExpr); err != nil {
		return err
	}
	return qv.validateWhereExpr(rightExpr)
}

func (qv *VisibilityQueryValidator) validateComparisonExpr(expr sqlparser.Expr) error {
	comparisonExpr := expr.(*sqlparser.ComparisonExpr)
	colName, ok := comparisonExpr.Left.(*sqlparser.ColName)
	if !ok {
		return errors.New("invalid comparison expression")
	}
	colNameStr := colName.Name.String()
	if qv.isValidSearchAttributes(colNameStr) {
		if !definition.IsSystemIndexedKey(colNameStr) { // add search attribute prefix
			comparisonExpr.Left = &sqlparser.ColName{
				Metadata:  colName.Metadata,
				Name:      sqlparser.NewColIdent(definition.Attr + "." + colNameStr),
				Qualifier: colName.Qualifier,
			}
		}
		return nil
	}
	return errors.New("invalid search attribute")
}

func (qv *VisibilityQueryValidator) validateRangeExpr(expr sqlparser.Expr) error {
	rangeCond := expr.(*sqlparser.RangeCond)
	colName, ok := rangeCond.Left.(*sqlparser.ColName)
	if !ok {
		return errors.New("invalid range expression")
	}
	colNameStr := colName.Name.String()
	if qv.isValidSearchAttributes(colNameStr) {
		if !definition.IsSystemIndexedKey(colNameStr) { // add search attribute prefix
			rangeCond.Left = &sqlparser.ColName{
				Metadata:  colName.Metadata,
				Name:      sqlparser.NewColIdent(definition.Attr + "." + colNameStr),
				Qualifier: colName.Qualifier,
			}
		}
		return nil
	}
	return errors.New("invalid search attribute")
}

func (qv *VisibilityQueryValidator) validateOrderByExpr(orderBy sqlparser.OrderBy) error {
	for _, orderByExpr := range orderBy {
		colName, ok := orderByExpr.Expr.(*sqlparser.ColName)
		if !ok {
			return errors.New("invalid order by expression")
		}
		colNameStr := colName.Name.String()
		if qv.isValidSearchAttributes(colNameStr) {
			if !definition.IsSystemIndexedKey(colNameStr) { // add search attribute prefix
				orderByExpr.Expr = &sqlparser.ColName{
					Metadata:  colName.Metadata,
					Name:      sqlparser.NewColIdent(definition.Attr + "." + colNameStr),
					Qualifier: colName.Qualifier,
				}
			}
		} else {
			return errors.New("invalid order by attribute")
		}
	}
	return nil
}

// isValidSearchAttributes return true if key is registered
func (qv *VisibilityQueryValidator) isValidSearchAttributes(key string) bool {
	validAttr := qv.validSearchAttributes()
	_, isValidKey := validAttr[key]
	return isValidKey
}
