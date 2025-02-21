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

	"github.com/temporalio/sqlparser"
)

type Evaluator interface {
	Evaluate(query string) (bool, error)
}

func getWhereCause(query string) (sqlparser.Expr, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, NewMatcherError("%s: %v", malformedSqlQueryErrMessage, err)
	}

	selectStmt, isSelect := stmt.(*sqlparser.Select)
	if !isSelect {
		return nil, NewMatcherError("%s: statement must be 'select' not %T", notSupportedErrMessage, stmt)
	}

	if selectStmt.Limit != nil {
		return nil, NewMatcherError("%s: 'limit' clause", notSupportedErrMessage)
	}

	if selectStmt.Where == nil {
		return nil, NewMatcherError("%s: 'where' clause is missing", notSupportedErrMessage)
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

func compareQueryString(inStr string, expectedStr string, operation string, fieldName string) (bool, error) {
	if len(inStr) == 0 {
		return false, NewMatcherError("%s cannot be empty", fieldName)
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
		return false, NewMatcherError("%s: operation %s is not supported for %s filter", invalidExpressionErrMessage, operation, fieldName)
	}
}

func compareQueryInt(inVal int, expectedVal int, operation string, fieldName string) (bool, error) {
	switch operation {
	case sqlparser.EqualStr:
		return inVal == expectedVal, nil
	case sqlparser.NotEqualStr:
		return inVal != expectedVal, nil
	case sqlparser.GreaterThanStr:
		return expectedVal > inVal, nil
	case sqlparser.GreaterEqualStr:
		return expectedVal >= inVal, nil
	case sqlparser.LessThanStr:
		return expectedVal < inVal, nil
	case sqlparser.LessEqualStr:
		return expectedVal <= inVal, nil
	default:
		return false, NewMatcherError("%s: operation %s is not supported for %s filter", invalidExpressionErrMessage, operation, fieldName)
	}
}
