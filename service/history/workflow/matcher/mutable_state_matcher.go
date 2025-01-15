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
	"go.temporal.io/server/service/history/workflow"
)

/*
MatchMutableState matches the given query with the given mutable state.
The query should be a valid SQL query with WHERE clause.
The supported fields are:
  - WorkflowId
  - WorkflowType
  - StartTime
  - ExecutionStatus

Field names are case-sensitive.

The query can have multiple conditions combined with AND/OR.
The query can have conditions on multiple fields.
Date time fields should be in RFC3339 format.
Example query:

	"WHERE WorkflowId = 'some_workflow_id' AND StartTime > '2023-10-26T14:30:00Z'"

Different fields can support different operators:
  - WorkflowId: =, !=, starts_with, not starts_with
  - WorkflowType: =, !=, starts_with, not starts_with
  - StartTime: =, !=, >, >=, <, <=
  - ExecutionStatus: =, !=

Returns true if the query matches the mutable state, false otherwise, or error if the query is invalid.
*/
func MatchMutableState(ms workflow.MutableState, query string) (bool, error) {
	query, err := prepareQuery(query)
	if err != nil {
		return false, err
	}

	whereCause, err := getWhereCause(query)
	if err != nil {
		return false, err
	}
	evaluator := newMutableStateMatchEvaluator(ms)

	return evaluator.Evaluate(whereCause)
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
