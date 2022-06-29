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

package workflow

import (
	"sync/atomic"

	"github.com/pborman/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
)

const (
	QueryCompletionTypeSucceeded QueryCompletionType = iota
	QueryCompletionTypeUnblocked
	QueryCompletionTypeFailed
)

var (
	errCompletionStateInvalid    = serviceerror.NewInternal("query completion state invalid")
	errAlreadyInCompletionState  = serviceerror.NewInternal("query is already in completion state")
	errQueryNotInCompletionState = serviceerror.NewInternal("query is not in completion state")
)

type (
	QueryCompletionType int

	query interface {
		getID() string
		getCompletionCh() <-chan struct{}
		getQueryInput() *querypb.WorkflowQuery
		GetCompletionState() (*QueryCompletionState, error)
		setCompletionState(*QueryCompletionState) error
	}

	queryImpl struct {
		id           string
		queryInput   *querypb.WorkflowQuery
		completionCh chan struct{}

		completionState atomic.Value
	}

	QueryCompletionState struct {
		Type   QueryCompletionType
		Result *querypb.WorkflowQueryResult
		Err    error
	}
)

func newQuery(queryInput *querypb.WorkflowQuery) query {
	return &queryImpl{
		id:           uuid.New(),
		queryInput:   queryInput,
		completionCh: make(chan struct{}),
	}
}

func (q *queryImpl) getID() string {
	return q.id
}

func (q *queryImpl) getCompletionCh() <-chan struct{} {
	return q.completionCh
}

func (q *queryImpl) getQueryInput() *querypb.WorkflowQuery {
	return q.queryInput
}

func (q *queryImpl) GetCompletionState() (*QueryCompletionState, error) {
	ts := q.completionState.Load()
	if ts == nil {
		return nil, errQueryNotInCompletionState
	}
	return ts.(*QueryCompletionState), nil
}

func (q *queryImpl) setCompletionState(completionState *QueryCompletionState) error {
	if err := q.validateCompletionState(completionState); err != nil {
		return err
	}
	currCompletionState, _ := q.GetCompletionState()
	if currCompletionState != nil {
		return errAlreadyInCompletionState
	}
	q.completionState.Store(completionState)
	close(q.completionCh)
	return nil
}

func (q *queryImpl) validateCompletionState(
	completionState *QueryCompletionState,
) error {
	if completionState == nil {
		return errCompletionStateInvalid
	}
	switch completionState.Type {
	case QueryCompletionTypeSucceeded:
		if completionState.Result == nil || completionState.Err != nil {
			return errCompletionStateInvalid
		}
		queryResult := completionState.Result
		validAnswered := queryResult.GetResultType() == enumspb.QUERY_RESULT_TYPE_ANSWERED &&
			queryResult.Answer != nil &&
			queryResult.GetErrorMessage() == ""
		validFailed := queryResult.GetResultType() == enumspb.QUERY_RESULT_TYPE_FAILED &&
			queryResult.Answer == nil &&
			queryResult.GetErrorMessage() != ""
		if !validAnswered && !validFailed {
			return errCompletionStateInvalid
		}
		return nil
	case QueryCompletionTypeUnblocked:
		if completionState.Result != nil || completionState.Err != nil {
			return errCompletionStateInvalid
		}
		return nil
	case QueryCompletionTypeFailed:
		if completionState.Result != nil || completionState.Err == nil {
			return errCompletionStateInvalid
		}
		return nil
	default:
		return errCompletionStateInvalid
	}
}
