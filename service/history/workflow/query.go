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
	QueryTerminationTypeCompleted QueryTerminationType = iota
	QueryTerminationTypeUnblocked
	QueryTerminationTypeFailed
)

var (
	errTerminationStateInvalid = serviceerror.NewInternal("query termination state invalid")
	errAlreadyInTerminalState  = serviceerror.NewInternal("query already in terminal state")
	errQueryNotInTerminalState = serviceerror.NewInternal("query not in terminal state")
)

type (
	QueryTerminationType int

	query interface {
		getQueryID() string
		getQueryTermCh() <-chan struct{}
		getQueryInput() *querypb.WorkflowQuery
		GetTerminationState() (*QueryTerminationState, error)
		setTerminationState(*QueryTerminationState) error
	}

	queryImpl struct {
		id         string
		queryInput *querypb.WorkflowQuery
		termCh     chan struct{}

		terminationState atomic.Value
	}

	QueryTerminationState struct {
		QueryTerminationType QueryTerminationType
		QueryResult          *querypb.WorkflowQueryResult
		Failure              error
	}
)

func newQuery(queryInput *querypb.WorkflowQuery) query {
	return &queryImpl{
		id:         uuid.New(),
		queryInput: queryInput,
		termCh:     make(chan struct{}),
	}
}

func (q *queryImpl) getQueryID() string {
	return q.id
}

func (q *queryImpl) getQueryTermCh() <-chan struct{} {
	return q.termCh
}

func (q *queryImpl) getQueryInput() *querypb.WorkflowQuery {
	return q.queryInput
}

func (q *queryImpl) GetTerminationState() (*QueryTerminationState, error) {
	ts := q.terminationState.Load()
	if ts == nil {
		return nil, errQueryNotInTerminalState
	}
	return ts.(*QueryTerminationState), nil
}

func (q *queryImpl) setTerminationState(terminationState *QueryTerminationState) error {
	if err := q.validateTerminationState(terminationState); err != nil {
		return err
	}
	currTerminationState, _ := q.GetTerminationState()
	if currTerminationState != nil {
		return errAlreadyInTerminalState
	}
	q.terminationState.Store(terminationState)
	close(q.termCh)
	return nil
}

func (q *queryImpl) validateTerminationState(
	terminationState *QueryTerminationState,
) error {
	if terminationState == nil {
		return errTerminationStateInvalid
	}
	switch terminationState.QueryTerminationType {
	case QueryTerminationTypeCompleted:
		if terminationState.QueryResult == nil || terminationState.Failure != nil {
			return errTerminationStateInvalid
		}
		queryResult := terminationState.QueryResult
		validAnswered := queryResult.GetResultType() == enumspb.QUERY_RESULT_TYPE_ANSWERED &&
			queryResult.Answer != nil &&
			queryResult.GetErrorMessage() == ""
		validFailed := queryResult.GetResultType() == enumspb.QUERY_RESULT_TYPE_FAILED &&
			queryResult.Answer == nil &&
			queryResult.GetErrorMessage() != ""
		if !validAnswered && !validFailed {
			return errTerminationStateInvalid
		}
		return nil
	case QueryTerminationTypeUnblocked:
		if terminationState.QueryResult != nil || terminationState.Failure != nil {
			return errTerminationStateInvalid
		}
		return nil
	case QueryTerminationTypeFailed:
		if terminationState.QueryResult != nil || terminationState.Failure == nil {
			return errTerminationStateInvalid
		}
		return nil
	default:
		return errTerminationStateInvalid
	}
}
