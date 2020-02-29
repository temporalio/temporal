// The MIT License (MIT)
//
// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package history

import (
	"sync/atomic"

	"github.com/pborman/uuid"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
)

const (
	queryTerminationTypeCompleted queryTerminationType = iota
	queryTerminationTypeUnblocked
	queryTerminationTypeFailed
)

var (
	errTerminationStateInvalid = serviceerror.NewInternal("query termination state invalid")
	errAlreadyInTerminalState  = serviceerror.NewInternal("query already in terminal state")
	errQueryNotInTerminalState = serviceerror.NewInternal("query not in terminal state")
)

type (
	queryTerminationType int

	query interface {
		getQueryID() string
		getQueryTermCh() <-chan struct{}
		getQueryInput() *commonproto.WorkflowQuery
		getTerminationState() (*queryTerminationState, error)
		setTerminationState(*queryTerminationState) error
	}

	queryImpl struct {
		id         string
		queryInput *commonproto.WorkflowQuery
		termCh     chan struct{}

		terminationState atomic.Value
	}

	queryTerminationState struct {
		queryTerminationType queryTerminationType
		queryResult          *commonproto.WorkflowQueryResult
		failure              error
	}
)

func newQuery(queryInput *commonproto.WorkflowQuery) query {
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

func (q *queryImpl) getQueryInput() *commonproto.WorkflowQuery {
	return q.queryInput
}

func (q *queryImpl) getTerminationState() (*queryTerminationState, error) {
	ts := q.terminationState.Load()
	if ts == nil {
		return nil, errQueryNotInTerminalState
	}
	return ts.(*queryTerminationState), nil
}

func (q *queryImpl) setTerminationState(terminationState *queryTerminationState) error {
	if err := q.validateTerminationState(terminationState); err != nil {
		return err
	}
	currTerminationState, _ := q.getTerminationState()
	if currTerminationState != nil {
		return errAlreadyInTerminalState
	}
	q.terminationState.Store(terminationState)
	close(q.termCh)
	return nil
}

func (q *queryImpl) validateTerminationState(
	terminationState *queryTerminationState,
) error {
	if terminationState == nil {
		return errTerminationStateInvalid
	}
	switch terminationState.queryTerminationType {
	case queryTerminationTypeCompleted:
		if terminationState.queryResult == nil || terminationState.failure != nil {
			return errTerminationStateInvalid
		}
		queryResult := terminationState.queryResult
		validAnswered := queryResult.GetResultType() == enums.QueryResultTypeAnswered &&
			queryResult.Answer != nil &&
			queryResult.GetErrorMessage() == ""
		validFailed := queryResult.GetResultType() == enums.QueryResultTypeFailed &&
			queryResult.Answer == nil &&
			queryResult.GetErrorMessage() != ""
		if !validAnswered && !validFailed {
			return errTerminationStateInvalid
		}
		return nil
	case queryTerminationTypeUnblocked:
		if terminationState.queryResult != nil || terminationState.failure != nil {
			return errTerminationStateInvalid
		}
		return nil
	case queryTerminationTypeFailed:
		if terminationState.queryResult != nil || terminationState.failure == nil {
			return errTerminationStateInvalid
		}
		return nil
	default:
		return errTerminationStateInvalid
	}
}
