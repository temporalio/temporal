// The MIT License (MIT)
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

package query

import (
	"sync/atomic"

	"github.com/pborman/uuid"

	"github.com/uber/cadence/.gen/go/shared"
)

const (
	// TerminationTypeCompleted means a query reaches its termination state because it has been completed
	TerminationTypeCompleted TerminationType = iota
	// TerminationTypeUnblocked means a query reaches its termination state because it has been unblocked
	TerminationTypeUnblocked
	// TerminationTypeFailed means a query reaches its termination state because it has failed
	TerminationTypeFailed
)

var (
	errTerminationStateInvalid = &shared.InternalServiceError{Message: "query termination state invalid"}
	errAlreadyInTerminalState  = &shared.InternalServiceError{Message: "query already in terminal state"}
	errQueryNotInTerminalState = &shared.InternalServiceError{Message: "query not in terminal state"}
)

type (
	// TerminationType is the type of a query's termination state
	TerminationType int

	// TerminationState describes a query's termination state
	TerminationState struct {
		TerminationType TerminationType
		QueryResult     *shared.WorkflowQueryResult
		Failure         error
	}

	query interface {
		getQueryID() string
		getQueryTermCh() <-chan struct{}
		getQueryInput() *shared.WorkflowQuery
		getTerminationState() (*TerminationState, error)
		setTerminationState(*TerminationState) error
	}

	queryImpl struct {
		id         string
		queryInput *shared.WorkflowQuery
		termCh     chan struct{}

		terminationState atomic.Value
	}
)

func newQuery(queryInput *shared.WorkflowQuery) query {
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

func (q *queryImpl) getQueryInput() *shared.WorkflowQuery {
	return q.queryInput
}

func (q *queryImpl) getTerminationState() (*TerminationState, error) {
	ts := q.terminationState.Load()
	if ts == nil {
		return nil, errQueryNotInTerminalState
	}
	return ts.(*TerminationState), nil
}

func (q *queryImpl) setTerminationState(terminationState *TerminationState) error {
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
	terminationState *TerminationState,
) error {
	if terminationState == nil {
		return errTerminationStateInvalid
	}
	switch terminationState.TerminationType {
	case TerminationTypeCompleted:
		if terminationState.QueryResult == nil || terminationState.Failure != nil {
			return errTerminationStateInvalid
		}
		queryResult := terminationState.QueryResult
		validAnswered := queryResult.GetResultType().Equals(shared.QueryResultTypeAnswered) &&
			queryResult.Answer != nil &&
			queryResult.ErrorMessage == nil
		validFailed := queryResult.GetResultType().Equals(shared.QueryResultTypeFailed) &&
			queryResult.Answer == nil &&
			queryResult.ErrorMessage != nil
		if !validAnswered && !validFailed {
			return errTerminationStateInvalid
		}
		return nil
	case TerminationTypeUnblocked:
		if terminationState.QueryResult != nil || terminationState.Failure != nil {
			return errTerminationStateInvalid
		}
		return nil
	case TerminationTypeFailed:
		if terminationState.QueryResult != nil || terminationState.Failure == nil {
			return errTerminationStateInvalid
		}
		return nil
	default:
		return errTerminationStateInvalid
	}
}
