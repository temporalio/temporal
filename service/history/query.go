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
	"sync"

	"github.com/pborman/uuid"
	"github.com/uber/cadence/.gen/go/shared"
)

const (
	queryStateBuffered queryState = iota
	queryStateCompleted
)

var (
	errQueryResultIsNil      = &shared.InternalServiceError{Message: "query result is nil"}
	errQueryResultIsInvalid  = &shared.InternalServiceError{Message: "query result is invalid"}
	errQueryAlreadyCompleted = &shared.InternalServiceError{Message: "query already completed"}
)

type (
	queryState int

	query interface {
		getQueryInternalState() *queryInternalState
		getQueryTermCh() <-chan struct{}
		completeQuery(*shared.WorkflowQueryResult) error
	}

	queryImpl struct {
		id         string
		queryInput *shared.WorkflowQuery
		termCh     chan struct{}

		sync.RWMutex
		queryResult *shared.WorkflowQueryResult
		state       queryState
	}

	queryInternalState struct {
		id          string
		queryInput  *shared.WorkflowQuery
		queryResult *shared.WorkflowQueryResult
		state       queryState
	}
)

func newQuery(queryInput *shared.WorkflowQuery) query {
	return &queryImpl{
		id:          uuid.New(),
		queryInput:  queryInput,
		queryResult: nil,
		termCh:      make(chan struct{}),
		state:       queryStateBuffered,
	}
}

func (q *queryImpl) getQueryInternalState() *queryInternalState {
	q.RLock()
	defer q.RUnlock()

	return &queryInternalState{
		id:          q.id,
		queryInput:  q.queryInput,
		queryResult: q.queryResult,
		state:       q.state,
	}
}

func (q *queryImpl) getQueryTermCh() <-chan struct{} {
	return q.termCh
}

func (q *queryImpl) completeQuery(
	queryResult *shared.WorkflowQueryResult,
) error {

	q.Lock()
	defer q.Unlock()

	if q.state == queryStateCompleted {
		return errQueryAlreadyCompleted
	}
	if err := validateQueryResult(queryResult); err != nil {
		return err
	}
	q.state = queryStateCompleted
	q.queryResult = queryResult
	close(q.termCh)
	return nil
}

func validateQueryResult(
	queryResult *shared.WorkflowQueryResult,
) error {

	if queryResult == nil {
		return errQueryResultIsNil
	}
	validAnswered := queryResult.GetResultType().Equals(shared.QueryResultTypeAnswered) &&
		queryResult.Answer != nil &&
		queryResult.ErrorDetails == nil &&
		queryResult.ErrorReason == nil

	validFailed := queryResult.GetResultType().Equals(shared.QueryResultTypeFailed) &&
		queryResult.Answer == nil &&
		queryResult.ErrorDetails != nil &&
		queryResult.ErrorReason != nil

	if !validAnswered && !validFailed {
		return errQueryResultIsInvalid
	}
	return nil
}
