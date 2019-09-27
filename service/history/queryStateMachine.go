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
	"errors"
	"sync"

	"github.com/pborman/uuid"
	"github.com/uber/cadence/.gen/go/shared"
)

var (
	errAlreadyCompleted      = errors.New("query has already been completed, cannot post any new events")
	errInvalidEvent          = errors.New("event cannot be applied to query in state")
	errResultAlreadyRecorded = errors.New("result already recorded cannot make state transition")
)

const (
	queryStateBuffered queryState = iota
	queryStateStarted
	queryStateCompleted
)

const (
	queryEventRebuffer queryEvent = iota
	queryEventStart
	queryEventRecordResult
	queryEventPersistenceConditionSatisfied
)

type (
	queryState int
	queryEvent int

	queryStateMachine interface {
		getQuerySnapshot() *querySnapshot
		getQueryTermCh() <-chan struct{}
		recordEvent(queryEvent, *shared.WorkflowQueryResult) (bool, error)
	}

	queryStateMachineImpl struct {
		sync.RWMutex

		id                            string
		queryInput                    *shared.WorkflowQuery
		queryResult                   *shared.WorkflowQueryResult
		persistenceConditionSatisfied bool
		termCh                        chan struct{}
		state                         queryState
	}

	querySnapshot struct {
		id          string
		queryInput  *shared.WorkflowQuery
		queryResult *shared.WorkflowQueryResult
		state       queryState
	}
)

func newQueryStateMachine(queryInput *shared.WorkflowQuery) queryStateMachine {
	return &queryStateMachineImpl{
		id:                            uuid.New(),
		queryInput:                    queryInput,
		queryResult:                   nil,
		persistenceConditionSatisfied: false,
		termCh:                        make(chan struct{}),
		state:                         queryStateBuffered,
	}
}

func (q *queryStateMachineImpl) getQuerySnapshot() *querySnapshot {
	q.RLock()
	defer q.RUnlock()

	return &querySnapshot{
		id:          q.id,
		queryInput:  q.queryInput,
		queryResult: q.queryResult,
		state:       q.state,
	}
}

func (q *queryStateMachineImpl) getQueryTermCh() <-chan struct{} {
	q.RLock()
	defer q.RUnlock()

	return q.termCh
}

func (q *queryStateMachineImpl) recordEvent(event queryEvent, queryResult *shared.WorkflowQueryResult) (bool, error) {
	q.Lock()
	defer q.Unlock()

	if q.state == queryStateCompleted {
		return false, errAlreadyCompleted
	}

	if event != queryEventRecordResult && queryResult != nil {
		return false, errInvalidEvent
	}

	switch event {
	case queryEventRebuffer:
		if q.state != queryStateStarted {
			return false, errInvalidEvent
		}
		if q.queryResult != nil {
			return false, errResultAlreadyRecorded
		}
		q.state = queryStateBuffered
		return true, nil
	case queryEventStart:
		if q.state != queryStateBuffered {
			return false, errInvalidEvent
		}
		q.state = queryStateStarted
		return true, nil
	case queryEventRecordResult:
		if q.state != queryStateStarted {
			return false, errInvalidEvent
		}
		if queryResult == nil {
			return false, errInvalidEvent
		}
		if q.queryResult != nil {
			return false, errResultAlreadyRecorded
		}
		q.queryResult = queryResult
		return q.handleComplete(), nil
	case queryEventPersistenceConditionSatisfied:
		q.persistenceConditionSatisfied = true
		return q.handleComplete(), nil
	default:
		panic("invalid event")
	}
}

func (q *queryStateMachineImpl) handleComplete() bool {
	if q.queryResult == nil || !q.persistenceConditionSatisfied {
		return false
	}
	q.state = queryStateCompleted
	close(q.termCh)
	return true
}
