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
	// ErrAlreadyTerminal indicates event cannot be applied to query because query is already in terminal state.
	ErrAlreadyTerminal = errors.New("query has already reached terminal state cannot post any new events")
	// ErrInvalidEvent indicates event cannot be applied to query in state.
	ErrInvalidEvent = errors.New("event cannot be applied to query in state")
	// ErrResultAlreadyRecorded indicates query cannot make state transition because result has already been recorded.
	ErrResultAlreadyRecorded = errors.New("result already recorded cannot make state transition")
)

const (
	// QueryStateBuffered indicates query is ready to be dispatched to worker.
	QueryStateBuffered QueryState = iota
	// QueryStateStarted indicates query has been dispatched to worker but query is not yet ready to return to caller.
	QueryStateStarted
	// QueryStateCompleted indicates query has been completed, this means result has been recorded and any
	// dependent history events have been persisted.
	QueryStateCompleted
	// QueryStateExpired means the query expired.
	QueryStateExpired
)

const (
	// QueryEventRebuffer event to move query from started back to buffered.
	QueryEventRebuffer QueryEvent = iota
	// QueryEventStart event to move query from buffered to started.
	QueryEventStart
	// QueryEventRecordResult event to record a query result, this may result in query entering complete state.
	QueryEventRecordResult
	// QueryEventPersistenceConditionSatisfied event to record that dependent history events have been persisted.
	QueryEventPersistenceConditionSatisfied
	// QueryEventExpire event to move query from buffered or started to expired.
	QueryEventExpire
)

type (
	// QueryState represents the state of the query.
	QueryState int

	// QueryEvent represents an event that can be posted to a query potentially updating the state that it is in.
	QueryEvent int

	// Query represents a query state machine.
	Query interface {
		ID() string
		QueryInput() *shared.WorkflowQuery
		QueryResult() *shared.WorkflowQueryResult
		State() QueryState
		TerminationCh() <-chan struct{}

		RecordEvent(QueryEvent, *shared.WorkflowQueryResult) (bool, error)
	}

	query struct {
		sync.RWMutex

		id                            string
		queryInput                    *shared.WorkflowQuery
		queryResult                   *shared.WorkflowQueryResult
		persistenceConditionSatisfied bool
		termCh                        chan struct{}
		state                         QueryState
	}
)

// NewQuery constructs a new Query.
func NewQuery(queryInput *shared.WorkflowQuery) Query {
	return &query{
		id:                            uuid.New(),
		queryInput:                    queryInput,
		queryResult:                   nil,
		persistenceConditionSatisfied: false,
		termCh:                        make(chan struct{}),
		state:                         QueryStateBuffered,
	}
}

// Id returns the unique identifier for this query.
func (q *query) ID() string {
	return q.id
}

// QueryInput returns the query input.
func (q *query) QueryInput() *shared.WorkflowQuery {
	return q.queryInput
}

// QueryResult returns the query result.
// This will be nil if a result has not already been recorded for this query.
// A query having a result does not mean the query is ready to unblock, use TerminationCh to know when query has entered terminal state.
func (q *query) QueryResult() *shared.WorkflowQueryResult {
	q.RLock()
	defer q.RUnlock()

	return q.queryResult
}

// State returns the state of the query.
func (q *query) State() QueryState {
	q.RLock()
	defer q.RUnlock()

	return q.state
}

// TerminationCh returns a channel that is closed once query enters terminal state.
func (q *query) TerminationCh() <-chan struct{} {
	return q.termCh
}

// RecordEvent records an event against query.
// Returns an error if event could not be applied.
// Returns true if state transitions, otherwise false.
// QueryResult can only be specified if event is QueryEventRecordResult.
// After a result is recorded it cannot be rerecorded and QueryEventRebuffer cannot be applied.
func (q *query) RecordEvent(event QueryEvent, queryResult *shared.WorkflowQueryResult) (bool, error) {
	q.Lock()
	defer q.Unlock()

	if q.state == QueryStateCompleted || q.state == QueryStateExpired {
		return false, ErrAlreadyTerminal
	}

	if event != QueryEventRecordResult && queryResult != nil {
		return false, ErrInvalidEvent
	}

	switch event {
	case QueryEventRebuffer:
		if q.state != QueryStateStarted {
			return false, ErrInvalidEvent
		}
		if q.queryResult != nil {
			return false, ErrResultAlreadyRecorded
		}
		q.state = QueryStateBuffered
		return true, nil
	case QueryEventStart:
		if q.state != QueryStateBuffered {
			return false, ErrInvalidEvent
		}
		q.state = QueryStateStarted
		return true, nil
	case QueryEventRecordResult:
		if q.state != QueryStateStarted {
			return false, ErrInvalidEvent
		}
		if queryResult == nil {
			return false, ErrInvalidEvent
		}
		if q.queryResult != nil {
			return false, ErrResultAlreadyRecorded
		}
		q.queryResult = queryResult
		return q.handleComplete(), nil
	case QueryEventPersistenceConditionSatisfied:
		q.persistenceConditionSatisfied = true
		return q.handleComplete(), nil
	case QueryEventExpire:
		q.state = QueryStateExpired
		close(q.termCh)
		return true, nil
	default:
		return false, ErrInvalidEvent
	}
}

func (q *query) handleComplete() bool {
	if q.queryResult == nil || !q.persistenceConditionSatisfied {
		return false
	}
	q.state = QueryStateCompleted
	close(q.termCh)
	return true
}
