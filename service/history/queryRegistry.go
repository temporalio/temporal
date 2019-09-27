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

	"github.com/uber/cadence/.gen/go/shared"
)

type (
	queryRegistry interface {
		hasBuffered() bool
		getBufferedSnapshot() []string

		hasStarted() bool
		getStartedSnapshot() []string

		hasCompleted() bool
		getCompletedSnapshot() []string

		getQuerySnapshot(string) (*querySnapshot, error)
		getQueryTermCh(string) (<-chan struct{}, error)

		bufferQuery(*shared.WorkflowQuery) (string, *querySnapshot, <-chan struct{})
		recordEvent(string, queryEvent, *shared.WorkflowQueryResult) (bool, error)
		removeQuery(string)
	}

	queryRegistryImpl struct {
		sync.RWMutex

		buffered  map[string]queryStateMachine
		started   map[string]queryStateMachine
		completed map[string]queryStateMachine
	}
)

func newQueryRegistry() queryRegistry {
	return &queryRegistryImpl{
		buffered:  make(map[string]queryStateMachine),
		started:   make(map[string]queryStateMachine),
		completed: make(map[string]queryStateMachine),
	}
}

func (r *queryRegistryImpl) hasBuffered() bool {
	r.RLock()
	defer r.RUnlock()

	return len(r.buffered) > 0
}

func (r *queryRegistryImpl) getBufferedSnapshot() []string {
	r.RLock()
	defer r.RUnlock()

	return getIDs(r.buffered)
}

func (r *queryRegistryImpl) hasStarted() bool {
	r.RLock()
	defer r.RUnlock()

	return len(r.started) > 0
}

func (r *queryRegistryImpl) getStartedSnapshot() []string {
	r.RLock()
	defer r.RUnlock()

	return getIDs(r.started)
}

func (r *queryRegistryImpl) hasCompleted() bool {
	r.RLock()
	defer r.RUnlock()

	return len(r.completed) > 0
}

func (r *queryRegistryImpl) getCompletedSnapshot() []string {
	r.RLock()
	defer r.RUnlock()

	return getIDs(r.completed)
}

func (r *queryRegistryImpl) getQuerySnapshot(id string) (*querySnapshot, error) {
	r.RLock()
	defer r.RUnlock()

	qsm, err := r.getQueryStateMachine(id)
	if err != nil {
		return nil, err
	}
	return qsm.getQuerySnapshot(), nil
}

func (r *queryRegistryImpl) getQueryTermCh(id string) (<-chan struct{}, error) {
	r.RLock()
	defer r.RUnlock()

	qsm, err := r.getQueryStateMachine(id)
	if err != nil {
		return nil, err
	}
	return qsm.getQueryTermCh(), nil
}

func (r *queryRegistryImpl) bufferQuery(queryInput *shared.WorkflowQuery) (string, *querySnapshot, <-chan struct{}) {
	r.Lock()
	defer r.Unlock()

	qsm := newQueryStateMachine(queryInput)
	id := qsm.getQuerySnapshot().id
	r.buffered[id] = qsm
	return id, qsm.getQuerySnapshot(), qsm.getQueryTermCh()
}

func (r *queryRegistryImpl) recordEvent(id string, event queryEvent, queryResult *shared.WorkflowQueryResult) (bool, error) {
	r.Lock()
	defer r.Unlock()

	qsm, err := r.getQueryStateMachine(id)
	if err != nil {
		return false, err
	}
	changed, err := qsm.recordEvent(event, queryResult)
	if err != nil {
		return false, err
	}
	if !changed {
		return false, nil
	}
	delete(r.buffered, id)
	delete(r.started, id)
	delete(r.completed, id)
	switch qsm.getQuerySnapshot().state {
	case queryStateBuffered:
		r.buffered[id] = qsm
	case queryStateStarted:
		r.started[id] = qsm
	case queryStateCompleted:
		r.completed[id] = qsm
	default:
		panic("unknown query state")
	}
	return true, nil
}

func (r *queryRegistryImpl) removeQuery(id string) {
	r.Lock()
	defer r.Unlock()

	delete(r.buffered, id)
	delete(r.started, id)
	delete(r.completed, id)
}

func (r *queryRegistryImpl) getQueryStateMachine(id string) (queryStateMachine, error) {
	if qsm, ok := r.buffered[id]; ok {
		return qsm, nil
	}
	if qsm, ok := r.started[id]; ok {
		return qsm, nil
	}
	if qsm, ok := r.completed[id]; ok {
		return qsm, nil
	}
	return nil, errors.New("query does not exist")
}

func getIDs(m map[string]queryStateMachine) []string {
	result := make([]string, len(m), len(m))
	index := 0
	for id := range m {
		result[index] = id
		index++
	}
	return result
}
