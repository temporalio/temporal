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

	"github.com/uber/cadence/.gen/go/shared"
)

var (
	errQueryNotExists = &shared.InternalServiceError{Message: "query does not exist"}
)

type (
	queryRegistry interface {
		hasBuffered() bool
		getBufferedSnapshot() []string

		hasCompleted() bool
		getCompletedSnapshot() []string

		getQueryInternalState(string) (*queryInternalState, error)
		getQueryTermCh(string) (<-chan struct{}, error)

		bufferQuery(*shared.WorkflowQuery) (string, <-chan struct{})
		completeQuery(string, *shared.WorkflowQueryResult) error
		removeQuery(string)
	}

	queryRegistryImpl struct {
		sync.RWMutex

		buffered  map[string]query
		completed map[string]query
	}
)

func newQueryRegistry() queryRegistry {
	return &queryRegistryImpl{
		buffered:  make(map[string]query),
		completed: make(map[string]query),
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

func (r *queryRegistryImpl) getQueryInternalState(id string) (*queryInternalState, error) {
	r.RLock()
	defer r.RUnlock()

	q, err := r.getQuery(id)
	if err != nil {
		return nil, err
	}
	return q.getQueryInternalState(), nil
}

func (r *queryRegistryImpl) getQueryTermCh(id string) (<-chan struct{}, error) {
	r.RLock()
	defer r.RUnlock()

	q, err := r.getQuery(id)
	if err != nil {
		return nil, err
	}
	return q.getQueryTermCh(), nil
}

func (r *queryRegistryImpl) bufferQuery(queryInput *shared.WorkflowQuery) (string, <-chan struct{}) {
	r.Lock()
	defer r.Unlock()

	q := newQuery(queryInput)
	id := q.getQueryInternalState().id
	r.buffered[id] = q
	return id, q.getQueryTermCh()
}

func (r *queryRegistryImpl) completeQuery(id string, queryResult *shared.WorkflowQueryResult) error {
	r.Lock()
	defer r.Unlock()

	q, ok := r.buffered[id]
	if !ok {
		return errQueryNotExists
	}
	if err := q.completeQuery(queryResult); err != nil {
		return err
	}
	delete(r.buffered, id)
	r.completed[id] = q
	return nil
}

func (r *queryRegistryImpl) removeQuery(id string) {
	r.Lock()
	defer r.Unlock()

	delete(r.buffered, id)
	delete(r.completed, id)
}

func (r *queryRegistryImpl) getQuery(id string) (query, error) {
	if q, ok := r.buffered[id]; ok {
		return q, nil
	}
	if q, ok := r.completed[id]; ok {
		return q, nil
	}
	return nil, errQueryNotExists
}

func getIDs(m map[string]query) []string {
	result := make([]string, len(m), len(m))
	index := 0
	for id := range m {
		result[index] = id
		index++
	}
	return result
}
