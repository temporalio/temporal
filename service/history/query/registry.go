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
	"sync"

	"github.com/uber/cadence/.gen/go/shared"
)

var (
	errQueryNotExists = &shared.InternalServiceError{Message: "query does not exist"}
)

type (
	// Registry manages all the queries for a workflow
	Registry interface {
		HasBufferedQuery() bool
		GetBufferedIDs() []string
		HasCompletedQuery() bool
		GetCompletedIDs() []string
		HasUnblockedQuery() bool
		GetUnblockedIDs() []string
		HasFailedQuery() bool
		GetFailedIDs() []string

		GetQueryTermCh(string) (<-chan struct{}, error)
		GetQueryInput(string) (*shared.WorkflowQuery, error)
		GetTerminationState(string) (*TerminationState, error)

		BufferQuery(queryInput *shared.WorkflowQuery) (string, <-chan struct{})
		SetTerminationState(string, *TerminationState) error
		RemoveQuery(id string)
	}

	registryImpl struct {
		sync.RWMutex

		buffered  map[string]query
		completed map[string]query
		unblocked map[string]query
		failed    map[string]query
	}
)

// NewRegistry creates a new query registry
func NewRegistry() Registry {
	return &registryImpl{
		buffered:  make(map[string]query),
		completed: make(map[string]query),
		unblocked: make(map[string]query),
		failed:    make(map[string]query),
	}
}

func (r *registryImpl) HasBufferedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.buffered) > 0
}

func (r *registryImpl) GetBufferedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.buffered)
}

func (r *registryImpl) HasCompletedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.completed) > 0
}

func (r *registryImpl) GetCompletedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.completed)
}

func (r *registryImpl) HasUnblockedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.unblocked) > 0
}

func (r *registryImpl) GetUnblockedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.unblocked)
}

func (r *registryImpl) HasFailedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.failed) > 0
}

func (r *registryImpl) GetFailedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.failed)
}

func (r *registryImpl) GetQueryTermCh(id string) (<-chan struct{}, error) {
	r.RLock()
	defer r.RUnlock()
	q, err := r.getQueryNoLock(id)
	if err != nil {
		return nil, err
	}
	return q.getQueryTermCh(), nil
}

func (r *registryImpl) GetQueryInput(id string) (*shared.WorkflowQuery, error) {
	r.RLock()
	defer r.RUnlock()
	q, err := r.getQueryNoLock(id)
	if err != nil {
		return nil, err
	}
	return q.getQueryInput(), nil
}

func (r *registryImpl) GetTerminationState(id string) (*TerminationState, error) {
	r.RLock()
	defer r.RUnlock()
	q, err := r.getQueryNoLock(id)
	if err != nil {
		return nil, err
	}
	return q.getTerminationState()
}

func (r *registryImpl) BufferQuery(queryInput *shared.WorkflowQuery) (string, <-chan struct{}) {
	r.Lock()
	defer r.Unlock()
	q := newQuery(queryInput)
	id := q.getQueryID()
	r.buffered[id] = q
	return id, q.getQueryTermCh()
}

func (r *registryImpl) SetTerminationState(id string, TerminationState *TerminationState) error {
	r.Lock()
	defer r.Unlock()
	q, ok := r.buffered[id]
	if !ok {
		return errQueryNotExists
	}
	if err := q.setTerminationState(TerminationState); err != nil {
		return err
	}
	delete(r.buffered, id)
	switch TerminationState.TerminationType {
	case TerminationTypeCompleted:
		r.completed[id] = q
	case TerminationTypeUnblocked:
		r.unblocked[id] = q
	case TerminationTypeFailed:
		r.failed[id] = q
	}
	return nil
}

func (r *registryImpl) RemoveQuery(id string) {
	r.Lock()
	defer r.Unlock()
	delete(r.buffered, id)
	delete(r.completed, id)
	delete(r.unblocked, id)
	delete(r.failed, id)
}

func (r *registryImpl) getQueryNoLock(id string) (query, error) {
	if q, ok := r.buffered[id]; ok {
		return q, nil
	}
	if q, ok := r.completed[id]; ok {
		return q, nil
	}
	if q, ok := r.unblocked[id]; ok {
		return q, nil
	}
	if q, ok := r.failed[id]; ok {
		return q, nil
	}
	return nil, errQueryNotExists
}

func (r *registryImpl) getIDs(m map[string]query) []string {
	result := make([]string, len(m), len(m))
	index := 0
	for id := range m {
		result[index] = id
		index++
	}
	return result
}
