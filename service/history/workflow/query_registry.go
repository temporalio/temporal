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
	"sync"

	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/definition"
)

var (
	errQueryNotExists = serviceerror.NewInternal("query does not exist")
)

type (
	queryRegistryImpl struct {
		sync.RWMutex

		buffered  map[string]definition.Query
		completed map[string]definition.Query
		unblocked map[string]definition.Query
		failed    map[string]definition.Query
	}
)

func NewQueryRegistry() definition.QueryRegistry {
	return &queryRegistryImpl{
		buffered:  make(map[string]definition.Query),
		completed: make(map[string]definition.Query),
		unblocked: make(map[string]definition.Query),
		failed:    make(map[string]definition.Query),
	}
}

func (r *queryRegistryImpl) HasBufferedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.buffered) > 0
}

func (r *queryRegistryImpl) GetBufferedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.buffered)
}

func (r *queryRegistryImpl) HasCompletedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.completed) > 0
}

func (r *queryRegistryImpl) GetCompletedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.completed)
}

func (r *queryRegistryImpl) HasUnblockedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.unblocked) > 0
}

func (r *queryRegistryImpl) GetUnblockedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.unblocked)
}

func (r *queryRegistryImpl) HasFailedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.failed) > 0
}

func (r *queryRegistryImpl) GetFailedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.failed)
}

func (r *queryRegistryImpl) GetQueryCompletionCh(id string) (<-chan struct{}, error) {
	r.RLock()
	defer r.RUnlock()
	q, err := r.getQueryNoLock(id)
	if err != nil {
		return nil, err
	}
	return q.GetCompletionCh(), nil
}

func (r *queryRegistryImpl) GetQueryInput(id string) (*querypb.WorkflowQuery, error) {
	r.RLock()
	defer r.RUnlock()
	q, err := r.getQueryNoLock(id)
	if err != nil {
		return nil, err
	}
	return q.GetQueryInput(), nil
}

func (r *queryRegistryImpl) GetCompletionState(id string) (*definition.QueryCompletionState, error) {
	r.RLock()
	defer r.RUnlock()
	q, err := r.getQueryNoLock(id)
	if err != nil {
		return nil, err
	}
	return q.GetCompletionState()
}

func (r *queryRegistryImpl) BufferQuery(queryInput *querypb.WorkflowQuery) (string, <-chan struct{}) {
	r.Lock()
	defer r.Unlock()
	q := newQuery(queryInput)
	id := q.GetID()
	r.buffered[id] = q
	return id, q.GetCompletionCh()
}

func (r *queryRegistryImpl) SetCompletionState(id string, completionState *definition.QueryCompletionState) error {
	r.Lock()
	defer r.Unlock()
	q, ok := r.buffered[id]
	if !ok {
		return errQueryNotExists
	}
	if err := q.SetCompletionState(completionState); err != nil {
		return err
	}
	delete(r.buffered, id)
	switch completionState.Type {
	case QueryCompletionTypeSucceeded:
		r.completed[id] = q
	case QueryCompletionTypeUnblocked:
		r.unblocked[id] = q
	case QueryCompletionTypeFailed:
		r.failed[id] = q
	}
	return nil
}

func (r *queryRegistryImpl) RemoveQuery(id string) {
	r.Lock()
	defer r.Unlock()
	delete(r.buffered, id)
	delete(r.completed, id)
	delete(r.unblocked, id)
	delete(r.failed, id)
}

func (r *queryRegistryImpl) Clear() {
	r.Lock()
	defer r.Unlock()
	for id, q := range r.buffered {
		_ = q.SetCompletionState(&definition.QueryCompletionState{
			Type: QueryCompletionTypeFailed,
			Err:  consts.ErrBufferedQueryCleared,
		})
		r.failed[id] = q
	}
	r.buffered = make(map[string]definition.Query)
}

func (r *queryRegistryImpl) getQueryNoLock(id string) (definition.Query, error) {
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

func (r *queryRegistryImpl) getIDs(m map[string]definition.Query) []string {
	result := make([]string, len(m))
	index := 0
	for id := range m {
		result[index] = id
		index++
	}
	return result
}
