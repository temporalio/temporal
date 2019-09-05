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

var (
	errQueryNotFound       = errors.New("callback function could not be applied because query was not found")
	errQueryAlreadyInState = errors.New("could not invoke callback because query is already in target state")
)

type (
	// QueryRegistry keeps track of all outstanding queries for a single workflow execution
	QueryRegistry interface {
		BufferQuery(*shared.WorkflowQuery) Query
		GetBuffered() map[string]Query
		GetStarted() map[string]Query
	}

	queryRegistry struct {
		sync.RWMutex

		buffered map[string]Query
		started  map[string]Query
	}
)

// NewQueryRegistry constructs a new QueryRegistry.
// One QueryRegistry exists per workflowExecutionContext, and it should be accessed through the workflowExecutionContext.
func NewQueryRegistry() QueryRegistry {
	return &queryRegistry{
		buffered: make(map[string]Query),
		started:  make(map[string]Query),
	}
}

// BufferQuery is used to buffer a new query.
func (r *queryRegistry) BufferQuery(queryInput *shared.WorkflowQuery) Query {
	r.Lock()
	defer r.Unlock()

	query := newQuery(queryInput, r.bufferedToStartedCallback, r.startedToBufferedCallback, r.terminalStateCallback)
	r.buffered[query.ID()] = query
	return query
}

// GetBuffered returns all buffered queries.
func (r *queryRegistry) GetBuffered() map[string]Query {
	r.RLock()
	defer r.RUnlock()

	return r.buffered
}

// GetStarted returns all started queries.
func (r *queryRegistry) GetStarted() map[string]Query {
	r.RLock()
	defer r.RUnlock()

	return r.started
}

func (r *queryRegistry) terminalStateCallback(id string) {
	r.Lock()
	defer r.Unlock()

	delete(r.buffered, id)
	delete(r.started, id)
}

func (r *queryRegistry) bufferedToStartedCallback(id string) error {
	r.Lock()
	defer r.Unlock()

	return move(r.buffered, r.started, id)
}

func (r *queryRegistry) startedToBufferedCallback(id string) error {
	r.Lock()
	defer r.Unlock()

	return move(r.started, r.buffered, id)
}

func move(source map[string]Query, target map[string]Query, key string) error {
	if _, ok := source[key]; !ok {
		return errQueryNotFound
	}
	if _, ok := target[key]; ok {
		return errQueryAlreadyInState
	}
	target[key] = source[key]
	delete(source, key)
	return nil
}
