package history

import (
	"sync"

	querypb "go.temporal.io/temporal-proto/query"
	"go.temporal.io/temporal-proto/serviceerror"
)

var (
	errQueryNotExists = serviceerror.NewInternal("query does not exist")
)

type (
	queryRegistry interface {
		hasBufferedQuery() bool
		getBufferedIDs() []string
		hasCompletedQuery() bool
		getCompletedIDs() []string
		hasUnblockedQuery() bool
		getUnblockedIDs() []string
		hasFailedQuery() bool
		getFailedIDs() []string

		getQueryTermCh(string) (<-chan struct{}, error)
		getQueryInput(string) (*querypb.WorkflowQuery, error)
		getTerminationState(string) (*queryTerminationState, error)

		bufferQuery(queryInput *querypb.WorkflowQuery) (string, <-chan struct{})
		setTerminationState(string, *queryTerminationState) error
		removeQuery(id string)
	}

	queryRegistryImpl struct {
		sync.RWMutex

		buffered  map[string]query
		completed map[string]query
		unblocked map[string]query
		failed    map[string]query
	}
)

func newQueryRegistry() queryRegistry {
	return &queryRegistryImpl{
		buffered:  make(map[string]query),
		completed: make(map[string]query),
		unblocked: make(map[string]query),
		failed:    make(map[string]query),
	}
}

func (r *queryRegistryImpl) hasBufferedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.buffered) > 0
}

func (r *queryRegistryImpl) getBufferedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.buffered)
}

func (r *queryRegistryImpl) hasCompletedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.completed) > 0
}

func (r *queryRegistryImpl) getCompletedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.completed)
}

func (r *queryRegistryImpl) hasUnblockedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.unblocked) > 0
}

func (r *queryRegistryImpl) getUnblockedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.unblocked)
}

func (r *queryRegistryImpl) hasFailedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.failed) > 0
}

func (r *queryRegistryImpl) getFailedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.failed)
}

func (r *queryRegistryImpl) getQueryTermCh(id string) (<-chan struct{}, error) {
	r.RLock()
	defer r.RUnlock()
	q, err := r.getQueryNoLock(id)
	if err != nil {
		return nil, err
	}
	return q.getQueryTermCh(), nil
}

func (r *queryRegistryImpl) getQueryInput(id string) (*querypb.WorkflowQuery, error) {
	r.RLock()
	defer r.RUnlock()
	q, err := r.getQueryNoLock(id)
	if err != nil {
		return nil, err
	}
	return q.getQueryInput(), nil
}

func (r *queryRegistryImpl) getTerminationState(id string) (*queryTerminationState, error) {
	r.RLock()
	defer r.RUnlock()
	q, err := r.getQueryNoLock(id)
	if err != nil {
		return nil, err
	}
	return q.getTerminationState()
}

func (r *queryRegistryImpl) bufferQuery(queryInput *querypb.WorkflowQuery) (string, <-chan struct{}) {
	r.Lock()
	defer r.Unlock()
	q := newQuery(queryInput)
	id := q.getQueryID()
	r.buffered[id] = q
	return id, q.getQueryTermCh()
}

func (r *queryRegistryImpl) setTerminationState(id string, terminationState *queryTerminationState) error {
	r.Lock()
	defer r.Unlock()
	q, ok := r.buffered[id]
	if !ok {
		return errQueryNotExists
	}
	if err := q.setTerminationState(terminationState); err != nil {
		return err
	}
	delete(r.buffered, id)
	switch terminationState.queryTerminationType {
	case queryTerminationTypeCompleted:
		r.completed[id] = q
	case queryTerminationTypeUnblocked:
		r.unblocked[id] = q
	case queryTerminationTypeFailed:
		r.failed[id] = q
	}
	return nil
}

func (r *queryRegistryImpl) removeQuery(id string) {
	r.Lock()
	defer r.Unlock()
	delete(r.buffered, id)
	delete(r.completed, id)
	delete(r.unblocked, id)
	delete(r.failed, id)
}

func (r *queryRegistryImpl) getQueryNoLock(id string) (query, error) {
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

func (r *queryRegistryImpl) getIDs(m map[string]query) []string {
	result := make([]string, len(m), len(m))
	index := 0
	for id := range m {
		result[index] = id
		index++
	}
	return result
}
