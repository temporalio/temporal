package workflow

import (
	"sync"

	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

var (
	errQueryNotExists = serviceerror.NewInternal("query does not exist")
)

type (
	queryRegistryImpl struct {
		sync.RWMutex

		buffered  map[string]query
		completed map[string]query
		unblocked map[string]query
		failed    map[string]query
	}
)

func NewQueryRegistry() historyi.QueryRegistry {
	return &queryRegistryImpl{
		buffered:  make(map[string]query),
		completed: make(map[string]query),
		unblocked: make(map[string]query),
		failed:    make(map[string]query),
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
	return q.getCompletionCh(), nil
}

func (r *queryRegistryImpl) GetQueryInput(id string) (*querypb.WorkflowQuery, error) {
	r.RLock()
	defer r.RUnlock()
	q, err := r.getQueryNoLock(id)
	if err != nil {
		return nil, err
	}
	return q.getQueryInput(), nil
}

func (r *queryRegistryImpl) GetCompletionState(id string) (*historyi.QueryCompletionState, error) {
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
	id := q.getID()
	r.buffered[id] = q
	return id, q.getCompletionCh()
}

func (r *queryRegistryImpl) SetCompletionState(id string, completionState *historyi.QueryCompletionState) error {
	r.Lock()
	defer r.Unlock()
	q, ok := r.buffered[id]
	if !ok {
		return errQueryNotExists
	}
	if err := q.setCompletionState(completionState); err != nil {
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
		_ = q.setCompletionState(&historyi.QueryCompletionState{
			Type: QueryCompletionTypeFailed,
			Err:  consts.ErrBufferedQueryCleared,
		})
		r.failed[id] = q
	}
	r.buffered = make(map[string]query)
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
	result := make([]string, len(m))
	index := 0
	for id := range m {
		result[index] = id
		index++
	}
	return result
}
