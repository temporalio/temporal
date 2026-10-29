package cache

import (
	"context"

	"go.temporal.io/server/common/locks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

// GetMutableState returns the MutableState for the given key from the cache.
// Exported for testing purposes.
func GetMutableState(cache Cache, key Key) historyi.MutableState {
	wfContext := getWorkflowContext(cache, key)
	if wfContext == nil {
		return nil
	}
	return wfContext.(*workflow.ContextImpl).MutableState
}

// PutContextIfNotExist puts the given workflow Context into the cache, if it doens't already exist.
// Exported for testing purposes.
func PutContextIfNotExist(cache Cache, key Key, value historyi.WorkflowContext) error {
	_, err := cache.(*cacheImpl).PutIfNotExist(key, &cacheItem{wfContext: value})
	return err
}

// ClearMutableState clears cached mutable state for the given key to
// force a reload from persistence on the next access.
func ClearMutableState(cache Cache, key Key) {
	wfContext := getWorkflowContext(cache, key)
	if wfContext == nil {
		return
	}
	if err := wfContext.Lock(context.Background(), locks.PriorityLow); err != nil {
		return
	}
	defer wfContext.Unlock()
	wfContext.Clear()
}

func getWorkflowContext(cache Cache, key Key) historyi.WorkflowContext {
	entry := cache.(*cacheImpl).Get(key)
	if entry == nil {
		return nil
	}
	return entry.(*cacheItem).wfContext
}
