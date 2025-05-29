package cache

import (
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

// GetMutableState returns the MutableState for the given key from the cache.
// Exported for testing purposes.
func GetMutableState(cache Cache, key Key) historyi.MutableState {
	return getWorkflowContext(cache, key).(*workflow.ContextImpl).MutableState
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
	getWorkflowContext(cache, key).Clear()
}

func getWorkflowContext(cache Cache, key Key) historyi.WorkflowContext {
	return cache.(*cacheImpl).Get(key).(*cacheItem).wfContext
}
