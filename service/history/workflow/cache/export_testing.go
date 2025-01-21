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

package cache

import (
	"go.temporal.io/server/service/history/workflow"
)

// GetMutableState returns the MutableState for the given key from the cache.
// Exported for testing purposes.
func GetMutableState(cache Cache, key Key) workflow.MutableState {
	return getWorkflowContext(cache, key).(*workflow.ContextImpl).MutableState
}

// PutContextIfNotExist puts the given workflow Context into the cache, if it doens't already exist.
// Exported for testing purposes.
func PutContextIfNotExist(cache Cache, key Key, value workflow.Context) error {
	_, err := cache.(*cacheImpl).PutIfNotExist(key, &cacheItem{wfContext: value})
	return err
}

// ClearMutableState clears cached mutable state for the given key to
// force a reload from persistence on the next access.
func ClearMutableState(cache Cache, key Key) {
	getWorkflowContext(cache, key).Clear()
}

func getWorkflowContext(cache Cache, key Key) workflow.Context {
	return cache.(*cacheImpl).Get(key).(*cacheItem).wfContext
}
