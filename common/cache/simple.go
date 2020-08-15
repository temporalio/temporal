// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
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

package cache

import (
	"container/list"
	"sync"
	"time"
)

var (
	// DummyCreateTime is the create time used by all entries in the cache.
	DummyCreateTime = time.Time{}
)

type (
	simple struct {
		sync.RWMutex
		accessMap   map[interface{}]*list.Element
		iterateList *list.List
		rmFunc      RemovedFunc
	}

	simpleItr struct {
		simple   *simple
		nextItem *list.Element
	}

	simpleEntry struct {
		key   interface{}
		value interface{}
	}
)

// Close closes the iterator
func (it *simpleItr) Close() {
	it.simple.RUnlock()
}

// HasNext return true if there is more items to be returned
func (it *simpleItr) HasNext() bool {
	return it.nextItem != nil
}

// Next returns the next item
func (it *simpleItr) Next() Entry {
	if it.nextItem == nil {
		panic("Simple cache iterator Next called when there is no next item")
	}

	entry := it.nextItem.Value.(*simpleEntry)
	it.nextItem = it.nextItem.Next()
	// make a copy of the entry so there will be no concurrent access to this entry
	entry = &simpleEntry{
		key:   entry.key,
		value: entry.value,
	}
	return entry
}

func (e *simpleEntry) Key() interface{} {
	return e.key
}

func (e *simpleEntry) Value() interface{} {
	return e.value
}

// CreateTime is not implemented for simple cache entries
func (e *simpleEntry) CreateTime() time.Time {
	return DummyCreateTime
}

// NewSimple creates a new simple cache with given options.
// Simple cache will never evict entries and it will never reorder the elements.
// Simple cache also does not have the concept of pinning that LRU cache has.
// Internally simple cache uses a RWMutex instead of the exclusive Mutex that LRU cache uses.
// The RWMutex makes simple cache readable by many threads without introducing lock contention.
func NewSimple(opts *SimpleOptions) Cache {
	if opts == nil {
		opts = &SimpleOptions{}
	}
	return &simple{
		iterateList: list.New(),
		accessMap:   make(map[interface{}]*list.Element, opts.InitialCapacity),
		rmFunc:      opts.RemovedFunc,
	}
}

// Get retrieves the value stored under the given key
func (c *simple) Get(key interface{}) interface{} {
	c.RLock()
	defer c.RUnlock()

	element := c.accessMap[key]
	if element == nil {
		return nil
	}
	return element.Value.(*simpleEntry).Value()
}

// Put puts a new value associated with a given key, returning the existing value (if present).
func (c *simple) Put(key interface{}, value interface{}) interface{} {
	c.Lock()
	defer c.Unlock()
	existing := c.putInternal(key, value, true)
	return existing
}

// PutIfNotExist puts a value associated with a given key if it does not exist
func (c *simple) PutIfNotExist(key interface{}, value interface{}) (interface{}, error) {
	c.Lock()
	defer c.Unlock()
	existing := c.putInternal(key, value, false)
	if existing == nil {
		// This is a new value
		return value, nil
	}
	return existing, nil
}

// Delete deletes a key, value pair associated with a key
func (c *simple) Delete(key interface{}) {
	c.Lock()
	defer c.Unlock()

	element := c.accessMap[key]
	if element == nil {
		return
	}
	entry := c.iterateList.Remove(element).(*simpleEntry)
	if c.rmFunc != nil {
		go c.rmFunc(entry.value)
	}
	delete(c.accessMap, entry.key)
}

// Release does nothing for simple cache
func (c *simple) Release(_ interface{}) {}

// Size returns the number of entries currently in the cache
func (c *simple) Size() int {
	c.RLock()
	defer c.RUnlock()

	return len(c.accessMap)
}

func (c *simple) Iterator() Iterator {
	c.RLock()
	iterator := &simpleItr{
		simple:   c,
		nextItem: c.iterateList.Front(),
	}
	return iterator
}

func (c *simple) putInternal(key interface{}, value interface{}, allowUpdate bool) interface{} {
	elt := c.accessMap[key]
	if elt != nil {
		entry := elt.Value.(*simpleEntry)
		existing := entry.value
		if allowUpdate {
			entry.value = value
		}
		return existing
	}
	entry := &simpleEntry{
		key:   key,
		value: value,
	}
	c.accessMap[key] = c.iterateList.PushFront(entry)
	return nil
}
