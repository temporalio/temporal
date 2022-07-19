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

package collection

import (
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/util"
)

const (
	// nShards represents the number of shards
	// At any given point of time, there can only
	// be nShards number of concurrent writers to
	// the map at max
	nShards = 32
)

type (

	// ShardedConcurrentTxMap is an implementation of
	// ConcurrentMap that internally uses multiple
	// sharded maps to increase parallelism
	ShardedConcurrentTxMap struct {
		shards     [nShards]mapShard
		hashfn     HashFunc
		size       int32
		initialCap int
	}

	// mapIteratorImpl represents an iterator type
	// for the concurrent map.
	mapIteratorImpl struct {
		stopCh chan struct{}
		dataCh chan *MapEntry
	}

	// mapShard represents a single instance
	// of thread safe map
	mapShard struct {
		sync.RWMutex
		items map[interface{}]interface{}
	}
)

// NewShardedConcurrentTxMap returns an instance of ShardedConcurrentMap
//
// ShardedConcurrentMap is a thread safe map that maintains upto nShards
// number of maps internally to allow nShards writers to be acive at the
// same time. This map *does not* use re-entrant locks, so access to the
// map during iterator can cause a dead lock.
//
// @param initialSz
//		The initial size for the map
// @param hashfn
// 		The hash function to use for sharding
func NewShardedConcurrentTxMap(initialCap int, hashfn HashFunc) ConcurrentTxMap {
	cmap := new(ShardedConcurrentTxMap)
	cmap.hashfn = hashfn
	cmap.initialCap = util.Max(nShards, initialCap/nShards)
	return cmap
}

// Get returns the value corresponding to the key, if it exist
func (cmap *ShardedConcurrentTxMap) Get(key interface{}) (interface{}, bool) {
	shard := cmap.getShard(key)
	var ok bool
	var value interface{}
	shard.RLock()
	if shard.items != nil {
		value, ok = shard.items[key]
	}
	shard.RUnlock()
	return value, ok
}

// Contains returns true if the key exist and false otherwise
func (cmap *ShardedConcurrentTxMap) Contains(key interface{}) bool {
	_, ok := cmap.Get(key)
	return ok
}

// Put records the given key value mapping. Overwrites previous values
func (cmap *ShardedConcurrentTxMap) Put(key interface{}, value interface{}) {
	shard := cmap.getShard(key)
	shard.Lock()
	cmap.lazyInitShard(shard)
	_, ok := shard.items[key]
	if !ok {
		atomic.AddInt32(&cmap.size, 1)
	}
	shard.items[key] = value
	shard.Unlock()
}

// PutIfNotExist records the mapping, if there is no mapping for this key already
// Returns true if the mapping was recorded, false otherwise
func (cmap *ShardedConcurrentTxMap) PutIfNotExist(key interface{}, value interface{}) bool {
	shard := cmap.getShard(key)
	var ok bool
	shard.Lock()
	cmap.lazyInitShard(shard)
	_, ok = shard.items[key]
	if !ok {
		shard.items[key] = value
		atomic.AddInt32(&cmap.size, 1)
	}
	shard.Unlock()
	return !ok
}

// Remove deletes the given key from the map
func (cmap *ShardedConcurrentTxMap) Remove(key interface{}) {
	shard := cmap.getShard(key)
	shard.Lock()
	cmap.lazyInitShard(shard)
	_, ok := shard.items[key]
	if ok {
		delete(shard.items, key)
		atomic.AddInt32(&cmap.size, -1)
	}
	shard.Unlock()
}

// GetAndDo returns the value corresponding to the key, and apply fn to key value before return value
// return (value, value exist or not, error when evaluation fn)
func (cmap *ShardedConcurrentTxMap) GetAndDo(key interface{}, fn ActionFunc) (interface{}, bool, error) {
	shard := cmap.getShard(key)
	var value interface{}
	var ok bool
	var err error
	shard.Lock()
	if shard.items != nil {
		value, ok = shard.items[key]
		if ok {
			err = fn(key, value)
		}
	}
	shard.Unlock()
	return value, ok, err
}

// PutOrDo put the key value in the map, if key does not exists, otherwise, call fn with existing key and value
// return (value, fn evaluated or not, error when evaluation fn)
func (cmap *ShardedConcurrentTxMap) PutOrDo(key interface{}, value interface{}, fn ActionFunc) (interface{}, bool, error) {
	shard := cmap.getShard(key)
	var err error
	shard.Lock()
	cmap.lazyInitShard(shard)
	v, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
		v = value
		atomic.AddInt32(&cmap.size, 1)
	} else {
		err = fn(key, v)
	}
	shard.Unlock()
	return v, ok, err
}

// RemoveIf deletes the given key from the map if fn return true
func (cmap *ShardedConcurrentTxMap) RemoveIf(key interface{}, fn PredicateFunc) bool {
	shard := cmap.getShard(key)
	var removed bool
	shard.Lock()
	if shard.items != nil {
		value, ok := shard.items[key]
		if ok && fn(key, value) {
			removed = true
			delete(shard.items, key)
			atomic.AddInt32(&cmap.size, -1)
		}
	}
	shard.Unlock()
	return removed
}

// Close closes the iterator
func (it *mapIteratorImpl) Close() {
	close(it.stopCh)
}

// Entries returns a channel of map entries
func (it *mapIteratorImpl) Entries() <-chan *MapEntry {
	return it.dataCh
}

// Iter returns an iterator to the map. This map
// does not use re-entrant locks, so access or modification
// to the map during iteration can cause a dead lock.
func (cmap *ShardedConcurrentTxMap) Iter() MapIterator {

	iterator := new(mapIteratorImpl)
	iterator.dataCh = make(chan *MapEntry, 8)
	iterator.stopCh = make(chan struct{})

	go func(iterator *mapIteratorImpl) {
		for i := 0; i < nShards; i++ {
			cmap.shards[i].RLock()
			for k, v := range cmap.shards[i].items {
				entry := &MapEntry{Key: k, Value: v}
				select {
				case iterator.dataCh <- entry:
				case <-iterator.stopCh:
					cmap.shards[i].RUnlock()
					close(iterator.dataCh)
					return
				}
			}
			cmap.shards[i].RUnlock()
		}
		close(iterator.dataCh)
	}(iterator)

	return iterator
}

// Len returns the number of items in the map
func (cmap *ShardedConcurrentTxMap) Len() int {
	return int(atomic.LoadInt32(&cmap.size))
}

func (cmap *ShardedConcurrentTxMap) getShard(key interface{}) *mapShard {
	shardIdx := cmap.hashfn(key) % nShards
	return &cmap.shards[shardIdx]
}

func (cmap *ShardedConcurrentTxMap) lazyInitShard(shard *mapShard) {
	if shard.items == nil {
		shard.items = make(map[interface{}]interface{}, cmap.initialCap)
	}
}
