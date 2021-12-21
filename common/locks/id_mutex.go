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

package locks

import (
	"sync"
)

type (
	// HashFunc represents a hash function for string
	HashFunc func(interface{}) uint32

	// UnlockFunc unlocks a lock
	UnlockFunc func()

	// IDMutex is an interface which can lock on specific comparable identifier
	IDMutex interface {
		LockID(identifier interface{}) UnlockFunc
	}

	// idMutexShardImpl is the implementation of IDMutex shard
	idMutexImpl struct {
		numShards uint32
		hashFn    HashFunc
		shards    []*idMutexShardImpl
		cleanup   bool
	}

	// idMutexShardImpl is the implementation of IDMutex shard
	idMutexShardImpl struct {
		sync.Mutex
		mutexInfos map[interface{}]*mutexInfo
	}

	mutexInfo struct {
		// actual lock
		sync.Mutex
		// how many callers are using this mutexInfo
		// this is guarded by lock in idMutexShardImpl, not this lock
		waitCount int
	}
)

// NewIDMutex create a new IDLock
func NewIDMutex(numShards uint32, hashFn HashFunc, cleanupLocks bool) IDMutex {
	impl := &idMutexImpl{
		numShards: numShards,
		hashFn:    hashFn,
		shards:    make([]*idMutexShardImpl, numShards),
		cleanup:   cleanupLocks,
	}
	for i := uint32(0); i < numShards; i++ {
		impl.shards[i] = &idMutexShardImpl{
			mutexInfos: make(map[interface{}]*mutexInfo),
		}
	}
	return impl
}

// LockID lock by specific identifier
func (idMutex *idMutexImpl) LockID(identifier interface{}) UnlockFunc {
	shard := idMutex.shards[idMutex.getShardIndex(identifier)]

	shard.Lock()
	mi, ok := shard.mutexInfos[identifier]
	if !ok {
		mi = new(mutexInfo)
		shard.mutexInfos[identifier] = mi
	}

	if !idMutex.cleanup {
		shard.Unlock()
		mi.Lock()
		return mi.Unlock
	}

	mi.waitCount++
	shard.Unlock()
	mi.Lock()

	return func() {
		mi.Unlock()
		shard.Lock()
		defer shard.Unlock()
		if mi.waitCount == 1 {
			delete(shard.mutexInfos, identifier)
		} else {
			mi.waitCount--
		}
	}
}

func (idMutex *idMutexImpl) getShardIndex(key interface{}) uint32 {
	return idMutex.hashFn(key) % idMutex.numShards
}
