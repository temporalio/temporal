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

	// IDMutex is an interface which can lock on specific comparable identifier
	IDMutex interface {
		LockID(identifier interface{})
		UnlockID(identifier interface{})
	}

	// idMutexShardImpl is the implementation of IDMutex shard
	idMutexImpl struct {
		numShard uint32
		hashFn   HashFunc
		shards   map[uint32]*idMutexShardImpl
	}

	// idMutexShardImpl is the implementation of IDMutex shard
	idMutexShardImpl struct {
		sync.Mutex
		mutexInfos map[interface{}]*mutexInfo
	}

	mutexInfo struct {
		// how many caller are using this lock info, including the
		// the caller already have the lock
		// this is guarded by lock in idLockImpl
		waitCount int

		// actual lock
		sync.Mutex
	}
)

// NewIDMutex create a new IDLock
func NewIDMutex(numShard uint32, hashFn HashFunc) IDMutex {
	impl := &idMutexImpl{
		numShard: numShard,
		hashFn:   hashFn,
		shards:   make(map[uint32]*idMutexShardImpl),
	}
	for i := uint32(0); i < numShard; i++ {
		impl.shards[i] = &idMutexShardImpl{
			mutexInfos: make(map[interface{}]*mutexInfo),
		}
	}

	return impl
}

func newMutexInfo() *mutexInfo {
	return &mutexInfo{
		waitCount: 1,
	}
}

// LockID lock by specific identifier
func (idMutex *idMutexImpl) LockID(identifier interface{}) {
	shard := idMutex.shards[idMutex.getShardIndex(identifier)]

	shard.Lock()
	mutexInfo, ok := shard.mutexInfos[identifier]
	if !ok {
		mutexInfo := newMutexInfo()
		shard.mutexInfos[identifier] = mutexInfo
		shard.Unlock()
		mutexInfo.Lock()
		return
	}

	mutexInfo.waitCount++
	shard.Unlock()
	mutexInfo.Lock()
}

// UnlockID unlock by specific identifier
func (idMutex *idMutexImpl) UnlockID(identifier interface{}) {
	shard := idMutex.shards[idMutex.getShardIndex(identifier)]

	shard.Lock()
	mutexInfo, ok := shard.mutexInfos[identifier]
	if !ok {
		panic("cannot find workflow lock")
	}
	mutexInfo.Unlock()
	if mutexInfo.waitCount == 1 {
		delete(shard.mutexInfos, identifier)
	} else {
		mutexInfo.waitCount--
	}
	shard.Unlock()
}

func (idMutex *idMutexImpl) getShardIndex(key interface{}) uint32 {
	return idMutex.hashFn(key) % idMutex.numShard
}
