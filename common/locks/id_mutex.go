package locks

import (
	"sync"
)

type (
	// HashFunc represents a hash function for string
	HashFunc func(any) uint32

	// IDMutex is an interface which can lock on specific comparable identifier
	IDMutex interface {
		LockID(identifier any)
		UnlockID(identifier any)
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
		mutexInfos map[any]*mutexInfo
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
	for i := range numShard {
		impl.shards[i] = &idMutexShardImpl{
			mutexInfos: make(map[any]*mutexInfo),
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
func (idMutex *idMutexImpl) LockID(identifier any) {
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
func (idMutex *idMutexImpl) UnlockID(identifier any) {
	shard := idMutex.shards[idMutex.getShardIndex(identifier)]

	shard.Lock()
	defer shard.Unlock()
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
}

func (idMutex *idMutexImpl) getShardIndex(key any) uint32 {
	return idMutex.hashFn(key) % idMutex.numShard
}
