package goro

import (
	"context"
	"sync"
)

// KeyedSet maintains a set of goroutines addressed by keys, creating new ones as necessary and
// canceling obsolete ones. A KeyedSet may not be copied.
type KeyedSet[K comparable] struct {
	baseCtx context.Context
	lock    sync.Mutex
	cancels map[K]context.CancelFunc
}

// NewKeyedSet returns a new KeyedSet where all goroutines inherit a context from baseCtx.
func NewKeyedSet[K comparable](baseCtx context.Context) *KeyedSet[K] {
	return &KeyedSet[K]{
		baseCtx: baseCtx,
		cancels: make(map[K]context.CancelFunc),
	}
}

// Sync cancels/starts goroutines as necessary so that the running set matches the set of keys
// in target. To start a new goroutine, it does "go f(ctx, key)".
// You can use Sync(nil, nil) to cancel all running goroutines.
//
// Note that even if f returns without its context being canceled, it will still be considered
// "running" for future calls to Sync. In general f should not return _until_ its context is
// canceled. If KeyedSet were to remove f from the running set when it returned, then we could
// have a race where f decides to return, a concurrent call to Sync includes f's key, then f
// returns and is removed, but the caller of f thinks it's now active. In other words, there
// should be one source of truth for what should be running.
func (s *KeyedSet[K]) Sync(target map[K]struct{}, f func(context.Context, K)) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for key, cancel := range s.cancels {
		if _, ok := target[key]; !ok {
			cancel()
			delete(s.cancels, key)
		}
	}

	for key := range target {
		if _, ok := s.cancels[key]; ok {
			continue
		}
		ctx, cancel := context.WithCancel(s.baseCtx)
		s.cancels[key] = cancel
		go f(ctx, key)
	}
}
