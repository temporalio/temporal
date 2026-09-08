package stream_batcher

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
)

// KeyedBatcher maintains an independent Batcher for each key. Entries are periodically cleared
// on calls to Add so that batchers for inactive keys can be collected. Clearing is opportunistic:
// an Add racing with a clear may briefly use a different Batcher than another Add for the same
// key.
type KeyedBatcher[K comparable, T, R any] struct {
	newBatcher    func(K) *Batcher[T, R]
	timeSource    clock.TimeSource
	clearInterval time.Duration
	nextClear     atomic.Int64 // unix nano
	batchers      sync.Map     // K -> *Batcher[T, R]
}

// NewKeyedBatcher creates a KeyedBatcher whose processing function returns one result shared by
// all items in a batch.
func NewKeyedBatcher[K comparable, T, R any](
	fn func(K, []T) R,
	opts BatcherOptions,
	timeSource clock.TimeSource,
) *KeyedBatcher[K, T, R] {
	return newKeyedBatcher(
		func(key K) *Batcher[T, R] {
			return NewBatcher(func(items []T) R { return fn(key, items) }, opts, timeSource)
		},
		opts.ClearInterval,
		timeSource,
	)
}

// NewKeyedBatcherWithPerItemResults creates a KeyedBatcher whose processing function returns
// one result for each input item, in the same order.
func NewKeyedBatcherWithPerItemResults[K comparable, T, R any](
	fn func(K, []T) []R,
	opts BatcherOptions,
	timeSource clock.TimeSource,
) *KeyedBatcher[K, T, R] {
	return newKeyedBatcher(
		func(key K) *Batcher[T, R] {
			return NewBatcherWithPerItemResults(func(items []T) []R { return fn(key, items) }, opts, timeSource)
		},
		opts.ClearInterval,
		timeSource,
	)
}

func newKeyedBatcher[K comparable, T, R any](
	newBatcher func(K) *Batcher[T, R],
	clearInterval time.Duration,
	timeSource clock.TimeSource,
) *KeyedBatcher[K, T, R] {
	b := &KeyedBatcher[K, T, R]{
		newBatcher:    newBatcher,
		timeSource:    timeSource,
		clearInterval: clearInterval,
	}
	b.maybeClear(true)
	return b
}

// Add adds an item to the stream identified by key.
func (b *KeyedBatcher[K, T, R]) Add(ctx context.Context, key K, item T) (R, error) {
	b.maybeClear(false)
	return b.get(key).Add(ctx, item)
}

func (b *KeyedBatcher[K, T, R]) get(key K) *Batcher[T, R] {
	if value, ok := b.batchers.Load(key); ok {
		return value.(*Batcher[T, R]) // nolint:revive
	}
	newBatcher := b.newBatcher(key)
	value, _ := b.batchers.LoadOrStore(key, newBatcher)
	return value.(*Batcher[T, R]) // nolint:revive
}

func (b *KeyedBatcher[K, T, R]) maybeClear(force bool) {
	if b.clearInterval <= 0 {
		return
	}
	now := b.timeSource.Now().UnixNano()
	nextClear := b.nextClear.Load()
	if now < nextClear && !force {
		return
	}
	newNextClear := now + int64(backoff.Jitter(b.clearInterval, 0.2))
	if b.nextClear.CompareAndSwap(nextClear, newNextClear) {
		b.batchers.Clear()
	}
}
