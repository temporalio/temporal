package tasks

import (
	"sync/atomic"
	"time"
)

const (
	WeightedChannelDefaultSize = 1000
)

type (
	WeightedChannels[T Task, K comparable] []*WeightedChannel[T, K]

	WeightedChannel[T Task, K comparable] struct {
		key            K
		weight         int
		channel        chan T
		lastActiveTime atomic.Value // time.Time
		refCount       atomic.Int32
	}
)

func NewWeightedChannel[T Task, K comparable](
	key K,
	weight int,
	size int,
	now time.Time,
) *WeightedChannel[T, K] {
	c := &WeightedChannel[T, K]{
		key:     key,
		weight:  weight,
		channel: make(chan T, size),
	}
	c.UpdateLastActiveTime(now)
	return c
}

func (c *WeightedChannel[T, K]) Key() K {
	return c.key
}

func (c *WeightedChannel[T, K]) Chan() chan T {
	return c.channel
}

func (c *WeightedChannel[T, K]) Weight() int {
	return c.weight
}

func (c *WeightedChannel[T, K]) SetWeight(newWeight int) {
	c.weight = newWeight
}

func (c *WeightedChannel[T, K]) LastActiveTime() time.Time {
	return c.lastActiveTime.Load().(time.Time) // nolint:revive
}

func (c *WeightedChannel[T, K]) UpdateLastActiveTime(now time.Time) {
	c.lastActiveTime.Store(now)
}

func (c *WeightedChannel[T, K]) IncrementRefCount() {
	c.refCount.Add(1)
}

func (c *WeightedChannel[T, K]) DecrementRefCount() {
	c.refCount.Add(-1)
}

func (c *WeightedChannel[T, K]) RefCount() int32 {
	return c.refCount.Load()
}

func (c *WeightedChannel[T, K]) Len() int {
	return len(c.channel)
}

func (c *WeightedChannel[T, K]) Cap() int {
	return cap(c.channel)
}

func (c WeightedChannels[T, K]) Len() int {
	return len(c)
}
func (c WeightedChannels[T, K]) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}
func (c WeightedChannels[T, K]) Less(i, j int) bool {
	return c[i].Weight() < c[j].Weight()
}
