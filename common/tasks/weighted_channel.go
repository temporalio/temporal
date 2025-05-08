package tasks

import (
	"sync/atomic"
	"time"
)

const (
	WeightedChannelDefaultSize = 1000
)

type (
	WeightedChannels[T Task] []*WeightedChannel[T]

	WeightedChannel[T Task] struct {
		weight         int
		channel        chan T
		lastActiveTime atomic.Value // time.Time
		refCount       atomic.Int32
	}
)

func NewWeightedChannel[T Task](
	weight int,
	size int,
	now time.Time,
) *WeightedChannel[T] {
	c := &WeightedChannel[T]{
		weight:  weight,
		channel: make(chan T, size),
	}
	c.UpdateLastActiveTime(now)
	return c
}

func (c *WeightedChannel[T]) Chan() chan T {
	return c.channel
}

func (c *WeightedChannel[T]) Weight() int {
	return c.weight
}

func (c *WeightedChannel[T]) SetWeight(newWeight int) {
	c.weight = newWeight
}

func (c *WeightedChannel[T]) LastActiveTime() time.Time {
	return c.lastActiveTime.Load().(time.Time) // nolint:revive
}

func (c *WeightedChannel[T]) UpdateLastActiveTime(now time.Time) {
	c.lastActiveTime.Store(now)
}

func (c *WeightedChannel[T]) IncrementRefCount() {
	c.refCount.Add(1)
}

func (c *WeightedChannel[T]) DecrementRefCount() {
	c.refCount.Add(-1)
}

func (c *WeightedChannel[T]) RefCount() int32 {
	return c.refCount.Load()
}

func (c *WeightedChannel[T]) Len() int {
	return len(c.channel)
}

func (c *WeightedChannel[T]) Cap() int {
	return cap(c.channel)
}

func (c WeightedChannels[T]) Len() int {
	return len(c)
}
func (c WeightedChannels[T]) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}
func (c WeightedChannels[T]) Less(i, j int) bool {
	return c[i].Weight() < c[j].Weight()
}
