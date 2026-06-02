package matching

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/metrics"
)

// Shards should be a power of 2.
const partitionCacheNumShards = 8

// How long for a full rotation of the cache.
const partitionCacheRotateInterval = time.Hour

// partitionCache is a cache of PartitionCounts values for task queues.
// It uses a sharded map+mutex and periodic rotation for efficiency.
type partitionCache struct {
	shards [partitionCacheNumShards]partitionCacheShard

	metricsHandler metrics.Handler
	rotate         *goro.Handle
}

type partitionCacheShard struct {
	lock   sync.RWMutex
	active map[string]PartitionCounts
	prev   map[string]PartitionCounts
	_      [64 - 24 - 8 - 8]byte // force to different cache lines to eliminate false sharing
}

// newPartitionCache returns a new partitionCache. Start() must be called before using it.
func newPartitionCache(
	metricsHandler metrics.Handler,
) *partitionCache {
	return &partitionCache{
		metricsHandler: metricsHandler,
	}
}

func (c *partitionCache) Start() {
	for i := range c.shards {
		c.shards[i].rotate()
	}
	c.rotate = goro.NewHandle(context.Background()).Go(func(ctx context.Context) error {
		t := time.NewTicker(partitionCacheRotateInterval / partitionCacheNumShards)
		defer t.Stop()
		for i := 0; ; i = (i + 1) % partitionCacheNumShards {
			select {
			case <-t.C:
				c.shards[i].rotate()
				c.emitMetrics()
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
}

func (c *partitionCache) Stop() {
	c.rotate.Cancel()
	<-c.rotate.Done()
}

func (c *partitionCache) emitMetrics() {
	totalSize := 0
	for i := range c.shards {
		totalSize += c.shards[i].size()
	}
	metrics.PartitionCacheSize.With(c.metricsHandler).Record(float64(totalSize))
}

func (*partitionCache) makeKey(
	nsid, tqname string, tqtype enumspb.TaskQueueType,
) string {
	// note we don't need delimiters to make unambiguous keys: nsid is always the same length,
	// the last byte is tqtype, and everything in between is the name.
	nsidBytes, err := uuid.Parse(nsid)
	if err != nil {
		// this shouldn't fail, but use the string form as a backup, append a 0xff to differentiate
		return nsid + string([]byte{0xff}) + tqname + string([]byte{byte(tqtype), 0xff})
	}
	return string(nsidBytes[:]) + tqname + string([]byte{byte(tqtype)})
}

func (*partitionCache) shardFromKey(key string) int {
	// mix a few bits to pick a shard
	l := len(key)
	shard := int(key[min(14, l-3)] ^ key[l-2] ^ key[l-1])
	return shard % partitionCacheNumShards
}

func (c *partitionCache) lookup(key string) PartitionCounts {
	return c.shards[c.shardFromKey(key)].lookup(key)
}

func (c *partitionCache) put(key string, pc PartitionCounts) {
	c.shards[c.shardFromKey(key)].put(key, pc)
}

func (s *partitionCacheShard) lookup(key string) PartitionCounts {
	s.lock.RLock()
	if pc, ok := s.active[key]; ok {
		s.lock.RUnlock()
		return pc
	} else if pc, ok := s.prev[key]; ok {
		s.lock.RUnlock()
		s.put(key, pc) // promote to active
		return pc
	}
	s.lock.RUnlock()
	return PartitionCounts{}
}

func (s *partitionCacheShard) put(key string, pc PartitionCounts) {
	s.lock.Lock()
	if pc.Valid() {
		s.active[key] = pc
	} else {
		// invalid PartitionCounts means disable this mechanism, so remove this key entirely
		delete(s.active, key)
	}
	delete(s.prev, key)
	s.lock.Unlock()
}

func (s *partitionCacheShard) rotate() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.prev = s.active
	s.active = make(map[string]PartitionCounts)
}

func (s *partitionCacheShard) size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.prev) + len(s.active)
}
