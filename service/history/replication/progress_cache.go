//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination progress_cache_mock.go

package replication

import (
	"context"
	"slices"
	"sync"
	"time"
	"unsafe"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/configs"
)

type (
	ProgressCache interface {
		Start()
		Stop()
		Get(
			runID string,
			targetClusterID int32,
		) *ReplicationProgress
		Update(
			runID string,
			targetClusterID int32,
			versionedTransitions []*persistencespb.VersionedTransition,
			eventVersionHistoryItems []*historyspb.VersionHistoryItem,
		) error
	}

	progressCacheImpl struct {
		cacheLock      sync.Mutex
		cache          cache.Cache
		maxSize        int
		maxSizeFn      dynamicconfig.IntPropertyFn
		cacheOptions   cache.Options
		metricsHandler metrics.Handler
		timeSource     clock.TimeSource
		startOnce      sync.Once
		loops          goro.Group
	}

	ReplicationProgress struct {
		versionedTransitions         [][]*persistencespb.VersionedTransition
		eventVersionHistoryItems     [][]*historyspb.VersionHistoryItem
		lastVersionTransitionIndex   int
		lastEventVersionHistoryIndex int
	}

	Key struct {
		RunID           string
		TargetClusterID int32
	}
)

func NewProgressCache(
	config *configs.Config,
	logger log.Logger,
	handler metrics.Handler,
) ProgressCache {
	c := &progressCacheImpl{
		maxSizeFn:      config.ReplicationProgressCacheMaxSize,
		timeSource:     clock.NewRealTimeSource(),
		cacheOptions:   cache.Options{TTL: config.ReplicationProgressCacheTTL()},
		metricsHandler: handler.WithTags(metrics.CacheTypeTag(metrics.ReplicationProgressCacheTypeTagValue)),
	}
	c.refreshCache()
	return c
}

func (c *progressCacheImpl) Start() {
	c.startOnce.Do(func() {
		c.loops.Go(c.refreshLoop)
	})
}

func (c *progressCacheImpl) Stop() {
	c.loops.Cancel()
	c.loops.Wait()
}

func (c *progressCacheImpl) refreshLoop(ctx context.Context) error {
	ch, timer := c.timeSource.NewTimer(time.Minute)
	defer timer.Stop()
	for {
		select {
		case <-ch:
			c.refreshCache()
			timer.Reset(time.Minute)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// refreshCache replaces the cache when its configured capacity changes. Cached progress
// is discarded, so receivers may need to fetch history while the new cache warms up.
func (c *progressCacheImpl) refreshCache() {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()
	maxSize := max(0, c.maxSizeFn())
	if c.cache != nil && maxSize == c.maxSize {
		return
	}
	c.cache = cache.NewWithMetrics(maxSize, &c.cacheOptions, c.metricsHandler)
	c.maxSize = maxSize
	metrics.CacheUsage.With(c.metricsHandler).Record(0)
}

func (c *progressCacheImpl) Get(
	runID string,
	targetClusterID int32,
) *ReplicationProgress {
	cacheKey := makeCacheKey(runID, targetClusterID)
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	progress, ok := c.cache.Get(cacheKey).(*ReplicationProgress)
	if !ok {
		return nil
	}
	return progress
}

func (c *progressCacheImpl) updateStates(
	item *ReplicationProgress,
	versionedTransitions []*persistencespb.VersionedTransition,
) bool {
	if len(versionedTransitions) == 0 {
		return false
	}

	if item.versionedTransitions == nil {
		item.versionedTransitions = [][]*persistencespb.VersionedTransition{
			transitionhistory.CopyVersionedTransitions(versionedTransitions),
		}
		item.lastVersionTransitionIndex = 0
		return true
	}

	for idx, transitions := range item.versionedTransitions {
		if transitionhistory.StalenessCheck(versionedTransitions, transitions[len(transitions)-1]) == nil {
			item.versionedTransitions[idx] = transitionhistory.CopyVersionedTransitions(versionedTransitions)
			item.lastVersionTransitionIndex = idx
			return true
		}
		if transitionhistory.StalenessCheck(transitions, versionedTransitions[len(versionedTransitions)-1]) == nil {
			// incoming versioned transitions are already included in the current versioned transitions
			return false
		}
	}
	item.lastVersionTransitionIndex = len(item.versionedTransitions)
	item.versionedTransitions = append(item.versionedTransitions, transitionhistory.CopyVersionedTransitions(versionedTransitions))
	return true
}

func (c *progressCacheImpl) updateEvents(
	item *ReplicationProgress,
	eventVersionHistoryItems []*historyspb.VersionHistoryItem,
) (bool, error) {
	if len(eventVersionHistoryItems) == 0 {
		return false, nil
	}

	if item.eventVersionHistoryItems == nil {
		item.eventVersionHistoryItems = [][]*historyspb.VersionHistoryItem{
			versionhistory.CopyVersionHistoryItems(eventVersionHistoryItems),
		}
		item.lastEventVersionHistoryIndex = 0
		return true, nil
	}

	for idx, historyItems := range item.eventVersionHistoryItems {
		lcaItem, err := versionhistory.FindLCAVersionHistoryItemFromItemSlice(historyItems, eventVersionHistoryItems)
		if err != nil {
			return false, err
		}
		if versionhistory.IsEqualVersionHistoryItem(eventVersionHistoryItems[len(eventVersionHistoryItems)-1], lcaItem) {
			// incoming version history is already included in the current version histories
			return false, nil
		}
		if versionhistory.IsEqualVersionHistoryItem(historyItems[len(historyItems)-1], lcaItem) {
			// incoming version history can be appended to the current version histories
			item.eventVersionHistoryItems[idx] = versionhistory.CopyVersionHistoryItems(eventVersionHistoryItems)
			item.lastEventVersionHistoryIndex = idx
			return true, nil
		}
	}

	item.lastEventVersionHistoryIndex = len(item.eventVersionHistoryItems)
	item.eventVersionHistoryItems = append(item.eventVersionHistoryItems, versionhistory.CopyVersionHistoryItems(eventVersionHistoryItems))
	return true, nil
}

func (c *progressCacheImpl) Update(
	runID string,
	targetClusterID int32,
	versionedTransitions []*persistencespb.VersionedTransition,
	eventVersionHistoryItems []*historyspb.VersionHistoryItem,
) error {
	cacheKey := makeCacheKey(runID, targetClusterID)
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	// Get hands out the cached progress without holding the lock while callers read it, so never mutate a cached
	// progress in place. Update a copy and swap it in instead. The inner slices are never mutated once stored, so
	// cloning the outer slices is enough.
	item := &ReplicationProgress{}
	if cached, ok := c.cache.Get(cacheKey).(*ReplicationProgress); ok {
		item = &ReplicationProgress{
			versionedTransitions:         slices.Clone(cached.versionedTransitions),
			eventVersionHistoryItems:     slices.Clone(cached.eventVersionHistoryItems),
			lastVersionTransitionIndex:   cached.lastVersionTransitionIndex,
			lastEventVersionHistoryIndex: cached.lastEventVersionHistoryIndex,
		}
	}

	stateDirty := c.updateStates(item, versionedTransitions)
	eventDirty, err := c.updateEvents(item, eventVersionHistoryItems)
	if err != nil {
		return err
	}

	if stateDirty || eventDirty {
		c.cache.Put(cacheKey, item)
	}
	return nil
}

func (c *ReplicationProgress) LastSyncedTransition() *persistencespb.VersionedTransition {
	if c == nil ||
		len(c.versionedTransitions) == 0 ||
		c.lastVersionTransitionIndex < 0 ||
		c.lastVersionTransitionIndex >= len(c.versionedTransitions) {
		return nil
	}
	transitions := c.versionedTransitions[c.lastVersionTransitionIndex]
	return transitions[len(transitions)-1]
}

func (c *ReplicationProgress) VersionedTransitionSent(versionedTransition *persistencespb.VersionedTransition) bool {
	if c == nil {
		return false
	}
	for _, transitions := range c.versionedTransitions {
		if transitionhistory.StalenessCheck(transitions, versionedTransition) == nil {
			return true
		}
	}
	return false
}

func (c *ReplicationProgress) CacheSize() int {
	size := int(unsafe.Sizeof(c.lastVersionTransitionIndex))
	for _, transitions := range c.versionedTransitions {
		for _, versionedTransition := range transitions {
			size += versionedTransition.Size()
		}
	}
	for _, items := range c.eventVersionHistoryItems {
		for _, item := range items {
			size += item.Size()
		}
	}
	return size
}

func makeCacheKey(
	runID string,
	targetClusterID int32,
) Key {
	return Key{
		RunID:           runID,
		TargetClusterID: targetClusterID,
	}
}
