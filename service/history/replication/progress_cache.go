//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination progress_cache_mock.go

package replication

import (
	"sync"
	"unsafe"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/configs"
)

type (
	ProgressCache interface {
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
		cacheLock sync.RWMutex
		cache     cache.Cache
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
	maxSize := config.ReplicationProgressCacheMaxSize()
	opts := &cache.Options{
		TTL: config.ReplicationProgressCacheTTL(),
	}
	return &progressCacheImpl{
		cache: cache.NewWithMetrics(maxSize, opts, handler.WithTags(metrics.CacheTypeTag(metrics.MutableStateCacheTypeTagValue))),
	}
}

func (c *progressCacheImpl) Get(
	runID string,
	targetClusterID int32,
) *ReplicationProgress {
	cacheKey := makeCacheKey(runID, targetClusterID)
	c.cacheLock.RLock()
	defer c.cacheLock.RUnlock()

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

	item, ok := c.cache.Get(cacheKey).(*ReplicationProgress)
	if !ok {
		item = &ReplicationProgress{}
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
