package matching

import (
	"time"

	tasklistpb "go.temporal.io/temporal-proto/tasklist"

	"github.com/temporalio/temporal/common/cache"
)

const (
	pollerHistoryInitSize    = 0
	pollerHistoryInitMaxSize = 1000
	pollerHistoryTTL         = 5 * time.Minute
)

type (
	pollerIdentity string

	pollerInfo struct {
		ratePerSecond float64
	}
)

type pollerHistory struct {
	// poller ID -> pollerInfo
	// pollers map[pollerID]pollerInfo
	history cache.Cache
}

func newPollerHistory() *pollerHistory {
	opts := &cache.Options{
		InitialCapacity: pollerHistoryInitSize,
		TTL:             pollerHistoryTTL,
		Pin:             false,
	}

	return &pollerHistory{
		history: cache.New(pollerHistoryInitMaxSize, opts),
	}
}

func (pollers *pollerHistory) updatePollerInfo(id pollerIdentity, ratePerSecond *float64) {
	rps := _defaultTaskDispatchRPS
	if ratePerSecond != nil {
		rps = *ratePerSecond
	}
	pollers.history.Put(id, &pollerInfo{ratePerSecond: rps})
}

func (pollers *pollerHistory) getAllPollerInfo() []*tasklistpb.PollerInfo {
	var result []*tasklistpb.PollerInfo

	ite := pollers.history.Iterator()
	defer ite.Close()
	for ite.HasNext() {
		entry := ite.Next()
		key := entry.Key().(pollerIdentity)
		value := entry.Value().(*pollerInfo)
		// TODO add IP, T1396795
		lastAccessTime := entry.CreateTime()
		result = append(result, &tasklistpb.PollerInfo{
			Identity:       string(key),
			LastAccessTime: lastAccessTime.UnixNano(),
			RatePerSecond:  value.ratePerSecond,
		})
	}

	return result
}
