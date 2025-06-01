package matching

import (
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/cache"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	pollerHistoryInitMaxSize = 1000
)

type (
	pollerIdentity string

	pollerInfo struct {
		pollMetadata
	}
)

type pollerHistory struct {
	// poller ID -> pollerInfo
	// pollers map[pollerID]pollerInfo
	history cache.Cache
}

func newPollerHistory(pollerHistoryTTL time.Duration) *pollerHistory {
	opts := &cache.Options{
		TTL: pollerHistoryTTL,
		Pin: false,
	}

	return &pollerHistory{
		history: cache.New(pollerHistoryInitMaxSize, opts),
	}
}

func (pollers *pollerHistory) updatePollerInfo(id pollerIdentity, pollMetadata *pollMetadata) {
	pollers.history.Put(id, &pollerInfo{pollMetadata: *pollMetadata})
}

func (pollers *pollerHistory) getPollerInfo(earliestAccessTime time.Time) []*taskqueuepb.PollerInfo {
	var result []*taskqueuepb.PollerInfo

	ite := pollers.history.Iterator()
	defer ite.Close()
	for ite.HasNext() {
		entry := ite.Next()
		key := entry.Key().(pollerIdentity)
		value := entry.Value().(*pollerInfo)
		lastAccessTime := entry.CreateTime()
		if earliestAccessTime.Before(lastAccessTime) {
			result = append(result, &taskqueuepb.PollerInfo{
				Identity:                  string(key),
				LastAccessTime:            timestamppb.New(lastAccessTime),
				RatePerSecond:             defaultRPS(value.taskQueueMetadata.GetMaxTasksPerSecond()),
				WorkerVersionCapabilities: value.workerVersionCapabilities,
				DeploymentOptions:         value.deploymentOptions,
			})
		}
	}

	return result
}

func defaultRPS(wrapper *wrapperspb.DoubleValue) float64 {
	if wrapper != nil {
		return wrapper.Value
	}
	return defaultTaskDispatchRPS
}
