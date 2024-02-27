// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package matching

import (
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/metrics"
)

const (
	pollerHistoryInitMaxSize = 1000
	pollerHistoryTTL         = 5 * time.Minute
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

func newPollerHistory() *pollerHistory {
	opts := &cache.Options{
		TTL: pollerHistoryTTL,
		Pin: false,
	}

	return &pollerHistory{
		history: cache.New(pollerHistoryInitMaxSize, opts, metrics.NoopMetricsHandler),
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
				RatePerSecond:             defaultValue(value.ratePerSecond, defaultTaskDispatchRPS),
				WorkerVersionCapabilities: value.workerVersionCapabilities,
			})
		}
	}

	return result
}

func defaultValue[T any, P ~*T](p P, def T) T {
	if p == nil {
		return def
	}
	return *p
}
