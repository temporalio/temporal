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

	"go.temporal.io/server/common/cache"
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
	rps := defaultTaskDispatchRPS
	if ratePerSecond != nil {
		rps = *ratePerSecond
	}
	pollers.history.Put(id, &pollerInfo{ratePerSecond: rps})
}

func (pollers *pollerHistory) getPollerInfo(earliestAccessTime time.Time) []*taskqueuepb.PollerInfo {
	var result []*taskqueuepb.PollerInfo

	ite := pollers.history.Iterator()
	defer ite.Close()
	for ite.HasNext() {
		entry := ite.Next()
		key := entry.Key().(pollerIdentity)
		value := entry.Value().(*pollerInfo)
		// TODO add IP, T1396795
		lastAccessTime := entry.CreateTime()
		if earliestAccessTime.Before(lastAccessTime) {
			result = append(result, &taskqueuepb.PollerInfo{
				Identity:       string(key),
				LastAccessTime: &lastAccessTime,
				RatePerSecond:  value.ratePerSecond,
			})
		}
	}

	return result
}
