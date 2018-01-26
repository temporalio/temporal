// Copyright (c) 2017 Uber Technologies, Inc.
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

	"github.com/uber/cadence/common/cache"
)

const (
	pollerHistoryInitSize    = 0
	pollerHistoryInitMaxSize = 1000
	pollerHistoryTTL         = 5 * time.Minute
)

type (
	pollerIdentity struct {
		identity string
		// TODO add IP, T1396795
	}

	pollerInfo struct {
		identity string
		// TODO add IP, T1396795
		lastAccessTime time.Time
	}
)

type pollerHistory struct {
	// poller ID -> last access time
	// pollers map[pollerID]time.Time
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

func (pollers *pollerHistory) updatePollerInfo(id pollerIdentity) {
	pollers.history.Put(id, nil)
}

func (pollers *pollerHistory) getAllPollerInfo() []*pollerInfo {
	result := []*pollerInfo{}

	ite := pollers.history.Iterator()
	defer ite.Close()
	for ite.HasNext() {
		entry := ite.Next()
		key := entry.Key().(pollerIdentity)
		lastAccessTime := entry.CreateTime()
		result = append(result, &pollerInfo{
			identity: key.identity,
			// TODO add IP, T1396795
			lastAccessTime: lastAccessTime,
		})
	}

	return result
}
