// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cache

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
)

const flushBufferedMetricsScopeDuration = 10 * time.Second

type (
	metricsScopeMap map[string]map[int]metrics.Scope

	buffer struct {
		sync.RWMutex
		bufferMap metricsScopeMap
	}

	domainMetricsScopeCache struct {
		status        int32
		buffer        *buffer
		cache         atomic.Value
		closeCh       chan struct{}
		flushDuration time.Duration
	}
)

// NewDomainMetricsScopeCache constructs a new domainMetricsScopeCache
func NewDomainMetricsScopeCache() DomainMetricsScopeCache {

	mc := &domainMetricsScopeCache{
		buffer: &buffer{
			bufferMap: make(metricsScopeMap),
		},
		closeCh:       make(chan struct{}),
		flushDuration: flushBufferedMetricsScopeDuration,
	}

	mc.cache.Store(make(metricsScopeMap))
	return mc
}

func (c *domainMetricsScopeCache) flushBufferedMetricsScope(flushDuration time.Duration) {
	for {
		select {
		case <-time.After(flushDuration):
			c.buffer.Lock()
			if len(c.buffer.bufferMap) > 0 {
				scopeMap := make(metricsScopeMap)

				data := c.cache.Load().(metricsScopeMap)
				// Copy everything over after atomic load
				for key, val := range data {
					scopeMap[key] = map[int]metrics.Scope{}
					for k, v := range val {
						scopeMap[key][k] = v
					}
				}

				// Copy from buffered array
				for key, val := range c.buffer.bufferMap {
					if _, ok := scopeMap[key]; !ok {
						scopeMap[key] = map[int]metrics.Scope{}
					}
					for k, v := range val {
						scopeMap[key][k] = v
					}
				}

				c.cache.Store(scopeMap)
				c.buffer.bufferMap = make(metricsScopeMap)
			}
			c.buffer.Unlock()

		case <-c.closeCh:
			return
		}
	}
}

// Get retrieves scope for domainID and scopeIdx
func (c *domainMetricsScopeCache) Get(domainID string, scopeIdx int) (metrics.Scope, bool) {
	data := c.cache.Load().(metricsScopeMap)

	if data == nil {
		return nil, false
	}

	m, ok := data[domainID]
	if !ok {
		return nil, false
	}
	metricsScope, ok := m[scopeIdx]

	return metricsScope, ok
}

// Put puts map of domainID and scopeIdx to metricsScope
func (c *domainMetricsScopeCache) Put(domainID string, scopeIdx int, scope metrics.Scope) {
	c.buffer.Lock()
	defer c.buffer.Unlock()

	if c.buffer.bufferMap[domainID] == nil {
		c.buffer.bufferMap[domainID] = map[int]metrics.Scope{}
	}
	c.buffer.bufferMap[domainID][scopeIdx] = scope
}

func (c *domainMetricsScopeCache) Start() {
	if !atomic.CompareAndSwapInt32(&c.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	go c.flushBufferedMetricsScope(c.flushDuration)
}

func (c *domainMetricsScopeCache) Stop() {
	if !atomic.CompareAndSwapInt32(&c.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	close(c.closeCh)
}
