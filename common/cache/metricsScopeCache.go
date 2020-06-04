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
	"bytes"
	"strconv"
	"sync"

	"github.com/uber/cadence/common/metrics"
)

type domainMetricsScopeCache struct {
	sync.RWMutex
	scopeMap map[string]metrics.Scope
}

// NewDomainMetricsScopeCache constructs a new domainMetricsScopeCache
func NewDomainMetricsScopeCache() DomainMetricsScopeCache {
	return &domainMetricsScopeCache{
		scopeMap: make(map[string]metrics.Scope),
	}
}

// Get retrieves scope for domainID and scopeIdx
func (c *domainMetricsScopeCache) Get(domainID string, scopeIdx int) (metrics.Scope, bool) {
	c.RLock()
	defer c.RUnlock()

	var buffer bytes.Buffer
	buffer.WriteString(domainID)
	buffer.WriteString("_")
	buffer.WriteString(strconv.Itoa(scopeIdx))
	key := buffer.String()

	metricsScope, ok := c.scopeMap[key]
	return metricsScope, ok
}

// Put puts map of domainID and scopeIdx to metricsScope
func (c *domainMetricsScopeCache) Put(domainID string, scopeIdx int, scope metrics.Scope) {
	c.Lock()
	defer c.Unlock()

	var buffer bytes.Buffer
	buffer.WriteString(domainID)
	buffer.WriteString("_")
	buffer.WriteString(strconv.Itoa(scopeIdx))
	key := buffer.String()

	c.scopeMap[key] = scope
}
