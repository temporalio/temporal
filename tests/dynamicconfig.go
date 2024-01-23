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

package tests

import (
	"sync"
	"testing"
	"time"

	"golang.org/x/exp/maps"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/visibility"
)

const NamespaceCacheRefreshInterval = time.Second

var (
	// Override values for dynamic configs
	staticOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.FrontendRPS:                                    3000,
		dynamicconfig.FrontendMaxNamespaceVisibilityRPSPerInstance:   50,
		dynamicconfig.FrontendMaxNamespaceVisibilityBurstPerInstance: 50,
		dynamicconfig.ReplicationTaskProcessorErrorRetryMaxAttempts:  1,
		dynamicconfig.SecondaryVisibilityWritingMode:                 visibility.SecondaryVisibilityWritingModeOff,
		dynamicconfig.WorkflowTaskHeartbeatTimeout:                   5 * time.Second,
		dynamicconfig.ReplicationTaskFetcherAggregationInterval:      200 * time.Millisecond,
		dynamicconfig.ReplicationTaskFetcherErrorRetryWait:           50 * time.Millisecond,
		dynamicconfig.ReplicationTaskProcessorErrorRetryWait:         time.Millisecond,
		dynamicconfig.ClusterMetadataRefreshInterval:                 100 * time.Millisecond,
		dynamicconfig.NamespaceCacheRefreshInterval:                  NamespaceCacheRefreshInterval,
		dynamicconfig.FrontendEnableUpdateWorkflowExecution:          true,
		dynamicconfig.FrontendAccessHistoryFraction:                  0.5,
	}
)

type dcClient struct {
	sync.RWMutex
	overrides map[dynamicconfig.Key]any
	fallback  dynamicconfig.Client
}

func (d *dcClient) getRawValue(name dynamicconfig.Key) (any, bool) {
	d.RLock()
	defer d.RUnlock()
	v, ok := d.overrides[name]
	return v, ok
}

func (d *dcClient) GetValue(name dynamicconfig.Key) []dynamicconfig.ConstrainedValue {
	if val, ok := d.getRawValue(name); ok {
		return []dynamicconfig.ConstrainedValue{{Value: val}}
	}
	return d.fallback.GetValue(name)
}

// OverrideValue overrides a value for the duration of a test. Once the test completes
// the previous value (if any) will be restored
func (d *dcClient) OverrideValue(t *testing.T, name dynamicconfig.Key, value any) {
	d.Lock()
	defer d.Unlock()
	priorValue, existed := d.overrides[name]
	d.overrides[name] = value

	t.Cleanup(func() {
		d.Lock()
		defer d.Unlock()

		if existed {
			d.overrides[name] = priorValue
		} else {
			delete(d.overrides, name)
		}
	})
}

func (d *dcClient) RemoveOverride(name dynamicconfig.Key) {
	d.Lock()
	defer d.Unlock()
	delete(d.overrides, name)
}

// newTestDCClient - returns a dynamic config client for functional testing
func newTestDCClient(fallback dynamicconfig.Client) *dcClient {
	return &dcClient{
		overrides: maps.Clone(staticOverrides),
		fallback:  fallback,
	}
}
