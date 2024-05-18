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
		dynamicconfig.FrontendRPS.Key():                                         3000,
		dynamicconfig.FrontendMaxNamespaceVisibilityRPSPerInstance.Key():        50,
		dynamicconfig.FrontendMaxNamespaceVisibilityBurstRatioPerInstance.Key(): 1,
		dynamicconfig.ReplicationTaskProcessorErrorRetryMaxAttempts.Key():       1,
		dynamicconfig.SecondaryVisibilityWritingMode.Key():                      visibility.SecondaryVisibilityWritingModeOff,
		dynamicconfig.WorkflowTaskHeartbeatTimeout.Key():                        5 * time.Second,
		dynamicconfig.ReplicationTaskFetcherAggregationInterval.Key():           200 * time.Millisecond,
		dynamicconfig.ReplicationTaskFetcherErrorRetryWait.Key():                50 * time.Millisecond,
		dynamicconfig.ReplicationTaskProcessorErrorRetryWait.Key():              time.Millisecond,
		dynamicconfig.ClusterMetadataRefreshInterval.Key():                      100 * time.Millisecond,
		dynamicconfig.NamespaceCacheRefreshInterval.Key():                       NamespaceCacheRefreshInterval,
		dynamicconfig.FrontendEnableUpdateWorkflowExecution.Key():               true,
		dynamicconfig.FrontendEnableUpdateWorkflowExecutionAsyncAccepted.Key():  true,
		dynamicconfig.FrontendAccessHistoryFraction.Key():                       0.5,
		dynamicconfig.ReplicationEnableUpdateWithNewTaskMerge.Key():             true,
		dynamicconfig.ValidateUTF8SampleRPCRequest.Key():                        1.0,
		dynamicconfig.ValidateUTF8SampleRPCResponse.Key():                       1.0,
		dynamicconfig.ValidateUTF8SamplePersistence.Key():                       1.0,
		dynamicconfig.ValidateUTF8FailRPCRequest.Key():                          true,
		dynamicconfig.ValidateUTF8FailRPCResponse.Key():                         true,
		dynamicconfig.ValidateUTF8FailPersistence.Key():                         true,
		dynamicconfig.EnableWorkflowExecutionTimeoutTimer.Key():                 true,
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
func (d *dcClient) OverrideValue(t *testing.T, setting dynamicconfig.GenericSetting, value any) {
	d.OverrideValueByKey(t, setting.Key(), value)
}

func (d *dcClient) OverrideValueByKey(t *testing.T, name dynamicconfig.Key, value any) {
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

func (d *dcClient) RemoveOverride(setting dynamicconfig.GenericSetting) {
	d.Lock()
	defer d.Unlock()
	delete(d.overrides, setting.Key())
}

// newTestDCClient - returns a dynamic config client for functional testing
func newTestDCClient(fallback dynamicconfig.Client) *dcClient {
	return &dcClient{
		overrides: maps.Clone(staticOverrides),
		fallback:  fallback,
	}
}
