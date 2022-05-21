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

package host

import (
	"sync"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/visibility"
)

const NamespaceCacheRefreshInterval = time.Second

var (
	// Override values for dynamic configs
	staticOverrides = map[dynamicconfig.Key]interface{}{
		dynamicconfig.FrontendRPS:                                   3000,
		dynamicconfig.FrontendESIndexMaxResultWindow:                defaultTestValueOfESIndexMaxResultWindow,
		dynamicconfig.MatchingNumTaskqueueWritePartitions:           3,
		dynamicconfig.MatchingNumTaskqueueReadPartitions:            3,
		dynamicconfig.TimerProcessorHistoryArchivalSizeLimit:        5 * 1024,
		dynamicconfig.ReplicationTaskProcessorErrorRetryMaxAttempts: 1,
		dynamicconfig.AdvancedVisibilityWritingMode:                 visibility.AdvancedVisibilityWritingModeOff,
		dynamicconfig.WorkflowTaskHeartbeatTimeout:                  5 * time.Second,
		dynamicconfig.ReplicationTaskFetcherAggregationInterval:     200 * time.Millisecond,
		dynamicconfig.ReplicationTaskFetcherErrorRetryWait:          50 * time.Millisecond,
		dynamicconfig.ReplicationTaskProcessorErrorRetryWait:        time.Millisecond,
		dynamicconfig.ClusterMetadataRefreshInterval:                100 * time.Millisecond,
		dynamicconfig.NamespaceCacheRefreshInterval:                 NamespaceCacheRefreshInterval,
	}
)

type dynamicClient struct {
	sync.RWMutex

	overrides map[dynamicconfig.Key]interface{}
	client    dynamicconfig.Client
}

func (d *dynamicClient) GetValue(name dynamicconfig.Key, defaultValue interface{}) (interface{}, error) {
	d.RLock()
	if val, ok := d.overrides[name]; ok {
		d.RUnlock()
		return val, nil
	}
	d.RUnlock()
	return d.client.GetValue(name, defaultValue)
}

func (d *dynamicClient) GetValueWithFilters(
	name dynamicconfig.Key, filters []map[dynamicconfig.Filter]interface{}, defaultValue interface{},
) (interface{}, error) {
	d.RLock()
	if val, ok := d.overrides[name]; ok {
		d.RUnlock()
		return val, nil
	}
	d.RUnlock()
	return d.client.GetValueWithFilters(name, filters, defaultValue)
}

func (d *dynamicClient) GetIntValue(name dynamicconfig.Key, filters []map[dynamicconfig.Filter]interface{}, defaultValue int) (int, error) {
	d.RLock()
	if val, ok := d.overrides[name]; ok {
		if intVal, ok := val.(int); ok {
			d.RUnlock()
			return intVal, nil
		}
	}
	d.RUnlock()
	return d.client.GetIntValue(name, filters, defaultValue)
}

func (d *dynamicClient) GetFloatValue(name dynamicconfig.Key, filters []map[dynamicconfig.Filter]interface{}, defaultValue float64) (float64, error) {
	d.RLock()
	if val, ok := d.overrides[name]; ok {
		if floatVal, ok := val.(float64); ok {
			d.RUnlock()
			return floatVal, nil
		}
	}
	d.RUnlock()
	return d.client.GetFloatValue(name, filters, defaultValue)
}

func (d *dynamicClient) GetBoolValue(name dynamicconfig.Key, filters []map[dynamicconfig.Filter]interface{}, defaultValue bool) (bool, error) {
	d.RLock()
	if val, ok := d.overrides[name]; ok {
		if boolVal, ok := val.(bool); ok {
			d.RUnlock()
			return boolVal, nil
		}
	}
	d.RUnlock()
	return d.client.GetBoolValue(name, filters, defaultValue)
}

func (d *dynamicClient) GetStringValue(name dynamicconfig.Key, filters []map[dynamicconfig.Filter]interface{}, defaultValue string) (string, error) {
	d.RLock()
	if val, ok := d.overrides[name]; ok {
		if stringVal, ok := val.(string); ok {
			d.RUnlock()
			return stringVal, nil
		}
	}
	d.RUnlock()
	return d.client.GetStringValue(name, filters, defaultValue)
}

func (d *dynamicClient) GetMapValue(
	name dynamicconfig.Key, filters []map[dynamicconfig.Filter]interface{}, defaultValue map[string]interface{},
) (map[string]interface{}, error) {
	d.RLock()
	if val, ok := d.overrides[name]; ok {
		if mapVal, ok := val.(map[string]interface{}); ok {
			d.RUnlock()
			return mapVal, nil
		}
	}
	d.RUnlock()
	return d.client.GetMapValue(name, filters, defaultValue)
}

func (d *dynamicClient) GetDurationValue(
	name dynamicconfig.Key, filters []map[dynamicconfig.Filter]interface{}, defaultValue time.Duration,
) (time.Duration, error) {
	d.RLock()
	if val, ok := d.overrides[name]; ok {
		if durationVal, ok := val.(time.Duration); ok {
			d.RUnlock()
			return durationVal, nil
		}
	}
	d.RUnlock()
	return d.client.GetDurationValue(name, filters, defaultValue)
}

func (d *dynamicClient) OverrideValue(name dynamicconfig.Key, value interface{}) {
	d.Lock()
	defer d.Unlock()
	d.overrides[name] = value
}

// newIntegrationConfigClient - returns a dynamic config client for integration testing
func newIntegrationConfigClient(client dynamicconfig.Client) *dynamicClient {
	integrationClient := &dynamicClient{
		overrides: make(map[dynamicconfig.Key]interface{}),
		client:    client,
	}

	for key, value := range staticOverrides {
		integrationClient.OverrideValue(key, value)
	}
	return integrationClient
}
