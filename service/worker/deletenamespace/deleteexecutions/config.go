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

package deleteexecutions

import (
	"encoding/json"
)

const (
	defaultDeleteActivityRPS                    = 100
	defaultPageSize                             = 1000
	defaultPagesPerExecutionCount               = 256
	defaultConcurrentDeleteExecutionsActivities = 4
	maxConcurrentDeleteExecutionsActivities     = 256
)

type (
	DeleteExecutionsConfig struct {
		// RPS per every parallel delete executions activity.
		// Total RPS is equal to DeleteActivityRPS * ConcurrentDeleteExecutionsActivities.
		DeleteActivityRPS int
		// Page size to read executions from visibility.
		PageSize int
		// Number of pages before returning ContinueAsNew.
		PagesPerExecutionCount int
		// Number of concurrent delete executions activities.
		// Must be not greater than PagesPerExecutionCount and number of worker cores in the cluster.
		ConcurrentDeleteExecutionsActivities int
	}
)

func (cfg *DeleteExecutionsConfig) ApplyDefaults() {
	if cfg.DeleteActivityRPS <= 0 {
		cfg.DeleteActivityRPS = defaultDeleteActivityRPS
	}
	if cfg.PageSize <= 0 {
		cfg.PageSize = defaultPageSize
	}
	if cfg.PagesPerExecutionCount <= 0 {
		cfg.PagesPerExecutionCount = defaultPagesPerExecutionCount
	}
	if cfg.ConcurrentDeleteExecutionsActivities <= 0 {
		cfg.ConcurrentDeleteExecutionsActivities = defaultConcurrentDeleteExecutionsActivities
	}
	if cfg.ConcurrentDeleteExecutionsActivities > maxConcurrentDeleteExecutionsActivities {
		cfg.ConcurrentDeleteExecutionsActivities = maxConcurrentDeleteExecutionsActivities
	}
	// It won't be able to start more than PagesPerExecutionCount activities.
	if cfg.ConcurrentDeleteExecutionsActivities > cfg.PagesPerExecutionCount {
		cfg.ConcurrentDeleteExecutionsActivities = cfg.PagesPerExecutionCount
	}
}

func (cfg DeleteExecutionsConfig) String() string {
	cfgBytes, _ := json.Marshal(cfg)
	return string(cfgBytes)
}
