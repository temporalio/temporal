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
	deleteActivityRPS                       = 100
	pageSize                                = 1000
	pagesPerExecutionCount                  = 256
	concurrentDeleteExecutionsActivities    = 4
	maxConcurrentDeleteExecutionsActivities = 256
)

type (
	DeleteExecutionsConfig struct {
		DeleteActivityRPS                    int
		PageSize                             int
		PagesPerExecutionCount               int // Before doing ContinueAsNew.
		ConcurrentDeleteExecutionsActivities int // Must be not greater than PagesPerExecutionCount and number of workers in the cluster.
	}
)

func (cfg *DeleteExecutionsConfig) ApplyDefaults() {
	if cfg.DeleteActivityRPS <= 0 {
		cfg.DeleteActivityRPS = deleteActivityRPS
	}
	if cfg.PageSize <= 0 {
		cfg.PageSize = pageSize
	}
	if cfg.PagesPerExecutionCount <= 0 {
		cfg.PagesPerExecutionCount = pagesPerExecutionCount
	}
	if cfg.ConcurrentDeleteExecutionsActivities <= 0 {
		cfg.ConcurrentDeleteExecutionsActivities = concurrentDeleteExecutionsActivities
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
