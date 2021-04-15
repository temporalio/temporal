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

package metrics

import (
	"fmt"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives"
)

const (
	distributionToTimerRatio = int(time.Millisecond / time.Nanosecond)
)

func mergeMapToRight(src map[string]string, dest map[string]string) {
	for k, v := range src {
		dest[k] = v
	}
}

func getMetricDefs(serviceIdx ServiceIdx) map[int]metricDefinition {
	defs := make(map[int]metricDefinition)
	for idx, def := range MetricDefs[Common] {
		defs[idx] = def
	}

	for idx, def := range MetricDefs[serviceIdx] {
		defs[idx] = def
	}

	return defs
}

// GetMetricsServiceIdx returns service id corresponding to serviceName
func GetMetricsServiceIdx(serviceName string, logger log.Logger) ServiceIdx {
	switch serviceName {
	case primitives.FrontendService:
		return Frontend
	case primitives.HistoryService:
		return History
	case primitives.MatchingService:
		return Matching
	case primitives.WorkerService:
		return Worker
	default:
		logger.Fatal("Unknown service name for metrics!", tag.Service(serviceName))
		panic(fmt.Sprintf("Unknown service name for metrics: %s", serviceName))
	}
}
