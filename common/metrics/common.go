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

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives"
)

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
	case primitives.ServerService:
		return Server
	case primitives.UnitTestService:
		return UnitTestService
	default:
		logger.Fatal("Unknown service name for metrics!", tag.Service(serviceName))
		panic(fmt.Sprintf("Unknown service name for metrics: %s", serviceName))
	}
}

// GetMetricsServiceIdx returns service id corresponding to serviceName
func MetricsServiceIdxToServiceName(serviceIdx ServiceIdx) (string, error) {
	switch serviceIdx {
	case Server:
		return primitives.ServerService, nil
	case Frontend:
		return primitives.FrontendService, nil
	case History:
		return primitives.HistoryService, nil
	case Matching:
		return primitives.MatchingService, nil
	case Worker:
		return primitives.WorkerService, nil
	case UnitTestService:
		return primitives.UnitTestService, nil
	default:
		return "", fmt.Errorf(fmt.Sprintf("Unknown service idx for metrics: %d", serviceIdx))
	}
}
