// The MIT License
//
// Copyright (c) 2021 Temporal Technologies Inc.  All rights reserved.
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

package cassandra

import (
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

var allowedFilters = []string{"WorkflowID", "WorkflowType", "ExecutionStatus", "StartTime"}

type (
	nameInterceptor   struct{}
	valuesInterceptor struct {
		queryFilters    *queryFilters
		nextInterceptor query.FieldValuesInterceptor
	}
)

func newNameInterceptor() *nameInterceptor {
	return &nameInterceptor{}
}

func newValuesInterceptor() *valuesInterceptor {
	return &valuesInterceptor{
		queryFilters:    &queryFilters{
			StartTimeFrom: time.Unix(0, 0),
			StartTimeTo: time.Now().Add(24*time.Hour),
		},
		nextInterceptor: elasticsearch.NewValuesInterceptor(),
	}
}

func (ni *nameInterceptor) Name(name string, usage query.FieldNameUsage) (string, error) {
	if usage == query.FieldNameSorter {
		return "", query.NewConverterError("order by not allowed for basic visibility")
	}

	for _, filter := range allowedFilters {
		if filter == name {
			return name, nil
		}
	}

	return "", query.NewConverterError("filter %v not supported for Cassandra visibility", name)
}

func (vi *valuesInterceptor) Values(name string, values ...interface{}) ([]interface{}, error) {
	switch name {
	case "WorkflowID":
		if vi.queryFilters.HasWorkflowTypeFilter || vi.queryFilters.HasExecutionStatusFilter {
			return nil, query.NewConverterError("too many filter conditions specified")
		}
		vi.queryFilters.HasWorkflowIDFilter = true
		values, err := vi.nextInterceptor.Values(name, values...)
		if err == nil {
			vi.queryFilters.WorkflowID = values[0].(string)
		}
		return values, err
	case "WorkflowType":
		if vi.queryFilters.HasWorkflowIDFilter || vi.queryFilters.HasExecutionStatusFilter {
			return nil, query.NewConverterError("too many filter conditions specified")
		}
		vi.queryFilters.HasWorkflowTypeFilter = true
		values, err := vi.nextInterceptor.Values(name, values...)
		if err == nil {
			vi.queryFilters.WorkflowType = values[0].(string)
		}
		return values, err
	case "ExecutionStatus":
		if vi.queryFilters.HasWorkflowIDFilter || vi.queryFilters.HasWorkflowTypeFilter {
			return nil, query.NewConverterError("too many filter conditions specified")
		}
		vi.queryFilters.HasExecutionStatusFilter = true
		values, err := vi.nextInterceptor.Values(name, values...)
		if err == nil {
			statusStr := values[0].(string)
			vi.queryFilters.ExecutionStatus = enums.WorkflowExecutionStatus(enums.WorkflowExecutionStatus_value[statusStr])
		}
		return values, err
	case "StartTime":
		vi.queryFilters.HasStartTimeRangeFilter = true
		values, err := vi.nextInterceptor.Values(name, values...)
		if err == nil {
			fromTime := values[0].(string)
			vi.queryFilters.StartTimeFrom, err = time.Parse(time.RFC3339Nano, fromTime)
			if err != nil {
				return nil, query.NewConverterError("invalid StartTime format: %v", fromTime)
			}

			toTime := values[1].(string)
			vi.queryFilters.StartTimeTo, err = time.Parse(time.RFC3339Nano, toTime)
			if err != nil {
				return nil, query.NewConverterError("invalid StartTime format: %v", toTime)
			}
		}
		return values, err
	default:
		return nil, query.NewConverterError("filter %v not supported for Cassandra visibility", name)
	}
}
