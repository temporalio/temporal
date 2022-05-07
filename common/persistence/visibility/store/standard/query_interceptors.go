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

package standard

import (
	"time"

	"go.temporal.io/server/common/searchattribute"

	"go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

var allowedFilters = []string{
	searchattribute.WorkflowID,
	searchattribute.WorkflowType,
	searchattribute.ExecutionStatus,
	searchattribute.StartTime,
}

type (
	nameInterceptor   struct{}
	valuesInterceptor struct {
		filter          *sqlplugin.VisibilitySelectFilter
		nextInterceptor query.FieldValuesInterceptor
	}
)

func newNameInterceptor() *nameInterceptor {
	return &nameInterceptor{}
}

func newValuesInterceptor() *valuesInterceptor {
	return &valuesInterceptor{
		filter:          &sqlplugin.VisibilitySelectFilter{},
		nextInterceptor: elasticsearch.NewValuesInterceptor(),
	}
}

func (ni *nameInterceptor) Name(name string, usage query.FieldNameUsage) (string, error) {
	if usage == query.FieldNameSorter {
		return "", query.NewConverterError("order by not allowed for standard visibility")
	}

	for _, filter := range allowedFilters {
		if filter == name {
			return name, nil
		}
	}

	return "", query.NewConverterError("filter by '%v' not supported for standard visibility", name)
}

func (vi *valuesInterceptor) Values(name string, values ...interface{}) ([]interface{}, error) {
	switch name {
	case searchattribute.WorkflowID:
		values, err := vi.nextInterceptor.Values(name, values...)
		if err == nil {
			vi.filter.WorkflowID = convert.StringPtr(values[0].(string))
		}
		return values, err
	case searchattribute.WorkflowType:
		values, err := vi.nextInterceptor.Values(name, values...)
		if err == nil {
			vi.filter.WorkflowTypeName = convert.StringPtr(values[0].(string))
		}
		return values, err
	case searchattribute.ExecutionStatus:
		values, err := vi.nextInterceptor.Values(name, values...)
		if err == nil {
			statusStr := values[0].(string)
			vi.filter.Status = enums.WorkflowExecutionStatus_value[statusStr]
		}
		return values, err
	case searchattribute.StartTime:
		if len(values) != 2 {
			return nil, query.NewConverterError("StartTime only supports BETWEEN ... AND ... filter")
		}

		values, err := vi.nextInterceptor.Values(name, values...)
		if err == nil {
			minTime, err := time.Parse(time.RFC3339Nano, values[0].(string))
			if err != nil {
				return nil, query.NewConverterError("invalid StartTime format: %v", minTime)
			}
			vi.filter.MinTime = &minTime

			maxTime, err := time.Parse(time.RFC3339Nano, values[1].(string))
			if err != nil {
				return nil, query.NewConverterError("invalid StartTime format: %v", maxTime)
			}
			vi.filter.MaxTime = &maxTime
		}
		return values, err
	default:
		return nil, query.NewConverterError("filter by '%v' not supported for standard visibility", name)
	}
}
