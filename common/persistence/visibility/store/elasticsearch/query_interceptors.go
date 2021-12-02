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

package elasticsearch

import (
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
)

type (
	nameInterceptor struct {
		namespace               namespace.Name
		index                   string
		searchAttributesTypeMap searchattribute.NameTypeMap
		searchAttributesMapper  searchattribute.Mapper
	}
	valuesInterceptor struct{}
)

func newNameInterceptor(
	namespace namespace.Name,
	index string,
	saTypeMap searchattribute.NameTypeMap,
	searchAttributesMapper searchattribute.Mapper,
) *nameInterceptor {
	return &nameInterceptor{
		namespace:               namespace,
		index:                   index,
		searchAttributesTypeMap: saTypeMap,
		searchAttributesMapper:  searchAttributesMapper,
	}
}

func NewValuesInterceptor() *valuesInterceptor {
	return &valuesInterceptor{}
}

func (ni *nameInterceptor) Name(name string, usage query.FieldNameUsage) (string, error) {
	fieldName := name
	if searchattribute.IsMappable(name) && ni.searchAttributesMapper != nil {
		var err error
		fieldName, err = ni.searchAttributesMapper.GetFieldName(name, ni.namespace.String())
		if err != nil {
			return "", err
		}
	}

	fieldType, err := ni.searchAttributesTypeMap.GetType(fieldName)
	if err != nil {
		return "", query.NewConverterError("invalid search attribute: %s", name)
	}

	if usage == query.FieldNameSorter {
		if fieldType == enumspb.INDEXED_VALUE_TYPE_TEXT {
			return "", query.NewConverterError("unable to sort by field of %s type, use field of type %s", enumspb.INDEXED_VALUE_TYPE_TEXT.String(), enumspb.INDEXED_VALUE_TYPE_KEYWORD.String())
		}
	}

	return fieldName, nil
}

func (vi *valuesInterceptor) Values(name string, values ...interface{}) ([]interface{}, error) {
	var result []interface{}
	for _, value := range values {

		switch name {
		case searchattribute.StartTime, searchattribute.CloseTime, searchattribute.ExecutionTime:
			if nanos, isNumber := value.(int64); isNumber {
				value = time.Unix(0, nanos).UTC().Format(time.RFC3339Nano)
			}
		case searchattribute.ExecutionStatus:
			if status, isNumber := value.(int64); isNumber {
				value = enumspb.WorkflowExecutionStatus_name[int32(status)]
			}
		case searchattribute.ExecutionDuration:
			if durationStr, isString := value.(string); isString {
				// To support durations passed as golang durations such as "300ms", "-1.5h" or "2h45m".
				// Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
				// Custom timestamp.ParseDuration also supports "d" as additional unit for days.
				if duration, err := timestamp.ParseDuration(durationStr); err == nil {
					value = duration.Nanoseconds()
				} else {
					// To support "hh:mm:ss" durations.
					durationNanos, err := vi.parseHHMMSSDuration(durationStr)
					var converterErr *query.ConverterError
					if errors.As(err, &converterErr) {
						return nil, converterErr
					}
					if err == nil {
						value = durationNanos
					}
				}
			}
		default:
		}

		result = append(result, value)
	}
	return result, nil
}

func (vi *valuesInterceptor) parseHHMMSSDuration(d string) (int64, error) {
	var hours, minutes, seconds, nanos int64
	_, err := fmt.Sscanf(d, "%d:%d:%d", &hours, &minutes, &seconds)
	if err != nil {
		return 0, errors.New("value is not a duration")
	}
	if hours < 0 {
		return 0, query.NewConverterError("invalid duration: hours must be positive number")
	}
	if minutes < 0 || minutes > 59 {
		return 0, query.NewConverterError("invalid duration: minutes must be from 0 to 59")
	}
	if seconds < 0 || seconds > 59 {
		return 0, query.NewConverterError("invalid duration: seconds must be from 0 to 59")
	}

	return hours*int64(time.Hour) + minutes*int64(time.Minute) + seconds*int64(time.Second) + nanos, nil
}
