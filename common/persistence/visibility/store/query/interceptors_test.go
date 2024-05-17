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

package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/searchattribute"

	"github.com/stretchr/testify/assert"
)

type (
	testNameInterceptor   struct{}
	testValuesInterceptor struct{}
)

func (t *testNameInterceptor) Name(name string, usage FieldNameUsage) (string, error) {
	if name == "error" {
		return "", errors.New("interceptor error")
	}

	return name + "1", nil
}

func (t *testValuesInterceptor) Values(name string, values ...interface{}) ([]interface{}, error) {
	if name == "error" {
		return nil, errors.New("interceptor error")
	}

	var result []interface{}
	for _, value := range values {
		if name == "ExecutionStatus" {
			intVal, isIntVal := value.(int64)
			if isIntVal {
				result = append(result, fmt.Sprintf("Status%v", intVal))
				continue
			}
		}
		result = append(result, value)
	}
	return result, nil
}

func TestNameInterceptor(t *testing.T) {
	c := getTestConverter(&testNameInterceptor{}, nil)

	queryParams, err := c.ConvertWhereOrderBy("ExecutionStatus='Running' order by StartTime")
	assert.NoError(t, err)
	actualQueryMap, _ := queryParams.Query.Source()
	actualQueryJson, _ := json.Marshal(actualQueryMap)
	assert.Equal(t, `{"bool":{"filter":{"term":{"ExecutionStatus1":"Running"}}}}`, string(actualQueryJson))
	var actualSorterMaps []interface{}
	for _, sorter := range queryParams.Sorter {
		actualSorterMap, _ := sorter.Source()
		actualSorterMaps = append(actualSorterMaps, actualSorterMap)
	}
	actualSorterJson, _ := json.Marshal(actualSorterMaps)
	assert.Equal(t, `[{"StartTime1":{"order":"asc"}}]`, string(actualSorterJson))

	_, err = c.ConvertWhereOrderBy("error='Running' order by StartTime")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "interceptor error")
}

func TestValuesInterceptor(t *testing.T) {
	c := getTestConverter(nil, &testValuesInterceptor{})
	queryParams, err := c.ConvertWhereOrderBy("ExecutionStatus=1")
	assert.NoError(t, err)
	actualQueryMap, _ := queryParams.Query.Source()
	actualQueryJson, _ := json.Marshal(actualQueryMap)
	assert.Equal(t, `{"bool":{"filter":{"term":{"ExecutionStatus":"Status1"}}}}`, string(actualQueryJson))

	queryParams, err = c.ConvertWhereOrderBy("ExecutionStatus in (1,2)")
	assert.NoError(t, err)
	actualQueryMap, _ = queryParams.Query.Source()
	actualQueryJson, _ = json.Marshal(actualQueryMap)
	assert.Equal(t, `{"bool":{"filter":{"terms":{"ExecutionStatus":["Status1","Status2"]}}}}`, string(actualQueryJson))

	queryParams, err = c.ConvertWhereOrderBy("ExecutionStatus between 5 and 7")
	assert.NoError(t, err)
	actualQueryMap, _ = queryParams.Query.Source()
	actualQueryJson, _ = json.Marshal(actualQueryMap)
	assert.Equal(t, `{"bool":{"filter":{"range":{"ExecutionStatus":{"from":"Status5","include_lower":true,"include_upper":true,"to":"Status7"}}}}}`, string(actualQueryJson))

	_, err = c.ConvertWhereOrderBy("error='Running'")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "interceptor error")
}

func getTestConverter(fnInterceptor FieldNameInterceptor, fvInterceptor FieldValuesInterceptor) *Converter {
	testNameTypeMap := searchattribute.NewNameTypeMapStub(
		map[string]enumspb.IndexedValueType{
			"ExecutionStatus1": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"StartTime1":       enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	)
	whereConverter := NewWhereConverter(
		nil,
		nil,
		NewRangeCondConverter(fnInterceptor, fvInterceptor, false),
		NewComparisonExprConverter(fnInterceptor, fvInterceptor, map[string]struct{}{sqlparser.EqualStr: {}, sqlparser.InStr: {}}, testNameTypeMap),
		nil)
	return NewConverter(fnInterceptor, whereConverter)
}
