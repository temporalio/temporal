// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package scheduler

import (
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	expmaps "golang.org/x/exp/maps"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

type (
	fieldNameAggInterceptor struct {
		baseInterceptor query.FieldNameInterceptor
		names           map[string]bool
	}
)

var _ query.FieldNameInterceptor = (*fieldNameAggInterceptor)(nil)

func (i *fieldNameAggInterceptor) Name(name string, usage query.FieldNameUsage) (string, error) {
	i.names[name] = true
	return i.baseInterceptor.Name(name, usage)
}

func newFieldNameAggInterceptor(
	namespaceName namespace.Name,
	saNameType searchattribute.NameTypeMap,
	saMapperProvider searchattribute.MapperProvider,
) *fieldNameAggInterceptor {
	return &fieldNameAggInterceptor{
		baseInterceptor: elasticsearch.NewNameInterceptor(namespaceName, saNameType, saMapperProvider),
		names:           make(map[string]bool),
	}
}

func ValidateVisibilityQuery(
	namespaceName namespace.Name,
	saNameType searchattribute.NameTypeMap,
	saMapperProvider searchattribute.MapperProvider,
	queryString string,
) error {
	fields, err := getQueryFields(namespaceName, saNameType, saMapperProvider, queryString)
	if err != nil {
		return err
	}
	for _, field := range fields {
		if searchattribute.IsReserved(field) && field != searchattribute.TemporalSchedulePaused {
			return serviceerror.NewInvalidArgument(
				fmt.Sprintf("invalid query filter for schedules: cannot filter on %q", field),
			)
		}
	}
	return nil
}

func getQueryFields(
	namespaceName namespace.Name,
	saNameType searchattribute.NameTypeMap,
	saMapperProvider searchattribute.MapperProvider,
	queryString string,
) ([]string, error) {
	fnInterceptor := newFieldNameAggInterceptor(namespaceName, saNameType, saMapperProvider)
	queryConverter := elasticsearch.NewQueryConverter(fnInterceptor, nil, saNameType)
	_, err := queryConverter.ConvertWhereOrderBy(queryString)
	if err != nil {
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}
	return expmaps.Keys(fnInterceptor.names), nil
}
