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
	"errors"
	"time"

	"github.com/xwb1989/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

var allowedComparisonOperators = map[string]struct{}{
	sqlparser.EqualStr: {},
}

type (
	converter struct {
		*query.Converter
		fvInterceptor *valuesInterceptor
	}
)

func newQueryConverter() *converter {
	fnInterceptor := newNameInterceptor()
	fvInterceptor := newValuesInterceptor()

	rangeCond := query.NewRangeCondConverter(fnInterceptor, fvInterceptor, false)
	comparisonExpr := query.NewComparisonExprConverter(fnInterceptor, fvInterceptor, allowedComparisonOperators)

	whereConverter := &query.WhereConverter{
		Or:             query.NewNotSupportedExprConverter(),
		RangeCond:      rangeCond,
		ComparisonExpr: comparisonExpr,
		Is:             query.NewNotSupportedExprConverter(),
	}
	whereConverter.And = query.NewAndConverter(whereConverter)

	return &converter{
		Converter:     query.NewConverter(fnInterceptor, whereConverter),
		fvInterceptor: fvInterceptor,
	}
}

func (c *converter) GetFilter(whereOrderBy string) (*sqlplugin.VisibilitySelectFilter, error) {
	_, _, err := c.ConvertWhereOrderBy(whereOrderBy)
	if err != nil {
		// Convert ConverterError to InvalidArgument and pass through all other errors.
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}

	filter := c.fvInterceptor.filter
	numPredicates := 0

	if filter.WorkflowID != nil {
		numPredicates++
	}

	if filter.WorkflowTypeName != nil {
		numPredicates++
	}

	if filter.Status != int32(enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED) {
		numPredicates++
	}

	if numPredicates > 1 {
		return nil, query.NewConverterError("too many filter conditions specified")
	}

	if filter.MinTime == nil {
		minTime := time.Unix(0, 0)
		filter.MinTime = &minTime
	}

	if filter.MaxTime == nil {
		maxTime := time.Now().Add(24 * time.Hour)
		filter.MaxTime = &maxTime
	}

	return filter, nil
}
