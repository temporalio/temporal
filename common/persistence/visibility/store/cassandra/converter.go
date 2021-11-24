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

	"github.com/xwb1989/sqlparser"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

var allowedComparisonOperators = map[string]bool{
	sqlparser.EqualStr: true,
}

type (
	converter struct {
		*query.Converter
		fvInterceptor *valuesInterceptor
	}

	queryFilters struct {
		HasWorkflowIDFilter      bool
		WorkflowID               string
		HasWorkflowTypeFilter    bool
		WorkflowType             string
		HasExecutionStatusFilter bool
		ExecutionStatus          enums.WorkflowExecutionStatus
		HasStartTimeRangeFilter  bool
		StartTimeFrom            time.Time
		StartTimeTo              time.Time
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

func (c *converter) QueryFilters() *queryFilters {
	return c.fvInterceptor.queryFilters
}
