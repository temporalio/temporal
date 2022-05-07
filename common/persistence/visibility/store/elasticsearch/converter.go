// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Copyright (c) 2017 Xargin
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
	"github.com/xwb1989/sqlparser"

	"go.temporal.io/server/common/persistence/visibility/store/query"
)

var allowedComparisonOperators = map[string]struct{}{
	sqlparser.EqualStr:        {},
	sqlparser.NotEqualStr:     {},
	sqlparser.GreaterThanStr:  {},
	sqlparser.GreaterEqualStr: {},
	sqlparser.LessThanStr:     {},
	sqlparser.LessEqualStr:    {},
	sqlparser.LikeStr:         {},
	sqlparser.NotLikeStr:      {},
	sqlparser.InStr:           {},
	sqlparser.NotInStr:        {},
}

func newQueryConverter(
	fnInterceptor query.FieldNameInterceptor,
	fvInterceptor query.FieldValuesInterceptor,
) *query.Converter {
	if fnInterceptor == nil {
		fnInterceptor = &query.NopFieldNameInterceptor{}
	}

	if fvInterceptor == nil {
		fvInterceptor = &query.NopFieldValuesInterceptor{}
	}

	rangeCond := query.NewRangeCondConverter(fnInterceptor, fvInterceptor, true)
	comparisonExpr := query.NewComparisonExprConverter(fnInterceptor, fvInterceptor, allowedComparisonOperators)
	is := query.NewIsConverter(fnInterceptor)

	whereConverter := &query.WhereConverter{
		RangeCond:      rangeCond,
		ComparisonExpr: comparisonExpr,
		Is:             is,
	}
	whereConverter.And = query.NewAndConverter(whereConverter)
	whereConverter.Or = query.NewOrConverter(whereConverter)

	return query.NewConverter(fnInterceptor, whereConverter)
}
