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

package sql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/temporalio/sqlparser"

	"go.temporal.io/server/common/persistence/visibility/store/query"
)

type (
	postgresqlQueryConverterSuite struct {
		queryConverterSuite
	}
)

func TestPostgreSQLQueryConverterSuite(t *testing.T) {
	s := &postgresqlQueryConverterSuite{
		queryConverterSuite: queryConverterSuite{
			pqc: &pgQueryConverter{},
		},
	}
	suite.Run(t, s)
}

func (s *postgresqlQueryConverterSuite) TestGetCoalesceCloseTimeExpr() {
	expr := s.queryConverter.getCoalesceCloseTimeExpr()
	s.Equal(
		"coalesce(close_time, '9999-12-31 23:59:59')",
		sqlparser.String(expr),
	)
}

func (s *postgresqlQueryConverterSuite) TestConvertKeywordListComparisonExpr() {
	var tests = []testCase{
		{
			name:   "invalid operator",
			input:  "AliasForKeywordList01 < 'foo'",
			output: "",
			err: query.NewConverterError(
				"%s: operator '%s' not supported for KeywordList type search attribute in `%s`",
				query.InvalidExpressionErrMessage,
				sqlparser.LessThanStr,
				"AliasForKeywordList01 < 'foo'",
			),
		},
		{
			name:   "valid equal expression",
			input:  "AliasForKeywordList01 = 'foo'",
			output: "KeywordList01 @> jsonb_build_array('foo')",
			err:    nil,
		},
		{
			name:   "valid not equal expression",
			input:  "AliasForKeywordList01 != 'foo'",
			output: "not KeywordList01 @> jsonb_build_array('foo')",
			err:    nil,
		},
		{
			name:   "valid in expression",
			input:  "AliasForKeywordList01 in ('foo', 'bar')",
			output: "(KeywordList01 @> jsonb_build_array('foo') or KeywordList01 @> jsonb_build_array('bar'))",
			err:    nil,
		},
		{
			name:   "valid not in expression",
			input:  "AliasForKeywordList01 not in ('foo', 'bar')",
			output: "not (KeywordList01 @> jsonb_build_array('foo') or KeywordList01 @> jsonb_build_array('bar'))",
			err:    nil,
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			sql := fmt.Sprintf("select * from table1 where %s", tc.input)
			stmt, err := sqlparser.Parse(sql)
			s.NoError(err)
			expr := stmt.(*sqlparser.Select).Where.Expr
			err = s.queryConverter.convertComparisonExpr(&expr)
			if tc.err == nil {
				s.NoError(err)
				s.Equal(tc.output, sqlparser.String(expr))
			} else {
				s.Error(err)
				s.Equal(err, tc.err)
			}
		})
	}
}

func (s *postgresqlQueryConverterSuite) TestConvertTextComparisonExpr() {
	var tests = []testCase{
		{
			name:   "invalid operator",
			input:  "AliasForText01 < 'foo'",
			output: "",
			err: query.NewConverterError(
				"%s: operator '%s' not supported for Text type search attribute in `%s`",
				query.InvalidExpressionErrMessage,
				sqlparser.LessThanStr,
				"AliasForText01 < 'foo'",
			),
		},
		{
			name:   "valid equal expression",
			input:  "AliasForText01 = 'foo bar'",
			output: "Text01 @@ 'foo | bar'::tsquery",
			err:    nil,
		},
		{
			name:   "valid not equal expression",
			input:  "AliasForText01 != 'foo bar'",
			output: "not Text01 @@ 'foo | bar'::tsquery",
			err:    nil,
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			sql := fmt.Sprintf("select * from table1 where %s", tc.input)
			stmt, err := sqlparser.Parse(sql)
			s.NoError(err)
			expr := stmt.(*sqlparser.Select).Where.Expr
			err = s.queryConverter.convertComparisonExpr(&expr)
			if tc.err == nil {
				s.NoError(err)
				s.Equal(tc.output, sqlparser.String(expr))
			} else {
				s.Error(err)
				s.Equal(err, tc.err)
			}
		})
	}
}
