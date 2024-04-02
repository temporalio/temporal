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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/temporalio/sqlparser"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

type (
	queryConverterSuite struct {
		suite.Suite
		*require.Assertions

		pqc            pluginQueryConverter
		queryConverter *QueryConverter
	}

	testCase struct {
		name     string
		input    string
		args     map[string]any
		output   any
		retValue any
		err      error
	}
)

const (
	testNamespaceName = namespace.Name("test-namespace")
	testNamespaceID   = namespace.ID("test-namespace-id")
)

func (s *queryConverterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.queryConverter = newQueryConverterInternal(
		s.pqc,
		testNamespaceName,
		testNamespaceID,
		searchattribute.TestNameTypeMap,
		&searchattribute.TestMapper{},
		"",
	)
}

// TestConvertWhereString tests convertSelectStmt since convertWhereString is
// just a wrapper for convertSelectStmt to parse users query string.
func (s *queryConverterSuite) TestConvertWhereString() {
	var tests = []testCase{
		{
			name:   "empty string",
			input:  "",
			output: &queryParams{queryString: "TemporalNamespaceDivision is null"},
			err:    nil,
		},
		{
			name:   "single condition int",
			input:  "AliasForInt01 = 1",
			output: &queryParams{queryString: "(Int01 = 1) and TemporalNamespaceDivision is null"},
			err:    nil,
		},
		{
			name:   "single condition keyword",
			input:  "AliasForKeyword01 = 1",
			output: &queryParams{queryString: "(Keyword01 = 1) and TemporalNamespaceDivision is null"},
			err:    nil,
		},
		{
			name:   "or condition keyword",
			input:  "AliasForInt01 = 1 OR AliasForKeyword01 = 1",
			output: &queryParams{queryString: "(Int01 = 1 or Keyword01 = 1) and TemporalNamespaceDivision is null"},
			err:    nil,
		},
		{
			name:   "no double parenthesis",
			input:  "(AliasForInt01 = 1 OR AliasForKeyword01 = 1)",
			output: &queryParams{queryString: "(Int01 = 1 or Keyword01 = 1) and TemporalNamespaceDivision is null"},
			err:    nil,
		},
		{
			name:   "has namespace division",
			input:  "(AliasForInt01 = 1 OR AliasForKeyword01 = 1) AND TemporalNamespaceDivision = 'foo'",
			output: &queryParams{queryString: "((Int01 = 1 or Keyword01 = 1) and TemporalNamespaceDivision = 'foo')"},
			err:    nil,
		},
		{
			name:  "group by one field",
			input: "GROUP BY ExecutionStatus",
			output: &queryParams{
				queryString: "TemporalNamespaceDivision is null",
				groupBy:     []string{searchattribute.ExecutionStatus},
			},
			err: nil,
		},
		{
			name:   "group by two fields not supported",
			input:  "GROUP BY ExecutionStatus, WorkflowType",
			output: nil,
			err: query.NewConverterError(
				"%s: 'group by' clause supports only a single field",
				query.NotSupportedErrMessage,
			),
		},
		{
			name:   "group by non ExecutionStatus",
			input:  "GROUP BY WorkflowType",
			output: nil,
			err: query.NewConverterError(
				"%s: 'group by' clause is only supported for %s search attribute",
				query.NotSupportedErrMessage,
				searchattribute.ExecutionStatus,
			),
		},
		{
			name:   "order by not supported",
			input:  "ORDER BY StartTime",
			output: nil,
			err:    query.NewConverterError("%s: 'order by' clause", query.NotSupportedErrMessage),
		},
		{
			name:   "group by with order by not supported",
			input:  "GROUP BY ExecutionStatus ORDER BY StartTime",
			output: nil,
			err:    query.NewConverterError("%s: 'order by' clause", query.NotSupportedErrMessage),
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			qc := newQueryConverterInternal(
				s.pqc,
				testNamespaceName,
				testNamespaceID,
				searchattribute.TestNameTypeMap,
				&searchattribute.TestMapper{},
				"",
			)
			qp, err := qc.convertWhereString(tc.input)
			if tc.err == nil {
				s.NoError(err)
				s.Equal(tc.output, qp)
			} else {
				s.Error(err)
				s.Equal(err, tc.err)
			}
		})
	}
}

func (s *queryConverterSuite) TestConvertAndExpr() {
	var tests = []testCase{
		{
			name:   "invalid",
			input:  "AliasForInt01 = 1",
			output: "",
			err:    query.NewConverterError("`AliasForInt01 = 1` is not an 'AND' expression"),
		},
		{
			name:   "two conditions",
			input:  "AliasForInt01 = 1 AND AliasForKeyword01 = 'foo'",
			output: "Int01 = 1 and Keyword01 = 'foo'",
			err:    nil,
		},
		{
			name:   "left side invalid",
			input:  "AliasForInt01 AND AliasForKeyword01 = 'foo'",
			output: "",
			err:    query.NewConverterError("%s: incomplete expression", query.InvalidExpressionErrMessage),
		},
		{
			name:   "right side invalid",
			input:  "AliasForInt01 = 1 AND AliasForKeyword01",
			output: "",
			err:    query.NewConverterError("%s: incomplete expression", query.InvalidExpressionErrMessage),
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			sql := fmt.Sprintf("select * from table1 where %s", tc.input)
			stmt, err := sqlparser.Parse(sql)
			s.NoError(err)
			expr := stmt.(*sqlparser.Select).Where.Expr
			err = s.queryConverter.convertAndExpr(&expr)
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

func (s *queryConverterSuite) TestConvertOrExpr() {
	var tests = []testCase{
		{
			name:   "invalid",
			input:  "AliasForInt01 = 1",
			output: "",
			err:    query.NewConverterError("`AliasForInt01 = 1` is not an 'OR' expression"),
		},
		{
			name:   "two conditions",
			input:  "AliasForInt01 = 1 OR AliasForKeyword01 = 'foo'",
			output: "Int01 = 1 or Keyword01 = 'foo'",
			err:    nil,
		},
		{
			name:   "left side invalid",
			input:  "AliasForInt01 OR AliasForKeyword01 = 'foo'",
			output: "",
			err:    query.NewConverterError("%s: incomplete expression", query.InvalidExpressionErrMessage),
		},
		{
			name:   "right side invalid",
			input:  "AliasForInt01 = 1 OR AliasForKeyword01",
			output: "",
			err:    query.NewConverterError("%s: incomplete expression", query.InvalidExpressionErrMessage),
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			sql := fmt.Sprintf("select * from table1 where %s", tc.input)
			stmt, err := sqlparser.Parse(sql)
			s.NoError(err)
			expr := stmt.(*sqlparser.Select).Where.Expr
			err = s.queryConverter.convertOrExpr(&expr)
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

func (s *queryConverterSuite) TestConvertComparisonExpr() {
	var tests = []testCase{
		{
			name:   "invalid",
			input:  "AliasForInt01",
			output: "",
			err:    query.NewConverterError("`AliasForInt01` is not a comparison expression"),
		},
		{
			name:   "equal expression",
			input:  "AliasForKeyword01 = 'foo'",
			output: "Keyword01 = 'foo'",
			err:    nil,
		},
		{
			name:   "not equal expression",
			input:  "AliasForKeyword01 != 'foo'",
			output: "Keyword01 != 'foo'",
			err:    nil,
		},
		{
			name:   "less than expression",
			input:  "AliasForInt01 < 10",
			output: "Int01 < 10",
			err:    nil,
		},
		{
			name:   "greater than expression",
			input:  "AliasForInt01 > 10",
			output: "Int01 > 10",
			err:    nil,
		},
		{
			name:   "less than or equal expression",
			input:  "AliasForInt01 <= 10",
			output: "Int01 <= 10",
			err:    nil,
		},
		{
			name:   "greater than or equal expression",
			input:  "AliasForInt01 >= 10",
			output: "Int01 >= 10",
			err:    nil,
		},
		{
			name:   "in expression",
			input:  "AliasForKeyword01 in ('foo', 'bar')",
			output: "Keyword01 in ('foo', 'bar')",
			err:    nil,
		},
		{
			name:   "not in expression",
			input:  "AliasForKeyword01 not in ('foo', 'bar')",
			output: "Keyword01 not in ('foo', 'bar')",
			err:    nil,
		},
		{
			name:   "starts_with expression",
			input:  "AliasForKeyword01 starts_with 'foo_bar%'",
			output: `Keyword01 like 'foo!_bar!%%' escape '!'`,
			err:    nil,
		},
		{
			name:   "not starts_with expression",
			input:  "AliasForKeyword01 not starts_with 'foo_bar%'",
			output: `Keyword01 not like 'foo!_bar!%%' escape '!'`,
			err:    nil,
		},
		{
			name:   "starts_with expression error",
			input:  "AliasForKeyword01 starts_with 123",
			output: "",
			err: query.NewConverterError(
				"%s: right-hand side of '%s' must be a literal string (got: 123)",
				query.InvalidExpressionErrMessage,
				sqlparser.StartsWithStr,
			),
		},
		{
			name:   "not starts_with expression error",
			input:  "AliasForKeyword01 not starts_with 123",
			output: "",
			err: query.NewConverterError(
				"%s: right-hand side of '%s' must be a literal string (got: 123)",
				query.InvalidExpressionErrMessage,
				sqlparser.NotStartsWithStr,
			),
		},
		{
			name:   "like expression",
			input:  "AliasForKeyword01 like 'foo%'",
			output: "",
			err: query.NewConverterError(
				"%s: invalid operator 'like' in `%s`",
				query.InvalidExpressionErrMessage,
				"AliasForKeyword01 like 'foo%'",
			),
		},
		{
			name:   "not like expression",
			input:  "AliasForKeyword01 NOT LIKE 'foo%'",
			output: "",
			err: query.NewConverterError(
				"%s: invalid operator 'not like' in `%s`",
				query.InvalidExpressionErrMessage,
				"AliasForKeyword01 not like 'foo%'",
			),
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

func (s *queryConverterSuite) TestConvertRangeCond() {
	fromDatetime, _ := time.Parse(time.RFC3339Nano, "2020-02-15T20:30:40Z")
	toDatetime, _ := time.Parse(time.RFC3339Nano, "2020-02-16T20:30:40Z")
	var tests = []testCase{
		{
			name:   "invalid",
			input:  "AliasForInt01 = 1",
			output: "",
			err:    query.NewConverterError("`AliasForInt01 = 1` is not a range condition expression"),
		},
		{
			name: "between expression",
			input: fmt.Sprintf(
				"AliasForDatetime01 BETWEEN '%s' AND '%s'",
				fromDatetime.Format(time.RFC3339Nano),
				toDatetime.Format(time.RFC3339Nano),
			),
			output: fmt.Sprintf(
				"Datetime01 between '%s' and '%s'",
				fromDatetime.Format(s.queryConverter.getDatetimeFormat()),
				toDatetime.Format(s.queryConverter.getDatetimeFormat()),
			),
			err: nil,
		},
		{
			name: "not between expression",
			input: fmt.Sprintf(
				"AliasForDatetime01 NOT BETWEEN '%s' AND '%s'",
				fromDatetime.Format(time.RFC3339Nano),
				toDatetime.Format(time.RFC3339Nano),
			),
			output: fmt.Sprintf(
				"Datetime01 not between '%s' and '%s'",
				fromDatetime.Format(s.queryConverter.getDatetimeFormat()),
				toDatetime.Format(s.queryConverter.getDatetimeFormat()),
			),
			err: nil,
		},
		{
			name:   "text type not supported",
			input:  "AliasForText01 BETWEEN 'abc' AND 'abd'",
			output: "",
			err: query.NewConverterError(
				"%s: cannot do range condition on search attribute '%s' of type %s",
				query.InvalidExpressionErrMessage,
				"AliasForText01",
				enumspb.INDEXED_VALUE_TYPE_TEXT.String(),
			),
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			sql := fmt.Sprintf("select * from table1 where %s", tc.input)
			stmt, err := sqlparser.Parse(sql)
			s.NoError(err)
			expr := stmt.(*sqlparser.Select).Where.Expr
			err = s.queryConverter.convertRangeCond(&expr)
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

func (s *queryConverterSuite) TestConvertIsExpr() {
	var tests = []testCase{
		{
			name:   "invalid",
			input:  "AliasForInt01 = 1",
			output: "",
			err:    query.NewConverterError("`AliasForInt01 = 1` is not an 'IS' expression"),
		},
		{
			name:   "is expression",
			input:  "AliasForKeyword01 IS NULL",
			output: "Keyword01 is null",
			err:    nil,
		},
		{
			name:   "is not expression",
			input:  "AliasForKeyword01 IS NOT NULL",
			output: "Keyword01 is not null",
			err:    nil,
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			sql := fmt.Sprintf("select * from table1 where %s", tc.input)
			stmt, err := sqlparser.Parse(sql)
			s.NoError(err)
			expr := stmt.(*sqlparser.Select).Where.Expr
			err = s.queryConverter.convertIsExpr(&expr)
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

func (s *queryConverterSuite) TestConvertColName() {
	var tests = []testCase{
		{
			name:     "invalid: column name expression",
			input:    "10",
			output:   "",
			retValue: nil,
			err: query.NewConverterError(
				"%s: must be a column name but was *sqlparser.SQLVal",
				query.InvalidExpressionErrMessage,
			),
		},
		{
			name:     "invalid search attribute",
			input:    "InvalidName",
			output:   "",
			retValue: nil,
			err: query.NewConverterError(
				"%s: column name '%s' is not a valid search attribute",
				query.InvalidExpressionErrMessage,
				"InvalidName",
			),
		},
		{
			name:   "valid system search attribute: ExecutionStatus",
			input:  "ExecutionStatus",
			output: "status",
			retValue: newSAColName(
				"status",
				"ExecutionStatus",
				"ExecutionStatus",
				enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			),
			err: nil,
		},
		{
			name:   "valid system search attribute: CloseTime",
			input:  "CloseTime",
			output: sqlparser.String(s.queryConverter.getCoalesceCloseTimeExpr()),
			retValue: newSAColName(
				"close_time",
				"CloseTime",
				"CloseTime",
				enumspb.INDEXED_VALUE_TYPE_DATETIME,
			),
			err: nil,
		},
		{
			name:   "valid predefined search attribute: BinaryChecksums",
			input:  "BinaryChecksums",
			output: "BinaryChecksums",
			retValue: newSAColName(
				"BinaryChecksums",
				"BinaryChecksums",
				"BinaryChecksums",
				enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
			),
			err: nil,
		},
		{
			name:   "valid predefined search attribute: TemporalNamespaceDivision",
			input:  "TemporalNamespaceDivision",
			output: "TemporalNamespaceDivision",
			retValue: newSAColName(
				"TemporalNamespaceDivision",
				"TemporalNamespaceDivision",
				"TemporalNamespaceDivision",
				enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			),
			err: nil,
		},
		{
			name:   "valid custom search attribute: int",
			input:  "AliasForInt01",
			output: "Int01",
			retValue: newSAColName(
				"Int01",
				"AliasForInt01",
				"Int01",
				enumspb.INDEXED_VALUE_TYPE_INT,
			),
			err: nil,
		},
		{
			name:   "valid custom search attribute: datetime",
			input:  "AliasForDatetime01",
			output: "Datetime01",
			retValue: newSAColName(
				"Datetime01",
				"AliasForDatetime01",
				"Datetime01",
				enumspb.INDEXED_VALUE_TYPE_DATETIME,
			),
			err: nil,
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			// reset internal state of seenNamespaceDivision
			s.queryConverter.seenNamespaceDivision = false
			sql := fmt.Sprintf("select * from table1 where %s", tc.input)
			stmt, err := sqlparser.Parse(sql)
			s.NoError(err)
			expr := stmt.(*sqlparser.Select).Where.Expr
			saColNameExpr, err := s.queryConverter.convertColName(&expr)
			if tc.err == nil {
				s.NoError(err)
				s.Equal(tc.output, sqlparser.String(expr))
				s.Equal(tc.retValue, saColNameExpr)
				if tc.input != searchattribute.CloseTime {
					_, ok := expr.(*saColName)
					s.True(ok)
				}
				if tc.input == searchattribute.TemporalNamespaceDivision {
					s.True(s.queryConverter.seenNamespaceDivision)
				} else {
					s.False(s.queryConverter.seenNamespaceDivision)
				}
			} else {
				s.Error(err)
				s.Equal(err, tc.err)
			}
		})
	}
}

func (s *queryConverterSuite) TestConvertValueExpr() {
	dt, _ := time.Parse(time.RFC3339Nano, "2020-02-15T20:30:40.123456789Z")
	var tests = []testCase{
		{
			name:   "invalid: column name expression",
			input:  "ExecutionStatus",
			args:   map[string]any{"saName": "ExecutionStatus", "saType": enumspb.INDEXED_VALUE_TYPE_KEYWORD},
			output: "",
			err: query.NewConverterError(
				"%s: column name on the right side of comparison expression (did you forget to quote '%s'?)",
				query.NotSupportedErrMessage,
				"ExecutionStatus",
			),
		},
		{
			name:  "valid string",
			input: "'foo'",
			args: map[string]any{
				"saName": "AliasForKeyword01",
				"saType": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			output: "'foo'",
			err:    nil,
		},
		{
			name:  "valid string escape char",
			input: "'\"foo'",
			args: map[string]any{
				"saName": "AliasForKeyword01",
				"saType": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			output: "'\\\"foo'",
			err:    nil,
		},
		{
			name:  "valid integer",
			input: "123",
			args: map[string]any{
				"saName": "AliasForInt01",
				"saType": enumspb.INDEXED_VALUE_TYPE_INT,
			},
			output: "123",
			err:    nil,
		},
		{
			name:  "valid float",
			input: "1.230",
			args: map[string]any{
				"saName": "AliasForDouble01",
				"saType": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			},
			output: "1.23",
			err:    nil,
		},
		{
			name:  "valid bool",
			input: "true",
			args: map[string]any{
				"saName": "AliasForBool01",
				"saType": enumspb.INDEXED_VALUE_TYPE_BOOL,
			},
			output: "true",
			err:    nil,
		},
		{
			name:  "valid datetime",
			input: fmt.Sprintf("'%s'", dt.Format(time.RFC3339Nano)),
			args: map[string]any{
				"saName": "AliasForDatetime01",
				"saType": enumspb.INDEXED_VALUE_TYPE_DATETIME,
			},
			output: fmt.Sprintf("'%s'", dt.Format(s.queryConverter.getDatetimeFormat())),
			err:    nil,
		},
		{
			name:  "valid tuple",
			input: "('foo', 'bar')",
			args: map[string]any{
				"saName": "AliasForKeywordList01",
				"saType": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
			},
			output: "('foo', 'bar')",
			err:    nil,
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			sql := fmt.Sprintf("select * from table1 where %s", tc.input)
			stmt, err := sqlparser.Parse(sql)
			s.NoError(err)
			expr := stmt.(*sqlparser.Select).Where.Expr
			err = s.queryConverter.convertValueExpr(
				&expr,
				tc.args["saName"].(string),
				tc.args["saType"].(enumspb.IndexedValueType),
			)
			if tc.err == nil {
				s.NoError(err)
				s.Equal(tc.output, sqlparser.String(expr))
				if len(tc.input) > 0 && tc.input[0] == '\'' {
					_, ok := expr.(*unsafeSQLString)
					s.True(ok)
				}
			} else {
				s.Error(err)
				s.Equal(err, tc.err)
			}
		})
	}
}

func (s *queryConverterSuite) TestParseSQLVal() {
	dt, _ := time.Parse(time.RFC3339Nano, "2020-02-15T20:30:40.123456789Z")
	var tests = []testCase{
		{
			name:  "valid string",
			input: "'foo'",
			args: map[string]any{
				"saName": "AliasForKeyword01",
				"saType": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			retValue: "foo",
			err:      nil,
		},
		{
			name:  "valid integer",
			input: "123",
			args: map[string]any{
				"saName": "AliasForInt01",
				"saType": enumspb.INDEXED_VALUE_TYPE_INT,
			},
			retValue: int64(123),
			err:      nil,
		},
		{
			name:  "valid float",
			input: "1.230",
			args: map[string]any{
				"saName": "AliasForDouble01",
				"saType": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			},
			retValue: float64(1.23),
			err:      nil,
		},
		{
			name:  "valid datetime",
			input: fmt.Sprintf("'%s'", dt.Format(time.RFC3339Nano)),
			args: map[string]any{
				"saName": "AliasForDatetime01",
				"saType": enumspb.INDEXED_VALUE_TYPE_DATETIME,
			},
			retValue: fmt.Sprintf("%s", dt.Format(s.queryConverter.getDatetimeFormat())),
			err:      nil,
		},
		{
			name:  "invalid datetime",
			input: fmt.Sprintf("'%s'", dt.String()),
			args: map[string]any{
				"saName": "AliasForDatetime01",
				"saType": enumspb.INDEXED_VALUE_TYPE_DATETIME,
			},
			retValue: nil,
			err: query.NewConverterError(
				"%s: unable to parse datetime '%s'",
				query.InvalidExpressionErrMessage,
				dt.String(),
			),
		},
		{
			name:  "valid ExecutionStatus keyword",
			input: "'Running'",
			args: map[string]any{
				"saName": "ExecutionStatus",
				"saType": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			retValue: int64(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
			err:      nil,
		},
		{
			name:  "valid ExecutionStatus code",
			input: "1",
			args: map[string]any{
				"saName": "ExecutionStatus",
				"saType": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			retValue: int64(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
			err:      nil,
		},
		{
			name:  "invalid ExecutionStatus keyword",
			input: "'Foo'",
			args: map[string]any{
				"saName": "ExecutionStatus",
				"saType": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			retValue: nil,
			err: query.NewConverterError(
				"%s: invalid ExecutionStatus value '%s'",
				query.InvalidExpressionErrMessage,
				"Foo",
			),
		},
		{
			name:  "valid ExecutionDuration day suffix",
			input: "'10d'",
			args: map[string]any{
				"saName": "ExecutionDuration",
				"saType": enumspb.INDEXED_VALUE_TYPE_INT,
			},
			retValue: int64(10 * 24 * time.Hour),
			err:      nil,
		},
		{
			name:  "valid ExecutionDuration hour suffix",
			input: "'10h'",
			args: map[string]any{
				"saName": "ExecutionDuration",
				"saType": enumspb.INDEXED_VALUE_TYPE_INT,
			},
			retValue: int64(10 * time.Hour),
			err:      nil,
		},
		{
			name:  "valid ExecutionDuration string nanos",
			input: "'100'",
			args: map[string]any{
				"saName": "ExecutionDuration",
				"saType": enumspb.INDEXED_VALUE_TYPE_INT,
			},
			retValue: int64(100),
			err:      nil,
		},
		{
			name:  "valid ExecutionDuration int nanos",
			input: "100",
			args: map[string]any{
				"saName": "ExecutionDuration",
				"saType": enumspb.INDEXED_VALUE_TYPE_INT,
			},
			retValue: int64(100),
			err:      nil,
		},
		{
			name:  "invalid ExecutionDuration",
			input: "'100q'",
			args: map[string]any{
				"saName": "ExecutionDuration",
				"saType": enumspb.INDEXED_VALUE_TYPE_INT,
			},
			retValue: nil,
			err: query.NewConverterError(
				"invalid value for search attribute ExecutionDuration: 100q (invalid duration)"),
		},
		{
			name:  "invalid ExecutionDuration out of bounds",
			input: "'10000000h'",
			args: map[string]any{
				"saName": "ExecutionDuration",
				"saType": enumspb.INDEXED_VALUE_TYPE_INT,
			},
			retValue: nil,
			err: query.NewConverterError(
				"invalid value for search attribute ExecutionDuration: 10000000h (invalid duration)"),
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			sql := fmt.Sprintf("select * from table1 where %s", tc.input)
			stmt, err := sqlparser.Parse(sql)
			s.NoError(err)
			expr := stmt.(*sqlparser.Select).Where.Expr
			value, err := s.queryConverter.parseSQLVal(
				expr.(*sqlparser.SQLVal),
				tc.args["saName"].(string),
				tc.args["saType"].(enumspb.IndexedValueType),
			)
			if tc.err == nil {
				s.NoError(err)
				s.Equal(tc.input, sqlparser.String(expr)) // parseSQLVal does not change input expr
				s.Equal(tc.retValue, value)
			} else {
				s.Error(err)
				s.Equal(err, tc.err)
			}
		})
	}
}

func TestSupportedComparisonOperators(t *testing.T) {
	s := assert.New(t)
	msg := "If you're changing the supported operators, remember to check they work with " +
		"MySQL, PostgreSQL and SQLite, and check their respective plugin converters."
	s.True(isSupportedComparisonOperator(sqlparser.EqualStr), msg)
	s.True(isSupportedComparisonOperator(sqlparser.NotEqualStr), msg)
	s.True(isSupportedComparisonOperator(sqlparser.LessThanStr), msg)
	s.True(isSupportedComparisonOperator(sqlparser.GreaterThanStr), msg)
	s.True(isSupportedComparisonOperator(sqlparser.LessEqualStr), msg)
	s.True(isSupportedComparisonOperator(sqlparser.GreaterEqualStr), msg)
	s.True(isSupportedComparisonOperator(sqlparser.InStr), msg)
	s.True(isSupportedComparisonOperator(sqlparser.NotInStr), msg)
	s.False(isSupportedComparisonOperator(sqlparser.LikeStr), msg)
	s.False(isSupportedComparisonOperator(sqlparser.NotLikeStr), msg)
}

func TestSupportedKeywordListOperators(t *testing.T) {
	s := assert.New(t)
	msg := "If you're changing the supported operators, remember to check they work with " +
		"MySQL, PostgreSQL and SQLite, and check their respective plugin converters."
	s.True(isSupportedKeywordListOperator(sqlparser.EqualStr), msg)
	s.True(isSupportedKeywordListOperator(sqlparser.NotEqualStr), msg)
	s.True(isSupportedKeywordListOperator(sqlparser.InStr), msg)
	s.True(isSupportedKeywordListOperator(sqlparser.NotInStr), msg)
	s.False(isSupportedKeywordListOperator(sqlparser.LessThanStr), msg)
	s.False(isSupportedKeywordListOperator(sqlparser.GreaterThanStr), msg)
	s.False(isSupportedKeywordListOperator(sqlparser.LessEqualStr), msg)
	s.False(isSupportedKeywordListOperator(sqlparser.GreaterEqualStr), msg)
	s.False(isSupportedKeywordListOperator(sqlparser.LikeStr), msg)
	s.False(isSupportedKeywordListOperator(sqlparser.NotLikeStr), msg)
}

func TestSupportedTextOperators(t *testing.T) {
	s := assert.New(t)
	msg := "If you're changing the supported operators, remember to check they work with " +
		"MySQL, PostgreSQL and SQLite, and check their respective plugin converters."
	s.True(isSupportedTextOperator(sqlparser.EqualStr), msg)
	s.True(isSupportedTextOperator(sqlparser.NotEqualStr), msg)
	s.False(isSupportedTextOperator(sqlparser.LessThanStr), msg)
	s.False(isSupportedTextOperator(sqlparser.GreaterThanStr), msg)
	s.False(isSupportedTextOperator(sqlparser.LessEqualStr), msg)
	s.False(isSupportedTextOperator(sqlparser.GreaterEqualStr), msg)
	s.False(isSupportedTextOperator(sqlparser.InStr), msg)
	s.False(isSupportedTextOperator(sqlparser.NotInStr), msg)
	s.False(isSupportedTextOperator(sqlparser.LikeStr), msg)
	s.False(isSupportedTextOperator(sqlparser.NotLikeStr), msg)
}

func TestSupportedTypeRangeCond(t *testing.T) {
	s := assert.New(t)
	msg := "If you're changing the supported types for range condition, " +
		"remember to check they work correctly with MySQL, PostgreSQL and SQLite."
	supportedTypesRangeCond = []enumspb.IndexedValueType{
		enumspb.INDEXED_VALUE_TYPE_DATETIME,
		enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		enumspb.INDEXED_VALUE_TYPE_INT,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}
	for tpCode := range enumspb.IndexedValueType_name {
		tp := enumspb.IndexedValueType(tpCode)
		switch tp {
		case enumspb.INDEXED_VALUE_TYPE_DATETIME,
			enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			enumspb.INDEXED_VALUE_TYPE_INT,
			enumspb.INDEXED_VALUE_TYPE_KEYWORD:
			s.True(isSupportedTypeRangeCond(tp), msg)
		default:
			s.False(isSupportedTypeRangeCond(tp), msg)
		}
	}
}
