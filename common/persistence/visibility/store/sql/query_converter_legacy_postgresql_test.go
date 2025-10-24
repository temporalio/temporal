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
