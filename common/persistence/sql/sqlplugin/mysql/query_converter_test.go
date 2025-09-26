package mysql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

type queryConverterSuite struct {
	suite.Suite
	*require.Assertions

	queryConverter *queryConverter
}

func TestQueryConverter(t *testing.T) {
	s := &queryConverterSuite{}
	suite.Run(t, s)
}

func (s *queryConverterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.queryConverter = &queryConverter{}
}

func (s *queryConverterSuite) TestGetCoalesceCloseTimeExpr() {
	expr := s.queryConverter.GetCoalesceCloseTimeExpr()
	s.Equal(
		"coalesce(close_time, cast('9999-12-31 23:59:59' as datetime))",
		sqlparser.String(expr),
	)
}

func (s *queryConverterSuite) TestConvertKeywordListComparisonExpr() {
	keywordListCol := query.NewSAColumnName(
		"AliasForKeywordList01",
		"KeywordList01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
	)

	testCases := []struct {
		name     string
		operator string
		col      *query.SAColumnName
		value    sqlparser.Expr
		out      string
		err      string
	}{
		{
			name:     "valid equal expression",
			operator: sqlparser.EqualStr,
			col:      keywordListCol,
			value:    query.NewUnsafeSQLString("foo"),
			out:      "'foo' member of (KeywordList01)",
		},
		{
			name:     "valid not equal expression",
			operator: sqlparser.NotEqualStr,
			col:      keywordListCol,
			value:    query.NewUnsafeSQLString("foo"),
			out:      "not 'foo' member of (KeywordList01)",
		},
		{
			name:     "valid in expression",
			operator: sqlparser.InStr,
			col:      keywordListCol,
			value: sqlparser.ValTuple{
				query.NewUnsafeSQLString("foo"),
				query.NewUnsafeSQLString("bar"),
			},
			out: "json_overlaps(KeywordList01, cast('[\"foo\",\"bar\"]' as json))",
		},
		{
			name:     "valid not in expression",
			operator: sqlparser.NotInStr,
			col:      keywordListCol,
			value: sqlparser.ValTuple{
				query.NewUnsafeSQLString("foo"),
				query.NewUnsafeSQLString("bar"),
			},
			out: "not json_overlaps(KeywordList01, cast('[\"foo\",\"bar\"]' as json))",
		},
		{
			name:     "invalid in expression",
			operator: sqlparser.InStr,
			col:      keywordListCol,
			value: sqlparser.ValTuple{
				query.NewUnsafeSQLString("foo"),
				sqlparser.NewIntVal([]byte("123")),
			},
			err: query.InvalidExpressionErrMessage,
		},
		{
			name:     "invalid operator",
			operator: sqlparser.LessThanStr,
			col:      keywordListCol,
			value:    query.NewUnsafeSQLString("foo"),
			err: fmt.Sprintf(
				"%s: operator '<' not supported for KeywordList type",
				query.InvalidExpressionErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			out, err := s.queryConverter.ConvertKeywordListComparisonExpr(tc.operator, tc.col, tc.value)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var expectedErr *query.ConverterError
				s.ErrorAs(err, &expectedErr)
			} else {
				s.NoError(err)
				s.Equal(tc.out, sqlparser.String(out))
			}
		})
	}
}

func (s *queryConverterSuite) TestConvertTextComparisonExpr() {
	textCol := query.NewSAColumnName(
		"AliasForText01",
		"Text01",
		enumspb.INDEXED_VALUE_TYPE_TEXT,
	)

	tests := []struct {
		name     string
		operator string
		col      *query.SAColumnName
		value    sqlparser.Expr
		out      string
	}{
		{
			name:     "valid equal expression",
			operator: sqlparser.EqualStr,
			col:      textCol,
			value:    query.NewUnsafeSQLString("foo bar"),
			out:      "match(Text01) against ('foo bar' in natural language mode)",
		},
		{
			name:     "valid not equal expression",
			operator: sqlparser.NotEqualStr,
			col:      textCol,
			value:    query.NewUnsafeSQLString("foo bar"),
			out:      "not match(Text01) against ('foo bar' in natural language mode)",
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			out, err := s.queryConverter.ConvertTextComparisonExpr(tc.operator, tc.col, tc.value)
			s.NoError(err)
			s.Equal(tc.out, sqlparser.String(out))
		})
	}
}
