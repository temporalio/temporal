package sqlite

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
		"coalesce(close_time, '9999-12-31 23:59:59+00:00')",
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
			out:      `rowid in (select rowid from executions_visibility_fts_keyword_list where executions_visibility_fts_keyword_list = 'KeywordList01 : ("foo")')`,
		},
		{
			name:     "valid not equal expression",
			operator: sqlparser.NotEqualStr,
			col:      keywordListCol,
			value:    query.NewUnsafeSQLString("foo"),
			out:      `rowid not in (select rowid from executions_visibility_fts_keyword_list where executions_visibility_fts_keyword_list = 'KeywordList01 : ("foo")')`,
		},
		{
			name:     "valid in expression",
			operator: sqlparser.InStr,
			col:      keywordListCol,
			value: sqlparser.ValTuple{
				query.NewUnsafeSQLString("foo"),
				query.NewUnsafeSQLString("bar"),
			},
			out: `rowid in (select rowid from executions_visibility_fts_keyword_list where executions_visibility_fts_keyword_list = 'KeywordList01 : ("foo" OR "bar")')`,
		},
		{
			name:     "valid not in expression",
			operator: sqlparser.NotInStr,
			col:      keywordListCol,
			value: sqlparser.ValTuple{
				query.NewUnsafeSQLString("foo"),
				query.NewUnsafeSQLString("bar"),
			},
			out: `rowid not in (select rowid from executions_visibility_fts_keyword_list where executions_visibility_fts_keyword_list = 'KeywordList01 : ("foo" OR "bar")')`,
		},
		{
			name:     "invalid equal expression",
			operator: sqlparser.EqualStr,
			col:      keywordListCol,
			value:    sqlparser.NewIntVal([]byte("123")),
			err: fmt.Sprintf(
				"%s: unexpected value type (expected string, got 123)",
				query.InvalidExpressionErrMessage,
			),
		},
		{
			name:     "invalid in expression",
			operator: sqlparser.InStr,
			col:      keywordListCol,
			value:    query.NewUnsafeSQLString("foo"),
			err: fmt.Sprintf(
				"%s: unexpected value type (expected tuple of strings, got 'foo')",
				query.InvalidExpressionErrMessage,
			),
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
		err      string
	}{
		{
			name:     "valid equal expression",
			operator: sqlparser.EqualStr,
			col:      textCol,
			value:    query.NewUnsafeSQLString("foo bar"),
			out:      `rowid in (select rowid from executions_visibility_fts_text where executions_visibility_fts_text = 'Text01 : ("foo" OR "bar")')`,
		},
		{
			name:     "valid not equal expression",
			operator: sqlparser.NotEqualStr,
			col:      textCol,
			value:    query.NewUnsafeSQLString("foo bar"),
			out:      `rowid not in (select rowid from executions_visibility_fts_text where executions_visibility_fts_text = 'Text01 : ("foo" OR "bar")')`,
		},
		{
			name:     "invalid value type",
			operator: sqlparser.EqualStr,
			col:      textCol,
			value:    sqlparser.NewIntVal([]byte("123")),
			err: fmt.Sprintf(
				"%s: unexpected value type (expected string, got 123)",
				query.InvalidExpressionErrMessage,
			),
		},
		{
			name:     "invalid no tokens expression",
			operator: sqlparser.EqualStr,
			col:      textCol,
			value:    query.NewUnsafeSQLString(""),
			err: fmt.Sprintf(
				"%s: unexpected value for Text type search attribute (no tokens found)",
				query.InvalidExpressionErrMessage,
			),
		},
		{
			name:     "invalid operator",
			operator: sqlparser.LessThanStr,
			col:      textCol,
			value:    query.NewUnsafeSQLString("foo"),
			err: fmt.Sprintf(
				"%s: operator '<' not supported for Text type",
				query.InvalidExpressionErrMessage,
			),
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			out, err := s.queryConverter.ConvertTextComparisonExpr(tc.operator, tc.col, tc.value)
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
