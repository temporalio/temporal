package sql

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.uber.org/mock/gomock"
)

type dummyVisQC struct {
	err error

	getDatetimeFormatCalls                int
	convertComparisonExprCalls            int
	convertKeywordComparisonExprCalls     int
	convertKeywordListComparisonExprCalls int
	convertTextComparisonExprCalls        int
	convertRangeExprCalls                 int
	convertIsExprCalls                    int
	buildSelectStmtCalls                  int
	buildCountStmtCalls                   int
}

var _ sqlplugin.VisibilityQueryConverter = (*dummyVisQC)(nil)

func (c *dummyVisQC) GetDatetimeFormat() string {
	c.getDatetimeFormatCalls++
	return time.RFC3339
}

func (c *dummyVisQC) ConvertComparisonExpr(
	operator string,
	col *query.SAColumnName,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	c.convertComparisonExprCalls++
	return nil, c.err
}

func (c *dummyVisQC) ConvertKeywordComparisonExpr(
	operator string,
	col *query.SAColumnName,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	c.convertKeywordComparisonExprCalls++
	return nil, c.err
}

func (c *dummyVisQC) ConvertKeywordListComparisonExpr(
	operator string,
	col *query.SAColumnName,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	c.convertKeywordListComparisonExprCalls++
	return nil, c.err
}

func (c *dummyVisQC) ConvertTextComparisonExpr(
	operator string,
	col *query.SAColumnName,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	c.convertTextComparisonExprCalls++
	return nil, c.err
}

func (c *dummyVisQC) ConvertRangeExpr(
	operator string,
	col *query.SAColumnName,
	from, to sqlparser.Expr,
) (sqlparser.Expr, error) {
	c.convertRangeExprCalls++
	return nil, c.err
}

func (c *dummyVisQC) ConvertIsExpr(
	operator string,
	col *query.SAColumnName,
) (sqlparser.Expr, error) {
	c.convertIsExprCalls++
	return nil, c.err
}

func (c *dummyVisQC) BuildSelectStmt(
	queryExpr *query.QueryParams[sqlparser.Expr],
	pageSize int,
	pageToken *sqlplugin.VisibilityPageToken,
) (string, []any) {
	c.buildSelectStmtCalls++
	return "", nil
}

func (c *dummyVisQC) BuildCountStmt(
	queryExpr *query.QueryParams[sqlparser.Expr],
) (string, []any) {
	c.buildCountStmtCalls++
	return "", nil
}

type (
	sqlQueryConverterSuite struct {
		suite.Suite
		*require.Assertions
		ctrl *gomock.Controller

		pluginVisQCMock *sqlplugin.MockVisibilityQueryConverter
		queryConverter  *SQLQueryConverter
	}
)

func TestSqlQueryConverter(t *testing.T) {
	s := &sqlQueryConverterSuite{}
	suite.Run(t, s)
}

func (s *sqlQueryConverterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ctrl = gomock.NewController(s.T())
	s.pluginVisQCMock = sqlplugin.NewMockVisibilityQueryConverter(s.ctrl)
	s.queryConverter = &SQLQueryConverter{
		VisibilityQueryConverter: s.pluginVisQCMock,
	}
}

func (s *sqlQueryConverterSuite) TestGetDatetimeFormat() {
	s.pluginVisQCMock.EXPECT().GetDatetimeFormat().Return("")
	s.queryConverter.GetDatetimeFormat()
}

func (s *sqlQueryConverterSuite) TestBuildParenExpr() {
	testCases := []struct {
		in  sqlparser.Expr
		out string
	}{
		{
			in:  parseWhereString("a = 1"),
			out: "(a = 1)",
		},
		{
			in:  parseWhereString("a = 1 and b = 'foo'"),
			out: "(a = 1 and b = 'foo')",
		},
		{
			in:  parseWhereString("a = 1 and (b = 'foo' or c = 'bar')"),
			out: "(a = 1 and (b = 'foo' or c = 'bar'))",
		},
		{
			in:  parseWhereString("(a = 1)"),
			out: "(a = 1)",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.out, func() {
			out, err := s.queryConverter.BuildParenExpr(tc.in)
			s.NoError(err)
			s.Equal(tc.out, sqlparser.String(out))
		})
	}
}

func (s *sqlQueryConverterSuite) TestBuildNotExpr() {
	testCases := []struct {
		in  sqlparser.Expr
		out string
	}{
		{
			in:  parseWhereString("a = 1"),
			out: "not (a = 1)",
		},
		{
			in:  parseWhereString("a = 1 and b = 'foo'"),
			out: "not (a = 1 and b = 'foo')",
		},
		{
			in:  parseWhereString("a = 1 and (b = 'foo' or c = 'bar')"),
			out: "not (a = 1 and (b = 'foo' or c = 'bar'))",
		},
		{
			in:  parseWhereString("(a = 1)"),
			out: "not (a = 1)",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.out, func() {
			out, err := s.queryConverter.BuildNotExpr(tc.in)
			s.NoError(err)
			s.Equal(tc.out, sqlparser.String(out))
		})
	}
}

func (s *sqlQueryConverterSuite) TestBuildAndExpr() {
	testCases := []struct {
		in  []sqlparser.Expr
		out string
	}{
		{
			in:  []sqlparser.Expr{},
			out: "<nil>", // nil value is stringified like this
		},
		{
			in: []sqlparser.Expr{
				parseWhereString("a = 1"),
			},
			out: "a = 1",
		},
		{
			in: []sqlparser.Expr{
				parseWhereString("a = 1 or b = 'foo'"),
			},
			out: "a = 1 or b = 'foo'",
		},
		{
			in: []sqlparser.Expr{
				parseWhereString("a = 1"),
				parseWhereString("b = 'foo'"),
			},
			out: "a = 1 and b = 'foo'",
		},
		{
			in: []sqlparser.Expr{
				parseWhereString("a = 1 and b = 'foo'"),
				parseWhereString("c = 'bar'"),
			},
			out: "a = 1 and b = 'foo' and c = 'bar'",
		},
		{
			in: []sqlparser.Expr{
				parseWhereString("a = 1 or b = 'foo'"),
				parseWhereString("c = 'bar'"),
				parseWhereString("a = 2 or b = 'zzz'"),
			},
			out: "(a = 1 or b = 'foo') and c = 'bar' and (a = 2 or b = 'zzz')",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.out, func() {
			out, err := s.queryConverter.BuildAndExpr(tc.in...)
			s.NoError(err)
			s.Equal(tc.out, sqlparser.String(out))
		})
	}
}

func (s *sqlQueryConverterSuite) TestBuildOrExpr() {
	testCases := []struct {
		in  []sqlparser.Expr
		out string
	}{
		{
			in:  []sqlparser.Expr{},
			out: "<nil>", // nil value is stringified like this
		},
		{
			in: []sqlparser.Expr{
				parseWhereString("a = 1"),
			},
			out: "a = 1",
		},
		{
			in: []sqlparser.Expr{
				parseWhereString("a = 1 or b = 'foo'"),
			},
			out: "a = 1 or b = 'foo'",
		},
		{
			in: []sqlparser.Expr{
				parseWhereString("a = 1"),
				parseWhereString("b = 'foo'"),
			},
			out: "a = 1 or b = 'foo'",
		},
		{
			in: []sqlparser.Expr{
				parseWhereString("a = 1 and b = 'foo'"),
				parseWhereString("c = 'bar'"),
			},
			out: "a = 1 and b = 'foo' or c = 'bar'",
		},
		{
			in: []sqlparser.Expr{
				parseWhereString("a = 1 and b = 'foo'"),
				parseWhereString("c = 'bar'"),
				parseWhereString("a = 2 and b = 'zzz'"),
			},
			out: "a = 1 and b = 'foo' or c = 'bar' or a = 2 and b = 'zzz'",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.out, func() {
			out, err := s.queryConverter.BuildOrExpr(tc.in...)
			s.NoError(err)
			s.Equal(tc.out, sqlparser.String(out))
		})
	}
}

func (s *sqlQueryConverterSuite) TestConvertComparisonExpr() {
	testCases := []struct {
		operator  string
		col       *query.SAColumnName
		value     any
		out       string
		errString string
	}{
		{
			operator: sqlparser.EqualStr,
			col:      query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:    "foo",
			out:      "a = 'foo'",
		},
		{
			operator: sqlparser.NotEqualStr,
			col:      query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:    "foo",
			out:      "a != 'foo'",
		},
		{
			operator: sqlparser.EqualStr,
			col:      query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_INT),
			value:    int64(123),
			out:      "a = 123",
		},
		{
			operator: sqlparser.EqualStr,
			col:      query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_DOUBLE),
			value:    123.456,
			out:      "a = 123.456",
		},
		{
			operator: sqlparser.EqualStr,
			col:      query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_BOOL),
			value:    true,
			out:      "a = true",
		},
		{
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:     123,
			errString: query.InvalidExpressionErrMessage,
			out:       "unexpected type a = 123",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.out, func() {
			out, err := s.queryConverter.ConvertComparisonExpr(tc.operator, tc.col, tc.value)
			if tc.errString != "" {
				s.Error(err)
				var expectedErr *query.ConverterError
				s.ErrorAs(err, &expectedErr)
				s.ErrorContains(err, tc.errString)
			} else {
				s.NoError(err)
				s.Equal(tc.out, sqlparser.String(out))
			}
		})
	}
}

func (s *sqlQueryConverterSuite) TestCustomConvertComparisonExpr() {
	visQC := &dummyVisQC{}
	s.queryConverter.VisibilityQueryConverter = visQC
	out, err := s.queryConverter.ConvertComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_INT),
		int64(123),
	)
	s.NoError(err)
	s.Nil(out)
	s.Equal(1, visQC.convertComparisonExprCalls)

	visQC.err = errors.New("custom error")
	_, err = s.queryConverter.ConvertComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_INT),
		int64(123),
	)
	s.Error(err)
	s.ErrorContains(err, "custom error")
	s.Equal(2, visQC.convertComparisonExprCalls)
}

func (s *sqlQueryConverterSuite) TestConvertKeywordComparisonExpr() {
	testCases := []struct {
		operator  string
		col       *query.SAColumnName
		value     any
		out       string
		errString string
	}{
		{
			operator: sqlparser.EqualStr,
			col:      query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:    "foo",
			out:      "a = 'foo'",
		},
		{
			operator: sqlparser.NotEqualStr,
			col:      query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:    "foo",
			out:      "a != 'foo'",
		},
		{
			operator: sqlparser.StartsWithStr,
			col:      query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:    "foo",
			out:      "a like 'foo%' escape '!'",
		},
		{
			operator: sqlparser.NotStartsWithStr,
			col:      query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:    "foo",
			out:      "a not like 'foo%' escape '!'",
		},
		{
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:     123,
			errString: query.InvalidExpressionErrMessage,
			out:       "unexpected type a = 123",
		},
		{
			operator:  sqlparser.StartsWithStr,
			col:       query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:     int64(123),
			errString: query.InvalidExpressionErrMessage,
			out:       "unexpected type a starts_with 123",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.out, func() {
			out, err := s.queryConverter.ConvertKeywordComparisonExpr(tc.operator, tc.col, tc.value)
			if tc.errString != "" {
				s.Error(err)
				var expectedErr *query.ConverterError
				s.ErrorAs(err, &expectedErr)
				s.ErrorContains(err, tc.errString)
			} else {
				s.NoError(err)
				s.Equal(tc.out, sqlparser.String(out))
			}
		})
	}
}

func (s *sqlQueryConverterSuite) TestCustomConvertKeywordComparisonExpr() {
	visQC := &dummyVisQC{}
	s.queryConverter.VisibilityQueryConverter = visQC
	out, err := s.queryConverter.ConvertKeywordComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
		"foo",
	)
	s.NoError(err)
	s.Nil(out)
	s.Equal(1, visQC.convertKeywordComparisonExprCalls)

	visQC.err = errors.New("custom error")
	_, err = s.queryConverter.ConvertKeywordComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
		"foo",
	)
	s.Error(err)
	s.ErrorContains(err, "custom error")
	s.Equal(2, visQC.convertKeywordComparisonExprCalls)
}

func (s *sqlQueryConverterSuite) TestConvertKeywordListComparisonExpr() {
	testCases := []struct {
		name      string
		operator  string
		col       *query.SAColumnName
		value     any
		valueExpr sqlparser.Expr
		mockErr   error
		errString string
	}{
		{
			name:      "a = 'foo'",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
			value:     "foo",
			valueExpr: query.NewUnsafeSQLString("foo"),
		},
		{
			name:      "a = 123",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
			value:     int64(123),
			valueExpr: sqlparser.NewIntVal([]byte("123")),
		},
		{
			name:      "unexpected type a = 123",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
			value:     123,
			errString: query.InvalidExpressionErrMessage,
		},
		{
			name:      "mock error",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
			value:     "foo",
			valueExpr: query.NewUnsafeSQLString("foo"),
			mockErr:   query.NewConverterError("%s", query.InvalidExpressionErrMessage),
			errString: query.InvalidExpressionErrMessage,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			if tc.valueExpr != nil {
				s.pluginVisQCMock.EXPECT().ConvertKeywordListComparisonExpr(tc.operator, tc.col, tc.valueExpr).
					Return(nil, tc.mockErr)
			}
			_, err := s.queryConverter.ConvertKeywordListComparisonExpr(tc.operator, tc.col, tc.value)
			if tc.errString != "" {
				s.Error(err)
				var expectedErr *query.ConverterError
				s.ErrorAs(err, &expectedErr)
				s.ErrorContains(err, tc.errString)
			}
		})
	}
}

func (s *sqlQueryConverterSuite) TestCustomConvertKeywordListComparisonExpr() {
	visQC := &dummyVisQC{}
	s.queryConverter.VisibilityQueryConverter = visQC
	out, err := s.queryConverter.ConvertKeywordListComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
		"foo",
	)
	s.NoError(err)
	s.Nil(out)
	s.Equal(1, visQC.convertKeywordListComparisonExprCalls)

	visQC.err = errors.New("custom error")
	_, err = s.queryConverter.ConvertKeywordListComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
		"foo",
	)
	s.Error(err)
	s.ErrorContains(err, "custom error")
	s.Equal(2, visQC.convertKeywordListComparisonExprCalls)
}

func (s *sqlQueryConverterSuite) TestConvertTextComparisonExpr() {
	testCases := []struct {
		name      string
		operator  string
		col       *query.SAColumnName
		value     any
		valueExpr sqlparser.Expr
		mockErr   error
		errString string
	}{
		{
			name:      "a = 'foo'",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_TEXT),
			value:     "foo",
			valueExpr: query.NewUnsafeSQLString("foo"),
		},
		{
			name:      "a = 123",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_TEXT),
			value:     int64(123),
			valueExpr: sqlparser.NewIntVal([]byte("123")),
		},
		{
			name:      "unexpected type a = 123",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_TEXT),
			value:     123,
			errString: query.InvalidExpressionErrMessage,
		},
		{
			name:      "mock error",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_TEXT),
			value:     "foo",
			valueExpr: query.NewUnsafeSQLString("foo"),
			mockErr:   query.NewConverterError("%s", query.InvalidExpressionErrMessage),
			errString: query.InvalidExpressionErrMessage,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			if tc.valueExpr != nil {
				s.pluginVisQCMock.EXPECT().ConvertTextComparisonExpr(tc.operator, tc.col, tc.valueExpr).
					Return(nil, tc.mockErr)
			}
			_, err := s.queryConverter.ConvertTextComparisonExpr(tc.operator, tc.col, tc.value)
			if tc.errString != "" {
				s.Error(err)
				var expectedErr *query.ConverterError
				s.ErrorAs(err, &expectedErr)
				s.ErrorContains(err, tc.errString)
			}
		})
	}
}

func (s *sqlQueryConverterSuite) TestCustomConvertTextComparisonExpr() {
	visQC := &dummyVisQC{}
	s.queryConverter.VisibilityQueryConverter = visQC
	out, err := s.queryConverter.ConvertTextComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_TEXT),
		"foo",
	)
	s.NoError(err)
	s.Nil(out)
	s.Equal(1, visQC.convertTextComparisonExprCalls)

	visQC.err = errors.New("custom error")
	_, err = s.queryConverter.ConvertTextComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_TEXT),
		"foo",
	)
	s.Error(err)
	s.ErrorContains(err, "custom error")
	s.Equal(2, visQC.convertTextComparisonExprCalls)
}

func (s *sqlQueryConverterSuite) TestConvertRangeExpr() {
	testCases := []struct {
		operator  string
		col       *query.SAColumnName
		from      any
		to        any
		errString string
		out       string
	}{
		{
			operator: sqlparser.BetweenStr,
			col:      query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			from:     "123",
			to:       "456",
			out:      "a between '123' and '456'",
		},
		{
			operator: sqlparser.NotBetweenStr,
			col:      query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_INT),
			from:     int64(123),
			to:       int64(456),
			out:      "a not between 123 and 456",
		},
		{
			operator:  sqlparser.BetweenStr,
			col:       query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_INT),
			from:      123,
			to:        int64(456),
			errString: query.InvalidExpressionErrMessage,
			out:       "unexpected type 123",
		},
		{
			operator:  sqlparser.BetweenStr,
			col:       query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_INT),
			from:      int64(123),
			to:        456,
			errString: query.InvalidExpressionErrMessage,
			out:       "unexpected type 456",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.out, func() {
			out, err := s.queryConverter.ConvertRangeExpr(tc.operator, tc.col, tc.from, tc.to)
			if tc.errString != "" {
				s.Error(err)
				var expectedErr *query.ConverterError
				s.ErrorAs(err, &expectedErr)
				s.ErrorContains(err, tc.errString)
			} else {
				s.NoError(err)
				s.Equal(tc.out, sqlparser.String(out))
			}
		})
	}
}

func (s *sqlQueryConverterSuite) TestCustomConvertRangeExpr() {
	visQC := &dummyVisQC{}
	s.queryConverter.VisibilityQueryConverter = visQC
	out, err := s.queryConverter.ConvertRangeExpr(
		sqlparser.BetweenStr,
		query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
		"123",
		"456",
	)
	s.NoError(err)
	s.Nil(out)
	s.Equal(1, visQC.convertRangeExprCalls)

	visQC.err = errors.New("custom error")
	_, err = s.queryConverter.ConvertRangeExpr(
		sqlparser.BetweenStr,
		query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
		"123",
		"456",
	)
	s.Error(err)
	s.ErrorContains(err, "custom error")
	s.Equal(2, visQC.convertRangeExprCalls)
}

func (s *sqlQueryConverterSuite) TestConvertIsExpr() {
	testCases := []struct {
		operator string
		col      *query.SAColumnName
		out      string
	}{
		{
			operator: sqlparser.IsNullStr,
			col:      query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			out:      "a is null",
		},
		{
			operator: sqlparser.IsNotNullStr,
			col:      query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			out:      "a is not null",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.out, func() {
			out, err := s.queryConverter.ConvertIsExpr(tc.operator, tc.col)
			s.NoError(err)
			s.Equal(tc.out, sqlparser.String(out))
		})
	}
}

func (s *sqlQueryConverterSuite) TestCustomConvertIsExpr() {
	visQC := &dummyVisQC{}
	s.queryConverter.VisibilityQueryConverter = visQC
	out, err := s.queryConverter.ConvertIsExpr(
		sqlparser.IsNullStr,
		query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
	)
	s.NoError(err)
	s.Nil(out)
	s.Equal(1, visQC.convertIsExprCalls)

	visQC.err = errors.New("custom error")
	_, err = s.queryConverter.ConvertIsExpr(
		sqlparser.IsNullStr,
		query.NewSAColumnName("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
	)
	s.Error(err)
	s.ErrorContains(err, "custom error")
	s.Equal(2, visQC.convertIsExprCalls)
}

func (s *sqlQueryConverterSuite) TestSelectStmt() {
	testCases := []struct {
		name        string
		queryParams *query.QueryParams[sqlparser.Expr]
		pageSize    int
		pageToken   *sqlplugin.VisibilityPageToken
	}{
		{
			name:        "tc1",
			queryParams: &query.QueryParams[sqlparser.Expr]{},
			pageSize:    10,
			pageToken:   &sqlplugin.VisibilityPageToken{},
		},
		{
			name: "tc2",
			queryParams: &query.QueryParams[sqlparser.Expr]{
				QueryExpr: parseWhereString("a = 1"),
			},
			pageSize: 10,
			pageToken: &sqlplugin.VisibilityPageToken{
				RunID: "test-run-id",
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.pluginVisQCMock.EXPECT().BuildSelectStmt(tc.queryParams, tc.pageSize, tc.pageToken).Return("", nil)
			s.queryConverter.BuildSelectStmt(tc.queryParams, tc.pageSize, tc.pageToken)
		})
	}
}

func (s *sqlQueryConverterSuite) TestCountStmt() {
	testCases := []struct {
		name        string
		queryParams *query.QueryParams[sqlparser.Expr]
	}{
		{
			name:        "tc1",
			queryParams: &query.QueryParams[sqlparser.Expr]{},
		},
		{
			name: "tc2",
			queryParams: &query.QueryParams[sqlparser.Expr]{
				QueryExpr: parseWhereString("a = 1"),
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.pluginVisQCMock.EXPECT().BuildCountStmt(tc.queryParams).Return("", nil)
			s.queryConverter.BuildCountStmt(tc.queryParams)
		})
	}
}

func (s *sqlQueryConverterSuite) TestBuildValueExpr() {
	testCases := []struct {
		name      string
		in        any
		out       sqlparser.Expr
		errString string
	}{
		{
			name: "string",
			in:   "foo",
			out:  query.NewUnsafeSQLString("foo"),
		},
		{
			name: "int64",
			in:   int64(123),
			out:  sqlparser.NewIntVal([]byte("123")),
		},
		{
			name: "float64",
			in:   123.456,
			out:  sqlparser.NewFloatVal([]byte("123.456")),
		},
		{
			name: "bool",
			in:   true,
			out:  sqlparser.BoolVal(true),
		},
		{
			name: "tuple",
			in:   []any{"foo", int64(123), true},
			out: sqlparser.ValTuple{
				query.NewUnsafeSQLString("foo"),
				sqlparser.NewIntVal([]byte("123")),
				sqlparser.BoolVal(true),
			},
		},
		{
			name:      "error int",
			in:        123,
			errString: query.InvalidExpressionErrMessage,
		},
		{
			name:      "error float32",
			in:        float32(123.456),
			errString: query.InvalidExpressionErrMessage,
		},
		{
			name:      "error tuple",
			in:        []string{"foo"},
			errString: query.InvalidExpressionErrMessage,
		},
		{
			name:      "error tuple item",
			in:        []any{"foo", 123},
			errString: query.InvalidExpressionErrMessage,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			out, err := s.queryConverter.buildValueExpr("CustomField", tc.in)
			if tc.errString != "" {
				s.Error(err)
				var expectedErr *query.ConverterError
				s.ErrorAs(err, &expectedErr)
				s.ErrorContains(err, tc.errString)
			} else {
				s.NoError(err)
				s.Equal(tc.out, out)
			}
		})
	}
}

func parseWhereString(where string) sqlparser.Expr {
	stmt, err := sqlparser.Parse(fmt.Sprintf("select * from t where %s", where))
	if err != nil {
		panic(err)
	}
	return stmt.(*sqlparser.Select).Where.Expr
}
