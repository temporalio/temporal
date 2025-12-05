package sql

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	col *query.SAColumn,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	c.convertComparisonExprCalls++
	return nil, c.err
}

func (c *dummyVisQC) ConvertKeywordComparisonExpr(
	operator string,
	col *query.SAColumn,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	c.convertKeywordComparisonExprCalls++
	return nil, c.err
}

func (c *dummyVisQC) ConvertKeywordListComparisonExpr(
	operator string,
	col *query.SAColumn,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	c.convertKeywordListComparisonExprCalls++
	return nil, c.err
}

func (c *dummyVisQC) ConvertTextComparisonExpr(
	operator string,
	col *query.SAColumn,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	c.convertTextComparisonExprCalls++
	return nil, c.err
}

func (c *dummyVisQC) ConvertRangeExpr(
	operator string,
	col *query.SAColumn,
	from, to sqlparser.Expr,
) (sqlparser.Expr, error) {
	c.convertRangeExprCalls++
	return nil, c.err
}

func (c *dummyVisQC) ConvertIsExpr(
	operator string,
	col *query.SAColumn,
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

func TestSQLQueryConverter_GetDatetimeFormat(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
	queryConverter := &SQLQueryConverter{
		VisibilityQueryConverter: pluginVisQCMock,
	}

	pluginVisQCMock.EXPECT().GetDatetimeFormat().Return("")
	queryConverter.GetDatetimeFormat()
}

func TestSQLQueryConverter_BuildParenExpr(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		in  sqlparser.Expr
		out string
	}{
		{
			in:  parseWhereString("a = 1"),
			out: "a = 1",
		},
		{
			in:  parseWhereString("a = 1 and b = 'foo'"),
			out: "(a = 1 and b = 'foo')",
		},
		{
			in:  parseWhereString("a = 1 or b = 'foo'"),
			out: "(a = 1 or b = 'foo')",
		},
		{
			in:  parseWhereString("not a = 1"),
			out: "(not a = 1)",
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
		t.Run(tc.out, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
			queryConverter := &SQLQueryConverter{
				VisibilityQueryConverter: pluginVisQCMock,
			}

			out, err := queryConverter.BuildParenExpr(tc.in)
			r.NoError(err)
			r.Equal(tc.out, sqlparser.String(out))
		})
	}
}

func TestSQLQueryConverter_BuildNotExpr(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		in  sqlparser.Expr
		out string
	}{
		{
			in:  parseWhereString("a = 1"),
			out: "not a = 1",
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
		t.Run(tc.out, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
			queryConverter := &SQLQueryConverter{
				VisibilityQueryConverter: pluginVisQCMock,
			}

			out, err := queryConverter.BuildNotExpr(tc.in)
			r.NoError(err)
			r.Equal(tc.out, sqlparser.String(out))
		})
	}
}

func TestSQLQueryConverter_BuildAndExpr(t *testing.T) {
	t.Parallel()
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
			out: "(a = 1 and b = 'foo') and c = 'bar'",
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
		t.Run(tc.out, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
			queryConverter := &SQLQueryConverter{
				VisibilityQueryConverter: pluginVisQCMock,
			}

			out, err := queryConverter.BuildAndExpr(tc.in...)
			r.NoError(err)
			r.Equal(tc.out, sqlparser.String(out))
		})
	}
}

func TestSQLQueryConverter_BuildOrExpr(t *testing.T) {
	t.Parallel()
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
		t.Run(tc.out, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
			queryConverter := &SQLQueryConverter{
				VisibilityQueryConverter: pluginVisQCMock,
			}

			out, err := queryConverter.BuildOrExpr(tc.in...)
			r.NoError(err)
			r.Equal(tc.out, sqlparser.String(out))
		})
	}
}

func TestSQLQueryConverter_ConvertComparisonExpr(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		operator  string
		col       *query.SAColumn
		value     any
		out       string
		errString string
	}{
		{
			operator: sqlparser.EqualStr,
			col:      query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:    "foo",
			out:      "a = 'foo'",
		},
		{
			operator: sqlparser.NotEqualStr,
			col:      query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:    "foo",
			out:      "a != 'foo'",
		},
		{
			operator: sqlparser.EqualStr,
			col:      query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_INT),
			value:    int64(123),
			out:      "a = 123",
		},
		{
			operator: sqlparser.EqualStr,
			col:      query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_DOUBLE),
			value:    123.456,
			out:      "a = 123.456",
		},
		{
			operator: sqlparser.EqualStr,
			col:      query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_BOOL),
			value:    true,
			out:      "a = true",
		},
		{
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:     123,
			errString: query.InvalidExpressionErrMessage,
			out:       "unexpected type a = 123",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.out, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
			queryConverter := &SQLQueryConverter{
				VisibilityQueryConverter: pluginVisQCMock,
			}

			out, err := queryConverter.ConvertComparisonExpr(tc.operator, tc.col, tc.value)
			if tc.errString != "" {
				r.Error(err)
				var expectedErr *query.ConverterError
				r.ErrorAs(err, &expectedErr)
				r.ErrorContains(err, tc.errString)
			} else {
				r.NoError(err)
				r.Equal(tc.out, sqlparser.String(out))
			}
		})
	}
}

func TestSQLQueryConverter_CustomConvertComparisonExpr(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	ctrl := gomock.NewController(t)
	pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
	queryConverter := &SQLQueryConverter{
		VisibilityQueryConverter: pluginVisQCMock,
	}

	visQC := &dummyVisQC{}
	queryConverter.VisibilityQueryConverter = visQC
	out, err := queryConverter.ConvertComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_INT),
		int64(123),
	)
	r.NoError(err)
	r.Nil(out)
	r.Equal(1, visQC.convertComparisonExprCalls)

	visQC.err = errors.New("custom error")
	_, err = queryConverter.ConvertComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_INT),
		int64(123),
	)
	r.Error(err)
	r.ErrorContains(err, "custom error")
	r.Equal(2, visQC.convertComparisonExprCalls)
}

func TestSQLQueryConverter_ConvertKeywordComparisonExpr(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		operator  string
		col       *query.SAColumn
		value     any
		out       string
		errString string
	}{
		{
			operator: sqlparser.EqualStr,
			col:      query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:    "foo",
			out:      "a = 'foo'",
		},
		{
			operator: sqlparser.NotEqualStr,
			col:      query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:    "foo",
			out:      "a != 'foo'",
		},
		{
			operator: sqlparser.StartsWithStr,
			col:      query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:    "foo",
			out:      "a like 'foo%' escape '!'",
		},
		{
			operator: sqlparser.NotStartsWithStr,
			col:      query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:    "foo",
			out:      "a not like 'foo%' escape '!'",
		},
		{
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:     123,
			errString: query.InvalidExpressionErrMessage,
			out:       "unexpected type a = 123",
		},
		{
			operator:  sqlparser.StartsWithStr,
			col:       query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			value:     int64(123),
			errString: query.InvalidExpressionErrMessage,
			out:       "unexpected type a starts_with 123",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.out, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
			queryConverter := &SQLQueryConverter{
				VisibilityQueryConverter: pluginVisQCMock,
			}

			out, err := queryConverter.ConvertKeywordComparisonExpr(tc.operator, tc.col, tc.value)
			if tc.errString != "" {
				r.Error(err)
				var expectedErr *query.ConverterError
				r.ErrorAs(err, &expectedErr)
				r.ErrorContains(err, tc.errString)
			} else {
				r.NoError(err)
				r.Equal(tc.out, sqlparser.String(out))
			}
		})
	}
}

func TestSQLQueryConverter_CustomConvertKeywordComparisonExpr(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	ctrl := gomock.NewController(t)
	pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
	queryConverter := &SQLQueryConverter{
		VisibilityQueryConverter: pluginVisQCMock,
	}

	visQC := &dummyVisQC{}
	queryConverter.VisibilityQueryConverter = visQC
	out, err := queryConverter.ConvertKeywordComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
		"foo",
	)
	r.NoError(err)
	r.Nil(out)
	r.Equal(1, visQC.convertKeywordComparisonExprCalls)

	visQC.err = errors.New("custom error")
	_, err = queryConverter.ConvertKeywordComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
		"foo",
	)
	r.Error(err)
	r.ErrorContains(err, "custom error")
	r.Equal(2, visQC.convertKeywordComparisonExprCalls)
}

func TestSQLQueryConverter_ConvertKeywordListComparisonExpr(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name      string
		operator  string
		col       *query.SAColumn
		value     any
		valueExpr sqlparser.Expr
		mockErr   error
		errString string
	}{
		{
			name:      "a = 'foo'",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
			value:     "foo",
			valueExpr: query.NewUnsafeSQLString("foo"),
		},
		{
			name:      "a = 123",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
			value:     int64(123),
			valueExpr: sqlparser.NewIntVal([]byte("123")),
		},
		{
			name:      "unexpected type a = 123",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
			value:     123,
			errString: query.InvalidExpressionErrMessage,
		},
		{
			name:      "mock error",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
			value:     "foo",
			valueExpr: query.NewUnsafeSQLString("foo"),
			mockErr:   query.NewConverterError("%s", query.InvalidExpressionErrMessage),
			errString: query.InvalidExpressionErrMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
			queryConverter := &SQLQueryConverter{
				VisibilityQueryConverter: pluginVisQCMock,
			}

			if tc.valueExpr != nil {
				pluginVisQCMock.EXPECT().ConvertKeywordListComparisonExpr(tc.operator, tc.col, tc.valueExpr).
					Return(nil, tc.mockErr)
			}
			_, err := queryConverter.ConvertKeywordListComparisonExpr(tc.operator, tc.col, tc.value)
			if tc.errString != "" {
				r.Error(err)
				var expectedErr *query.ConverterError
				r.ErrorAs(err, &expectedErr)
				r.ErrorContains(err, tc.errString)
			}
		})
	}
}

func TestSQLQueryConverter_CustomConvertKeywordListComparisonExpr(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	ctrl := gomock.NewController(t)
	pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
	queryConverter := &SQLQueryConverter{
		VisibilityQueryConverter: pluginVisQCMock,
	}

	visQC := &dummyVisQC{}
	queryConverter.VisibilityQueryConverter = visQC
	out, err := queryConverter.ConvertKeywordListComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
		"foo",
	)
	r.NoError(err)
	r.Nil(out)
	r.Equal(1, visQC.convertKeywordListComparisonExprCalls)

	visQC.err = errors.New("custom error")
	_, err = queryConverter.ConvertKeywordListComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
		"foo",
	)
	r.Error(err)
	r.ErrorContains(err, "custom error")
	r.Equal(2, visQC.convertKeywordListComparisonExprCalls)
}

func TestSQLQueryConverter_ConvertTextComparisonExpr(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name      string
		operator  string
		col       *query.SAColumn
		value     any
		valueExpr sqlparser.Expr
		mockErr   error
		errString string
	}{
		{
			name:      "a = 'foo'",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_TEXT),
			value:     "foo",
			valueExpr: query.NewUnsafeSQLString("foo"),
		},
		{
			name:      "a = 123",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_TEXT),
			value:     int64(123),
			valueExpr: sqlparser.NewIntVal([]byte("123")),
		},
		{
			name:      "unexpected type a = 123",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_TEXT),
			value:     123,
			errString: query.InvalidExpressionErrMessage,
		},
		{
			name:      "mock error",
			operator:  sqlparser.EqualStr,
			col:       query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_TEXT),
			value:     "foo",
			valueExpr: query.NewUnsafeSQLString("foo"),
			mockErr:   query.NewConverterError("%s", query.InvalidExpressionErrMessage),
			errString: query.InvalidExpressionErrMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
			queryConverter := &SQLQueryConverter{
				VisibilityQueryConverter: pluginVisQCMock,
			}

			if tc.valueExpr != nil {
				pluginVisQCMock.EXPECT().ConvertTextComparisonExpr(tc.operator, tc.col, tc.valueExpr).
					Return(nil, tc.mockErr)
			}
			_, err := queryConverter.ConvertTextComparisonExpr(tc.operator, tc.col, tc.value)
			if tc.errString != "" {
				r.Error(err)
				var expectedErr *query.ConverterError
				r.ErrorAs(err, &expectedErr)
				r.ErrorContains(err, tc.errString)
			}
		})
	}
}

func TestSQLQueryConverter_CustomConvertTextComparisonExpr(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	ctrl := gomock.NewController(t)
	pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
	queryConverter := &SQLQueryConverter{
		VisibilityQueryConverter: pluginVisQCMock,
	}

	visQC := &dummyVisQC{}
	queryConverter.VisibilityQueryConverter = visQC
	out, err := queryConverter.ConvertTextComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_TEXT),
		"foo",
	)
	r.NoError(err)
	r.Nil(out)
	r.Equal(1, visQC.convertTextComparisonExprCalls)

	visQC.err = errors.New("custom error")
	_, err = queryConverter.ConvertTextComparisonExpr(
		sqlparser.EqualStr,
		query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_TEXT),
		"foo",
	)
	r.Error(err)
	r.ErrorContains(err, "custom error")
	r.Equal(2, visQC.convertTextComparisonExprCalls)
}

func TestSQLQueryConverter_ConvertRangeExpr(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		operator  string
		col       *query.SAColumn
		from      any
		to        any
		errString string
		out       string
	}{
		{
			operator: sqlparser.BetweenStr,
			col:      query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			from:     "123",
			to:       "456",
			out:      "a between '123' and '456'",
		},
		{
			operator: sqlparser.NotBetweenStr,
			col:      query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_INT),
			from:     int64(123),
			to:       int64(456),
			out:      "a not between 123 and 456",
		},
		{
			operator:  sqlparser.BetweenStr,
			col:       query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_INT),
			from:      123,
			to:        int64(456),
			errString: query.InvalidExpressionErrMessage,
			out:       "unexpected type 123",
		},
		{
			operator:  sqlparser.BetweenStr,
			col:       query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_INT),
			from:      int64(123),
			to:        456,
			errString: query.InvalidExpressionErrMessage,
			out:       "unexpected type 456",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.out, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
			queryConverter := &SQLQueryConverter{
				VisibilityQueryConverter: pluginVisQCMock,
			}

			out, err := queryConverter.ConvertRangeExpr(tc.operator, tc.col, tc.from, tc.to)
			if tc.errString != "" {
				r.Error(err)
				var expectedErr *query.ConverterError
				r.ErrorAs(err, &expectedErr)
				r.ErrorContains(err, tc.errString)
			} else {
				r.NoError(err)
				r.Equal(tc.out, sqlparser.String(out))
			}
		})
	}
}

func TestSQLQueryConverter_CustomConvertRangeExpr(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	ctrl := gomock.NewController(t)
	pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
	queryConverter := &SQLQueryConverter{
		VisibilityQueryConverter: pluginVisQCMock,
	}

	visQC := &dummyVisQC{}
	queryConverter.VisibilityQueryConverter = visQC
	out, err := queryConverter.ConvertRangeExpr(
		sqlparser.BetweenStr,
		query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
		"123",
		"456",
	)
	r.NoError(err)
	r.Nil(out)
	r.Equal(1, visQC.convertRangeExprCalls)

	visQC.err = errors.New("custom error")
	_, err = queryConverter.ConvertRangeExpr(
		sqlparser.BetweenStr,
		query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
		"123",
		"456",
	)
	r.Error(err)
	r.ErrorContains(err, "custom error")
	r.Equal(2, visQC.convertRangeExprCalls)
}

func TestSQLQueryConverter_ConvertIsExpr(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		operator string
		col      *query.SAColumn
		out      string
	}{
		{
			operator: sqlparser.IsNullStr,
			col:      query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			out:      "a is null",
		},
		{
			operator: sqlparser.IsNotNullStr,
			col:      query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			out:      "a is not null",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.out, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
			queryConverter := &SQLQueryConverter{
				VisibilityQueryConverter: pluginVisQCMock,
			}

			out, err := queryConverter.ConvertIsExpr(tc.operator, tc.col)
			r.NoError(err)
			r.Equal(tc.out, sqlparser.String(out))
		})
	}
}

func TestSQLQueryConverter_CustomConvertIsExpr(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	ctrl := gomock.NewController(t)
	pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
	queryConverter := &SQLQueryConverter{
		VisibilityQueryConverter: pluginVisQCMock,
	}

	visQC := &dummyVisQC{}
	queryConverter.VisibilityQueryConverter = visQC
	out, err := queryConverter.ConvertIsExpr(
		sqlparser.IsNullStr,
		query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
	)
	r.NoError(err)
	r.Nil(out)
	r.Equal(1, visQC.convertIsExprCalls)

	visQC.err = errors.New("custom error")
	_, err = queryConverter.ConvertIsExpr(
		sqlparser.IsNullStr,
		query.NewSAColumn("a", "a", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
	)
	r.Error(err)
	r.ErrorContains(err, "custom error")
	r.Equal(2, visQC.convertIsExprCalls)
}

func TestSQLQueryConverter_SelectStmt(t *testing.T) {
	t.Parallel()

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
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
			queryConverter := &SQLQueryConverter{
				VisibilityQueryConverter: pluginVisQCMock,
			}

			pluginVisQCMock.EXPECT().BuildSelectStmt(tc.queryParams, tc.pageSize, tc.pageToken).Return("", nil)
			queryConverter.BuildSelectStmt(tc.queryParams, tc.pageSize, tc.pageToken)
		})
	}
}

func TestSQLQueryConverter_CountStmt(t *testing.T) {
	t.Parallel()
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
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
			queryConverter := &SQLQueryConverter{
				VisibilityQueryConverter: pluginVisQCMock,
			}

			pluginVisQCMock.EXPECT().BuildCountStmt(tc.queryParams).Return("", nil)
			queryConverter.BuildCountStmt(tc.queryParams)
		})
	}
}

func TestSQLQueryConverter_BuildValueExpr(t *testing.T) {
	t.Parallel()
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
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			pluginVisQCMock := sqlplugin.NewMockVisibilityQueryConverter(ctrl)
			queryConverter := &SQLQueryConverter{
				VisibilityQueryConverter: pluginVisQCMock,
			}

			out, err := queryConverter.buildValueExpr("CustomField", tc.in)
			if tc.errString != "" {
				r.Error(err)
				var expectedErr *query.ConverterError
				r.ErrorAs(err, &expectedErr)
				r.ErrorContains(err, tc.errString)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
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
