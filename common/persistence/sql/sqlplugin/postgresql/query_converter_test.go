package postgresql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

func TestQueryConverter_GetCoalesceCloseTimeExpr(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	qc := &queryConverter{}
	expr := qc.GetCoalesceCloseTimeExpr()
	r.Equal(
		"coalesce(close_time, '9999-12-31 23:59:59')",
		sqlparser.String(expr),
	)
}

func TestQueryConverter_ConvertKeywordListComparisonExpr(t *testing.T) {
	t.Parallel()

	keywordListCol := query.NewSAColumn(
		"AliasForKeywordList01",
		"KeywordList01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
	)

	testCases := []struct {
		name     string
		operator string
		col      *query.SAColumn
		value    sqlparser.Expr
		out      string
		err      string
	}{
		{
			name:     "valid equal expression",
			operator: sqlparser.EqualStr,
			col:      keywordListCol,
			value:    query.NewUnsafeSQLString("foo"),
			out:      "KeywordList01 @> jsonb_build_array('foo')",
		},
		{
			name:     "valid not equal expression",
			operator: sqlparser.NotEqualStr,
			col:      keywordListCol,
			value:    query.NewUnsafeSQLString("foo"),
			out:      "not KeywordList01 @> jsonb_build_array('foo')",
		},
		{
			name:     "valid in expression",
			operator: sqlparser.InStr,
			col:      keywordListCol,
			value: sqlparser.ValTuple{
				query.NewUnsafeSQLString("foo"),
				query.NewUnsafeSQLString("bar"),
			},
			out: "(KeywordList01 @> jsonb_build_array('foo') or KeywordList01 @> jsonb_build_array('bar'))",
		},
		{
			name:     "valid not in expression",
			operator: sqlparser.NotInStr,
			col:      keywordListCol,
			value: sqlparser.ValTuple{
				query.NewUnsafeSQLString("foo"),
				query.NewUnsafeSQLString("bar"),
			},
			out: "not (KeywordList01 @> jsonb_build_array('foo') or KeywordList01 @> jsonb_build_array('bar'))",
		},
		{
			name:     "invalid in expression",
			operator: sqlparser.InStr,
			col:      keywordListCol,
			value:    query.NewUnsafeSQLString("foo"),
			err:      query.InvalidExpressionErrMessage,
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
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			qc := &queryConverter{}
			out, err := qc.ConvertKeywordListComparisonExpr(tc.operator, tc.col, tc.value)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *query.ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, sqlparser.String(out))
			}
		})
	}
}

func TestQueryConverter_ConvertTextComparisonExpr(t *testing.T) {
	t.Parallel()

	textCol := query.NewSAColumn(
		"AliasForText01",
		"Text01",
		enumspb.INDEXED_VALUE_TYPE_TEXT,
	)

	tests := []struct {
		name     string
		operator string
		col      *query.SAColumn
		value    sqlparser.Expr
		out      string
		err      string
	}{
		{
			name:     "valid equal expression",
			operator: sqlparser.EqualStr,
			col:      textCol,
			value:    query.NewUnsafeSQLString("foo bar"),
			out:      "Text01 @@ 'foo | bar'::tsquery",
		},
		{
			name:     "valid not equal expression",
			operator: sqlparser.NotEqualStr,
			col:      textCol,
			value:    query.NewUnsafeSQLString("foo bar"),
			out:      "not Text01 @@ 'foo | bar'::tsquery",
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			qc := &queryConverter{}
			out, err := qc.ConvertTextComparisonExpr(tc.operator, tc.col, tc.value)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *query.ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, sqlparser.String(out))
			}
		})
	}
}
