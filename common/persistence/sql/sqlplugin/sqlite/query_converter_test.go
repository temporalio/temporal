package sqlite

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

func TestQueryConverter_GetCoalesceCloseTimeExpr(t *testing.T) {
	r := require.New(t)
	qc := &queryConverter{}
	expr := qc.GetCoalesceCloseTimeExpr()
	r.Equal(
		"coalesce(close_time, '9999-12-31 23:59:59+00:00')",
		sqlparser.String(expr),
	)
}

func TestQueryConverter_ConvertKeywordListComparisonExpr(t *testing.T) {
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
