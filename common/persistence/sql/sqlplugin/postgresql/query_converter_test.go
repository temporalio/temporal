package postgresql

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute/sadefs"
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

func TestQueryConverter_BuildSelectStmt(t *testing.T) {
	closeTime := time.Date(2025, 11, 10, 13, 34, 56, 0, time.UTC)
	startTime := time.Date(2025, 11, 10, 12, 34, 56, 0, time.UTC)
	runID := "test-run-id"
	keywordCol := query.NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	tests := []struct {
		name      string
		queryExpr sqlparser.Expr
		pageSize  int
		token     *sqlplugin.VisibilityPageToken
		stmt      string
		queryArgs []any
	}{
		{
			name:     "empty",
			pageSize: 10,
			stmt: fmt.Sprintf(
				"SELECT %s FROM executions_visibility ORDER BY coalesce(close_time, '9999-12-31 23:59:59') DESC, start_time DESC, run_id LIMIT ?",
				strings.Join(sqlplugin.DbFields, ", "),
			),
			queryArgs: []any{10},
		},
		{
			name: "non-empty",
			queryExpr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     keywordCol,
				Right:    query.NewUnsafeSQLString("foo"),
			},
			pageSize: 20,
			stmt: fmt.Sprintf(
				"SELECT %s FROM executions_visibility WHERE Keyword01 = 'foo' ORDER BY coalesce(close_time, '9999-12-31 23:59:59') DESC, start_time DESC, run_id LIMIT ?",
				strings.Join(sqlplugin.DbFields, ", "),
			),
			queryArgs: []any{20},
		},
		{
			name: "token",
			queryExpr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     keywordCol,
				Right:    query.NewUnsafeSQLString("foo"),
			},
			pageSize: 20,
			token: &sqlplugin.VisibilityPageToken{
				CloseTime: closeTime,
				StartTime: startTime,
				RunID:     runID,
			},
			stmt: fmt.Sprintf(
				"SELECT %s FROM executions_visibility WHERE Keyword01 = 'foo' AND ((coalesce(close_time, '9999-12-31 23:59:59') = ? AND start_time = ? AND run_id > ?) OR (coalesce(close_time, '9999-12-31 23:59:59') = ? AND start_time < ?) OR coalesce(close_time, '9999-12-31 23:59:59') < ?) ORDER BY coalesce(close_time, '9999-12-31 23:59:59') DESC, start_time DESC, run_id LIMIT ?",
				strings.Join(sqlplugin.DbFields, ", "),
			),
			queryArgs: []any{
				closeTime,
				startTime,
				runID,
				closeTime,
				startTime,
				closeTime,
				20,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			qc := &queryConverter{}
			qp := &query.QueryParams[sqlparser.Expr]{
				QueryExpr: tc.queryExpr,
			}
			stmt, queryArgs := qc.BuildSelectStmt(qp, tc.pageSize, tc.token)
			r.Equal(tc.stmt, stmt)
			r.Equal(tc.queryArgs, queryArgs)
		})
	}
}

func TestQueryConverter_BuildCountStmt(t *testing.T) {
	keywordCol := query.NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	tests := []struct {
		name      string
		queryExpr sqlparser.Expr
		groupBy   []*query.SAColumn
		stmt      string
	}{
		{
			name: "empty",
			stmt: "SELECT COUNT(*) FROM executions_visibility",
		},
		{
			name: "non-empty",
			queryExpr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     keywordCol,
				Right:    query.NewUnsafeSQLString("foo"),
			},
			stmt: "SELECT COUNT(*) FROM executions_visibility WHERE Keyword01 = 'foo'",
		},
		{
			name: "group by",
			queryExpr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     keywordCol,
				Right:    query.NewUnsafeSQLString("foo"),
			},
			groupBy: []*query.SAColumn{
				query.NewSAColumn(sadefs.ExecutionStatus, sadefs.ExecutionStatus, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			},
			stmt: "SELECT status, COUNT(*) FROM executions_visibility WHERE Keyword01 = 'foo' GROUP BY status",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			qc := &queryConverter{}
			qp := &query.QueryParams[sqlparser.Expr]{
				QueryExpr: tc.queryExpr,
				GroupBy:   tc.groupBy,
			}
			stmt, queryArgs := qc.BuildCountStmt(qp)
			r.Equal(tc.stmt, stmt)
			r.Nil(queryArgs)
		})
	}
}
