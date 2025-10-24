package elasticsearch

import (
	"fmt"
	"testing"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

func TestQueryConverter_GetDatetimeFormat(t *testing.T) {
	qc := &queryConverter{}
	require.Equal(t, time.RFC3339Nano, qc.GetDatetimeFormat())
}

func TestQueryConverter_BuildParenExpr(t *testing.T) {
	testCases := []struct {
		name string
		in   elastic.Query
		out  elastic.Query
	}{
		{
			name: "empty",
			in:   nil,
			out:  nil,
		},
		{
			name: "term query",
			in:   elastic.NewTermQuery("field", "foo"),
			out:  elastic.NewTermQuery("field", "foo"),
		},
		{
			name: "bool query",
			in:   elastic.NewBoolQuery().Filter(elastic.NewTermQuery("field", "foo")),
			out:  elastic.NewBoolQuery().Filter(elastic.NewTermQuery("field", "foo")),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			qc := &queryConverter{}
			out, err := qc.BuildParenExpr(tc.in)
			r.NoError(err)
			r.Equal(tc.out, out)
		})
	}
}

func TestQueryConverter_BuildNotExpr(t *testing.T) {
	testCases := []struct {
		name string
		in   elastic.Query
		out  elastic.Query
	}{
		{
			name: "empty",
			in:   nil,
			out:  nil,
		},
		{
			name: "term query",
			in:   elastic.NewTermQuery("field", "foo"),
			out:  elastic.NewBoolQuery().MustNot(elastic.NewTermQuery("field", "foo")),
		},
		{
			name: "bool query",
			in:   elastic.NewBoolQuery().Filter(elastic.NewTermQuery("field", "foo")),
			out:  elastic.NewBoolQuery().MustNot(elastic.NewBoolQuery().Filter(elastic.NewTermQuery("field", "foo"))),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			qc := &queryConverter{}
			out, err := qc.BuildNotExpr(tc.in)
			r.NoError(err)
			r.Equal(tc.out, out)
		})
	}
}

func TestQueryConverter_BuildAndExpr(t *testing.T) {
	testCases := []struct {
		name string
		in   []elastic.Query
		out  elastic.Query
	}{
		{
			name: "empty",
			in:   nil,
			out:  nil,
		},
		{
			name: "slice of empty values",
			in:   []elastic.Query{nil, nil},
			out:  nil,
		},
		{
			name: "one query",
			in:   []elastic.Query{elastic.NewTermQuery("field", "foo")},
			out:  elastic.NewTermQuery("field", "foo"),
		},
		{
			name: "two queries",
			in: []elastic.Query{
				elastic.NewTermQuery("field1", "foo"),
				elastic.NewBoolQuery().Filter(elastic.NewTermQuery("field2", "bar")),
			},
			out: elastic.NewBoolQuery().Filter(
				elastic.NewTermQuery("field1", "foo"),
				elastic.NewBoolQuery().Filter(elastic.NewTermQuery("field2", "bar")),
			),
		},
		{
			name: "multiple queries",
			in: []elastic.Query{
				elastic.NewTermQuery("field1", "foo"),
				nil,
				elastic.NewBoolQuery().Filter(elastic.NewTermQuery("field2", "bar")),
				elastic.NewBoolQuery().MustNot(elastic.NewTermQuery("field3", "zzz")),
			},
			out: elastic.NewBoolQuery().Filter(
				elastic.NewTermQuery("field1", "foo"),
				elastic.NewBoolQuery().Filter(elastic.NewTermQuery("field2", "bar")),
				elastic.NewBoolQuery().MustNot(elastic.NewTermQuery("field3", "zzz")),
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			qc := &queryConverter{}
			out, err := qc.BuildAndExpr(tc.in...)
			r.NoError(err)
			r.Equal(tc.out, out)
		})
	}
}

func TestQueryConverter_BuildOrExpr(t *testing.T) {
	testCases := []struct {
		name string
		in   []elastic.Query
		out  elastic.Query
	}{
		{
			name: "empty",
			in:   nil,
			out:  nil,
		},
		{
			name: "slice of empty values",
			in:   []elastic.Query{nil, nil},
			out:  nil,
		},
		{
			name: "one query",
			in:   []elastic.Query{elastic.NewTermQuery("field", "foo")},
			out:  elastic.NewTermQuery("field", "foo"),
		},
		{
			name: "two queries",
			in: []elastic.Query{
				elastic.NewTermQuery("field1", "foo"),
				elastic.NewBoolQuery().Filter(elastic.NewTermQuery("field2", "bar")),
			},
			out: elastic.NewBoolQuery().
				Should(
					elastic.NewTermQuery("field1", "foo"),
					elastic.NewBoolQuery().Filter(elastic.NewTermQuery("field2", "bar")),
				).
				MinimumNumberShouldMatch(1),
		},
		{
			name: "multiple queries",
			in: []elastic.Query{
				elastic.NewTermQuery("field1", "foo"),
				nil,
				elastic.NewBoolQuery().Filter(elastic.NewTermQuery("field2", "bar")),
				elastic.NewBoolQuery().MustNot(elastic.NewTermQuery("field3", "zzz")),
			},
			out: elastic.NewBoolQuery().
				Should(
					elastic.NewTermQuery("field1", "foo"),
					elastic.NewBoolQuery().Filter(elastic.NewTermQuery("field2", "bar")),
					elastic.NewBoolQuery().MustNot(elastic.NewTermQuery("field3", "zzz")),
				).
				MinimumNumberShouldMatch(1),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			qc := &queryConverter{}
			out, err := qc.BuildOrExpr(tc.in...)
			r.NoError(err)
			r.Equal(tc.out, out)
		})
	}
}

func TestQueryConverter_ConvertComparisonExpr(t *testing.T) {
	intCol := query.NewSAColumn(
		"AliasForInt01",
		"Int01",
		enumspb.INDEXED_VALUE_TYPE_INT,
	)

	testCases := []struct {
		name     string
		operator string
		col      *query.SAColumn
		value    any
		out      elastic.Query
		err      string
	}{
		{
			name:     "operator greater equal",
			operator: sqlparser.GreaterEqualStr,
			col:      intCol,
			value:    123,
			out:      elastic.NewRangeQuery(intCol.FieldName).Gte(123),
		},
		{
			name:     "operator less equal",
			operator: sqlparser.LessEqualStr,
			col:      intCol,
			value:    123,
			out:      elastic.NewRangeQuery(intCol.FieldName).Lte(123),
		},
		{
			name:     "operator greater than",
			operator: sqlparser.GreaterThanStr,
			col:      intCol,
			value:    123,
			out:      elastic.NewRangeQuery(intCol.FieldName).Gt(123),
		},
		{
			name:     "operator less than",
			operator: sqlparser.LessThanStr,
			col:      intCol,
			value:    123,
			out:      elastic.NewRangeQuery(intCol.FieldName).Lt(123),
		},
		{
			name:     "operator equal",
			operator: sqlparser.EqualStr,
			col:      intCol,
			value:    123,
			out:      elastic.NewTermQuery(intCol.FieldName, 123),
		},
		{
			name:     "operator not equal",
			operator: sqlparser.NotEqualStr,
			col:      intCol,
			value:    123,
			out:      elastic.NewBoolQuery().MustNot(elastic.NewTermQuery(intCol.FieldName, 123)),
		},
		{
			name:     "operator in",
			operator: sqlparser.InStr,
			col:      intCol,
			value:    []any{123, 456},
			out:      elastic.NewTermsQuery(intCol.FieldName, 123, 456),
		},
		{
			name:     "operator not in",
			operator: sqlparser.NotInStr,
			col:      intCol,
			value:    []any{123, 456},
			out: elastic.NewBoolQuery().MustNot(
				elastic.NewTermsQuery(intCol.FieldName, 123, 456),
			),
		},
		{
			name:     "invalid operator",
			operator: sqlparser.LikeStr,
			col:      intCol,
			value:    123,
			err: fmt.Sprintf(
				"%s: operator 'LIKE' not supported for Int type search attribute 'AliasForInt01'",
				query.NotSupportedErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			qc := &queryConverter{}
			out, err := qc.ConvertComparisonExpr(tc.operator, tc.col, tc.value)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *query.ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestQueryConverter_ConvertKeywordComparisonExpr(t *testing.T) {
	keywordCol := query.NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name     string
		operator string
		col      *query.SAColumn
		value    any
		out      elastic.Query
		err      string
	}{
		{
			name:     "operator greater equal",
			operator: sqlparser.GreaterEqualStr,
			col:      keywordCol,
			value:    "foo",
			out:      elastic.NewRangeQuery(keywordCol.FieldName).Gte("foo"),
		},
		{
			name:     "operator less equal",
			operator: sqlparser.LessEqualStr,
			col:      keywordCol,
			value:    "foo",
			out:      elastic.NewRangeQuery(keywordCol.FieldName).Lte("foo"),
		},
		{
			name:     "operator greater than",
			operator: sqlparser.GreaterThanStr,
			col:      keywordCol,
			value:    "foo",
			out:      elastic.NewRangeQuery(keywordCol.FieldName).Gt("foo"),
		},
		{
			name:     "operator less than",
			operator: sqlparser.LessThanStr,
			col:      keywordCol,
			value:    "foo",
			out:      elastic.NewRangeQuery(keywordCol.FieldName).Lt("foo"),
		},
		{
			name:     "operator equal",
			operator: sqlparser.EqualStr,
			col:      keywordCol,
			value:    "foo",
			out:      elastic.NewTermQuery(keywordCol.FieldName, "foo"),
		},
		{
			name:     "operator not equal",
			operator: sqlparser.NotEqualStr,
			col:      keywordCol,
			value:    "foo",
			out:      elastic.NewBoolQuery().MustNot(elastic.NewTermQuery(keywordCol.FieldName, "foo")),
		},
		{
			name:     "operator in",
			operator: sqlparser.InStr,
			col:      keywordCol,
			value:    []any{"foo", "bar"},
			out:      elastic.NewTermsQuery(keywordCol.FieldName, "foo", "bar"),
		},
		{
			name:     "operator not in",
			operator: sqlparser.NotInStr,
			col:      keywordCol,
			value:    []any{"foo", "bar"},
			out: elastic.NewBoolQuery().MustNot(
				elastic.NewTermsQuery(keywordCol.FieldName, "foo", "bar"),
			),
		},
		{
			name:     "operator starts with",
			operator: sqlparser.StartsWithStr,
			col:      keywordCol,
			value:    "foo",
			out:      elastic.NewPrefixQuery(keywordCol.FieldName, "foo"),
		},
		{
			name:     "operator not starts with",
			operator: sqlparser.NotStartsWithStr,
			col:      keywordCol,
			value:    "foo",
			out:      elastic.NewBoolQuery().MustNot(elastic.NewPrefixQuery(keywordCol.FieldName, "foo")),
		},
		{
			name:     "operator starts with invalid value",
			operator: sqlparser.StartsWithStr,
			col:      keywordCol,
			value:    123,
			err: fmt.Sprintf(
				"%s: right-hand side of operator 'STARTS_WITH' must be a string",
				query.InvalidExpressionErrMessage,
			),
		},
		{
			name:     "operator not starts with invalid value",
			operator: sqlparser.NotStartsWithStr,
			col:      keywordCol,
			value:    123,
			err: fmt.Sprintf(
				"%s: right-hand side of operator 'NOT STARTS_WITH' must be a string",
				query.InvalidExpressionErrMessage,
			),
		},
		{
			name:     "invalid operator",
			operator: sqlparser.LikeStr,
			col:      keywordCol,
			value:    "foo",
			err: fmt.Sprintf(
				"%s: operator 'LIKE' not supported for Keyword type search attribute 'AliasForKeyword01'",
				query.NotSupportedErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			qc := &queryConverter{}
			out, err := qc.ConvertKeywordComparisonExpr(tc.operator, tc.col, tc.value)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *query.ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
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
		value    any
		out      elastic.Query
		err      string
	}{
		{
			name:     "operator equal",
			operator: sqlparser.EqualStr,
			col:      keywordListCol,
			value:    "foo",
			out:      elastic.NewTermQuery(keywordListCol.FieldName, "foo"),
		},
		{
			name:     "operator not equal",
			operator: sqlparser.NotEqualStr,
			col:      keywordListCol,
			value:    "foo",
			out:      elastic.NewBoolQuery().MustNot(elastic.NewTermQuery(keywordListCol.FieldName, "foo")),
		},
		{
			name:     "operator in",
			operator: sqlparser.InStr,
			col:      keywordListCol,
			value:    []any{"foo", "bar"},
			out:      elastic.NewTermsQuery(keywordListCol.FieldName, "foo", "bar"),
		},
		{
			name:     "operator not in",
			operator: sqlparser.NotInStr,
			col:      keywordListCol,
			value:    []any{"foo", "bar"},
			out: elastic.NewBoolQuery().MustNot(
				elastic.NewTermsQuery(keywordListCol.FieldName, "foo", "bar"),
			),
		},
		{
			name:     "invalid operator",
			operator: sqlparser.LikeStr,
			col:      keywordListCol,
			value:    "foo",
			err: fmt.Sprintf(
				"%s: operator 'LIKE' not supported for KeywordList type search attribute 'AliasForKeywordList01'",
				query.NotSupportedErrMessage,
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
				r.Equal(tc.out, out)
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

	testCases := []struct {
		name     string
		operator string
		col      *query.SAColumn
		value    any
		out      elastic.Query
		err      string
	}{
		{
			name:     "operator equal",
			operator: sqlparser.EqualStr,
			col:      textCol,
			value:    "foo",
			out:      elastic.NewMatchQuery(textCol.FieldName, "foo"),
		},
		{
			name:     "operator not equal",
			operator: sqlparser.NotEqualStr,
			col:      textCol,
			value:    "foo",
			out:      elastic.NewBoolQuery().MustNot(elastic.NewMatchQuery(textCol.FieldName, "foo")),
		},
		{
			name:     "invalid operator",
			operator: sqlparser.LikeStr,
			col:      textCol,
			value:    "foo",
			err: fmt.Sprintf(
				"%s: operator 'LIKE' not supported for Text type search attribute 'AliasForText01'",
				query.NotSupportedErrMessage,
			),
		},
	}

	for _, tc := range testCases {
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
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestQueryConverter_ConvertRangeExpr(t *testing.T) {
	keywordCol := query.NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name     string
		operator string
		col      *query.SAColumn
		from     any
		to       any
		out      elastic.Query
		err      string
	}{
		{
			name:     "operator between",
			operator: sqlparser.BetweenStr,
			col:      keywordCol,
			from:     "123",
			to:       "456",
			out:      elastic.NewRangeQuery(keywordCol.FieldName).Gte("123").Lte("456"),
		},
		{
			name:     "operator not between",
			operator: sqlparser.NotBetweenStr,
			col:      keywordCol,
			from:     "123",
			to:       "456",
			out: elastic.NewBoolQuery().MustNot(
				elastic.NewRangeQuery(keywordCol.FieldName).Gte("123").Lte("456"),
			),
		},
		{
			name:     "invalid operator",
			operator: sqlparser.LikeStr,
			col:      keywordCol,
			from:     "foo",
			err: fmt.Sprintf(
				"%s: unexpected operator 'LIKE' for range condition",
				query.MalformedSqlQueryErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			qc := &queryConverter{}
			out, err := qc.ConvertRangeExpr(tc.operator, tc.col, tc.from, tc.to)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *query.ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestQueryConverter_ConvertIsExpr(t *testing.T) {
	keywordCol := query.NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name     string
		operator string
		col      *query.SAColumn
		out      elastic.Query
		err      string
	}{
		{
			name:     "operator is null",
			operator: sqlparser.IsNullStr,
			col:      keywordCol,
			out:      elastic.NewBoolQuery().MustNot(elastic.NewExistsQuery(keywordCol.FieldName)),
		},
		{
			name:     "operator is not null",
			operator: sqlparser.IsNotNullStr,
			col:      keywordCol,
			out:      elastic.NewExistsQuery(keywordCol.FieldName),
		},
		{
			name:     "invalid operator",
			operator: sqlparser.LikeStr,
			col:      keywordCol,
			err: fmt.Sprintf(
				"%s: 'IS' operator can only be used as 'IS NULL' or 'IS NOT NULL'",
				query.InvalidExpressionErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			qc := &queryConverter{}
			out, err := qc.ConvertIsExpr(tc.operator, tc.col)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *query.ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}
