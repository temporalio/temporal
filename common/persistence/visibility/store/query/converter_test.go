package query

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.uber.org/mock/gomock"
)

const (
	testNamespaceName = namespace.Name("test-namespace")
)

func TestWithSearchAttributeInterceptor(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	ctrl := gomock.NewController(t)
	storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)

	// QueryConverter without explicit SearchAttributeInterceptor sets the nop interceptor.
	c := newTestQueryConverter(storeQCMock)
	r.Equal(nopSearchAttributeInterceptor, c.saInterceptor)

	// Setting nil interceptor sets the nop interceptor.
	c = newTestQueryConverter(storeQCMock).
		WithSearchAttributeInterceptor(nil)
	r.Equal(nopSearchAttributeInterceptor, c.saInterceptor)

	// Setting non-nil interceptor
	i := &testSearchAttributeInterceptor{}
	c = newTestQueryConverter(storeQCMock).
		WithSearchAttributeInterceptor(i)
	r.Equal(i, c.saInterceptor)
}

func TestQueryConverter_Convert(t *testing.T) {
	t.Parallel()

	keywordCol := NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	namespaceDivisionExpr := &sqlparser.IsExpr{
		Operator: sqlparser.IsNullStr,
		Expr:     NamespaceIDSAColumn,
	}

	testCases := []struct {
		name                      string
		in                        string
		inExpr                    sqlparser.Expr
		setupMocks                func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr])
		mockNamespaceDivisionExpr bool
		mockNamespaceDivisionErr  error
		mockBuildFinalAndExpr     bool
		mockBuildFinalAndRes      sqlparser.Expr
		mockBuildFinalAndErr      error
		err                       string
	}{
		{
			name: "success",
			in:   "AliasForKeyword01 = 'foo'",
			inExpr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     keywordCol,
				Right:    NewUnsafeSQLString("foo"),
			},
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				e := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				}
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(e, nil)
			},
			mockNamespaceDivisionExpr: true,
			mockBuildFinalAndExpr:     true,
			mockBuildFinalAndRes: &sqlparser.AndExpr{
				Left: namespaceDivisionExpr,
				Right: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				},
			},
		},

		{
			name:                      "success empty",
			in:                        "",
			mockNamespaceDivisionExpr: true,
			mockBuildFinalAndExpr:     true,
			mockBuildFinalAndRes:      namespaceDivisionExpr,
		},

		{
			// Grouping by TemporalNamespaceDivision must suppress the default
			// namespace division filter so that results span all divisions.
			// mockNamespaceDivisionExpr is false, so the default filter is not
			// applied and SeenNamespaceDivision() is expected to be true.
			name:                  "success group by TemporalNamespaceDivision suppresses default filter",
			in:                    "group by TemporalNamespaceDivision",
			mockBuildFinalAndExpr: true,
			mockBuildFinalAndRes:  nil,
		},

		{
			name: "success with namespace division",
			in:   "AliasForKeyword01 = 'foo' and TemporalNamespaceDivision = 'bar'",
			inExpr: &sqlparser.AndExpr{
				Left: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				},
				Right: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     NamespaceDivisionSAColumn(),
					Right:    NewUnsafeSQLString("bar"),
				},
			},
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				e1 := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				}
				e2 := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     NamespaceDivisionSAColumn(),
					Right:    NewUnsafeSQLString("bar"),
				}
				e1e2 := &sqlparser.AndExpr{
					Left:  e1,
					Right: e2,
				}
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(e1, nil)
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, NamespaceDivisionSAColumn(), "bar").
					Return(e2, nil)
				storeQCMock.EXPECT().BuildAndExpr(e1, e2).Return(e1e2, nil)
			},
			mockBuildFinalAndExpr: true,
			mockBuildFinalAndRes: &sqlparser.ParenExpr{
				Expr: &sqlparser.AndExpr{
					Left: &sqlparser.ComparisonExpr{
						Operator: sqlparser.EqualStr,
						Left:     keywordCol,
						Right:    NewUnsafeSQLString("foo"),
					},
					Right: &sqlparser.ComparisonExpr{
						Operator: sqlparser.EqualStr,
						Left:     NamespaceDivisionSAColumn(),
						Right:    NewUnsafeSQLString("bar"),
					},
				},
			},
		},

		{
			name: "fail malformed query",
			in:   "AliasForKeyword01 = 'foo",
			err:  MalformedSqlQueryErrMessage,
		},

		{
			name: "fail namespace division expr",
			in:   "AliasForKeyword01 = 'foo'",
			inExpr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     keywordCol,
				Right:    NewUnsafeSQLString("foo"),
			},
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(
						&sqlparser.ComparisonExpr{
							Operator: sqlparser.EqualStr,
							Left:     keywordCol,
							Right:    NewUnsafeSQLString("foo"),
						},
						nil,
					)
			},
			mockNamespaceDivisionExpr: true,
			mockNamespaceDivisionErr:  errors.New("mock error"),
			err:                       "mock error",
		},

		{
			name: "fail final and expr",
			in:   "AliasForKeyword01 = 'foo'",
			inExpr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     keywordCol,
				Right:    NewUnsafeSQLString("foo"),
			},
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				e := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				}
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(e, nil)
			},
			mockNamespaceDivisionExpr: true,
			mockBuildFinalAndExpr:     true,
			mockBuildFinalAndErr:      errors.New("mock error"),
			err:                       "mock error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)

			if tc.setupMocks != nil {
				tc.setupMocks(storeQCMock)
			}

			exprs := make([]any, 2)
			if tc.mockNamespaceDivisionExpr {
				storeQCMock.EXPECT().
					ConvertIsExpr(sqlparser.IsNullStr, NamespaceDivisionSAColumn()).
					Return(namespaceDivisionExpr, tc.mockNamespaceDivisionErr)
				exprs[0] = namespaceDivisionExpr
			}
			if tc.mockBuildFinalAndExpr {
				exprs[1] = tc.inExpr
				storeQCMock.EXPECT().
					BuildAndExpr(exprs...).
					Return(tc.mockBuildFinalAndRes, tc.mockBuildFinalAndErr)
			}
			out, err := queryConverter.Convert(tc.in)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				if tc.mockNamespaceDivisionErr == nil && tc.mockBuildFinalAndErr == nil {
					var expectedErr *ConverterError
					r.ErrorAs(err, &expectedErr)
				}
			} else {
				r.NoError(err)
				r.Equal(tc.mockBuildFinalAndRes, out.QueryExpr)
				if tc.mockNamespaceDivisionExpr {
					r.False(queryConverter.SeenNamespaceDivision())
				} else {
					r.True(queryConverter.SeenNamespaceDivision())
				}
			}
		})
	}
}

func TestQueryConverter_ConvertWhereString(t *testing.T) {
	t.Parallel()

	keywordCol := NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         string
		setupMocks func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr])
		out        *QueryParams[sqlparser.Expr]
		err        string
	}{
		{
			name: "success",
			in:   "AliasForKeyword01 = 'foo'",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(
						&sqlparser.ComparisonExpr{
							Operator: sqlparser.EqualStr,
							Left:     keywordCol,
							Right:    NewUnsafeSQLString("foo"),
						},
						nil,
					)
			},
			out: &QueryParams[sqlparser.Expr]{
				QueryExpr: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				},
			},
		},

		{
			name: "success empty",
			in:   "",
			out:  &QueryParams[sqlparser.Expr]{},
		},

		{
			name: "success empty order by",
			in:   "order by WorkflowType",
			out: &QueryParams[sqlparser.Expr]{
				OrderBy: sqlparser.OrderBy{
					&sqlparser.Order{
						Expr: NewSAColumn(
							sadefs.WorkflowType,
							sadefs.WorkflowType,
							enumspb.INDEXED_VALUE_TYPE_KEYWORD,
						),
						Direction: sqlparser.AscScr,
					},
				},
			},
		},

		{
			name: "success empty group by",
			in:   "group by ExecutionStatus",
			out: &QueryParams[sqlparser.Expr]{
				GroupBy: []*SAColumn{
					NewSAColumn(
						sadefs.ExecutionStatus,
						sadefs.ExecutionStatus,
						enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					),
				},
			},
		},

		{
			name: "fail malformed query",
			in:   "AliasForKeyword01 = 'foo",
			err:  MalformedSqlQueryErrMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)

			if tc.setupMocks != nil {
				tc.setupMocks(storeQCMock)
			}
			out, err := queryConverter.convertWhereString(tc.in)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestQueryConverter_ConvertSelectStmt(t *testing.T) {
	t.Parallel()

	keywordCol := NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         string
		setupMocks func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr])
		out        *QueryParams[sqlparser.Expr]
		err        string
	}{
		{
			name: "success",
			in:   "select * from t where AliasForKeyword01 = 'foo'",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(
						&sqlparser.ComparisonExpr{
							Operator: sqlparser.EqualStr,
							Left:     keywordCol,
							Right:    NewUnsafeSQLString("foo"),
						},
						nil,
					)
			},
			out: &QueryParams[sqlparser.Expr]{
				QueryExpr: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				},
			},
		},

		{
			name: "success empty",
			in:   "select * from t",
			out:  &QueryParams[sqlparser.Expr]{},
		},

		{
			name: "success empty order by WorkflowType",
			in:   "select * from t order by WorkflowType",
			out: &QueryParams[sqlparser.Expr]{
				OrderBy: sqlparser.OrderBy{
					&sqlparser.Order{
						Expr: NewSAColumn(
							sadefs.WorkflowType,
							sadefs.WorkflowType,
							enumspb.INDEXED_VALUE_TYPE_KEYWORD,
						),
						Direction: sqlparser.AscScr,
					},
				},
			},
		},

		{
			name: "success empty group by ExecutionStatus",
			in:   "select * from t group by ExecutionStatus",
			out: &QueryParams[sqlparser.Expr]{
				GroupBy: []*SAColumn{
					NewSAColumn(
						sadefs.ExecutionStatus,
						sadefs.ExecutionStatus,
						enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					),
				},
			},
		},

		{
			name: "fail limit not supported",
			in:   "select * from t limit 10",
			err:  fmt.Sprintf("%s: 'LIMIT' clause", NotSupportedErrMessage),
		},

		{
			name: "fail invalid query",
			in:   "select * from t where true",
			err:  NotSupportedErrMessage,
		},

		{
			name: "fail multiple group by",
			in:   "select * from t group by ExecutionStatus, RunId",
			err: fmt.Sprintf(
				"%s: 'GROUP BY' clause supports only a single field",
				NotSupportedErrMessage,
			),
		},

		{
			name: "success group by TemporalNamespaceDivision",
			in:   "select * from t group by TemporalNamespaceDivision",
			out: &QueryParams[sqlparser.Expr]{
				GroupBy: []*SAColumn{
					NewSAColumn(
						sadefs.TemporalNamespaceDivision,
						sadefs.TemporalNamespaceDivision,
						enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					),
				},
			},
		},

		{
			name: "fail not supported group by field",
			in:   "select * from t group by RunId",
			err: fmt.Sprintf(
				"%s: 'GROUP BY' clause is not supported for search attribute %s",
				NotSupportedErrMessage,
				"RunId",
			),
		},

		{
			name: "fail invalid group by field",
			in:   "select * from t group by InvalidField",
			err:  InvalidSearchAttribute,
		},

		{
			name: "fail invalid order by field",
			in:   "select * from t order by InvalidField",
			err:  InvalidSearchAttribute,
		},

		{
			name: "fail invalid order by field type",
			in:   "select * from t order by AliasForText01",
			err: fmt.Sprintf(
				"%s: unable to sort by search attribute type Text",
				NotSupportedErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)

			if tc.setupMocks != nil {
				tc.setupMocks(storeQCMock)
			}
			stmt, err := sqlparser.Parse(tc.in)
			r.NoError(err)
			selectStmt, _ := stmt.(*sqlparser.Select)
			out, err := queryConverter.convertSelectStmt(selectStmt)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestQueryConverter_ConvertWhereExpr(t *testing.T) {
	t.Parallel()

	keywordCol := NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	statusCol := NewSAColumn(
		sadefs.ExecutionStatus,
		sadefs.ExecutionStatus,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         string
		setupMocks func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr])
		out        sqlparser.Expr
		err        string
	}{
		{
			name: "success nil",
			in:   "select * from t",
			err:  "where expression is nil",
		},

		{
			name: "success comparison",
			in:   "select * from t where AliasForKeyword01 = 'foo'",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(
						&sqlparser.ComparisonExpr{
							Operator: sqlparser.EqualStr,
							Left:     keywordCol,
							Right:    NewUnsafeSQLString("foo"),
						},
						nil,
					)
			},
			out: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     keywordCol,
				Right:    NewUnsafeSQLString("foo"),
			},
		},

		{
			name: "success parenthesis",
			in:   "select * from t where (AliasForKeyword01 = 'foo')",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				e := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				}
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(e, nil)
				storeQCMock.EXPECT().BuildParenExpr(e).Return(&sqlparser.ParenExpr{Expr: e}, nil)
			},
			out: &sqlparser.ParenExpr{
				Expr: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				},
			},
		},

		{
			name: "success not",
			in:   "select * from t where not AliasForKeyword01 = 'foo'",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				e := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				}
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(e, nil)
				storeQCMock.EXPECT().BuildNotExpr(e).Return(&sqlparser.NotExpr{Expr: e}, nil)
			},
			out: &sqlparser.NotExpr{
				Expr: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				},
			},
		},

		{
			name: "success and",
			in:   "select * from t where AliasForKeyword01 = 'foo' and ExecutionStatus = 'Running'",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				e1 := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				}
				e2 := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     statusCol,
					Right:    sqlparser.NewIntVal([]byte("1")),
				}
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(e1, nil)
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, statusCol, "Running").
					Return(e2, nil)
				storeQCMock.EXPECT().BuildAndExpr(e1, e2).
					Return(
						&sqlparser.AndExpr{Left: e1, Right: e2},
						nil,
					)
			},
			out: &sqlparser.AndExpr{
				Left: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				},
				Right: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     statusCol,
					Right:    sqlparser.NewIntVal([]byte("1")),
				},
			},
		},

		{
			name: "success or",
			in:   "select * from t where AliasForKeyword01 = 'foo' or ExecutionStatus = 'Running'",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				e1 := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				}
				e2 := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     statusCol,
					Right:    sqlparser.NewIntVal([]byte("1")),
				}
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(e1, nil)
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, statusCol, "Running").
					Return(e2, nil)
				storeQCMock.EXPECT().BuildOrExpr(e1, e2).
					Return(
						&sqlparser.OrExpr{Left: e1, Right: e2},
						nil,
					)
			},
			out: &sqlparser.OrExpr{
				Left: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				},
				Right: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     statusCol,
					Right:    sqlparser.NewIntVal([]byte("1")),
				},
			},
		},

		{
			name: "success range",
			in:   "select * from t where AliasForKeyword01 between '123' and '456'",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				storeQCMock.EXPECT().
					ConvertRangeExpr(sqlparser.BetweenStr, keywordCol, "123", "456").
					Return(
						&sqlparser.RangeCond{
							Operator: sqlparser.BetweenStr,
							Left:     keywordCol,
							From:     NewUnsafeSQLString("123"),
							To:       NewUnsafeSQLString("456"),
						},
						nil,
					)
			},
			out: &sqlparser.RangeCond{
				Operator: sqlparser.BetweenStr,
				Left:     keywordCol,
				From:     NewUnsafeSQLString("123"),
				To:       NewUnsafeSQLString("456"),
			},
		},

		{
			name: "success is",
			in:   "select * from t where AliasForKeyword01 is null",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				storeQCMock.EXPECT().
					ConvertIsExpr(sqlparser.IsNullStr, keywordCol).
					Return(
						&sqlparser.IsExpr{
							Operator: sqlparser.IsNullStr,
							Expr:     keywordCol,
						},
						nil,
					)
			},
			out: &sqlparser.IsExpr{
				Operator: sqlparser.IsNullStr,
				Expr:     keywordCol,
			},
		},

		{
			name: "fail func",
			in:   "select * from t where coalesce(AliasForKeyword01, 'foo')",
			err:  NotSupportedErrMessage,
		},

		{
			name: "fail col name",
			in:   "select * from t where AliasForKeyword01",
			err:  InvalidExpressionErrMessage,
		},

		{
			name: "fail literal",
			in:   "select * from t where true",
			err:  NotSupportedErrMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)

			if tc.setupMocks != nil {
				tc.setupMocks(storeQCMock)
			}
			stmt, err := sqlparser.Parse(tc.in)
			r.NoError(err)
			selectStmt, _ := stmt.(*sqlparser.Select)
			if selectStmt.Where == nil {
				selectStmt.Where = &sqlparser.Where{
					Type: sqlparser.WhereStr,
					Expr: nil,
				}
			}
			out, err := queryConverter.convertWhereExpr(selectStmt.Where.Expr)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				if _, ok := err.(*serviceerror.Internal); !ok {
					var expectedErr *ConverterError
					r.ErrorAs(err, &expectedErr)
				}
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestQueryConverter_ConvertParenExpr(t *testing.T) {
	t.Parallel()

	keywordCol := NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         string
		setupMocks func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr])
		out        sqlparser.Expr
		err        string
	}{
		{
			name: "success",
			in:   "(AliasForKeyword01 IS NULL)",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				e := &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				}
				storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNullStr, keywordCol).Return(e, nil)
				storeQCMock.EXPECT().BuildParenExpr(e).Return(
					&sqlparser.ParenExpr{
						Expr: &sqlparser.IsExpr{
							Operator: sqlparser.IsNullStr,
							Expr:     keywordCol,
						},
					},
					nil,
				)
			},
			out: &sqlparser.ParenExpr{
				Expr: &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				},
			},
		},

		{
			name: "fail",
			in:   "(FALSE)",
			err:  NotSupportedErrMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)

			if tc.setupMocks != nil {
				tc.setupMocks(storeQCMock)
			}
			inExpr := parseWhereString(tc.in).(*sqlparser.ParenExpr)
			out, err := queryConverter.convertParenExpr(inExpr)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestQueryConverter_ConvertNotExpr(t *testing.T) {
	t.Parallel()

	keywordCol := NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         string
		setupMocks func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr])
		out        sqlparser.Expr
		err        string
	}{
		{
			name: "success",
			in:   "NOT AliasForKeyword01 IS NULL",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				e := &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				}
				storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNullStr, keywordCol).Return(e, nil)
				storeQCMock.EXPECT().BuildNotExpr(e).Return(
					&sqlparser.NotExpr{
						Expr: &sqlparser.IsExpr{
							Operator: sqlparser.IsNullStr,
							Expr:     keywordCol,
						},
					},
					nil,
				)
			},
			out: &sqlparser.NotExpr{
				Expr: &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				},
			},
		},

		{
			name: "fail",
			in:   "NOT TRUE",
			err:  NotSupportedErrMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)

			if tc.setupMocks != nil {
				tc.setupMocks(storeQCMock)
			}
			inExpr := parseWhereString(tc.in).(*sqlparser.NotExpr)
			out, err := queryConverter.convertNotExpr(inExpr)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestQueryConverter_ConvertAndExpr(t *testing.T) {
	t.Parallel()

	keywordCol := NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	wfTypeCol := NewSAColumn(
		sadefs.WorkflowType,
		sadefs.WorkflowType,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         string
		setupMocks func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr])
		out        sqlparser.Expr
		err        string
	}{
		{
			name: "success",
			in:   "AliasForKeyword01 IS NULL AND WorkflowType = 'test-wf-type'",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				e1 := &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				}
				e2 := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     wfTypeCol,
					Right:    NewUnsafeSQLString("test-wf-type"),
				}
				storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNullStr, keywordCol).Return(e1, nil)
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, wfTypeCol, "test-wf-type").
					Return(e2, nil)
				storeQCMock.EXPECT().BuildAndExpr(e1, e2).Return(
					&sqlparser.AndExpr{
						Left:  e1,
						Right: e2,
					},
					nil,
				)
			},
			out: &sqlparser.AndExpr{
				Left: &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				},
				Right: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     wfTypeCol,
					Right:    NewUnsafeSQLString("test-wf-type"),
				},
			},
		},

		{
			name: "fail left",
			in:   "FALSE AND AliasForKeyword01 IS NULL",
			err:  NotSupportedErrMessage,
		},

		{
			name: "fail right",
			in:   "AliasForKeyword01 IS NULL AND FALSE",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				e1 := &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				}
				storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNullStr, keywordCol).Return(e1, nil)
			},
			err: NotSupportedErrMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)

			if tc.setupMocks != nil {
				tc.setupMocks(storeQCMock)
			}
			inExpr := parseWhereString(tc.in).(*sqlparser.AndExpr)
			out, err := queryConverter.convertAndExpr(inExpr)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestQueryConverter_ConvertOrExpr(t *testing.T) {
	t.Parallel()

	keywordCol := NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	wfTypeCol := NewSAColumn(
		sadefs.WorkflowType,
		sadefs.WorkflowType,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         string
		setupMocks func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr])
		out        sqlparser.Expr
		err        string
	}{
		{
			name: "success",
			in:   "AliasForKeyword01 IS NULL OR WorkflowType = 'test-wf-type'",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				e1 := &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				}
				e2 := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     wfTypeCol,
					Right:    NewUnsafeSQLString("test-wf-type"),
				}
				storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNullStr, keywordCol).Return(e1, nil)
				storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, wfTypeCol, "test-wf-type").
					Return(e2, nil)
				storeQCMock.EXPECT().BuildOrExpr(e1, e2).Return(
					&sqlparser.OrExpr{
						Left:  e1,
						Right: e2,
					},
					nil,
				)
			},
			out: &sqlparser.OrExpr{
				Left: &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				},
				Right: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     wfTypeCol,
					Right:    NewUnsafeSQLString("test-wf-type"),
				},
			},
		},

		{
			name: "fail left",
			in:   "FALSE OR AliasForKeyword01 IS NULL",
			err:  NotSupportedErrMessage,
		},

		{
			name: "fail right",
			in:   "AliasForKeyword01 IS NULL OR FALSE",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				e1 := &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				}
				storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNullStr, keywordCol).Return(e1, nil)
			},
			err: NotSupportedErrMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)

			if tc.setupMocks != nil {
				tc.setupMocks(storeQCMock)
			}
			inExpr := parseWhereString(tc.in).(*sqlparser.OrExpr)
			out, err := queryConverter.convertOrExpr(inExpr)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestQueryConverter_ConvertComparisonExprStoreQueryConverterCalled(t *testing.T) {
	t.Parallel()

	keywordCol := NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	keywordListCol := NewSAColumn(
		"AliasForKeywordList01",
		"KeywordList01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
	)
	textCol := NewSAColumn(
		"AliasForText01",
		"Text01",
		enumspb.INDEXED_VALUE_TYPE_TEXT,
	)
	datetimeCol := NewSAColumn(
		"AliasForDatetime01",
		"Datetime01",
		enumspb.INDEXED_VALUE_TYPE_DATETIME,
	)

	testCases := []struct {
		name     string
		col      *SAColumn
		value    string
		mockFunc func(*MockStoreQueryConverterMockRecorder[sqlparser.Expr], any, any, any) *gomock.Call
		mockErr  error
	}{
		{
			name:     "success keyword",
			col:      keywordCol,
			value:    "foo",
			mockFunc: (*MockStoreQueryConverterMockRecorder[sqlparser.Expr]).ConvertKeywordComparisonExpr,
		},

		{
			name:     "fail keyword",
			col:      keywordCol,
			value:    "foo",
			mockFunc: (*MockStoreQueryConverterMockRecorder[sqlparser.Expr]).ConvertKeywordComparisonExpr,
			mockErr:  errors.New("mock error"),
		},

		{
			name:     "success keyword list",
			col:      keywordListCol,
			value:    "foo",
			mockFunc: (*MockStoreQueryConverterMockRecorder[sqlparser.Expr]).ConvertKeywordListComparisonExpr,
		},

		{
			name:     "fail keyword list",
			col:      keywordListCol,
			value:    "foo",
			mockFunc: (*MockStoreQueryConverterMockRecorder[sqlparser.Expr]).ConvertKeywordListComparisonExpr,
			mockErr:  errors.New("mock error"),
		},

		{
			name:     "success text",
			col:      textCol,
			value:    "foo",
			mockFunc: (*MockStoreQueryConverterMockRecorder[sqlparser.Expr]).ConvertTextComparisonExpr,
		},

		{
			name:     "fail text",
			col:      textCol,
			value:    "foo",
			mockFunc: (*MockStoreQueryConverterMockRecorder[sqlparser.Expr]).ConvertTextComparisonExpr,
			mockErr:  errors.New("mock error"),
		},

		{
			name:     "success datetime",
			col:      datetimeCol,
			value:    "2025-01-01T12:34:56Z",
			mockFunc: (*MockStoreQueryConverterMockRecorder[sqlparser.Expr]).ConvertComparisonExpr,
		},

		{
			name:     "fail datetime",
			col:      datetimeCol,
			value:    "2025-01-01T12:34:56Z",
			mockFunc: (*MockStoreQueryConverterMockRecorder[sqlparser.Expr]).ConvertComparisonExpr,
			mockErr:  errors.New("mock error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)
			storeQCMock.EXPECT().GetDatetimeFormat().Return(time.RFC3339Nano).AnyTimes()

			input := &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(tc.col.Alias)},
				Right:    sqlparser.NewStrVal([]byte(tc.value)),
			}
			output := &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     tc.col,
				Right:    NewUnsafeSQLString(tc.value),
			}
			tc.mockFunc(storeQCMock.EXPECT(), sqlparser.EqualStr, tc.col, tc.value).
				Return(output, tc.mockErr)
			out, err := queryConverter.convertComparisonExpr(input)
			if tc.mockErr != nil {
				r.Equal(tc.mockErr, err)
			} else {
				r.NoError(err)
				r.Equal(output, out)
			}
		})
	}
}

func TestQueryConverter_ConvertComparisonExprFail(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		in   string
		err  string
	}{
		{
			name: "invalid col name",
			in:   "InvalidField = 'foo'",
			err:  InvalidSearchAttribute,
		},

		{
			name: "invalid value",
			in:   "AliasForKeyword01 = InvalidValue",
			err:  NotSupportedErrMessage,
		},

		{
			name: "unsupported keyword operator",
			in:   "AliasForKeyword01 LIKE 'foo'",
			err: fmt.Sprintf(
				"%s: operator 'LIKE' not supported for Keyword type search attribute 'AliasForKeyword01'",
				NotSupportedErrMessage,
			),
		},

		{
			name: "unsupported keyword list operator",
			in:   "AliasForKeywordList01 LIKE 'foo'",
			err: fmt.Sprintf(
				"%s: operator 'LIKE' not supported for KeywordList type search attribute 'AliasForKeywordList01'",
				NotSupportedErrMessage,
			),
		},

		{
			name: "unsupported text operator",
			in:   "AliasForText01 LIKE 'foo'",
			err: fmt.Sprintf(
				"%s: operator 'LIKE' not supported for Text type search attribute 'AliasForText01'",
				NotSupportedErrMessage,
			),
		},

		{
			name: "unsupported int operator",
			in:   "AliasForInt01 LIKE 123",
			err: fmt.Sprintf(
				"%s: operator 'LIKE' not supported for Int type search attribute 'AliasForInt01'",
				NotSupportedErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)

			inExpr := parseWhereString(tc.in).(*sqlparser.ComparisonExpr)
			_, err := queryConverter.convertComparisonExpr(inExpr)
			r.Error(err)
			r.ErrorContains(err, tc.err)
			var expectedErr *ConverterError
			r.ErrorAs(err, &expectedErr)
		})
	}
}

func TestQueryConverter_ConvertRangeCond(t *testing.T) {
	t.Parallel()

	keywordCol := NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         string
		setupMocks func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr])
		out        sqlparser.Expr
		err        string
	}{
		{
			name: "success",
			in:   "AliasForKeyword01 BETWEEN '123' AND '456'",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				storeQCMock.EXPECT().
					ConvertRangeExpr(
						sqlparser.BetweenStr,
						keywordCol,
						"123",
						"456",
					).
					Return(
						&sqlparser.RangeCond{
							Operator: sqlparser.BetweenStr,
							Left:     keywordCol,
							From:     NewUnsafeSQLString("123"),
							To:       NewUnsafeSQLString("456"),
						},
						nil,
					)
			},
			out: &sqlparser.RangeCond{
				Operator: sqlparser.BetweenStr,
				Left:     keywordCol,
				From:     NewUnsafeSQLString("123"),
				To:       NewUnsafeSQLString("456"),
			},
		},

		{
			name: "fail invalid col name",
			in:   "InvalidField BETWEEN '123' AND '456'",
			err:  InvalidSearchAttribute,
		},

		{
			name: "fail unsupported type",
			in:   "AliasForText01 BETWEEN '123' AND '456'",
			err: fmt.Sprintf(
				"%s: cannot do range condition on search attribute 'AliasForText01' of type Text",
				InvalidExpressionErrMessage,
			),
		},

		{
			name: "fail invalid from value",
			in:   "AliasForKeyword01 BETWEEN InvalidValue AND '456'",
			err:  NotSupportedErrMessage,
		},

		{
			name: "fail invalid to value",
			in:   "AliasForKeyword01 BETWEEN '123' AND InvalidValue",
			err:  NotSupportedErrMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)

			if tc.setupMocks != nil {
				tc.setupMocks(storeQCMock)
			}
			inExpr := parseWhereString(tc.in).(*sqlparser.RangeCond)
			out, err := queryConverter.convertRangeCond(inExpr)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestQueryConverter_ConvertIsExpr(t *testing.T) {
	t.Parallel()

	keywordCol := NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         string
		setupMocks func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr])
		out        sqlparser.Expr
		err        string
	}{
		{
			name: "success is null",
			in:   "AliasForKeyword01 IS NULL",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNullStr, keywordCol).
					Return(
						&sqlparser.IsExpr{
							Operator: sqlparser.IsNullStr,
							Expr:     keywordCol,
						},
						nil,
					)
			},
			out: &sqlparser.IsExpr{
				Operator: sqlparser.IsNullStr,
				Expr:     keywordCol,
			},
		},

		{
			name: "success is not null",
			in:   "AliasForKeyword01 IS NOT NULL",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNotNullStr, keywordCol).
					Return(
						&sqlparser.IsExpr{
							Operator: sqlparser.IsNotNullStr,
							Expr:     keywordCol,
						},
						nil,
					)
			},
			out: &sqlparser.IsExpr{
				Operator: sqlparser.IsNotNullStr,
				Expr:     keywordCol,
			},
		},

		{
			name: "fail invalid col name",
			in:   "InvalidField IS NOT NULL",
			err:  InvalidSearchAttribute,
		},

		{
			name: "fail unsupported is true",
			in:   "AliasForKeyword01 IS TRUE",
			err: fmt.Sprintf(
				"%s: 'IS' operator can only be used as 'IS NULL' or 'IS NOT NULL'",
				InvalidExpressionErrMessage,
			),
		},

		{
			name: "fail unsupported is not true",
			in:   "AliasForKeyword01 IS NOT TRUE",
			err: fmt.Sprintf(
				"%s: 'IS' operator can only be used as 'IS NULL' or 'IS NOT NULL'",
				InvalidExpressionErrMessage,
			),
		},

		{
			name: "fail unsupported is false",
			in:   "AliasForKeyword01 IS FALSE",
			err: fmt.Sprintf(
				"%s: 'IS' operator can only be used as 'IS NULL' or 'IS NOT NULL'",
				InvalidExpressionErrMessage,
			),
		},

		{
			name: "fail unsupported is not false",
			in:   "AliasForKeyword01 IS NOT FALSE",
			err: fmt.Sprintf(
				"%s: 'IS' operator can only be used as 'IS NULL' or 'IS NOT NULL'",
				InvalidExpressionErrMessage,
			),
		},

		{
			name: "fail mock error",
			in:   "AliasForKeyword01 IS NULL",
			setupMocks: func(storeQCMock *MockStoreQueryConverter[sqlparser.Expr]) {
				storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNullStr, keywordCol).
					Return(nil, errors.New("mock error"))
			},
			err: "mock error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)

			if tc.setupMocks != nil {
				tc.setupMocks(storeQCMock)
			}
			inExpr := parseWhereString(tc.in).(*sqlparser.IsExpr)
			out, err := queryConverter.convertIsExpr(inExpr)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				if tc.err != "mock error" {
					var expectedErr *ConverterError
					r.ErrorAs(err, &expectedErr)
				}
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestQueryConverter_ConvertColName(t *testing.T) {
	t.Parallel()

	keywordCol := NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	statusCol := NewSAColumn(
		sadefs.ExecutionStatus,
		sadefs.ExecutionStatus,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name string
		in   sqlparser.Expr
		out  *SAColumn
		err  string
	}{
		{
			name: "success",
			in: &sqlparser.ColName{
				Name: sqlparser.NewColIdent(sadefs.ExecutionStatus),
			},
			out: statusCol,
		},

		{
			name: "success custom search attribute",
			in: &sqlparser.ColName{
				Name: sqlparser.NewColIdent("AliasForKeyword01"),
			},
			out: keywordCol,
		},

		{
			name: "success backticks",
			in: &sqlparser.ColName{
				Name: sqlparser.NewColIdent("`AliasForKeyword01`"),
			},
			out: keywordCol,
		},

		{
			name: "success TemporalNamespaceDivision",
			in: &sqlparser.ColName{
				Name: sqlparser.NewColIdent("TemporalNamespaceDivision"),
			},
			out: NamespaceDivisionSAColumn(),
		},

		{
			name: "fail not column",
			in:   sqlparser.BoolVal(true),
			err: fmt.Sprintf(
				"%s: must be a column name but was sqlparser.BoolVal",
				InvalidExpressionErrMessage,
			),
		},

		{
			name: "unknown search attribute",
			in: &sqlparser.ColName{
				Name: sqlparser.NewColIdent("InvalidField"),
			},
			err: fmt.Sprintf("%s: InvalidField", InvalidSearchAttribute),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)

			out, err := queryConverter.convertColName(tc.in)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
				if tc.out.FieldName == sadefs.TemporalNamespaceDivision {
					r.True(queryConverter.seenNamespaceDivision)
				} else {
					r.False(queryConverter.seenNamespaceDivision)
				}
			}
		})
	}
}

func TestQueryConverter_ParseValueExpr(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		expr   sqlparser.Expr
		alias  string
		field  string
		saType enumspb.IndexedValueType
		out    any
		err    string
	}{
		{
			name:   "success SQLVal",
			expr:   sqlparser.NewStrVal([]byte("foo")),
			alias:  sadefs.WorkflowType,
			field:  sadefs.WorkflowType,
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			out:    "foo",
		},

		{
			name:   "success bool",
			expr:   sqlparser.BoolVal(true),
			alias:  "AliasForBool01",
			field:  "Bool01",
			saType: enumspb.INDEXED_VALUE_TYPE_BOOL,
			out:    true,
		},

		{
			name: "success tuple",
			expr: sqlparser.ValTuple{
				sqlparser.NewStrVal([]byte("foo")),
				sqlparser.NewStrVal([]byte("bar")),
			},
			alias:  "AliasForKeywordList01",
			field:  "KeywordList01",
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
			out:    []any{"foo", "bar"},
		},

		{
			name:   "fail SQLVal",
			expr:   sqlparser.NewStrVal([]byte("foo")),
			alias:  sadefs.WorkflowType,
			field:  sadefs.WorkflowType,
			saType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
			err:    InvalidExpressionErrMessage,
		},

		{
			name: "fail tuple invalid value",
			expr: sqlparser.ValTuple{
				sqlparser.NewStrVal([]byte("foo")),
				&sqlparser.ColName{Name: sqlparser.NewColIdent("InvalidField")},
			},
			alias:  "AliasForKeywordList01",
			field:  "KeywordList01",
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
			err:    NotSupportedErrMessage,
		},

		{
			name: "fail group concat",
			expr: &sqlparser.GroupConcatExpr{
				Exprs: sqlparser.SelectExprs{
					&sqlparser.StarExpr{},
				},
			},
			alias:  "AliasForKeyword01",
			field:  "Keyword01",
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			err:    NotSupportedErrMessage,
		},

		{
			name: "fail func",
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("coalesce"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.StarExpr{},
				},
			},
			alias:  "AliasForKeyword01",
			field:  "Keyword01",
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			err:    NotSupportedErrMessage,
		},

		{
			name:   "fail ColName",
			expr:   &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeyword01")},
			alias:  "AliasForKeyword01",
			field:  "Keyword01",
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			err:    NotSupportedErrMessage,
		},

		{
			name:   "invalid value type",
			expr:   &sqlparser.NotExpr{},
			alias:  "AliasForKeyword01",
			field:  "Keyword01",
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			err: fmt.Sprintf(
				"%s: unexpected value type *sqlparser.NotExpr",
				InvalidExpressionErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)

			out, err := queryConverter.parseValueExpr(tc.expr, tc.alias, tc.field, tc.saType)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestQueryConverter_ParseSQLVal(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		expr        *sqlparser.SQLVal
		saName      string
		saFieldName string // Field name for special handling (e.g., ExecutionStatus, ExecutionDuration)
		saType      enumspb.IndexedValueType
		out         any
		err         string
	}{
		{
			name:        "success string",
			expr:        sqlparser.NewStrVal([]byte("foo")),
			saName:      "AliasForKeyword01",
			saFieldName: "Keyword01",
			saType:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			out:         "foo",
		},

		{
			name:        "success int",
			expr:        sqlparser.NewIntVal([]byte("123")),
			saName:      "AliasForInt01",
			saFieldName: "Int01",
			saType:      enumspb.INDEXED_VALUE_TYPE_INT,
			out:         int64(123),
		},

		{
			name:        "success float",
			expr:        sqlparser.NewFloatVal([]byte("123.456")),
			saName:      "AliasForDouble01",
			saFieldName: "Double01",
			saType:      enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			out:         123.456,
		},

		{
			name:        "fail parse value",
			expr:        sqlparser.NewFloatVal([]byte("123.456.789")),
			saName:      "AliasForDouble01",
			saFieldName: "Double01",
			saType:      enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			err: fmt.Sprintf(
				"%s: unable to parse value \"123.456.789\"",
				InvalidExpressionErrMessage,
			),
		},

		{
			name:        "success ExecutionStatus (system)",
			expr:        sqlparser.NewStrVal([]byte("Running")),
			saName:      sadefs.ExecutionStatus,
			saFieldName: sadefs.ExecutionStatus, // System ExecutionStatus uses field name = alias
			saType:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			out:         "Running",
		},

		{
			name:        "fail ExecutionStatus (system)",
			expr:        sqlparser.NewStrVal([]byte("Invalid")),
			saName:      sadefs.ExecutionStatus,
			saFieldName: sadefs.ExecutionStatus, // System ExecutionStatus uses field name = alias
			saType:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			err:         InvalidExpressionErrMessage,
		},

		{
			name:        "success ExecutionStatus alias (CHASM)",
			expr:        sqlparser.NewStrVal([]byte("CustomStatus")),
			saName:      sadefs.ExecutionStatus,
			saFieldName: "TemporalLowCardinalityKeyword01", // CHASM alias maps to different field
			saType:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			out:         "CustomStatus", // No validation against enum, just returns the string
		},

		{
			name:        "success ExecutionDuration",
			expr:        sqlparser.NewStrVal([]byte("1m")),
			saName:      sadefs.ExecutionDuration,
			saFieldName: sadefs.ExecutionDuration,
			saType:      enumspb.INDEXED_VALUE_TYPE_INT,
			out:         int64(1 * time.Minute),
		},

		{
			name:        "fail ExecutionDuration",
			expr:        sqlparser.NewStrVal([]byte("1t")),
			saName:      sadefs.ExecutionDuration,
			saFieldName: sadefs.ExecutionDuration,
			saType:      enumspb.INDEXED_VALUE_TYPE_INT,
			err:         InvalidExpressionErrMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)
			storeQCMock.EXPECT().GetDatetimeFormat().Return(time.RFC3339Nano).AnyTimes()

			out, err := queryConverter.parseSQLVal(tc.expr, tc.saName, tc.saFieldName, tc.saType)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestQueryConverter_ValidateValueType(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		saName string
		saType enumspb.IndexedValueType
		value  any
		out    any
		err    string
	}{
		{
			name:   "success int",
			saName: "AliasForInt01",
			saType: enumspb.INDEXED_VALUE_TYPE_INT,
			value:  int64(123),
			out:    int64(123),
		},

		{
			name:   "success int cmp float",
			saName: "AliasForInt01",
			saType: enumspb.INDEXED_VALUE_TYPE_INT,
			value:  123.456,
			out:    123.456,
		},

		{
			name:   "fail int",
			saName: "AliasForInt01",
			saType: enumspb.INDEXED_VALUE_TYPE_INT,
			value:  "foo",
			err: fmt.Sprintf(
				"%s: invalid value type for search attribute AliasForInt01 of type Int: \"foo\" (type: string)",
				InvalidExpressionErrMessage,
			),
		},

		{
			name:   "success double",
			saName: "AliasForDouble01",
			saType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			value:  123.456,
			out:    123.456,
		},

		{
			name:   "success double cmp int",
			saName: "AliasForDouble01",
			saType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			value:  int64(123),
			out:    int64(123),
		},

		{
			name:   "fail double",
			saName: "AliasForDouble01",
			saType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			value:  "foo",
			err: fmt.Sprintf(
				"%s: invalid value type for search attribute AliasForDouble01 of type Double: \"foo\" (type: string)",
				InvalidExpressionErrMessage,
			),
		},

		{
			name:   "success bool",
			saName: "AliasForBool01",
			saType: enumspb.INDEXED_VALUE_TYPE_BOOL,
			value:  true,
			out:    true,
		},

		{
			name:   "fail bool",
			saName: "AliasForBool01",
			saType: enumspb.INDEXED_VALUE_TYPE_BOOL,
			value:  "foo",
			err: fmt.Sprintf(
				"%s: invalid value type for search attribute AliasForBool01 of type Bool: \"foo\" (type: string)",
				InvalidExpressionErrMessage,
			),
		},

		{
			name:   "success datetime nanoseconds",
			saName: "AliasForDatetime01",
			saType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
			value:  int64(1735734896000000000),
			out:    "2025-01-01T12:34:56Z",
		},

		{
			name:   "success datetime string",
			saName: "AliasForDatetime01",
			saType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
			value:  "2025-01-01T12:34:56Z",
			out:    "2025-01-01T12:34:56Z",
		},

		{
			name:   "fail parse datetime",
			saName: "AliasForDatetime01",
			saType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
			value:  "2025-01-01 12:34:56Z",
			err: fmt.Sprintf(
				"%s: unable to parse datetime '2025-01-01 12:34:56Z'",
				InvalidExpressionErrMessage,
			),
		},

		{
			name:   "fail datetime invalid value type",
			saName: "AliasForDatetime01",
			saType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
			value:  123.456,
			err: fmt.Sprintf(
				"%s: invalid value type for search attribute AliasForDatetime01 of type Datetime: 123.456 (type: float64)",
				InvalidExpressionErrMessage,
			),
		},

		{
			name:   "success keyword",
			saName: "AliasForKeyword01",
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			value:  "foo",
			out:    "foo",
		},

		{
			name:   "fail keyword",
			saName: "AliasForKeyword01",
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			value:  int64(123),
			err: fmt.Sprintf(
				"%s: invalid value type for search attribute AliasForKeyword01 of type Keyword: 123 (type: int64)",
				InvalidExpressionErrMessage,
			),
		},

		{
			name:   "success keyword list",
			saName: "AliasForKeywordList01",
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
			value:  "foo",
			out:    "foo",
		},

		{
			name:   "fail keyword list",
			saName: "AliasForKeywordList01",
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
			value:  int64(123),
			err: fmt.Sprintf(
				"%s: invalid value type for search attribute AliasForKeywordList01 of type KeywordList: 123 (type: int64)",
				InvalidExpressionErrMessage,
			),
		},

		{
			name:   "success text",
			saName: "AliasForText01",
			saType: enumspb.INDEXED_VALUE_TYPE_TEXT,
			value:  "foo",
			out:    "foo",
		},

		{
			name:   "fail keyword",
			saName: "AliasForText01",
			saType: enumspb.INDEXED_VALUE_TYPE_TEXT,
			value:  int64(123),
			err: fmt.Sprintf(
				"%s: invalid value type for search attribute AliasForText01 of type Text: 123 (type: int64)",
				InvalidExpressionErrMessage,
			),
		},

		{
			name:   "fail unknown search attribute type",
			saName: "AliasForKeyword01",
			saType: enumspb.IndexedValueType(999),
			err: fmt.Sprintf(
				"%s: unknown search attribute type 999 for AliasForKeyword01",
				InvalidExpressionErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := newTestQueryConverter(storeQCMock)
			storeQCMock.EXPECT().GetDatetimeFormat().Return(time.RFC3339Nano).AnyTimes()

			out, err := queryConverter.validateValueType(tc.saName, tc.saType, tc.value)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestParseExecutionStatusValue(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		value any
		out   string
		err   string
	}{
		{
			name:  "success int",
			value: int64(1),
			out:   "Running",
		},
		{
			name:  "invalid int",
			value: int64(999),
			err: fmt.Sprintf(
				"%s: invalid ExecutionStatus value 999",
				InvalidExpressionErrMessage,
			),
		},
		{
			name:  "success string",
			value: "Running",
			out:   "Running",
		},
		{
			name:  "invalid string",
			value: "Invalid",
			err: fmt.Sprintf(
				"%s: invalid ExecutionStatus value 'Invalid'",
				InvalidExpressionErrMessage,
			),
		},
		{
			name:  "invalid type",
			value: 1,
			err: fmt.Sprintf(
				"%s: unexpected value type int for search attribute ExecutionStatus",
				InvalidExpressionErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			out, err := parseExecutionStatusValue(tc.value)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.out, out)
			}
		})
	}
}

func TestParseExecutionDurationValue(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		value any
		out   int64
		err   string
	}{
		{
			name:  "success int",
			value: int64(123),
			out:   int64(123),
		},
		{
			name:  "success string",
			value: "123m",
			out:   int64(123 * time.Minute),
		},
		{
			name:  "invalid string",
			value: "1t",
			err: fmt.Sprintf(
				"%s: invalid duration value for search attribute ExecutionDuration: 1t",
				InvalidExpressionErrMessage,
			),
		},
		{
			name:  "invalid type",
			value: 1,
			err: fmt.Sprintf(
				"%s: unexpected value type int for search attribute ExecutionDuration",
				InvalidExpressionErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			out, err := parseExecutionDurationValue(tc.value)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
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

func TestQueryConverter_WithChasmMapper(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	ctrl := gomock.NewController(t)
	storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)

	chasmMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalKeyword01": "ChasmStatus",
		},
		map[string]enumspb.IndexedValueType{
			"TemporalKeyword01": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	)

	c := newTestQueryConverter(storeQCMock)
	r.Nil(c.chasmMapper)

	c = c.WithChasmMapper(chasmMapper)
	r.Equal(chasmMapper, c.chasmMapper)

	c = c.WithChasmMapper(nil)
	r.Nil(c.chasmMapper)
}

func TestQueryConverter_WithArchetypeID(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	ctrl := gomock.NewController(t)
	storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)

	c := newTestQueryConverter(storeQCMock)
	r.Equal(chasm.UnspecifiedArchetypeID, c.archetypeID)

	c = c.WithArchetypeID(123)
	r.Equal(chasm.ArchetypeID(123), c.archetypeID)

	c = c.WithArchetypeID(chasm.UnspecifiedArchetypeID)
	r.Equal(chasm.UnspecifiedArchetypeID, c.archetypeID)
}

func TestQueryConverter_TemporalSystemExecutionStatus(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)

	// Test that TemporalSystemExecutionStatus maps to ExecutionStatus only for SchedulerArchetypeID
	t.Run("with SchedulerArchetypeID", func(t *testing.T) {
		r := require.New(t)
		queryConverter := newTestQueryConverter(storeQCMock).
			WithArchetypeID(chasm.SchedulerArchetypeID)

		in := &sqlparser.ColName{
			Name: sqlparser.NewColIdent("TemporalSystemExecutionStatus"),
		}
		out, err := queryConverter.convertColName(in)
		r.NoError(err)
		r.Equal(NewSAColumn(
			"TemporalSystemExecutionStatus",
			sadefs.ExecutionStatus,
			enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		), out)
	})

	t.Run("without SchedulerArchetypeID", func(t *testing.T) {
		r := require.New(t)
		queryConverter := newTestQueryConverter(storeQCMock)

		in := &sqlparser.ColName{
			Name: sqlparser.NewColIdent("TemporalSystemExecutionStatus"),
		}
		_, err := queryConverter.convertColName(in)
		r.Error(err)
		r.ErrorContains(err, InvalidSearchAttribute)
	})
}

func TestQueryConverter_CapturePanic(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	ctrl := gomock.NewController(t)
	metricsHandlerMock := metrics.NewMockHandler(ctrl)
	loggerMock := log.NewMockLogger(ctrl)
	storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
	queryConverter := newTestQueryConverter(storeQCMock)
	queryConverter.metricsHandler = metricsHandlerMock
	queryConverter.logger = loggerMock

	keywordCol := NewSAColumn(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	counterMock := metrics.NewMockCounterIface(ctrl)
	counterMock.EXPECT().Record(int64(1))
	metricsHandlerMock.EXPECT().Counter(metrics.ServicePanic.Name()).Return(counterMock)
	loggerMock.EXPECT().Error("Panic is captured", gomock.Any(), gomock.Any()).Return()
	storeQCMock.EXPECT().ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
		DoAndReturn(
			func(operator string, col *SAColumn, value any) (sqlparser.Expr, error) {
				panic("random")
			},
		)
	out, err := queryConverter.Convert("AliasForKeyword01 = 'foo'")
	r.ErrorContains(err, "panic: random")
	r.Nil(out)
}

func TestQueryConverter_FieldNameFilterMetric(t *testing.T) {
	t.Parallel()

	// Maps the CHASM alias "ChasmKeyword" to the field name "TemporalKeyword01".
	chasmMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalKeyword01": "ChasmKeyword",
		},
		map[string]enumspb.IndexedValueType{
			"TemporalKeyword01": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	)

	// Models chasm.WithBusinessIDAlias, which every archetype with a visibility component must
	// register: it puts the WorkflowId *system* field into the CHASM type map, keyed by field.
	chasmBusinessIDMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			sadefs.WorkflowID: "ScheduleId",
		},
		map[string]enumspb.IndexedValueType{
			sadefs.WorkflowID: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	)

	testCases := []struct {
		name string
		// query is converted with the nil store query converter, so only the search attribute
		// resolution (and thus the metric emission) is exercised.
		query string
		// chasmMapper, when set, is installed in the query converter.
		chasmMapper *chasm.VisibilitySearchAttributesMapper
		// customSAs overrides the namespace's custom search attributes. Defaults to
		// searchattribute.TestNameTypeMap(), whose field names are the preallocated SQL ones
		// (Keyword01, Int01, ...).
		customSAs map[string]enumspb.IndexedValueType
		// saMapper overrides the search attribute mapper. Defaults to searchattribute.TestMapper.
		saMapper searchattribute.Mapper
		// count is the expected number of times the field name counter was recorded. It's
		// recorded at most once per query, and only if the query is converted successfully.
		count int
		err   string
	}{
		{
			// A custom search attribute referenced by its alias is the expected usage.
			name:  "alias of custom search attribute",
			query: "AliasForKeyword01 = 'foo'",
			count: 0,
		},
		{
			// A custom search attribute referenced by its field name is what the metric counts.
			name:  "field name of custom search attribute",
			query: "Keyword01 = 'foo'",
			count: 1,
		},
		{
			// System search attributes aren't mappable: alias and field name are always equal.
			name:  "system search attribute",
			query: "WorkflowId = 'foo'",
			count: 0,
		},
		{
			// Predefined search attributes aren't mappable either.
			name:  "predefined search attribute",
			query: "TemporalNamespaceDivision = 'foo'",
			count: 0,
		},
		{
			name:  "predefined search attribute keyword list",
			query: "TemporalChangeVersion = 'foo'",
			count: 0,
		},
		{
			// ScheduleId resolves to the WorkflowId field, so alias != field name.
			name:  "special alias",
			query: "ScheduleId = 'foo'",
			count: 0,
		},
		{
			name:        "alias of CHASM search attribute",
			query:       "ChasmKeyword = 'foo'",
			chasmMapper: chasmMapper,
			count:       0,
		},
		{
			// Backticks are stripped before the alias is resolved.
			name:  "field name with backticks",
			query: "`Keyword01` = 'foo'",
			count: 1,
		},
		{
			name:  "field name in range condition",
			query: "Int01 between 1 and 2",
			count: 1,
		},
		{
			name:  "field name in is expression",
			query: "Keyword01 is null",
			count: 1,
		},
		{
			name:  "field name in order by",
			query: "order by Keyword01",
			count: 1,
		},
		{
			name:  "alias in order by",
			query: "order by AliasForKeyword01",
			count: 0,
		},
		{
			// The counter is recorded once per query, regardless of the number of occurrences.
			name:  "repeated field name",
			query: "Keyword01 = 'foo' or Keyword01 = 'bar'",
			count: 1,
		},
		{
			// Multiple distinct field names are still recorded once per query.
			name:  "mixed aliases and field names",
			query: "Keyword01 = 'foo' and AliasForInt01 > 1 and Double01 < 1.5",
			count: 1,
		},
		{
			name:  "field names in filter and order by",
			query: "Keyword01 = 'foo' order by Int01",
			count: 1,
		},
		{
			// The whole query is rejected, so nothing is counted.
			name:  "unknown search attribute",
			query: "InvalidField = 'foo'",
			count: 0,
			err:   InvalidSearchAttribute,
		},
		{
			// The field name is resolved before the error, but the query is rejected, so
			// nothing is counted.
			name:  "field name with unknown search attribute",
			query: "Keyword01 = 'foo' and InvalidField = 'bar'",
			count: 0,
			err:   InvalidSearchAttribute,
		},
		{
			// convertColName is not reached for the right hand side of a comparison.
			name:  "field name on right hand side",
			query: "AliasForKeyword01 = Keyword01",
			count: 0,
			err:   NotSupportedErrMessage,
		},

		// Preallocated SQL field names of every type are counted. The name must match the shape
		// for the *resolved* type, so these pin the type-to-prefix pairing.
		{
			name:  "preallocated bool field name",
			query: "Bool01 = true",
			count: 1,
		},
		{
			name:  "preallocated int field name",
			query: "Int01 = 1",
			count: 1,
		},
		{
			name:  "preallocated keyword list field name",
			query: "KeywordList01 = 'foo'",
			count: 1,
		},
		{
			name:  "preallocated text field name",
			query: "Text01 = 'foo'",
			count: 1,
		},
		{
			// IS NULL avoids parsing a datetime value, which the nil store converter can't format.
			name:  "preallocated datetime field name",
			query: "Datetime01 is null",
			count: 1,
		},
		{
			// A namespace may register a custom search attribute whose *alias* happens to look
			// like a preallocated field name. The shape is checked against the resolved type, so
			// a mismatched type is not counted.
			name:      "preallocated shape with mismatched type",
			query:     "Keyword01 = 'foo'",
			customSAs: map[string]enumspb.IndexedValueType{"Keyword01": enumspb.INDEXED_VALUE_TYPE_TEXT},
			count:     0,
		},

		// A custom search attribute that legitimately maps to itself (the Elasticsearch layout,
		// and the back-compat mapper's pass-through behaviour) has alias == field name but is not
		// a physical preallocated column, so it must not be counted.
		{
			name:      "self-mapped custom search attribute",
			query:     "CustomKeywordField = 'foo'",
			customSAs: map[string]enumspb.IndexedValueType{"CustomKeywordField": enumspb.INDEXED_VALUE_TYPE_KEYWORD},
			count:     0,
		},
		{
			// TestMapper.GetFieldName returns "pass-through" unchanged, like the back-compat
			// mapper does for legacy attributes.
			name:      "pass-through mapper",
			query:     "`pass-through` = 'foo'",
			customSAs: map[string]enumspb.IndexedValueType{"pass-through": enumspb.INDEXED_VALUE_TYPE_KEYWORD},
			count:     0,
		},

		// A namespace may register an alias that is identical to the preallocated field name it is
		// mapped to. Querying it is a legitimate use of the alias, so it must not be counted.
		{
			name:     "alias identical to its preallocated field name",
			query:    "Keyword01 = 'foo'",
			saMapper: aliasToFieldMapper{"Keyword01": "Keyword01"},
			count:    0,
		},
		{
			// Same alias as above, but the query uses another field name directly.
			name:     "field name alongside alias identical to field name",
			query:    "Keyword01 = 'foo' and Keyword02 = 'bar'",
			saMapper: aliasToFieldMapper{"Keyword01": "Keyword01"},
			count:    1,
		},
		{
			// Field name that is the alias of another field: resolves as the alias.
			name:     "alias identical to another preallocated field name",
			query:    "Keyword01 = 'foo'",
			saMapper: aliasToFieldMapper{"Keyword01": "Keyword02"},
			count:    0,
		},

		// A raw CHASM field name resolves by stripping the Temporal prefix, so alias != field
		// name; it is recognised via the CHASM mapper's type map instead.
		{
			name:        "field name of CHASM search attribute",
			query:       "TemporalKeyword01 = 'foo'",
			chasmMapper: chasmMapper,
			count:       1,
		},
		{
			// Without a CHASM mapper the same name is just the Temporal-prefixed spelling of the
			// Keyword01 alias, which the resolver accepts for ordinary workflow queries.
			name:  "Temporal prefixed name without CHASM mapper",
			query: "TemporalKeyword01 = 'foo'",
			count: 0,
		},
		{
			name:        "business ID alias of CHASM search attribute",
			query:       "ScheduleId = 'foo'",
			chasmMapper: chasmBusinessIDMapper,
			count:       0,
		},
		{
			// WithBusinessIDAlias registers WorkflowId as a CHASM *field*, so it is in the CHASM
			// type map. It is still a system search attribute that any caller may query by name,
			// so it must not be counted.
			name:        "system search attribute backing a business ID alias",
			query:       "WorkflowId = 'foo'",
			chasmMapper: chasmBusinessIDMapper,
			count:       0,
		},
		{
			// The system guard is keyed on the queried name, so a CHASM field remains counted
			// even when the same mapper also registers a system field.
			name:  "CHASM field name alongside a business ID alias",
			query: "TemporalKeyword01 = 'foo'",
			chasmMapper: chasm.NewTestVisibilitySearchAttributesMapper(
				map[string]string{
					sadefs.WorkflowID:   "ScheduleId",
					"TemporalKeyword01": "ChasmKeyword",
				},
				map[string]enumspb.IndexedValueType{
					sadefs.WorkflowID:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					"TemporalKeyword01": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			),
			count: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := require.New(t)
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)

			saTypeMap := searchattribute.TestNameTypeMap()
			if tc.customSAs != nil {
				saTypeMap = searchattribute.NewNameTypeMapStub(tc.customSAs)
			}
			var saMapper searchattribute.Mapper = &searchattribute.TestMapper{}
			if tc.saMapper != nil {
				saMapper = tc.saMapper
			}
			queryConverter := NewNilQueryConverter(
				testNamespaceName,
				saTypeMap,
				saMapper,
				metricsHandler,
				log.NewNoopLogger(),
			)
			if tc.chasmMapper != nil {
				queryConverter = queryConverter.WithChasmMapper(tc.chasmMapper)
			}

			_, err := queryConverter.Convert(tc.query)
			if tc.err != "" {
				r.ErrorContains(err, tc.err)
			} else {
				r.NoError(err)
			}

			recordings := capture.SnapshotMetric(fieldNameFilterAccepted.Name())
			r.Len(recordings, tc.count)
			for _, rec := range recordings {
				r.Equal(int64(1), rec.Value)
				r.Equal(testNamespaceName.String(), rec.Tags["namespace"])
			}
		})
	}
}

func TestQueryConverter_FieldNameFilterMetricGroupBy(t *testing.T) {
	t.Parallel()

	// GROUP BY is restricted to an allowlist of field names. The field name is flagged while
	// resolving the column name, before that restriction is applied, but the counter is only
	// recorded if the whole query is converted successfully.
	testCases := []struct {
		name  string
		query string
		count int
		err   string
	}{
		{
			name:  "allowed system field",
			query: "group by ExecutionStatus",
			count: 0,
		},
		{
			name:  "allowed predefined field",
			query: "group by TemporalNamespaceDivision",
			count: 0,
		},
		{
			name:  "disallowed custom field name",
			query: "group by Keyword01",
			count: 0,
			err:   NotSupportedErrMessage,
		},
		{
			name:  "field name in filter with allowed group by",
			query: "Keyword01 = 'foo' group by ExecutionStatus",
			count: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := require.New(t)
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)

			queryConverter := NewNilQueryConverter(
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
				metricsHandler,
				log.NewNoopLogger(),
			)

			_, err := queryConverter.Convert(tc.query)
			if tc.err != "" {
				r.ErrorContains(err, tc.err)
			} else {
				r.NoError(err)
			}

			r.Len(capture.SnapshotMetric(fieldNameFilterAccepted.Name()), tc.count)
		})
	}
}

// aliasToFieldMapper mimics the namespace custom search attributes mapper: it only resolves the
// registered aliases, and returns an error for anything else.
type aliasToFieldMapper map[string]string

func (m aliasToFieldMapper) GetAlias(fieldName string, _ string) (string, error) {
	for alias, fn := range m {
		if fn == fieldName {
			return alias, nil
		}
	}
	return "", serviceerror.NewInvalidArgument("no alias for field name")
}

func (m aliasToFieldMapper) GetFieldName(alias string, _ string) (string, error) {
	if fn, ok := m[alias]; ok {
		return fn, nil
	}
	return "", serviceerror.NewInvalidArgument("no mapping for alias")
}

func newTestQueryConverter(
	storeQC StoreQueryConverter[sqlparser.Expr],
) *QueryConverter[sqlparser.Expr] {
	return NewQueryConverter(
		storeQC,
		testNamespaceName,
		searchattribute.TestNameTypeMap(),
		&searchattribute.TestMapper{},
		metrics.NoopMetricsHandler,
		log.NewNoopLogger(),
	)
}
