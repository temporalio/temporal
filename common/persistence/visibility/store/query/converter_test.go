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
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.uber.org/mock/gomock"
)

const (
	testNamespaceName = namespace.Name("test-namespace")
	testNamespaceID   = namespace.ID("test-namespace-id")
)

func TestWithSearchAttributeInterceptor(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	ctrl := gomock.NewController(t)
	storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)

	// QueryConverter without explicit SearchAttributeInterceptor sets the nop interceptor.
	c := NewQueryConverter(
		storeQCMock,
		testNamespaceName,
		searchattribute.TestNameTypeMap(),
		&searchattribute.TestMapper{},
	)
	r.Equal(nopSearchAttributeInterceptor, c.saInterceptor)

	// Setting nil interceptor sets the nop interceptor.
	c = NewQueryConverter(
		storeQCMock,
		testNamespaceName,
		searchattribute.TestNameTypeMap(),
		&searchattribute.TestMapper{},
	).WithSearchAttributeInterceptor(nil)
	r.Equal(nopSearchAttributeInterceptor, c.saInterceptor)

	// Setting non-nil interceptor
	i := &testSearchAttributeInterceptor{}
	c = NewQueryConverter(
		storeQCMock,
		testNamespaceName,
		searchattribute.TestNameTypeMap(),
		&searchattribute.TestMapper{},
	).WithSearchAttributeInterceptor(i)
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
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)

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
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)

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
			name: "fail not supported group by field",
			in:   "select * from t group by RunId",
			err: fmt.Sprintf(
				"%s: 'GROUP BY' clause is only supported for ExecutionStatus",
				NotSupportedErrMessage,
			),
		},

		{
			name: "fail invalid group by field",
			in:   "select * from t group by InvalidField",
			err:  InvalidExpressionErrMessage,
		},

		{
			name: "fail invalid order by field",
			in:   "select * from t order by InvalidField",
			err:  InvalidExpressionErrMessage,
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
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)

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
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)

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
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)

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
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)

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
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)

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
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)

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
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)
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
			err:  InvalidExpressionErrMessage,
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
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)

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
			err:  InvalidExpressionErrMessage,
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
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)

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
			err:  InvalidExpressionErrMessage,
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
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)

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
			name: "success special ScheduleID",
			in: &sqlparser.ColName{
				Name: sqlparser.NewColIdent(sadefs.ScheduleID),
			},
			out: NewSAColumn(
				sadefs.ScheduleID,
				sadefs.WorkflowID,
				enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			),
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
			err: fmt.Sprintf(
				"%s: column name 'InvalidField' is not a valid search attribute",
				InvalidExpressionErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)

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

func TestQueryConverter_ResolveSearchAttributeAlias(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                 string
		in                   string
		withCustomScheduleID bool
		useNoopMapper        bool
		outFn                string
		outFt                enumspb.IndexedValueType
		err                  string
	}{
		{
			name:  "success system StartTime",
			in:    "StartTime",
			outFn: "StartTime",
			outFt: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},

		{
			name:  "success system WorkflowId",
			in:    "WorkflowId",
			outFn: "WorkflowId",
			outFt: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:  "success reserved BuildIds",
			in:    "BuildIds",
			outFn: "BuildIds",
			outFt: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},

		{
			name:  "success reserved TemporalBuildIds",
			in:    "TemporalBuildIds",
			outFn: "BuildIds",
			outFt: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},

		{
			name:  "success reserved TemporalWorkerDeployment",
			in:    "TemporalWorkerDeployment",
			outFn: "TemporalWorkerDeployment",
			outFt: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:  "success reserved WorkerDeployment",
			in:    "WorkerDeployment",
			outFn: "TemporalWorkerDeployment",
			outFt: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:  "success custom AliasForInt01",
			in:    "AliasForInt01",
			outFn: "Int01",
			outFt: enumspb.INDEXED_VALUE_TYPE_INT,
		},

		{
			name:          "success custom noop mapper Int01",
			in:            "Int01",
			useNoopMapper: true,
			outFn:         "Int01",
			outFt:         enumspb.INDEXED_VALUE_TYPE_INT,
		},

		{
			name:  "success special ScheduleId",
			in:    "ScheduleId",
			outFn: "WorkflowId",
			outFt: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:  "success special TemporalScheduleId",
			in:    "TemporalScheduleId",
			outFn: "WorkflowId",
			outFt: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:                 "success custom ScheduleId",
			in:                   "ScheduleId",
			withCustomScheduleID: true,
			outFn:                searchattribute.TestScheduleIDFieldName,
			outFt:                enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:                 "success custom ScheduleId reserved TemporalScheduleId",
			in:                   "TemporalScheduleId",
			withCustomScheduleID: true,
			outFn:                "WorkflowId",
			outFt:                enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:                 "success noop mapper ScheduleId",
			in:                   "ScheduleId",
			withCustomScheduleID: false,
			outFn:                "WorkflowId",
			outFt:                enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:                 "success noop mapper TemporalScheduleId",
			in:                   "TemporalScheduleId",
			withCustomScheduleID: false,
			outFn:                "WorkflowId",
			outFt:                enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name: "invalid search attribute",
			in:   "Foo",
			err: fmt.Sprintf(
				"%s: column name 'Foo' is not a valid search attribute",
				InvalidExpressionErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{
					WithCustomScheduleID: tc.withCustomScheduleID,
				},
			)

			if tc.useNoopMapper {
				queryConverter.saMapper = searchattribute.NewNoopMapper()
			}

			fn, ft, err := queryConverter.resolveSearchAttributeAlias(tc.in)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
			} else {
				r.NoError(err)
				r.Equal(tc.outFn, fn)
				r.Equal(tc.outFt, ft)
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
			name:   "success special ScheduleID",
			expr:   sqlparser.NewStrVal([]byte("foo")),
			alias:  sadefs.ScheduleID,
			field:  sadefs.WorkflowID,
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			out:    primitives.ScheduleWorkflowIDPrefix + "foo",
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
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)

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
		name   string
		expr   *sqlparser.SQLVal
		saName string
		saType enumspb.IndexedValueType
		out    any
		err    string
	}{
		{
			name:   "success string",
			expr:   sqlparser.NewStrVal([]byte("foo")),
			saName: "AliasForKeyword01",
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			out:    "foo",
		},

		{
			name:   "success int",
			expr:   sqlparser.NewIntVal([]byte("123")),
			saName: "AliasForInt01",
			saType: enumspb.INDEXED_VALUE_TYPE_INT,
			out:    int64(123),
		},

		{
			name:   "success float",
			expr:   sqlparser.NewFloatVal([]byte("123.456")),
			saName: "AliasForDouble01",
			saType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			out:    123.456,
		},

		{
			name:   "fail parse value",
			expr:   sqlparser.NewFloatVal([]byte("123.456.789")),
			saName: "AliasForDouble01",
			saType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			err: fmt.Sprintf(
				"%s: unable to parse value \"123.456.789\"",
				InvalidExpressionErrMessage,
			),
		},

		{
			name:   "success ExecutionStatus",
			expr:   sqlparser.NewStrVal([]byte("Running")),
			saName: sadefs.ExecutionStatus,
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			out:    "Running",
		},

		{
			name:   "fail ExecutionStatus",
			expr:   sqlparser.NewStrVal([]byte("Invalid")),
			saName: sadefs.ExecutionStatus,
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			err:    InvalidExpressionErrMessage,
		},

		{
			name:   "success ExecutionDuration",
			expr:   sqlparser.NewStrVal([]byte("1m")),
			saName: sadefs.ExecutionDuration,
			saType: enumspb.INDEXED_VALUE_TYPE_INT,
			out:    int64(1 * time.Minute),
		},

		{
			name:   "fail ExecutionDuration",
			expr:   sqlparser.NewStrVal([]byte("1t")),
			saName: sadefs.ExecutionDuration,
			saType: enumspb.INDEXED_VALUE_TYPE_INT,
			err:    InvalidExpressionErrMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)
			storeQCMock.EXPECT().GetDatetimeFormat().Return(time.RFC3339Nano).AnyTimes()

			out, err := queryConverter.parseSQLVal(tc.expr, tc.saName, tc.saType)
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
			queryConverter := NewQueryConverter(
				storeQCMock,
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
			)
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

	c := NewQueryConverter(
		storeQCMock,
		testNamespaceName,
		searchattribute.TestNameTypeMap(),
		&searchattribute.TestMapper{},
	)
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

	c := NewQueryConverter(
		storeQCMock,
		testNamespaceName,
		searchattribute.TestNameTypeMap(),
		&searchattribute.TestMapper{},
	)
	r.Equal(chasm.UnspecifiedArchetypeID, c.archetypeID)

	c = c.WithArchetypeID(123)
	r.Equal(chasm.ArchetypeID(123), c.archetypeID)

	c = c.WithArchetypeID(chasm.UnspecifiedArchetypeID)
	r.Equal(chasm.UnspecifiedArchetypeID, c.archetypeID)
}

func TestQueryConverter_ResolveSearchAttributeAlias_WithChasmMapper(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	storeQCMock := NewMockStoreQueryConverter[sqlparser.Expr](ctrl)

	chasmMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalBool01":    "ChasmCompleted",
			"TemporalKeyword01": "ChasmStatus",
		},
		map[string]enumspb.IndexedValueType{
			"TemporalBool01":    enumspb.INDEXED_VALUE_TYPE_BOOL,
			"TemporalKeyword01": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	)

	queryConverter := NewQueryConverter(
		storeQCMock,
		testNamespaceName,
		searchattribute.TestNameTypeMap(),
		&searchattribute.TestMapper{},
	).WithChasmMapper(chasmMapper)

	testCases := []struct {
		name                    string
		expectedFieldName       string
		expectedFieldType       enumspb.IndexedValueType
		expectedErr             bool
		expectNamespaceDivision bool
	}{
		{
			name:                    "ChasmCompleted",
			expectedFieldName:       "TemporalBool01",
			expectedFieldType:       enumspb.INDEXED_VALUE_TYPE_BOOL,
			expectedErr:             false,
			expectNamespaceDivision: false,
		},
		{
			name:                    "ChasmStatus",
			expectedFieldName:       "TemporalKeyword01",
			expectedFieldType:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			expectedErr:             false,
			expectNamespaceDivision: false,
		},
		{
			name:                    "TemporalNamespaceDivision",
			expectedFieldName:       "TemporalNamespaceDivision",
			expectedFieldType:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			expectedErr:             false,
			expectNamespaceDivision: true,
		},
		{
			name:                    "NonExistentChasmAlias",
			expectedFieldName:       "",
			expectedFieldType:       enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED,
			expectedErr:             true,
			expectNamespaceDivision: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			fieldName, fieldType, err := queryConverter.resolveSearchAttributeAlias(tc.name)
			if tc.expectedErr {
				r.Error(err)
				// Note: fieldName may have been set during resolution attempts
				r.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, fieldType)
			} else {
				r.NoError(err)
				r.Equal(tc.expectedFieldName, fieldName)
				r.Equal(tc.expectedFieldType, fieldType)
				// Note: seenNamespaceDivision is only set in convertColName, not resolveSearchAttributeAlias
			}
		})
	}
}
