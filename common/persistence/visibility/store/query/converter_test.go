package query

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute"
	"go.uber.org/mock/gomock"
)

const (
	testNamespaceName = namespace.Name("test-namespace")
	testNamespaceID   = namespace.ID("test-namespace-id")
)

type queryConverterSuite struct {
	suite.Suite
	*require.Assertions
	ctrl *gomock.Controller

	storeQCMock    *MockStoreQueryConverter[sqlparser.Expr]
	queryConverter *QueryConverter[sqlparser.Expr]
}

func TestQueryConverter(t *testing.T) {
	s := &queryConverterSuite{}
	suite.Run(t, s)
}

func (s *queryConverterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ctrl = gomock.NewController(s.T())
	s.storeQCMock = NewMockStoreQueryConverter[sqlparser.Expr](s.ctrl)
	s.queryConverter = NewQueryConverter(
		s.storeQCMock,
		testNamespaceName,
		testNamespaceID,
		searchattribute.TestNameTypeMap,
		&searchattribute.TestMapper{},
	)
}

func (s *queryConverterSuite) SetupSubTest() {
	s.queryConverter = NewQueryConverter(
		s.storeQCMock,
		testNamespaceName,
		testNamespaceID,
		searchattribute.TestNameTypeMap,
		&searchattribute.TestMapper{},
	)
}

func (s *queryConverterSuite) TestWithSearchAttributeInterceptor() {
	r := require.New(s.T())

	// QueryConverter without explicit SearchAttributeInterceptor sets the nop interceptor.
	c := NewQueryConverter(
		s.storeQCMock,
		testNamespaceName,
		testNamespaceID,
		searchattribute.TestNameTypeMap,
		&searchattribute.TestMapper{},
	)
	r.Equal(nopSearchAttributeInterceptor, c.saInterceptor)

	// Setting nil interceptor sets the nop interceptor.
	c = NewQueryConverter(
		s.storeQCMock,
		testNamespaceName,
		testNamespaceID,
		searchattribute.TestNameTypeMap,
		&searchattribute.TestMapper{},
	).WithSearchAttributeInterceptor(nil)
	r.Equal(nopSearchAttributeInterceptor, c.saInterceptor)

	// Setting non-nil interceptor
	i := &testSearchAttributeInterceptor{}
	c = NewQueryConverter(
		s.storeQCMock,
		testNamespaceName,
		testNamespaceID,
		searchattribute.TestNameTypeMap,
		&searchattribute.TestMapper{},
	).WithSearchAttributeInterceptor(i)
	r.Equal(i, c.saInterceptor)
}

func (s *queryConverterSuite) TestConvert() {
	keywordCol := NewSAColumnName(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	namespaceIDExpr := &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualStr,
		Left:     NamespaceIDSAColumnName,
		Right:    NewUnsafeSQLString(testNamespaceID.String()),
	}
	namespaceDivisionExpr := &sqlparser.IsExpr{
		Operator: sqlparser.IsNullStr,
		Expr:     NamespaceIDSAColumnName,
	}

	testCases := []struct {
		name                      string
		in                        string
		inExpr                    sqlparser.Expr
		setupMock                 func()
		mockNamespaceIDExpr       bool
		mockNamespaceIDErr        error
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
			setupMock: func() {
				s.storeQCMock.EXPECT().
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
			mockNamespaceIDExpr:       true,
			mockNamespaceDivisionExpr: true,
			mockBuildFinalAndExpr:     true,
			mockBuildFinalAndRes: &sqlparser.AndExpr{
				Left: &sqlparser.AndExpr{
					Left:  namespaceIDExpr,
					Right: namespaceDivisionExpr,
				},
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
			mockNamespaceIDExpr:       true,
			mockNamespaceDivisionExpr: true,
			mockBuildFinalAndExpr:     true,
			mockBuildFinalAndRes: &sqlparser.AndExpr{
				Left:  namespaceIDExpr,
				Right: namespaceDivisionExpr,
			},
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
					Left:     NamespaceDivisionSAColumnName,
					Right:    NewUnsafeSQLString("bar"),
				},
			},
			setupMock: func() {
				e1 := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				}
				e2 := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     NamespaceDivisionSAColumnName,
					Right:    NewUnsafeSQLString("bar"),
				}
				s.storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(e1, nil)
				s.storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, NamespaceDivisionSAColumnName, "bar").
					Return(e2, nil)
				s.storeQCMock.EXPECT().BuildAndExpr(e1, e2).Return(
					&sqlparser.AndExpr{
						Left:  e1,
						Right: e2,
					},
					nil,
				)
			},
			mockNamespaceIDExpr:   true,
			mockBuildFinalAndExpr: true,
			mockBuildFinalAndRes: &sqlparser.AndExpr{
				Left: namespaceIDExpr,
				Right: &sqlparser.AndExpr{
					Left: &sqlparser.ComparisonExpr{
						Operator: sqlparser.EqualStr,
						Left:     keywordCol,
						Right:    NewUnsafeSQLString("foo"),
					},
					Right: &sqlparser.ComparisonExpr{
						Operator: sqlparser.EqualStr,
						Left:     NamespaceDivisionSAColumnName,
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
			name: "fail namespace expr",
			in:   "AliasForKeyword01 = 'foo'",
			inExpr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     keywordCol,
				Right:    NewUnsafeSQLString("foo"),
			},
			setupMock: func() {
				s.storeQCMock.EXPECT().
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
			mockNamespaceIDExpr: true,
			mockNamespaceIDErr:  errors.New("mock error"),
			err:                 "mock error",
		},
		{
			name: "fail namespace division expr",
			in:   "AliasForKeyword01 = 'foo'",
			inExpr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     keywordCol,
				Right:    NewUnsafeSQLString("foo"),
			},
			setupMock: func() {
				s.storeQCMock.EXPECT().
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
			mockNamespaceIDExpr:       true,
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
			setupMock: func() {
				s.storeQCMock.EXPECT().
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
			mockNamespaceIDExpr:       true,
			mockNamespaceDivisionExpr: true,
			mockBuildFinalAndExpr:     true,
			mockBuildFinalAndErr:      errors.New("mock error"),
			err:                       "mock error",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			if tc.setupMock != nil {
				tc.setupMock()
			}
			exprs := make([]sqlparser.Expr, 3)
			if tc.mockNamespaceIDExpr {
				s.storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(
						sqlparser.EqualStr,
						NamespaceIDSAColumnName,
						testNamespaceID.String(),
					).
					Return(namespaceIDExpr, tc.mockNamespaceIDErr)
				exprs[0] = namespaceIDExpr
			}

			if tc.mockNamespaceDivisionExpr {
				s.storeQCMock.EXPECT().
					ConvertIsExpr(sqlparser.IsNullStr, NamespaceDivisionSAColumnName).
					Return(namespaceDivisionExpr, tc.mockNamespaceDivisionErr)
				exprs[1] = namespaceDivisionExpr
			}
			if tc.inExpr != nil {
				exprs[2] = tc.inExpr
			}
			if tc.mockBuildFinalAndExpr {
				s.storeQCMock.EXPECT().
					BuildAndExpr(exprs[0], exprs[1], exprs[2]).
					Return(tc.mockBuildFinalAndRes, tc.mockBuildFinalAndErr)
			}
			out, err := s.queryConverter.Convert(tc.in)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				if tc.mockNamespaceIDErr == nil && tc.mockNamespaceDivisionErr == nil &&
					tc.mockBuildFinalAndErr == nil {
					var expectedErr *ConverterError
					s.ErrorAs(err, &expectedErr)
				}
			} else {
				s.NoError(err)
				s.Equal(tc.mockBuildFinalAndRes, out.QueryExpr)
			}
		})
	}
}

func (s *queryConverterSuite) TestConvertWhereString() {
	keywordCol := NewSAColumnName(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         string
		setupMocks func()
		out        *QueryParams[sqlparser.Expr]
		err        string
	}{
		{
			name: "success",
			in:   "AliasForKeyword01 = 'foo'",
			setupMocks: func() {
				s.storeQCMock.EXPECT().
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
						Expr: NewSAColumnName(
							searchattribute.WorkflowType,
							searchattribute.WorkflowType,
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
				GroupBy: []*SAColumnName{
					NewSAColumnName(
						searchattribute.ExecutionStatus,
						searchattribute.ExecutionStatus,
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
		if tc.setupMocks != nil {
			tc.setupMocks()
		}
		out, err := s.queryConverter.convertWhereString(tc.in)
		if tc.err != "" {
			s.Error(err)
			s.ErrorContains(err, tc.err)
			var expectedErr *ConverterError
			s.ErrorAs(err, &expectedErr)
		} else {
			s.NoError(err)
			s.Equal(tc.out, out)
		}
	}
}

func (s *queryConverterSuite) TestConvertSelectStmt() {
	keywordCol := NewSAColumnName(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         string
		setupMocks func()
		out        *QueryParams[sqlparser.Expr]
		err        string
	}{
		{
			name: "success",
			in:   "select * from t where AliasForKeyword01 = 'foo'",
			setupMocks: func() {
				s.storeQCMock.EXPECT().
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
						Expr: NewSAColumnName(
							searchattribute.WorkflowType,
							searchattribute.WorkflowType,
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
				GroupBy: []*SAColumnName{
					NewSAColumnName(
						searchattribute.ExecutionStatus,
						searchattribute.ExecutionStatus,
						enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					),
				},
			},
		},
		{
			name: "fail limit not supported",
			in:   "select * from t limit 10",
			err:  NotSupportedErrMessage,
		},
		{
			name: "fail invalid query",
			in:   "select * from t where true",
			err:  NotSupportedErrMessage,
		},
		{
			name: "fail multiple group by",
			in:   "select * from t group by ExecutionStatus, RunId",
			err:  NotSupportedErrMessage,
		},
		{
			name: "fail not supported group by field",
			in:   "select * from t group by RunId",
			err:  NotSupportedErrMessage,
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
		s.Run(tc.name, func() {
			if tc.setupMocks != nil {
				tc.setupMocks()
			}
			stmt, err := sqlparser.Parse(tc.in)
			s.NoError(err)
			selectStmt, _ := stmt.(*sqlparser.Select)
			out, err := s.queryConverter.convertSelectStmt(selectStmt)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				s.ErrorAs(err, &expectedErr)
			} else {
				s.NoError(err)
				s.Equal(tc.out, out)
			}
		})
	}
}

func (s *queryConverterSuite) TestConvertWhereExpr() {
	keywordCol := NewSAColumnName(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	statusCol := NewSAColumnName(
		searchattribute.ExecutionStatus,
		searchattribute.ExecutionStatus,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         string
		setupMocks func()
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
			setupMocks: func() {
				s.storeQCMock.EXPECT().
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
			setupMocks: func() {
				e := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				}
				s.storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(e, nil)
				s.storeQCMock.EXPECT().BuildParenExpr(e).Return(&sqlparser.ParenExpr{Expr: e}, nil)
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
			setupMocks: func() {
				e := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     keywordCol,
					Right:    NewUnsafeSQLString("foo"),
				}
				s.storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(e, nil)
				s.storeQCMock.EXPECT().BuildNotExpr(e).Return(&sqlparser.NotExpr{Expr: e}, nil)
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
			setupMocks: func() {
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
				s.storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(e1, nil)
				s.storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, statusCol, "Running").
					Return(e2, nil)
				s.storeQCMock.EXPECT().BuildAndExpr(e1, e2).
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
			setupMocks: func() {
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
				s.storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, keywordCol, "foo").
					Return(e1, nil)
				s.storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, statusCol, "Running").
					Return(e2, nil)
				s.storeQCMock.EXPECT().BuildOrExpr(e1, e2).
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
			setupMocks: func() {
				s.storeQCMock.EXPECT().
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
			setupMocks: func() {
				s.storeQCMock.EXPECT().
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
		s.Run(tc.name, func() {
			if tc.setupMocks != nil {
				tc.setupMocks()
			}
			stmt, err := sqlparser.Parse(tc.in)
			s.NoError(err)
			selectStmt, _ := stmt.(*sqlparser.Select)
			if selectStmt.Where == nil {
				selectStmt.Where = &sqlparser.Where{
					Type: sqlparser.WhereStr,
					Expr: nil,
				}
			}
			out, err := s.queryConverter.convertWhereExpr(selectStmt.Where.Expr)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				if _, ok := err.(*serviceerror.Internal); !ok {
					var expectedErr *ConverterError
					s.ErrorAs(err, &expectedErr)
				}
			} else {
				s.NoError(err)
				s.Equal(tc.out, out)
			}
		})
	}
}

func (s *queryConverterSuite) TestConvertParenExpr() {
	keywordCol := NewSAColumnName(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         *sqlparser.ParenExpr
		setupMocks func()
		out        sqlparser.Expr
		err        string
	}{
		{
			name: "success",
			in: &sqlparser.ParenExpr{
				Expr: &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeyword01")},
				},
			},
			setupMocks: func() {
				e := &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				}
				s.storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNullStr, keywordCol).Return(e, nil)
				s.storeQCMock.EXPECT().BuildParenExpr(e).Return(
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
			in: &sqlparser.ParenExpr{
				Expr: sqlparser.BoolVal(true),
			},
			err: NotSupportedErrMessage,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			if tc.setupMocks != nil {
				tc.setupMocks()
			}
			out, err := s.queryConverter.convertParenExpr(tc.in)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				s.ErrorAs(err, &expectedErr)
			} else {
				s.NoError(err)
				s.Equal(tc.out, out)
			}
		})
	}
}

func (s *queryConverterSuite) TestConvertNotExpr() {
	keywordCol := NewSAColumnName(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         *sqlparser.NotExpr
		setupMocks func()
		out        sqlparser.Expr
		err        string
	}{
		{
			name: "success",
			in: &sqlparser.NotExpr{
				Expr: &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeyword01")},
				},
			},
			setupMocks: func() {
				e := &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				}
				s.storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNullStr, keywordCol).Return(e, nil)
				s.storeQCMock.EXPECT().BuildNotExpr(e).Return(
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
			in: &sqlparser.NotExpr{
				Expr: sqlparser.BoolVal(true),
			},
			err: NotSupportedErrMessage,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			if tc.setupMocks != nil {
				tc.setupMocks()
			}
			out, err := s.queryConverter.convertNotExpr(tc.in)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				s.ErrorAs(err, &expectedErr)
			} else {
				s.NoError(err)
				s.Equal(tc.out, out)
			}
		})
	}
}

func (s *queryConverterSuite) TestConvertAndExpr() {
	keywordCol := NewSAColumnName(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	wfTypeCol := NewSAColumnName(
		searchattribute.WorkflowType,
		searchattribute.WorkflowType,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         *sqlparser.AndExpr
		setupMocks func()
		out        sqlparser.Expr
		err        string
	}{
		{
			name: "success",
			in: &sqlparser.AndExpr{
				Left: &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeyword01")},
				},
				Right: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(searchattribute.WorkflowType)},
					Right:    sqlparser.NewStrVal([]byte("test-wf-type")),
				},
			},
			setupMocks: func() {
				e1 := &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				}
				e2 := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     wfTypeCol,
					Right:    NewUnsafeSQLString("test-wf-type"),
				}
				s.storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNullStr, keywordCol).Return(e1, nil)
				s.storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, wfTypeCol, "test-wf-type").
					Return(e2, nil)
				s.storeQCMock.EXPECT().BuildAndExpr(e1, e2).Return(
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
			in: &sqlparser.AndExpr{
				Left: sqlparser.BoolVal(false),
				Right: &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeyword01")},
				},
			},
			err: NotSupportedErrMessage,
		},
		{
			name: "fail right",
			in: &sqlparser.AndExpr{
				Left: &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeyword01")},
				},
				Right: sqlparser.BoolVal(false),
			},
			setupMocks: func() {
				e1 := &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				}
				s.storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNullStr, keywordCol).Return(e1, nil)
			},
			err: NotSupportedErrMessage,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			if tc.setupMocks != nil {
				tc.setupMocks()
			}
			out, err := s.queryConverter.convertAndExpr(tc.in)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				s.ErrorAs(err, &expectedErr)
			} else {
				s.NoError(err)
				s.Equal(tc.out, out)
			}
		})
	}
}

func (s *queryConverterSuite) TestConvertOrExpr() {
	keywordCol := NewSAColumnName(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	wfTypeCol := NewSAColumnName(
		searchattribute.WorkflowType,
		searchattribute.WorkflowType,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         *sqlparser.OrExpr
		setupMocks func()
		out        sqlparser.Expr
		err        string
	}{
		{
			name: "success",
			in: &sqlparser.OrExpr{
				Left: &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeyword01")},
				},
				Right: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(searchattribute.WorkflowType)},
					Right:    sqlparser.NewStrVal([]byte("test-wf-type")),
				},
			},
			setupMocks: func() {
				e1 := &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				}
				e2 := &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     wfTypeCol,
					Right:    NewUnsafeSQLString("test-wf-type"),
				}
				s.storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNullStr, keywordCol).Return(e1, nil)
				s.storeQCMock.EXPECT().
					ConvertKeywordComparisonExpr(sqlparser.EqualStr, wfTypeCol, "test-wf-type").
					Return(e2, nil)
				s.storeQCMock.EXPECT().BuildOrExpr(e1, e2).Return(
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
			in: &sqlparser.OrExpr{
				Left: sqlparser.BoolVal(false),
				Right: &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeyword01")},
				},
			},
			err: NotSupportedErrMessage,
		},
		{
			name: "fail right",
			in: &sqlparser.OrExpr{
				Left: &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeyword01")},
				},
				Right: sqlparser.BoolVal(false),
			},
			setupMocks: func() {
				e1 := &sqlparser.IsExpr{
					Operator: sqlparser.IsNullStr,
					Expr:     keywordCol,
				}
				s.storeQCMock.EXPECT().ConvertIsExpr(sqlparser.IsNullStr, keywordCol).Return(e1, nil)
			},
			err: NotSupportedErrMessage,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			if tc.setupMocks != nil {
				tc.setupMocks()
			}
			out, err := s.queryConverter.convertOrExpr(tc.in)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				s.ErrorAs(err, &expectedErr)
			} else {
				s.NoError(err)
				s.Equal(tc.out, out)
			}
		})
	}
}

func (s *queryConverterSuite) TestConvertComparisonExprStoreQueryConverterCalled() {
	keywordCol := NewSAColumnName(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	keywordListCol := NewSAColumnName(
		"AliasForKeywordList01",
		"KeywordList01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
	)
	textCol := NewSAColumnName(
		"AliasForText01",
		"Text01",
		enumspb.INDEXED_VALUE_TYPE_TEXT,
	)
	datetimeCol := NewSAColumnName(
		"AliasForDatetime01",
		"Datetime01",
		enumspb.INDEXED_VALUE_TYPE_DATETIME,
	)

	testCases := []struct {
		name     string
		col      *SAColumnName
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

	s.storeQCMock.EXPECT().GetDatetimeFormat().Return(time.RFC3339Nano).AnyTimes()
	for _, tc := range testCases {
		s.Run(tc.name, func() {
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
			tc.mockFunc(s.storeQCMock.EXPECT(), sqlparser.EqualStr, tc.col, tc.value).
				Return(output, tc.mockErr)
			out, err := s.queryConverter.convertComparisonExpr(input)
			if tc.mockErr != nil {
				s.Equal(tc.mockErr, err)
			} else {
				s.NoError(err)
				s.Equal(output, out)
			}
		})
	}
}

func (s *queryConverterSuite) TestConvertComparisonExprFail() {
	testCases := []struct {
		name string
		in   *sqlparser.ComparisonExpr
		err  string
	}{
		{
			name: "invalid col name",
			in: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("InvalidField")},
				Right:    sqlparser.NewStrVal([]byte("foo")),
			},
			err: InvalidExpressionErrMessage,
		},
		{
			name: "invalid value",
			in: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeyword01")},
				Right:    &sqlparser.ColName{Name: sqlparser.NewColIdent("InvalidValue")},
			},
			err: NotSupportedErrMessage,
		},
		{
			name: "unsupported keyword operator",
			in: &sqlparser.ComparisonExpr{
				Operator: sqlparser.LikeStr,
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeyword01")},
				Right:    sqlparser.NewStrVal([]byte("foo")),
			},
			err: fmt.Sprintf(
				"%s: operator 'LIKE' not supported for Keyword type search attribute 'AliasForKeyword01'",
				NotSupportedErrMessage,
			),
		},
		{
			name: "unsupported keyword list operator",
			in: &sqlparser.ComparisonExpr{
				Operator: sqlparser.LikeStr,
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeywordList01")},
				Right:    sqlparser.NewStrVal([]byte("foo")),
			},
			err: fmt.Sprintf(
				"%s: operator 'LIKE' not supported for KeywordList type search attribute 'AliasForKeywordList01'",
				NotSupportedErrMessage,
			),
		},
		{
			name: "unsupported text operator",
			in: &sqlparser.ComparisonExpr{
				Operator: sqlparser.LikeStr,
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForText01")},
				Right:    sqlparser.NewStrVal([]byte("foo")),
			},
			err: fmt.Sprintf(
				"%s: operator 'LIKE' not supported for Text type search attribute 'AliasForText01'",
				NotSupportedErrMessage,
			),
		},
		{
			name: "unsupported int operator",
			in: &sqlparser.ComparisonExpr{
				Operator: sqlparser.LikeStr,
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForInt01")},
				Right:    sqlparser.NewIntVal([]byte("123")),
			},
			err: fmt.Sprintf(
				"%s: operator 'LIKE' not supported for Int type search attribute 'AliasForInt01'",
				NotSupportedErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			_, err := s.queryConverter.convertComparisonExpr(tc.in)
			s.Error(err)
			s.ErrorContains(err, tc.err)
			var expectedErr *ConverterError
			s.ErrorAs(err, &expectedErr)
		})
	}
}

func (s *queryConverterSuite) TestConvertRangeCond() {
	keywordCol := NewSAColumnName(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name       string
		in         *sqlparser.RangeCond
		setupMocks func()
		out        sqlparser.Expr
		err        string
	}{
		{
			name: "success",
			in: &sqlparser.RangeCond{
				Operator: sqlparser.BetweenStr,
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeyword01")},
				From:     sqlparser.NewStrVal([]byte("123")),
				To:       sqlparser.NewStrVal([]byte("456")),
			},
			setupMocks: func() {
				s.storeQCMock.EXPECT().
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
			in: &sqlparser.RangeCond{
				Operator: sqlparser.BetweenStr,
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("InvalidField")},
				From:     sqlparser.NewStrVal([]byte("123")),
				To:       sqlparser.NewStrVal([]byte("456")),
			},
			err: InvalidExpressionErrMessage,
		},
		{
			name: "fail unsupported type",
			in: &sqlparser.RangeCond{
				Operator: sqlparser.BetweenStr,
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForText01")},
				From:     sqlparser.NewStrVal([]byte("123")),
				To:       sqlparser.NewStrVal([]byte("456")),
			},
			err: fmt.Sprintf(
				"%s: cannot do range condition on search attribute 'AliasForText01' of type Text",
				InvalidExpressionErrMessage,
			),
		},
		{
			name: "fail invalid from value",
			in: &sqlparser.RangeCond{
				Operator: sqlparser.BetweenStr,
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeyword01")},
				From:     &sqlparser.ColName{Name: sqlparser.NewColIdent("InvalidValue")},
				To:       sqlparser.NewStrVal([]byte("456")),
			},
			err: NotSupportedErrMessage,
		},
		{
			name: "fail invalid from value",
			in: &sqlparser.RangeCond{
				Operator: sqlparser.BetweenStr,
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("AliasForKeyword01")},
				From:     sqlparser.NewStrVal([]byte("123")),
				To:       &sqlparser.ColName{Name: sqlparser.NewColIdent("InvalidValue")},
			},
			err: NotSupportedErrMessage,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			if tc.setupMocks != nil {
				tc.setupMocks()
			}
			out, err := s.queryConverter.convertRangeCond(tc.in)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				s.ErrorAs(err, &expectedErr)
			} else {
				s.NoError(err)
				s.Equal(tc.out, out)
			}
		})
	}
}

func (s *queryConverterSuite) TestConvertIsExpr() {
	keywordCol := NewSAColumnName(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	invalidCol := NewSAColumnName(
		"InvalidField",
		"InvalidField",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name     string
		operator string
		colName  *SAColumnName
		mockErr  error
		err      string
	}{
		{
			name:     "success is null",
			operator: sqlparser.IsNullStr,
			colName:  keywordCol,
		},
		{
			name:     "success is not null",
			operator: sqlparser.IsNotNullStr,
			colName:  keywordCol,
		},
		{
			name:     "fail invalid col name",
			operator: sqlparser.IsNullStr,
			colName:  invalidCol,
			err:      InvalidExpressionErrMessage,
		},
		{
			name:     "fail unsupported is true",
			operator: sqlparser.IsTrueStr,
			colName:  keywordCol,
			err: fmt.Sprintf(
				"%s: 'IS' operator can only be used as 'IS NULL' or 'IS NOT NULL'",
				InvalidExpressionErrMessage,
			),
		},
		{
			name:     "fail unsupported is not true",
			operator: sqlparser.IsNotTrueStr,
			colName:  keywordCol,
			err: fmt.Sprintf(
				"%s: 'IS' operator can only be used as 'IS NULL' or 'IS NOT NULL'",
				InvalidExpressionErrMessage,
			),
		},
		{
			name:     "fail unsupported is false",
			operator: sqlparser.IsFalseStr,
			colName:  keywordCol,
			err: fmt.Sprintf(
				"%s: 'IS' operator can only be used as 'IS NULL' or 'IS NOT NULL'",
				InvalidExpressionErrMessage,
			),
		},
		{
			name:     "fail unsupported is not false",
			operator: sqlparser.IsNotFalseStr,
			colName:  keywordCol,
			err: fmt.Sprintf(
				"%s: 'IS' operator can only be used as 'IS NULL' or 'IS NOT NULL'",
				InvalidExpressionErrMessage,
			),
		},
		{
			name:     "fail mock error",
			operator: sqlparser.IsNullStr,
			colName:  keywordCol,
			mockErr:  errors.New("mock error"),
			err:      "mock error",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			if tc.err == "" || tc.mockErr != nil {
				s.storeQCMock.EXPECT().ConvertIsExpr(tc.operator, keywordCol).
					Return(
						&sqlparser.IsExpr{
							Operator: tc.operator,
							Expr:     keywordCol,
						},
						tc.mockErr,
					)
			}
			in := &sqlparser.IsExpr{
				Operator: tc.operator,
				Expr:     &sqlparser.ColName{Name: sqlparser.NewColIdent(tc.colName.Alias)},
			}
			out, err := s.queryConverter.convertIsExpr(in)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				if tc.mockErr == nil {
					var expectedErr *ConverterError
					s.ErrorAs(err, &expectedErr)
				}
			} else {
				s.NoError(err)
				s.Equal(&sqlparser.IsExpr{Operator: tc.operator, Expr: tc.colName}, out)
			}
		})
	}
}

func (s *queryConverterSuite) TestConvertColName() {
	keywordCol := NewSAColumnName(
		"AliasForKeyword01",
		"Keyword01",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	statusCol := NewSAColumnName(
		searchattribute.ExecutionStatus,
		searchattribute.ExecutionStatus,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)

	testCases := []struct {
		name string
		in   sqlparser.Expr
		out  *SAColumnName
		err  string
	}{
		{
			name: "success",
			in: &sqlparser.ColName{
				Name: sqlparser.NewColIdent(searchattribute.ExecutionStatus),
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
				Name: sqlparser.NewColIdent(searchattribute.ScheduleID),
			},
			out: NewSAColumnName(
				searchattribute.ScheduleID,
				searchattribute.WorkflowID,
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
			out: NamespaceDivisionSAColumnName,
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
		{
			// This case should never happen. This test makes sure that if it fails, it returns an error.
			name: "unknown search attribute type",
			in: &sqlparser.ColName{
				Name: sqlparser.NewColIdent("AliasForInvalidField"),
			},
			err: fmt.Sprintf(
				"%s: column name 'AliasForInvalidField' is not a valid search attribute",
				InvalidExpressionErrMessage,
			),
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			out, err := s.queryConverter.convertColName(tc.in)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				s.ErrorAs(err, &expectedErr)
			} else {
				s.NoError(err)
				s.Equal(tc.out, out)
				if tc.out.FieldName == searchattribute.TemporalNamespaceDivision {
					s.True(s.queryConverter.seenNamespaceDivision)
				} else {
					s.False(s.queryConverter.seenNamespaceDivision)
				}
			}
		})
	}
}

func (s *queryConverterSuite) TestParseValueExpr() {
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
			alias:  searchattribute.WorkflowType,
			field:  searchattribute.WorkflowType,
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			out:    "foo",
		},
		{
			name:   "success special ScheduleID",
			expr:   sqlparser.NewStrVal([]byte("foo")),
			alias:  searchattribute.ScheduleID,
			field:  searchattribute.WorkflowID,
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
			alias:  searchattribute.WorkflowType,
			field:  searchattribute.WorkflowType,
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
		s.Run(tc.name, func() {
			out, err := s.queryConverter.parseValueExpr(tc.expr, tc.alias, tc.field, tc.saType)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				s.ErrorAs(err, &expectedErr)
			} else {
				s.NoError(err)
				s.Equal(tc.out, out)
			}
		})
	}
}

func (s *queryConverterSuite) TestParseSQLVal() {
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
			saName: searchattribute.ExecutionStatus,
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			out:    "Running",
		},
		{
			name:   "fail ExecutionStatus",
			expr:   sqlparser.NewStrVal([]byte("Invalid")),
			saName: searchattribute.ExecutionStatus,
			saType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			err:    InvalidExpressionErrMessage,
		},
		{
			name:   "success ExecutionDuration",
			expr:   sqlparser.NewStrVal([]byte("1m")),
			saName: searchattribute.ExecutionDuration,
			saType: enumspb.INDEXED_VALUE_TYPE_INT,
			out:    int64(1 * time.Minute),
		},
		{
			name:   "fail ExecutionDuration",
			expr:   sqlparser.NewStrVal([]byte("1t")),
			saName: searchattribute.ExecutionDuration,
			saType: enumspb.INDEXED_VALUE_TYPE_INT,
			err:    InvalidExpressionErrMessage,
		},
	}

	s.storeQCMock.EXPECT().GetDatetimeFormat().Return(time.RFC3339Nano).AnyTimes()
	for _, tc := range testCases {
		s.Run(tc.name, func() {
			out, err := s.queryConverter.parseSQLVal(tc.expr, tc.saName, tc.saType)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				s.ErrorAs(err, &expectedErr)
			} else {
				s.NoError(err)
				s.Equal(tc.out, out)
			}
		})
	}
}

func (s *queryConverterSuite) TestValidateValueType() {
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

	s.storeQCMock.EXPECT().GetDatetimeFormat().Return(time.RFC3339Nano).AnyTimes()
	for _, tc := range testCases {
		s.Run(tc.name, func() {
			out, err := s.queryConverter.validateValueTypeV2(tc.saName, tc.saType, tc.value)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				s.ErrorAs(err, &expectedErr)
			} else {
				s.NoError(err)
				s.Equal(tc.out, out)
			}
		})
	}
}

func (s *queryConverterSuite) TestParseExecutionStatusValue() {
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
		s.Run(tc.name, func() {
			out, err := parseExecutionStatusValue(tc.value)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				s.ErrorAs(err, &expectedErr)
			} else {
				s.NoError(err)
				s.Equal(tc.out, out)
			}
		})
	}
}

func (s *queryConverterSuite) TestParseExecutionDurationValue() {
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
		s.Run(tc.name, func() {
			out, err := parseExecutionDurationValue(tc.value)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				s.ErrorAs(err, &expectedErr)
			} else {
				s.NoError(err)
				s.Equal(tc.out, out)
			}
		})
	}
}
