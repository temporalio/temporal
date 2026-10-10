package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/olivere/elastic/v7"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.uber.org/mock/gomock"
)

func (s *ESVisibilitySuite) TestListExecutions() {
	s.mockNamespaceRegistry.EXPECT().GetNamespaceID(testNamespace).Return(testNamespaceID, nil)
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, p *client.SearchParameters) (*elastic.SearchResult, error) {
			s.Equal(testIndex, p.Index)
			s.Equal(
				newBoolQuery().Filter(
					elastic.NewTermQuery(sadefs.NamespaceID, testNamespaceID.String()),
					newTermQuery(sadefs.ExecutionStatus, "Terminated"),
				),
				p.Query,
			)
			return testSearchResult, nil
		})

	_, err := s.visibilityStore.ListExecutions(
		context.Background(),
		&manager.AdminListExecutionsRequest{
			Namespace: testNamespace,
			Query:     `ExecutionStatus = "Terminated"`,
			PageSize:  10,
		},
	)
	s.NoError(err)
}

// TestListExecutions_AllNamespaces covers a request with no namespace: no namespace is
// resolved and the query is not narrowed to a single namespace.
func (s *ESVisibilitySuite) TestListExecutions_AllNamespaces() {
	s.mockMetricsHandler.EXPECT().
		WithTags(metrics.NamespaceTag("")).
		Return(s.mockMetricsHandler).
		AnyTimes()

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, p *client.SearchParameters) (*elastic.SearchResult, error) {
			s.Equal(testIndex, p.Index)
			s.Equal(
				newBoolQuery().Filter(newTermQuery(sadefs.ExecutionStatus, "Terminated")),
				p.Query,
			)
			return testSearchResult, nil
		})

	_, err := s.visibilityStore.ListExecutions(
		context.Background(),
		&manager.AdminListExecutionsRequest{
			Query:    `ExecutionStatus = "Terminated"`,
			PageSize: 10,
		},
	)
	s.NoError(err)
}

// TestListExecutions_AllNamespacesNoMapper covers that custom search attributes must be
// referenced by field name when there is no namespace to pick a mapper from.
func (s *ESVisibilitySuite) TestListExecutions_AllNamespacesNoMapper() {
	s.mockMetricsHandler.EXPECT().
		WithTags(metrics.NamespaceTag("")).
		Return(s.mockMetricsHandler).
		AnyTimes()

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, p *client.SearchParameters) (*elastic.SearchResult, error) {
			s.Equal(
				newBoolQuery().Filter(newTermQuery("CustomKeywordField", "foo")),
				p.Query,
			)
			return testSearchResult, nil
		})

	request := &manager.AdminListExecutionsRequest{
		Query:    `CustomKeywordField = "foo"`,
		PageSize: 10,
	}
	_, err := s.visibilityStore.ListExecutions(context.Background(), request)
	s.NoError(err)

	request.Query = `AliasForCustomKeywordField = "foo"`
	_, err = s.visibilityStore.ListExecutions(context.Background(), request)
	s.ErrorContains(err, "invalid search attribute: AliasForCustomKeywordField")
}

// TestListExecutions_OrQueryPagination covers a second page of a query whose top level is
// a disjunction. Such a query converts to a bool query that already carries `should`
// clauses; appending the pagination clauses to those would let `minimum_should_match` be
// satisfied by a pagination clause alone, returning executions the query excludes. The
// converted query must be nested as a filter of the pagination query instead.
func (s *ESVisibilitySuite) TestListExecutions_OrQueryPagination() {
	closeTime := time.Unix(0, 1700000000000000000).UTC()
	startTime := closeTime.Add(-time.Minute)
	pageToken, err := s.visibilityStore.serializePageToken(&visibilityPageToken{
		SearchAfter: []any{closeTime.UnixNano(), startTime.UnixNano()},
	})
	s.NoError(err)

	orQuery := newBoolQuery().
		Should(
			newTermQuery(sadefs.WorkflowID, "wid-1"),
			newTermQuery(sadefs.WorkflowID, "wid-2"),
		)

	s.mockMetricsHandler.EXPECT().
		WithTags(metrics.NamespaceTag("")).
		Return(s.mockMetricsHandler)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, p *client.SearchParameters) (*elastic.SearchResult, error) {
			s.Equal(
				newBoolQuery().
					Filter(orQuery).
					Should(
						elastic.NewBoolQuery().Filter(
							elastic.NewRangeQuery(sadefs.CloseTime).
								Lt(closeTime.Format(paginationDatetimeFormat)),
						),
						elastic.NewBoolQuery().Filter(
							elastic.NewTermQuery(
								sadefs.CloseTime,
								closeTime.Format(paginationDatetimeFormat),
							),
							elastic.NewRangeQuery(sadefs.StartTime).
								Lt(startTime.Format(paginationDatetimeFormat)),
						),
					),
				p.Query,
			)
			return testSearchResult, nil
		})

	_, err = s.visibilityStore.ListExecutions(
		context.Background(),
		&manager.AdminListExecutionsRequest{
			Query:         `WorkflowId = "wid-1" OR WorkflowId = "wid-2"`,
			PageSize:      10,
			NextPageToken: pageToken,
		},
	)
	s.NoError(err)
}

// TestConvertQuery_QueryType guards the invariant processPageToken relies on: convertQuery
// never returns a bare non-bool clause, including on the admin path where there is no
// namespace filter to wrap the converted expression. Otherwise manual pagination fails with
// an "unexpected query type" internal error on the second page.
//
// A nil query is allowed and means match-all; it must be left as nil rather than wrapped,
// since a bool query holding a nil clause panics when serialized.
func (s *ESVisibilitySuite) TestConvertQuery_QueryType() {
	testCases := []struct {
		name          string
		namespaceName namespace.Name
		namespaceID   namespace.ID
		query         string
		// wantNil is true when the query expression is expected to be nil (match-all).
		wantNil bool
	}{
		{
			name:          "namespace scoped",
			namespaceName: testNamespace,
			namespaceID:   testNamespaceID,
			query:         `WorkflowId = 'wid'`,
		},
		{
			name:          "namespace scoped group by namespace division",
			namespaceName: testNamespace,
			namespaceID:   testNamespaceID,
			query:         "GROUP BY " + sadefs.TemporalNamespaceDivision,
		},
		{
			name:  "all namespaces",
			query: `WorkflowId = 'wid'`,
		},
		{
			name:  "all namespaces empty query",
			query: "",
		},
		{
			// The query filters on the namespace division itself, so the converter
			// suppresses the default filter and returns a bare term query.
			name:  "all namespaces namespace division seen",
			query: fmt.Sprintf("%s = 'x'", sadefs.TemporalNamespaceDivision),
		},
		{
			name:  "all namespaces namespace division is null",
			query: fmt.Sprintf("%s is null", sadefs.TemporalNamespaceDivision),
		},
		{
			// Grouping by the namespace division suppresses the default filter, and
			// with no namespace there is nothing left to filter on at all.
			name:    "all namespaces group by namespace division",
			query:   "GROUP BY " + sadefs.TemporalNamespaceDivision,
			wantNil: true,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.mockMetricsHandler.EXPECT().
				WithTags(metrics.NamespaceTag(tc.namespaceName.String())).
				Return(s.mockMetricsHandler).
				AnyTimes()

			queryConverter, err := s.visibilityStore.newQueryConverter(
				tc.namespaceName,
				nil, // chasmMapper,
				chasm.UnspecifiedArchetypeID,
			)
			s.NoError(err)
			queryParams, err := s.visibilityStore.convertQuery(tc.namespaceID, queryConverter, tc.query)
			s.NoError(err)
			if tc.wantNil {
				s.Nil(queryParams.Query)
				return
			}
			s.IsType(&boolQuery{}, queryParams.Query)

			// A bool query holding a nil clause panics when serialized, so the query
			// must also be serializable.
			_, err = queryParams.Query.Source()
			s.NoError(err)
		})
	}
}

func (s *ESVisibilitySuite) TestListExecutions_NamespaceNotFound() {
	nsNotFoundErr := serviceerror.NewNamespaceNotFound(testNamespace.String())
	s.mockNamespaceRegistry.EXPECT().
		GetNamespaceID(testNamespace).
		Return(namespace.EmptyID, nsNotFoundErr)

	_, err := s.visibilityStore.ListExecutions(
		context.Background(),
		&manager.AdminListExecutionsRequest{Namespace: testNamespace, PageSize: 10},
	)
	s.ErrorIs(err, nsNotFoundErr)
}

func (s *ESVisibilitySuite) TestListExecutions_Error() {
	request := &manager.AdminListExecutionsRequest{
		Query:    `ExecutionStatus = "Terminated"`,
		PageSize: 10,
	}

	s.mockMetricsHandler.EXPECT().
		WithTags(metrics.NamespaceTag("")).
		Return(s.mockMetricsHandler).
		AnyTimes()

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err := s.visibilityStore.ListExecutions(context.Background(), request)
	var unavailableErr *serviceerror.Unavailable
	s.ErrorAs(err, &unavailableErr)
	s.Contains(err.Error(), "ListExecutions failed")

	request.Query = `invalid query`
	_, err = s.visibilityStore.ListExecutions(context.Background(), request)
	var invalidArgErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgErr)
}

func (s *ESVisibilitySuite) TestCountExecutions() {
	s.mockNamespaceRegistry.EXPECT().GetNamespaceID(testNamespace).Return(testNamespaceID, nil)
	s.mockESClient.EXPECT().Count(gomock.Any(), testIndex, gomock.Any()).DoAndReturn(
		func(ctx context.Context, index string, query elastic.Query) (int64, error) {
			s.Equal(
				newBoolQuery().Filter(
					elastic.NewTermQuery(sadefs.NamespaceID, testNamespaceID.String()),
					newTermQuery(sadefs.ExecutionStatus, "Terminated"),
				),
				query,
			)
			return int64(1), nil
		})

	resp, err := s.visibilityStore.CountExecutions(
		context.Background(),
		&manager.AdminCountExecutionsRequest{
			Namespace: testNamespace,
			Query:     `ExecutionStatus = "Terminated"`,
		},
	)
	s.NoError(err)
	s.Equal(&store.InternalCountExecutionsResponse{Count: 1}, resp)
}

func (s *ESVisibilitySuite) TestCountExecutions_AllNamespaces() {
	s.mockMetricsHandler.EXPECT().
		WithTags(metrics.NamespaceTag("")).
		Return(s.mockMetricsHandler).
		AnyTimes()

	s.mockESClient.EXPECT().Count(gomock.Any(), testIndex, gomock.Any()).DoAndReturn(
		func(ctx context.Context, index string, query elastic.Query) (int64, error) {
			s.Equal(
				newBoolQuery().Filter(newTermQuery(sadefs.ExecutionStatus, "Terminated")),
				query,
			)
			return int64(1), nil
		})

	resp, err := s.visibilityStore.CountExecutions(
		context.Background(),
		&manager.AdminCountExecutionsRequest{Query: `ExecutionStatus = "Terminated"`},
	)
	s.NoError(err)
	s.Equal(&store.InternalCountExecutionsResponse{Count: 1}, resp)
}

func (s *ESVisibilitySuite) TestCountExecutions_GroupBy() {
	s.mockMetricsHandler.EXPECT().
		WithTags(metrics.NamespaceTag("")).
		Return(s.mockMetricsHandler).
		AnyTimes()

	s.mockESClient.EXPECT().
		CountGroupBy(
			gomock.Any(),
			testIndex,
			nil,
			sadefs.ExecutionStatus,
			elastic.NewTermsAggregation().Field(sadefs.ExecutionStatus),
		).
		Return(
			&elastic.SearchResult{
				Aggregations: map[string]json.RawMessage{
					sadefs.ExecutionStatus: json.RawMessage(
						`{"buckets":[{"key":"Completed","doc_count":100},{"key":"Running","doc_count":10}]}`,
					),
				},
			},
			nil,
		)

	resp, err := s.visibilityStore.CountExecutions(
		context.Background(),
		&manager.AdminCountExecutionsRequest{Query: "GROUP BY ExecutionStatus"},
	)
	s.NoError(err)
	expectedResp := &store.InternalCountExecutionsResponse{
		Count: 110,
		Groups: []store.InternalAggregationGroup{
			{
				GroupValues: []*commonpb.Payload{
					mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
				},
				Count: 100,
			},
			{
				GroupValues: []*commonpb.Payload{
					mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
				},
				Count: 10,
			},
		},
	}
	s.True(temporalproto.DeepEqual(expectedResp, resp))
}

func (s *ESVisibilitySuite) TestCountExecutions_NamespaceNotFound() {
	nsNotFoundErr := serviceerror.NewNamespaceNotFound(testNamespace.String())
	s.mockNamespaceRegistry.EXPECT().
		GetNamespaceID(testNamespace).
		Return(namespace.EmptyID, nsNotFoundErr)

	_, err := s.visibilityStore.CountExecutions(
		context.Background(),
		&manager.AdminCountExecutionsRequest{Namespace: testNamespace},
	)
	s.ErrorIs(err, nsNotFoundErr)
}

func (s *ESVisibilitySuite) TestCountExecutions_Error() {
	s.mockMetricsHandler.EXPECT().
		WithTags(metrics.NamespaceTag("")).
		Return(s.mockMetricsHandler).
		AnyTimes()

	request := &manager.AdminCountExecutionsRequest{Query: `ExecutionStatus = "Terminated"`}

	s.mockESClient.EXPECT().
		Count(gomock.Any(), testIndex, gomock.Any()).
		Return(int64(0), errTestESSearch)
	_, err := s.visibilityStore.CountExecutions(context.Background(), request)
	var unavailableErr *serviceerror.Unavailable
	s.ErrorAs(err, &unavailableErr)
	s.Contains(err.Error(), "CountExecutions failed")

	request.Query = `invalid query`
	_, err = s.visibilityStore.CountExecutions(context.Background(), request)
	var invalidArgErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgErr)
}
