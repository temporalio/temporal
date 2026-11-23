package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.uber.org/mock/gomock"
)

const (
	testHitCloseTime int64 = 1547596872371234567
	testHitStartTime int64 = 1547596872371000000
)

// TestListNamespaceFirstIndexSort verifies that a List on an index sorted by NamespaceId first sorts by NamespaceId
// first, reads the index settings only once, and keeps page tokens in the two-value default format.
func (s *ESVisibilitySuite) TestListNamespaceFirstIndexSort() {
	s.visibilityStore.indexSortKnown.Store(false)
	// search_after is only sent without manual pagination, which builds range clauses from the token instead
	s.visibilityStore.enableManualPagination = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
	s.mockESClient.EXPECT().GetIndexSettings(gomock.Any(), testIndex).Return(namespaceFirstSettings(testIndex), nil)
	var searches []*client.SearchParameters
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, p *client.SearchParameters) (*elastic.SearchResult, error) {
			searches = append(searches, p)
			return sortedSearchResult(testNamespaceID.String(), testHitCloseTime, testHitStartTime), nil
		}).Times(2)

	request := &manager.ListWorkflowExecutionsRequestV2{NamespaceID: testNamespaceID, Namespace: testNamespace, PageSize: 1}
	resp, err := s.visibilityStore.ListWorkflowExecutions(s.T().Context(), request)
	s.NoError(err)
	token, err := s.visibilityStore.deserializePageToken(resp.NextPageToken)
	s.NoError(err)
	s.Equal(testSortValues(), token.SearchAfter)

	request.NextPageToken = resp.NextPageToken
	_, err = s.visibilityStore.ListWorkflowExecutions(s.T().Context(), request)
	s.NoError(err)

	s.Len(searches, 2)
	s.Equal(namespaceFirstSorter, searches[0].Sorter)
	s.Empty(searches[0].SearchAfter)
	s.Equal(namespaceFirstSorter, searches[1].Sorter)
	s.Equal(append([]any{testNamespaceID.String()}, testSortValues()...), searches[1].SearchAfter)
}

// TestListUnsortedIndex verifies that a List on an index without a NamespaceId-first sort searches with the default
// sort and keeps every sort value in its page token.
func (s *ESVisibilitySuite) TestListUnsortedIndex() {
	s.visibilityStore.indexSortKnown.Store(false)
	s.mockESClient.EXPECT().GetIndexSettings(gomock.Any(), testIndex).
		Return(map[string]map[string]any{testIndex: {"index.number_of_shards": "1"}}, nil)
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, p *client.SearchParameters) (*elastic.SearchResult, error) {
			s.True(isDefaultSorter(p.Sorter))
			return sortedSearchResult(testHitCloseTime, testHitStartTime), nil
		})

	request := &manager.ListWorkflowExecutionsRequestV2{NamespaceID: testNamespaceID, Namespace: testNamespace, PageSize: 1}
	resp, err := s.visibilityStore.ListWorkflowExecutions(s.T().Context(), request)
	s.NoError(err)
	token, err := s.visibilityStore.deserializePageToken(resp.NextPageToken)
	s.NoError(err)
	s.Equal(testSortValues(), token.SearchAfter)
}

// TestListIndexSortReadFails verifies that a List fails without searching while the index settings can't be read,
// and that the next List reads them again.
func (s *ESVisibilitySuite) TestListIndexSortReadFails() {
	s.visibilityStore.indexSortKnown.Store(false)
	gomock.InOrder(
		s.mockESClient.EXPECT().GetIndexSettings(gomock.Any(), testIndex).Return(nil, errors.New("connection refused")),
		s.mockESClient.EXPECT().GetIndexSettings(gomock.Any(), testIndex).Return(namespaceFirstSettings(testIndex), nil),
	)
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, p *client.SearchParameters) (*elastic.SearchResult, error) {
			s.Equal(namespaceFirstSorter, p.Sorter)
			return testSearchResult, nil
		})

	request := &manager.ListWorkflowExecutionsRequestV2{NamespaceID: testNamespaceID, Namespace: testNamespace, PageSize: 1}
	_, err := s.visibilityStore.ListWorkflowExecutions(s.T().Context(), request)
	var unavailable *serviceerror.Unavailable
	s.ErrorAs(err, &unavailable)
	s.ErrorContains(err, "unable to read the index sort of test-index")

	_, err = s.visibilityStore.ListWorkflowExecutions(s.T().Context(), request)
	s.NoError(err)
}

// TestAdminListAllNamespacesKeepsDefaultSort verifies that an admin List across namespaces keeps the default sort on
// a NamespaceId-first index, since sorting by namespace would reorder its results.
func (s *ESVisibilitySuite) TestAdminListAllNamespacesKeepsDefaultSort() {
	s.visibilityStore.namespaceFirstSort = true
	s.mockMetricsHandler.EXPECT().WithTags(metrics.NamespaceTag("")).Return(s.mockMetricsHandler).AnyTimes()
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, p *client.SearchParameters) (*elastic.SearchResult, error) {
			s.True(isDefaultSorter(p.Sorter))
			return testSearchResult, nil
		})

	_, err := s.visibilityStore.AdminListExecutions(s.T().Context(), &adminservice.ListExecutionsRequest{PageSize: 10})
	s.NoError(err)
}

// TestIsNamespaceFirstIndexSort verifies which index settings count as sorted by NamespaceId first.
func TestIsNamespaceFirstIndexSort(t *testing.T) {
	for name, tc := range map[string]struct {
		settings map[string]map[string]any
		want     bool
	}{
		"namespace first":         {settings: namespaceFirstSettings("a"), want: true},
		"no settings":             {settings: map[string]map[string]any{}},
		"no index sort":           {settings: map[string]map[string]any{"a": {"index.number_of_shards": "20"}}},
		"single field":            {settings: map[string]map[string]any{"a": {"index.sort.field": "NamespaceId"}}, want: true},
		"no order or missing":     {settings: map[string]map[string]any{"a": {"index.sort.field": []any{"NamespaceId", "CloseTime"}}}, want: true},
		"close time first":        {settings: map[string]map[string]any{"a": {"index.sort.field": []any{"CloseTime", "StartTime", "RunId"}}}},
		"namespace descending":    {settings: withSetting(namespaceFirstSettings("a"), "a", "index.sort.order", []any{"desc", "desc", "desc", "desc"})},
		"namespace missing first": {settings: withSetting(namespaceFirstSettings("a"), "a", "index.sort.missing", []any{"_first", "_first", "_first", "_first"})},
		"alias that agrees": {settings: map[string]map[string]any{
			"a": namespaceFirstSettings("a")["a"],
			"b": namespaceFirstSettings("b")["b"],
		}, want: true},
		"alias that disagrees": {settings: map[string]map[string]any{
			"a": namespaceFirstSettings("a")["a"],
			"b": {"index.sort.field": []any{"CloseTime", "StartTime", "RunId"}},
		}},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, isNamespaceFirstIndexSort(tc.settings))
		})
	}
}

// namespaceFirstSettings returns the flat settings of an index created with the namespace-first sort.
func namespaceFirstSettings(index string) map[string]map[string]any {
	return map[string]map[string]any{index: {
		"index.number_of_shards": "20",
		"index.sort.field":       []any{"NamespaceId", "CloseTime", "StartTime", "RunId"},
		"index.sort.order":       []any{"asc", "desc", "desc", "desc"},
		"index.sort.missing":     []any{"_last", "_first", "_first", "_first"},
	}}
}

func withSetting(settings map[string]map[string]any, index string, key string, value any) map[string]map[string]any {
	settings[index][key] = value
	return settings
}

// sortedSearchResult returns one hit with the given sort values, built fresh so each search gets its own.
func sortedSearchResult(sort ...any) *elastic.SearchResult {
	return &elastic.SearchResult{Hits: &elastic.SearchHits{Hits: []*elastic.SearchHit{{
		Source: json.RawMessage(`{"ExecutionStatus": "Running", "NamespaceId": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
			"RunId": "e481009e-14b3-45ae-91af-dce6e2a88365", "StartTime": "2021-06-11T15:04:07.980-07:00",
			"WorkflowId": "wid", "WorkflowType": "wf-type"}`),
		Sort: sort,
	}}}}
}

// testSortValues is a page token's search_after for sortedSearchResult's hit, as deserializePageToken reads it.
func testSortValues() []any {
	return []any{json.Number("1547596872371234567"), json.Number("1547596872371000000")}
}
