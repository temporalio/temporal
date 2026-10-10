package sql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"go.uber.org/mock/gomock"
)

const (
	testNamespaceName = namespace.Name("test-namespace")
	testNamespaceID   = namespace.ID("test-namespace-id")
)

var pluginNames = []string{
	mysql.PluginName,
	postgresql.PluginName,
	sqlite.PluginName,
}

func TestBuildQueryParams(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		query string
		out   string
		err   string
	}{
		{
			name:  "empty",
			query: "",
			out:   fmt.Sprintf("namespace_id = '%s' and TemporalNamespaceDivision is null", testNamespaceID),
		},
		{
			name:  "one comparison",
			query: "AliasForKeyword01 = 'foo'",
			out:   fmt.Sprintf("namespace_id = '%s' and (TemporalNamespaceDivision is null and Keyword01 = 'foo')", testNamespaceID),
		},
		{
			name:  "two comparisons",
			query: "AliasForKeyword01 = 'foo' and AliasForInt01 = 123",
			out:   fmt.Sprintf("namespace_id = '%s' and (TemporalNamespaceDivision is null and (Keyword01 = 'foo' and Int01 = 123))", testNamespaceID),
		},
		{
			name:  "with TemporalNamespaceDivision",
			query: "AliasForKeyword01 = 'foo' and TemporalNamespaceDivision = 'bar'",
			out:   fmt.Sprintf("namespace_id = '%s' and (Keyword01 = 'foo' and TemporalNamespaceDivision = 'bar')", testNamespaceID),
		},
		{
			name:  "fail invalid custom search attribute",
			query: "AliasForFoo = 'foo'",
			err:   "invalid search attribute: AliasForFoo",
		},
		{
			name:  "fail order by not supported",
			query: "AliasForKeyword01 = 'foo' ORDER BY WorkflowType",
			err:   "operation is not supported: 'ORDER BY' clause",
		},
	}

	for _, pluginName := range pluginNames {
		for _, tc := range testCases {
			tcName := fmt.Sprintf("%s/%s", pluginName, tc.name)
			t.Run(tcName, func(t *testing.T) {
				r := require.New(t)
				sqlQC, err := NewSQLQueryConverter(pluginName)
				r.NoError(err)

				queryConverter := query.NewQueryConverter(
					sqlQC,
					testNamespaceName,
					searchattribute.TestNameTypeMap(),
					&searchattribute.TestMapper{},
					metrics.NoopMetricsHandler,
					log.NewNoopLogger(),
				)

				qp, err := buildQueryParams(
					testNamespaceID,
					&queryConverterWrapper{
						QueryConverter:    queryConverter,
						SQLQueryConverter: sqlQC,
					},
					tc.query,
				)
				if tc.err != "" {
					r.Error(err)
					r.ErrorContains(err, tc.err)
				} else {
					r.NoError(err)
					r.Equal(tc.out, sqlparser.String(qp.QueryExpr))
				}
			})
		}
	}
}

// TestBuildQueryParams_AllNamespaces covers the admin visibility path, where the request has
// no namespace: the query must not be filtered by namespace_id, and custom search attributes
// must be referenced by field name since there is no namespace mapper to resolve aliases.
func TestBuildQueryParams_AllNamespaces(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		query string
		out   string
		err   string
	}{
		{
			name:  "empty",
			query: "",
			out:   "TemporalNamespaceDivision is null",
		},
		{
			name:  "one comparison",
			query: "Keyword01 = 'foo'",
			out:   "TemporalNamespaceDivision is null and Keyword01 = 'foo'",
		},
		{
			name:  "with TemporalNamespaceDivision",
			query: "Keyword01 = 'foo' and TemporalNamespaceDivision = 'bar'",
			out:   "Keyword01 = 'foo' and TemporalNamespaceDivision = 'bar'",
		},
		{
			// Aliases can't be resolved without a namespace mapper.
			name:  "fail aliased custom search attribute",
			query: "AliasForKeyword01 = 'foo'",
			err:   "invalid search attribute: AliasForKeyword01",
		},
	}

	for _, pluginName := range pluginNames {
		for _, tc := range testCases {
			tcName := fmt.Sprintf("%s/%s", pluginName, tc.name)
			t.Run(tcName, func(t *testing.T) {
				r := require.New(t)
				sqlQC, err := NewSQLQueryConverter(pluginName)
				r.NoError(err)

				queryConverter := query.NewQueryConverter(
					sqlQC,
					namespace.EmptyName,
					searchattribute.TestNameTypeMap(),
					nil, // saMapper
					metrics.NoopMetricsHandler,
					log.NewNoopLogger(),
				)

				qp, err := buildQueryParams(
					namespace.EmptyID,
					&queryConverterWrapper{
						QueryConverter:    queryConverter,
						SQLQueryConverter: sqlQC,
					},
					tc.query,
				)
				if tc.err != "" {
					r.Error(err)
					r.ErrorContains(err, tc.err)
				} else {
					r.NoError(err)
					r.Equal(tc.out, sqlparser.String(qp.QueryExpr))
				}
			})
		}
	}
}

func TestListExecutions(t *testing.T) {
	t.Parallel()

	startTime := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	closeTime := startTime.Add(time.Minute)
	executionDuration := int64(time.Minute)
	historyLength := int64(29)
	historySizeBytes := int64(1024)
	stateTransitionCount := int64(22)

	row := sqlplugin.VisibilityRow{
		NamespaceID:          testNamespaceID.String(),
		WorkflowID:           "test-workflow-id",
		RunID:                "test-run-id",
		WorkflowTypeName:     "test-workflow-type",
		StartTime:            startTime,
		ExecutionTime:        startTime,
		Status:               int32(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED),
		CloseTime:            &closeTime,
		ExecutionDuration:    &executionDuration,
		HistoryLength:        &historyLength,
		HistorySizeBytes:     &historySizeBytes,
		StateTransitionCount: &stateTransitionCount,
	}

	testCases := []struct {
		name string
		// nsName is the namespace on the request; empty means all namespaces.
		nsName namespace.Name
		// expectNamespaceFilter is the namespace_id predicate expected in the SQL query.
		expectNamespaceFilter bool
	}{
		{
			name:                  "single namespace",
			nsName:                testNamespaceName,
			expectNamespaceFilter: true,
		},
		{
			name:                  "all namespaces",
			nsName:                namespace.EmptyName,
			expectNamespaceFilter: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			visStore := newTestVisibilityStore(t, ctrl, mysql.PluginName)

			if tc.nsName != namespace.EmptyName {
				nsRegistry := namespace.NewMockRegistry(ctrl)
				nsRegistry.EXPECT().GetNamespaceID(tc.nsName).Return(testNamespaceID, nil)
				visStore.namespaceRegistry = nsRegistry
			}

			visStore.sqlStore.DB.(*sqlplugin.MockDB).EXPECT().
				SelectFromVisibility(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					filter sqlplugin.VisibilitySelectFilter,
				) ([]sqlplugin.VisibilityRow, error) {
					nsFilter := fmt.Sprintf("WHERE (namespace_id = '%s'", testNamespaceID)
					if tc.expectNamespaceFilter {
						r.Contains(filter.Query, nsFilter)
					} else {
						r.NotContains(filter.Query, nsFilter)
					}
					return []sqlplugin.VisibilityRow{row}, nil
				})

			resp, err := visStore.ListExecutions(
				context.Background(),
				&manager.AdminListExecutionsRequest{
					Namespace: tc.nsName,
					Query:     "ExecutionStatus = 'Completed'",
					PageSize:  10,
				},
			)
			r.NoError(err)
			r.Empty(resp.NextPageToken) // fewer rows than the page size
			r.Equal(
				[]*store.InternalExecutionInfo{
					{
						NamespaceID:          testNamespaceID.String(),
						WorkflowID:           "test-workflow-id",
						RunID:                "test-run-id",
						TypeName:             "test-workflow-type",
						StartTime:            startTime,
						ExecutionTime:        startTime,
						CloseTime:            closeTime,
						ExecutionDuration:    time.Minute,
						Status:               enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
						HistoryLength:        historyLength,
						HistorySizeBytes:     historySizeBytes,
						StateTransitionCount: stateTransitionCount,
						Memo:                 persistence.NewDataBlob(nil, ""),
					},
				},
				resp.Executions,
			)
		})
	}
}

func TestListExecutions_NamespaceNotFound(t *testing.T) {
	t.Parallel()

	r := require.New(t)
	ctrl := gomock.NewController(t)
	visStore := newTestVisibilityStore(t, ctrl, mysql.PluginName)

	nsNotFoundErr := serviceerror.NewNamespaceNotFound(testNamespaceName.String())
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceID(testNamespaceName).Return(namespace.EmptyID, nsNotFoundErr)
	visStore.namespaceRegistry = nsRegistry

	_, err := visStore.ListExecutions(
		context.Background(),
		&manager.AdminListExecutionsRequest{Namespace: testNamespaceName, PageSize: 10},
	)
	r.ErrorIs(err, nsNotFoundErr)
}

func TestCountExecutions(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                  string
		nsName                namespace.Name
		expectNamespaceFilter bool
	}{
		{
			name:                  "single namespace",
			nsName:                testNamespaceName,
			expectNamespaceFilter: true,
		},
		{
			name:                  "all namespaces",
			nsName:                namespace.EmptyName,
			expectNamespaceFilter: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			ctrl := gomock.NewController(t)
			visStore := newTestVisibilityStore(t, ctrl, mysql.PluginName)

			if tc.nsName != namespace.EmptyName {
				nsRegistry := namespace.NewMockRegistry(ctrl)
				nsRegistry.EXPECT().GetNamespaceID(tc.nsName).Return(testNamespaceID, nil)
				visStore.namespaceRegistry = nsRegistry
			}

			visStore.sqlStore.DB.(*sqlplugin.MockDB).EXPECT().
				CountFromVisibility(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					filter sqlplugin.VisibilitySelectFilter,
				) (int64, error) {
					nsFilter := fmt.Sprintf("WHERE namespace_id = '%s'", testNamespaceID)
					if tc.expectNamespaceFilter {
						r.Contains(filter.Query, nsFilter)
					} else {
						r.NotContains(filter.Query, nsFilter)
					}
					return int64(42), nil
				})

			resp, err := visStore.CountExecutions(
				context.Background(),
				&manager.AdminCountExecutionsRequest{
					Namespace: tc.nsName,
					Query:     "ExecutionStatus = 'Completed'",
				},
			)
			r.NoError(err)
			r.Equal(&store.InternalCountExecutionsResponse{Count: 42}, resp)
		})
	}
}

func TestCountExecutions_NamespaceNotFound(t *testing.T) {
	t.Parallel()

	r := require.New(t)
	ctrl := gomock.NewController(t)
	visStore := newTestVisibilityStore(t, ctrl, mysql.PluginName)

	nsNotFoundErr := serviceerror.NewNamespaceNotFound(testNamespaceName.String())
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceID(testNamespaceName).Return(namespace.EmptyID, nsNotFoundErr)
	visStore.namespaceRegistry = nsRegistry

	_, err := visStore.CountExecutions(
		context.Background(),
		&manager.AdminCountExecutionsRequest{Namespace: testNamespaceName},
	)
	r.ErrorIs(err, nsNotFoundErr)
}

func newTestVisibilityStore(
	t *testing.T,
	ctrl *gomock.Controller,
	pluginName string,
) *VisibilityStore {
	t.Helper()
	mockDB := sqlplugin.NewMockDB(ctrl)
	mockDB.EXPECT().PluginName().Return(pluginName).AnyTimes()
	mockDB.EXPECT().DbName().Return("test-db").AnyTimes()
	return &VisibilityStore{
		sqlStore:                       sql.SqlStore{DB: mockDB},
		searchAttributesProvider:       searchattribute.NewTestProvider(),
		searchAttributesMapperProvider: searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{}),
		metricsHandler:                 metrics.NoopMetricsHandler,
		logger:                         log.NewNoopLogger(),
	}
}
