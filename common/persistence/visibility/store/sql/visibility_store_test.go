package sql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/searchattribute"
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
			err:   "invalid expression: column name 'AliasForFoo' is not a valid search attribute",
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

				qp, err := buildQueryParams(
					testNamespaceID,
					testNamespaceName,
					tc.query,
					sqlQC,
					searchattribute.TestNameTypeMap(),
					&searchattribute.TestMapper{},
					nil,
					chasm.UnspecifiedArchetypeID,
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
