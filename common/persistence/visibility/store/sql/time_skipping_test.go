package sql

import (
	stdsql "database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

func TestTimeSkippingFlagNullSemantics(t *testing.T) {
	t.Parallel()
	for _, flag := range []struct {
		name string
		rows string
	}{
		{"TimeSkippingEnabled", "(null, null, null), (null, 'FF', false), (null, 'FT', false), (null, 'TF', true), (null, 'TT', true)"},
		{"TimeSkippingHasSkippedDuration", "(null, null, null), (null, 'FF', false), (null, 'FT', true), (null, 'TF', false), (null, 'TT', true)"},
	} {
		t.Run(flag.name, func(t *testing.T) {
			t.Parallel()
			db, err := stdsql.Open("sqlite", ":memory:")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			db.SetMaxOpenConns(1)
			for _, condition := range []string{
				"%s = true", "%s = false", "%s != true", "%s != false",
				"%s IN (true)", "%s NOT IN (true)", "%s NOT IN (false)",
				"%s IN (false, true)", "%s NOT IN (false, true)",
				"NOT (%s != true)", "NOT (%s NOT IN (true))",
				"NOT (%s NOT IN (false, true))", "NOT (NOT (%s != true))",
			} {
				t.Run(condition, func(t *testing.T) {
					t.Parallel()
					var results [][]string
					for _, name := range []string{"AliasForBool01", flag.name} {
						storeQC, err := NewSQLQueryConverter(sqlite.PluginName)
						require.NoError(t, err)
						qc := query.NewQueryConverter(storeQC, namespace.Name("test-namespace"),
							searchattribute.TestNameTypeMap(), &searchattribute.TestMapper{},
							metrics.NoopMetricsHandler, log.NewNoopLogger())
						params, err := qc.Convert(strings.ReplaceAll(condition, "%s", name))
						require.NoError(t, err)
						where, err := serializeSQLQuery(params.QueryExpr)
						require.NoError(t, err)
						rows, err := db.QueryContext(t.Context(),
							"WITH workflows (TemporalNamespaceDivision, TemporalTimeSkippingState, Bool01) AS (VALUES "+flag.rows+") "+
								"SELECT coalesce(TemporalTimeSkippingState, 'missing') FROM workflows WHERE "+where+" ORDER BY 1")
						require.NoError(t, err)
						states := []string{}
						for rows.Next() {
							var state string
							require.NoError(t, rows.Scan(&state))
							states = append(states, state)
						}
						require.NoError(t, rows.Err())
						require.NoError(t, rows.Close())
						results = append(results, states)
					}
					require.Equal(t, results[0], results[1], "packed flag must match an ordinary Boolean, including NULL")
				})
			}
		})
	}
}
