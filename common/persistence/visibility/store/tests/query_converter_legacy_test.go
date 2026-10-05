package tests

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	vissql "go.temporal.io/server/common/persistence/visibility/store/sql"
	"go.temporal.io/server/common/searchattribute"
)

const (
	testNamespaceID = namespace.ID("test-namespace-id")
	testPageSize    = 10
)

// legacyDivergence describes a query from queryConverterTestCases that the legacy query
// converter accepts or rejects differently from the unified one. The legacy converters build
// the whole statement (SQL) or leave the namespace filters to the store (Elasticsearch), so
// their output can't be compared against the expectations in queryConverterTestCases; only
// whether the query is accepted is.
type legacyDivergence struct {
	// accepted is whether the legacy query converter accepts the query, and plugins
	// overrides it for the SQL plugins that behave differently.
	accepted bool
	plugins  map[string]bool
	// why documents why the legacy query converter behaves differently.
	why string
}

func (d legacyDivergence) isAccepted(store string) bool {
	if accepted, ok := d.plugins[store]; ok {
		return accepted
	}
	return d.accepted
}

// legacySQLDivergences maps a test case name from queryConverterTestCases to how the legacy
// SQL query converter handles it. Every entry is a behavior change intentionally introduced
// by the unified query converter: queries that used to be silently accepted are now validated,
// and queries that used to be rejected are now supported.
var legacySQLDivergences = map[string]legacyDivergence{
	// The legacy query converter doesn't validate the value against the search attribute type.
	"Keyword invalid value type": {
		accepted: true,
		why:      "legacy doesn't validate the value type of Keyword search attributes",
	},
	"Keyword with bool value": {
		accepted: true,
		why:      "legacy doesn't validate the value type of Keyword search attributes",
	},
	"KeywordList invalid value type": {
		accepted: true,
		// SQLite requires a string to build the full text search query.
		plugins: map[string]bool{sqliteStore: false},
		why:     "legacy doesn't validate the value type of KeywordList search attributes",
	},
	"Int invalid value type": {
		accepted: true,
		why:      "legacy doesn't validate the value type of Int search attributes",
	},
	"Int mixed types in list": {
		accepted: true,
		why:      "legacy doesn't validate the value type of the items of a list",
	},
	"Double invalid value type": {
		accepted: true,
		why:      "legacy doesn't validate the value type of Double search attributes",
	},
	"Bool invalid value type": {
		accepted: true,
		why:      "legacy doesn't validate the value type of Bool search attributes",
	},
	"Datetime with bool value": {
		accepted: true,
		why:      "legacy doesn't validate the value type of Datetime search attributes",
	},
	"ExecutionStatus invalid code": {
		accepted: true,
		why:      "legacy doesn't validate that the ExecutionStatus code is a valid enum value",
	},
	"ExecutionDuration with bool value": {
		accepted: true,
		why:      "legacy doesn't validate the value type of ExecutionDuration",
	},
	"Text invalid value type": {
		accepted: true,
		// PostgreSQL and SQLite require a string to build the full text search query.
		plugins: map[string]bool{postgresStore: false, sqliteStore: false},
		why:     "legacy doesn't validate the value type of Text search attributes",
	},
	"Text empty value": {
		accepted: true,
		// PostgreSQL and SQLite tokenize the value to build the full text search query.
		plugins: map[string]bool{postgresStore: false, sqliteStore: false},
		why:     "legacy only rejects a blank Text value on the stores that tokenize it",
	},
	"Text blank value": {
		accepted: true,
		plugins:  map[string]bool{postgresStore: false, sqliteStore: false},
		why:      "legacy only rejects a blank Text value on the stores that tokenize it",
	},

	// The legacy query converter rejects ORDER BY for every SQL plugin. The unified query
	// converter parses it, and the SQL visibility store rejects it instead.
	"order by": {
		why: "legacy rejects 'ORDER BY' in the query converter instead of the store",
	},
	"order by desc": {
		why: "legacy rejects 'ORDER BY' in the query converter instead of the store",
	},
	"order by multiple search attributes": {
		why: "legacy rejects 'ORDER BY' in the query converter instead of the store",
	},
	"order by with where clause": {
		why: "legacy rejects 'ORDER BY' in the query converter instead of the store",
	},
	"order by custom search attribute": {
		why: "legacy rejects 'ORDER BY' in the query converter instead of the store",
	},
}

func newLegacyQueryConverter(plugin string, queryString string) *vissql.QueryConverterLegacy {
	return vissql.NewQueryConverterLegacy(
		plugin,
		testNamespaceName,
		testNamespaceID,
		searchattribute.TestNameTypeMap(),
		&searchattribute.TestMapper{},
		queryString,
		nil, // chasmMapper
		chasm.UnspecifiedArchetypeID,
	)
}

func TestSQLQueryConverterLegacy(t *testing.T) {
	t.Parallel()
	for _, plugin := range sqlPlugins {
		t.Run(plugin, func(t *testing.T) {
			t.Parallel()
			for _, tc := range queryConverterTestCases {
				t.Run(tc.name, func(t *testing.T) {
					r := require.New(t)

					// The legacy query converter output can't be compared against the
					// expectations in queryConverterTestCases, so the test only asserts that
					// both query converters accept the same queries, except for the
					// differences listed in legacySQLDivergences.
					_, unifiedErr := tc.expected(plugin)
					accepted := unifiedErr == ""
					reason := "unified query converter and legacy query converter must agree"
					if divergence, ok := legacySQLDivergences[tc.name]; ok {
						accepted = divergence.isAccepted(plugin)
						reason = divergence.why
					}

					// Count queries support the GROUP BY clause.
					countFilter, err := newLegacyQueryConverter(plugin, tc.in).BuildCountStmt()
					assertLegacyOutcome(r, countFilter, err, accepted, reason)
					if accepted {
						r.Equal(tc.groupBy, countFilter.GroupBy)
					}

					// List queries don't support the GROUP BY clause.
					selectFilter, err := newLegacyQueryConverter(plugin, tc.in).
						BuildSelectStmt(testPageSize, nil)
					if accepted && len(tc.groupBy) > 0 {
						r.Error(err)
						r.EqualError(err, "operation is not supported: 'GROUP BY' clause")
						r.Nil(selectFilter)
						return
					}
					assertLegacyOutcome(r, selectFilter, err, accepted, reason)
				})
			}
		})
	}
}

// TestSQLQueryConverterLegacyDivergences keeps legacySQLDivergences honest: an entry that
// names a test case that no longer exists, or that no longer differs from the unified query
// converter, silently weakens TestSQLQueryConverterLegacy.
func TestSQLQueryConverterLegacyDivergences(t *testing.T) {
	t.Parallel()
	assertDivergencesAreCurrent(t, "legacySQLDivergences", legacySQLDivergences, sqlPlugins)
}

func assertDivergencesAreCurrent(
	t *testing.T,
	varName string,
	divergences map[string]legacyDivergence,
	stores []string,
) {
	t.Helper()
	r := require.New(t)

	testCases := make(map[string]queryConverterTestCase, len(queryConverterTestCases))
	for _, tc := range queryConverterTestCases {
		testCases[tc.name] = tc
	}

	for name, divergence := range divergences {
		tc, ok := testCases[name]
		r.True(ok, "%s has an entry for unknown test case %q", varName, name)
		r.NotEmpty(divergence.why, "%s entry %q must document why", varName, name)

		diverges := false
		for _, store := range stores {
			_, unifiedErr := tc.expected(store)
			diverges = diverges || divergence.isAccepted(store) != (unifiedErr == "")
		}
		r.True(
			diverges,
			"%s entry %q agrees with the unified query converter for every store: remove it",
			varName,
			name,
		)
	}
}

// legacyESDivergences maps a test case name from queryConverterTestCases to how the legacy
// Elasticsearch query converter handles it. Every entry is a behavior change intentionally
// introduced by the unified query converter: queries that used to be silently accepted are
// now validated, and queries that used to be rejected are now supported.
var legacyESDivergences = map[string]legacyDivergence{
	// The legacy query converter doesn't validate the value against the search attribute type.
	"Keyword invalid value type": {
		accepted: true,
		why:      "legacy doesn't validate the value type of Keyword search attributes",
	},
	"Keyword with bool value": {
		accepted: true,
		why:      "legacy doesn't validate the value type of Keyword search attributes",
	},
	"KeywordList invalid value type": {
		accepted: true,
		why:      "legacy doesn't validate the value type of KeywordList search attributes",
	},
	"Text invalid value type": {
		accepted: true,
		why:      "legacy doesn't validate the value type of Text search attributes",
	},
	"Text empty value": {
		accepted: true,
		why:      "legacy doesn't reject a blank Text value",
	},
	"Text blank value": {
		accepted: true,
		why:      "legacy doesn't reject a blank Text value",
	},
	"ExecutionStatus invalid name": {
		accepted: true,
		why:      "legacy doesn't validate that the ExecutionStatus name is a valid enum value",
	},
	"ExecutionStatus invalid value in list": {
		accepted: true,
		why:      "legacy doesn't validate that the ExecutionStatus name is a valid enum value",
	},
	"ExecutionStatus invalid value type": {
		accepted: true,
		why:      "legacy doesn't validate the value type of ExecutionStatus",
	},

	// The legacy query converter doesn't validate the operator against the search attribute
	// type, and relies on Elasticsearch to reject the query instead.
	"KeywordList unsupported operator": {
		accepted: true,
		why:      "legacy doesn't validate the operator of KeywordList search attributes",
	},
	"KeywordList starts with not supported": {
		accepted: true,
		why:      "legacy doesn't validate the operator of KeywordList search attributes",
	},
	"Text unsupported operator": {
		accepted: true,
		why:      "legacy doesn't validate the operator of Text search attributes",
	},
	"Text in not supported": {
		accepted: true,
		why:      "legacy doesn't validate the operator of Text search attributes",
	},
	"Text range condition not supported": {
		accepted: true,
		why:      "legacy doesn't validate the type of a range condition",
	},

	// The legacy query converter doesn't support the NOT operator.
	"not expression": {
		why: "legacy doesn't support the 'NOT' operator",
	},
	"not parenthesized and expression": {
		why: "legacy doesn't support the 'NOT' operator",
	},
	"not parenthesized or expression": {
		why: "legacy doesn't support the 'NOT' operator",
	},
	"not is null expression": {
		why: "legacy doesn't support the 'NOT' operator",
	},
	"complex query": {
		why: "legacy doesn't support the 'NOT' operator",
	},
}

func newLegacyESQueryConverter() *query.ConverterLegacy {
	saTypeMap := searchattribute.TestNameTypeMap()
	return elasticsearch.NewQueryConverterLegacy(
		elasticsearch.NewNameInterceptor(
			testNamespaceName,
			saTypeMap,
			searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{}),
			nil, // chasmMapper
			chasm.UnspecifiedArchetypeID,
		),
		elasticsearch.NewValuesInterceptor(
			testNamespaceName,
			saTypeMap,
			nil, // chasmMapper
			metrics.NoopMetricsHandler,
			log.NewNoopLogger(),
		),
		saTypeMap,
		nil, // chasmMapper
	)
}

func TestElasticsearchQueryConverterLegacy(t *testing.T) {
	t.Parallel()
	for _, tc := range queryConverterTestCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)

			// The legacy query converter doesn't add the namespace and namespace division
			// filters, which the visibility store adds instead, so its output can't be
			// compared against the expectations in queryConverterTestCases. The test only
			// asserts that both query converters accept the same queries, except for the
			// differences listed in legacyESDivergences.
			_, unifiedErr := tc.expected(esStore)
			accepted := unifiedErr == ""
			reason := "unified query converter and legacy query converter must agree"
			if divergence, ok := legacyESDivergences[tc.name]; ok {
				accepted = divergence.isAccepted(esStore)
				reason = divergence.why
			}

			queryParams, err := newLegacyESQueryConverter().ConvertWhereOrderBy(tc.in)
			if !accepted {
				r.Error(err, reason)
				// The visibility store converts a ConverterError into an InvalidArgument
				// and returns any other error as is, so a rejected query must fail with
				// one of the two to reach the caller as an invalid argument.
				var converterErr *query.ConverterError
				var invalidArgumentErr *serviceerror.InvalidArgument
				r.True(
					errors.As(err, &converterErr) || errors.As(err, &invalidArgumentErr),
					"expected a ConverterError or an InvalidArgument, got %T: %v",
					err,
					err,
				)
				r.Nil(queryParams)
				return
			}
			r.NoError(err, reason)

			// Unlike the SQL stores, Elasticsearch supports both clauses.
			r.Equal(tc.groupBy, queryParams.GroupBy)
			if tc.orderBy == "" {
				r.Empty(queryParams.Sorter)
			} else {
				r.NotEmpty(queryParams.Sorter)
			}

			if hasFilter(tc.in) {
				r.NotNil(queryParams.Query)
			} else {
				r.Nil(queryParams.Query)
			}
		})
	}
}

// TestElasticsearchQueryConverterLegacyDivergences keeps legacyESDivergences honest: an entry
// that names a test case that no longer exists, or that no longer differs from the unified
// query converter, silently weakens TestElasticsearchQueryConverterLegacy.
func TestElasticsearchQueryConverterLegacyDivergences(t *testing.T) {
	t.Parallel()
	assertDivergencesAreCurrent(t, "legacyESDivergences", legacyESDivergences, []string{esStore})
}

// hasFilter returns whether the query has a filtering clause, following the same rule the
// query converters use to tell a filter from a standalone GROUP BY or ORDER BY clause.
func hasFilter(queryString string) bool {
	queryString = strings.ToLower(strings.TrimSpace(queryString))
	return queryString != "" &&
		!strings.HasPrefix(queryString, "group by") &&
		!strings.HasPrefix(queryString, "order by")
}

func assertLegacyOutcome(
	r *require.Assertions,
	filter *sqlplugin.VisibilitySelectFilter,
	err error,
	accepted bool,
	reason string,
) {
	if !accepted {
		r.Error(err, reason)
		var converterErr *query.ConverterError
		r.ErrorAs(err, &converterErr)
		r.Nil(filter)
		return
	}
	r.NoError(err, reason)
	r.NotEmpty(filter.Query)
}
