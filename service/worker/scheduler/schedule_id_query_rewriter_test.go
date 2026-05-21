package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute"
)

// TestMapper only processes "test-namespace"; use it so custom SA lookups resolve correctly.
var testNS = namespace.Name("test-namespace")

// emptyNameTypeMap has no custom SAs — ScheduleId is synthetic.
var emptyNameTypeMap = searchattribute.NewNameTypeMap(nil)

// customScheduleIDNameTypeMap simulates a namespace that registered ScheduleId as a custom SA.
var customScheduleIDNameTypeMap = searchattribute.NewNameTypeMap(map[string]enumspb.IndexedValueType{
	"ScheduleId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
})

func TestRewriteScheduleIDQuery(t *testing.T) {
	t.Parallel()

	prefix := primitives.ScheduleWorkflowIDPrefix

	tests := []struct {
		name         string
		query        string
		chasmEnabled bool
		mapper       searchattribute.Mapper
		saNameType   *searchattribute.NameTypeMap // nil means emptyNameTypeMap
		want         string
	}{
		// Empty / no-op cases.
		{
			name:  "empty query",
			query: "",
			want:  "",
		},
		{
			// ScheduleId is rewritten and GROUP BY is preserved so that when GROUP BY support
			// is added to prepareSchedulerQuery the rewrite is already in place.
			name:         "CHASM ScheduleId with GROUP BY rewrites WHERE preserves GROUP BY",
			query:        "ScheduleId = 'my-sched' Group By TemporalSchedulePaused",
			chasmEnabled: true,
			want:         "(WorkflowId = '" + prefix + "my-sched' or WorkflowId = 'my-sched') group by TemporalSchedulePaused",
		},
		{
			name:  "whitespace query",
			query: "   ",
			want:  "   ",
		},
		{
			name:  "no ScheduleId no rewrite",
			query: "ExecutionStatus = 'Running'",
			want:  "ExecutionStatus = 'Running'",
		},

		// V1 (chasmEnabled=false) — single-value operators.
		{
			name:         "V1 equal",
			query:        "ScheduleId = 'my-sched'",
			chasmEnabled: false,
			want:         "WorkflowId = '" + prefix + "my-sched'",
		},
		{
			name:         "V1 not equal",
			query:        "ScheduleId != 'my-sched'",
			chasmEnabled: false,
			want:         "WorkflowId != '" + prefix + "my-sched'",
		},
		{
			name:         "V1 starts with",
			query:        "ScheduleId STARTS_WITH 'my-'",
			chasmEnabled: false,
			want:         "WorkflowId starts_with '" + prefix + "my-'",
		},
		{
			name:         "V1 not starts with",
			query:        "ScheduleId NOT STARTS_WITH 'my-'",
			chasmEnabled: false,
			want:         "WorkflowId not starts_with '" + prefix + "my-'",
		},
		{
			name:         "V1 IN",
			query:        "ScheduleId IN ('foo', 'bar')",
			chasmEnabled: false,
			want:         "WorkflowId in ('" + prefix + "foo', '" + prefix + "bar')",
		},
		{
			name:         "V1 NOT IN",
			query:        "ScheduleId NOT IN ('foo', 'bar')",
			chasmEnabled: false,
			want:         "WorkflowId not in ('" + prefix + "foo', '" + prefix + "bar')",
		},

		// V1 — reserved TemporalScheduleId alias.
		{
			name:         "V1 TemporalScheduleId alias",
			query:        "TemporalScheduleId = 'my-sched'",
			chasmEnabled: false,
			want:         "WorkflowId = '" + prefix + "my-sched'",
		},

		// V1 — IS NOT NULL (no prefix, just column rename).
		{
			name:         "V1 IS NOT NULL",
			query:        "ScheduleId IS NOT NULL",
			chasmEnabled: false,
			want:         "WorkflowId is not null",
		},

		// CHASM (chasmEnabled=true) — positive operators produce OR.
		{
			name:         "CHASM equal OR",
			query:        "ScheduleId = 'my-sched'",
			chasmEnabled: true,
			want:         "(WorkflowId = '" + prefix + "my-sched' or WorkflowId = 'my-sched')",
		},
		{
			name:         "CHASM TemporalScheduleId alias OR",
			query:        "TemporalScheduleId = 'my-sched'",
			chasmEnabled: true,
			want:         "(WorkflowId = '" + prefix + "my-sched' or WorkflowId = 'my-sched')",
		},
		{
			name:         "CHASM starts with OR",
			query:        "ScheduleId STARTS_WITH 'my-'",
			chasmEnabled: true,
			want:         "(WorkflowId starts_with '" + prefix + "my-' or WorkflowId starts_with 'my-')",
		},
		{
			name:         "CHASM IN OR",
			query:        "ScheduleId IN ('foo', 'bar')",
			chasmEnabled: true,
			want:         "(WorkflowId in ('" + prefix + "foo', '" + prefix + "bar') or WorkflowId in ('foo', 'bar'))",
		},

		// CHASM — negative operators produce AND.
		{
			name:         "CHASM not equal AND",
			query:        "ScheduleId != 'my-sched'",
			chasmEnabled: true,
			want:         "(WorkflowId != '" + prefix + "my-sched' and WorkflowId != 'my-sched')",
		},
		{
			name:         "CHASM not starts with AND",
			query:        "ScheduleId NOT STARTS_WITH 'my-'",
			chasmEnabled: true,
			want:         "(WorkflowId not starts_with '" + prefix + "my-' and WorkflowId not starts_with 'my-')",
		},
		{
			name:         "CHASM NOT IN AND",
			query:        "ScheduleId NOT IN ('foo', 'bar')",
			chasmEnabled: true,
			want:         "(WorkflowId not in ('" + prefix + "foo', '" + prefix + "bar') and WorkflowId not in ('foo', 'bar'))",
		},

		// CHASM — IS NOT NULL (just column rename, no OR/AND needed).
		{
			name:         "CHASM IS NOT NULL",
			query:        "ScheduleId IS NOT NULL",
			chasmEnabled: true,
			want:         "WorkflowId is not null",
		},

		// ScheduleId combined with another filter.
		{
			name:         "CHASM combined AND with other filter",
			query:        "ScheduleId = 'my-sched' AND TemporalSchedulePaused = true",
			chasmEnabled: true,
			want:         "(WorkflowId = '" + prefix + "my-sched' or WorkflowId = 'my-sched') and TemporalSchedulePaused = true",
		},

		// Custom SA named ScheduleId with explicit alias mapping (check 1: mapper).
		{
			name:         "custom SA with alias mapping not rewritten",
			query:        "ScheduleId = 'my-sched'",
			chasmEnabled: true,
			mapper:       &searchattribute.TestMapper{WithCustomScheduleID: true},
			want:         "ScheduleId = 'my-sched'",
		},

		// Custom SA named ScheduleId registered in the type map without alias (check 2: type map).
		// This is the common case when a user adds ScheduleId via AddSearchAttributes.
		{
			name:         "custom SA in type map not rewritten",
			query:        "ScheduleId = 'my-sched'",
			chasmEnabled: true,
			saNameType:   &customScheduleIDNameTypeMap,
			want:         "ScheduleId = 'my-sched'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mapper := tc.mapper
			if mapper == nil {
				mapper = &searchattribute.NoopMapper{}
			}
			saNameType := emptyNameTypeMap
			if tc.saNameType != nil {
				saNameType = *tc.saNameType
			}
			got, err := RewriteScheduleIDQuery(tc.query, tc.chasmEnabled, mapper, saNameType, testNS)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
