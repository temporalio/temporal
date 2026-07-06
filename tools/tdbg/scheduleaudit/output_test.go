package scheduleaudit

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestWriteFlat(t *testing.T) {
	t.Run("emits one self-contained row per flagged schedule", func(t *testing.T) {
		missTime := mustParseTime("2026-05-19T19:00:00Z")
		skipTime := mustParseTime("2026-05-19T20:00:00Z")
		closed := mustParseTime("2026-05-19T18:05:00Z")
		results := []Result{{
			Namespace:     "ns1",
			ScheduleID:    "s1",
			WorkflowType:  "Foo,Bar",
			WindowStart:   mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:     mustParseTime("2026-05-19T22:00:00Z"),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			CatchupWindow: 10 * time.Minute,
			Expected:      5, Actual: 4, Matched: 3,
			Scheduled: []time.Time{missTime, skipTime},
			Observed: []Execution{{
				WorkflowID: "w1", RunID: "r1",
				StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
				CloseTime:   &closed,
				Status:      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
			}},
			Missed: map[time.Time]string{
				missTime: categoryRealMiss,
				skipTime: categorySkipOverlap,
			},
		}}
		var buf bytes.Buffer
		require.NoError(t, WriteFlat(&buf, results))
		lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
		require.Len(t, lines, 1)

		var got row
		require.NoError(t, json.Unmarshal([]byte(lines[0]), &got))
		require.Equal(t, "ns1", got.Namespace)
		require.Equal(t, "Foo,Bar", got.WorkflowType) // comma preserved (no CSV escaping)
		require.Equal(t, mustParseTime("2026-05-19T18:00:00Z"), got.Window.Start)
		require.Equal(t, mustParseTime("2026-05-19T22:00:00Z"), got.Window.End)
		require.Equal(t, "Skip", got.OverlapPolicy)
		require.Equal(t, "drops", got.OverlapClass)
		require.Equal(t, int64(600), got.CatchupWindowSecond)
		require.Equal(t, 2, got.Missed)
		require.Equal(t, 1, got.Counts.RealMiss)
		require.Equal(t, 1, got.Counts.SkipOverlap)
		require.Equal(t, []missRow{
			{Nominal: missTime, Category: categoryRealMiss},
			{Nominal: skipTime, Category: categorySkipOverlap},
		}, got.Misses)
		require.Len(t, got.Observed, 1)
		require.Equal(t, "Completed", got.Observed[0].Status)
		require.Equal(t, "w1", got.Observed[0].WorkflowID)
		require.NotNil(t, got.Observed[0].Close)
	})

	t.Run("skips results with no missed times", func(t *testing.T) {
		results := []Result{{Namespace: "ns1", ScheduleID: "perfect", Expected: 5, Matched: 5}}
		var buf bytes.Buffer
		require.NoError(t, WriteFlat(&buf, results))
		require.Empty(t, buf.String())
	})

	t.Run("running execution renders close as null", func(t *testing.T) {
		miss := mustParseTime("2026-05-19T19:00:00Z")
		results := []Result{{
			Namespace: "ns1", ScheduleID: "s1",
			Observed: []Execution{{
				WorkflowID:  "w1",
				StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
				Status:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
			}},
			Missed: map[time.Time]string{miss: categoryRealMiss},
		}}
		var buf bytes.Buffer
		require.NoError(t, WriteFlat(&buf, results))
		require.Contains(t, buf.String(), `"close":null`)
		require.Contains(t, buf.String(), `"status":"Running"`)
	})
}
