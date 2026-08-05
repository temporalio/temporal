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
		require.Equal(t, "10m0s", got.CatchupWindow)
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

	t.Run("delay threshold flags a slow schedule with no missed times", func(t *testing.T) {
		slow := Result{
			Namespace: "ns1", ScheduleID: "slow",
			Delays: []ActionDelay{{WorkflowID: "w1", DispatchDelay: 10 * time.Minute}},
		}
		emit := func(threshold time.Duration) string {
			var buf bytes.Buffer
			rw := NewRowWriter(&buf, threshold)
			_, err := rw.Write(slow)
			require.NoError(t, err)
			return buf.String()
		}
		require.Empty(t, emit(0), "threshold 0 flags on missed times only")
		require.Empty(t, emit(30*time.Minute), "dispatch delay below threshold is not flagged")
		require.NotEmpty(t, emit(5*time.Minute), "dispatch delay at/over threshold is flagged with no misses")
	})

	t.Run("delays are emitted on the row", func(t *testing.T) {
		miss := mustParseTime("2026-05-19T19:00:00Z")
		results := []Result{{
			Namespace: "ns1", ScheduleID: "s1",
			Delays: []ActionDelay{{
				WorkflowID:    "w1",
				Nominal:       mustParseTime("2026-05-19T19:00:00Z"),
				Actual:        mustParseTime("2026-05-19T19:00:00Z"),
				Desired:       mustParseTime("2026-05-19T19:00:00Z"),
				Start:         mustParseTime("2026-05-19T19:10:00Z"),
				DispatchDelay: 10 * time.Minute,
				E2EDelay:      10 * time.Minute,
			}},
			Missed: map[time.Time]string{miss: categoryRealMiss},
		}}
		var buf bytes.Buffer
		require.NoError(t, WriteFlat(&buf, results))
		var got row
		require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &got))
		require.Len(t, got.Delays, 1)
		require.Equal(t, "w1", got.Delays[0].WorkflowID)
		require.Equal(t, "10m0s", got.Delays[0].DispatchDelay)
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
