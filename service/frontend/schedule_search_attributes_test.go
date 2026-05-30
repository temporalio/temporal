package frontend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

func keywordPayload(t *testing.T, v string) *commonpb.Payload {
	t.Helper()
	return chasm.VisibilityValueKeyword(v).MustEncode()
}

// TestMergeChasmSearchAttributes covers the field-mapping merge that folds a CHASM execution's
// separately-stored (already-aliased) CHASM search attributes into the entry's custom search
// attributes for ListSchedules.
func TestMergeChasmSearchAttributes(t *testing.T) {
	t.Parallel()

	nextActionTime := time.Date(2026, 5, 30, 0, 44, 0, 0, time.UTC)

	t.Run("no chasm fields returns custom unchanged", func(t *testing.T) {
		custom := map[string]*commonpb.Payload{"CustomKeyword": keywordPayload(t, "v")}
		got := mergeChasmSearchAttributes(custom, chasm.NewSearchAttributesMap(nil))
		require.Equal(t, custom, got)
	})

	t.Run("nil custom with chasm fields", func(t *testing.T) {
		chasmSAs := chasm.NewSearchAttributesMap(map[string]chasm.VisibilityValue{
			chasmscheduler.ScheduleNextActionTimeName: chasm.VisibilityValueTime(nextActionTime),
		})
		got := mergeChasmSearchAttributes(nil, chasmSAs)
		require.Len(t, got, 1)
		require.Contains(t, got, chasmscheduler.ScheduleNextActionTimeName)
	})

	t.Run("merges chasm into custom", func(t *testing.T) {
		custom := map[string]*commonpb.Payload{"CustomKeyword": keywordPayload(t, "v")}
		chasmSAs := chasm.NewSearchAttributesMap(map[string]chasm.VisibilityValue{
			chasmscheduler.ScheduleNextActionTimeName: chasm.VisibilityValueTime(nextActionTime),
			sadefs.ExecutionStatus:                    chasm.VisibilityValueKeyword("Running"),
		})

		got := mergeChasmSearchAttributes(custom, chasmSAs)

		require.Len(t, got, 3)
		require.Contains(t, got, "CustomKeyword")
		require.Contains(t, got, chasmscheduler.ScheduleNextActionTimeName)
		require.Contains(t, got, sadefs.ExecutionStatus)
		// Values are re-encoded from the CHASM map.
		require.Equal(t,
			chasm.VisibilityValueTime(nextActionTime).MustEncode().GetData(),
			got[chasmscheduler.ScheduleNextActionTimeName].GetData())

		// Inputs are not mutated.
		require.Len(t, custom, 1)
	})

	t.Run("chasm value wins on key collision", func(t *testing.T) {
		custom := map[string]*commonpb.Payload{
			sadefs.ExecutionStatus: keywordPayload(t, "Completed"),
		}
		chasmSAs := chasm.NewSearchAttributesMap(map[string]chasm.VisibilityValue{
			sadefs.ExecutionStatus: chasm.VisibilityValueKeyword("Running"),
		})

		got := mergeChasmSearchAttributes(custom, chasmSAs)

		require.Len(t, got, 1)
		require.Equal(t,
			keywordPayload(t, "Running").GetData(),
			got[sadefs.ExecutionStatus].GetData())
		// Original custom map untouched.
		require.Equal(t, keywordPayload(t, "Completed").GetData(), custom[sadefs.ExecutionStatus].GetData())
	})
}

// TestScheduleSearchAttributes_MergeThenClean verifies the end-to-end field-mapping behavior of a
// ListSchedules entry: CHASM search attributes (ScheduleNextActionTime, ExecutionStatus) are
// surfaced, while cleanScheduleSearchAttributes still strips the internal ones that should not be
// shown to clients (TemporalSchedulePaused, TemporalNamespaceDivision, BuildIds).
func TestScheduleSearchAttributes_MergeThenClean(t *testing.T) {
	t.Parallel()

	nextActionTime := time.Date(2026, 5, 30, 0, 44, 0, 0, time.UTC)

	custom := map[string]*commonpb.Payload{
		"CustomKeyword":                  keywordPayload(t, "v"),
		sadefs.TemporalSchedulePaused:    chasm.VisibilityValueBool(false).MustEncode(),
		sadefs.TemporalNamespaceDivision: keywordPayload(t, "TemporalScheduler"),
		sadefs.BuildIds:                  keywordPayload(t, "unversioned"),
	}
	chasmSAs := chasm.NewSearchAttributesMap(map[string]chasm.VisibilityValue{
		chasmscheduler.ScheduleNextActionTimeName: chasm.VisibilityValueTime(nextActionTime),
		sadefs.ExecutionStatus:                    chasm.VisibilityValueKeyword("Running"),
	})

	merged := mergeChasmSearchAttributes(custom, chasmSAs)

	wh := &WorkflowHandler{}
	cleaned := wh.cleanScheduleSearchAttributes(&commonpb.SearchAttributes{IndexedFields: merged})

	fields := cleaned.GetIndexedFields()
	// Surfaced to the client.
	require.Contains(t, fields, chasmscheduler.ScheduleNextActionTimeName)
	require.Contains(t, fields, sadefs.ExecutionStatus)
	require.Contains(t, fields, "CustomKeyword")
	// Stripped by cleanScheduleSearchAttributes.
	require.NotContains(t, fields, sadefs.TemporalSchedulePaused)
	require.NotContains(t, fields, sadefs.TemporalNamespaceDivision)
	require.NotContains(t, fields, sadefs.BuildIds)
}
