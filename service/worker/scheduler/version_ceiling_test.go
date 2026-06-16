package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
)

// Tests for the worker.schedulerVersionCeiling dynamic config, which clamps the recorded
// TweakablePolicies.Version (e.g. during a cross-version multi-cluster migration with a
// rollback window to an older binary).

// legacyTweakablePolicies mirrors the tweakables layout an older rollback peer reads: the field
// set before SpecFieldLengthLimit and EnableCHASMMigration were added. Concretely this is the
// v1.23.1 layout.
type legacyTweakablePolicies struct {
	DefaultCatchupWindow              time.Duration
	MinCatchupWindow                  time.Duration
	RetentionTime                     time.Duration
	CanceledTerminatedCountAsFailures bool
	AlwaysAppendTimestamp             bool
	FutureActionCount                 int
	RecentActionCount                 int
	FutureActionCountForList          int
	RecentActionCountForList          int
	IterationsBeforeContinueAsNew     int
	SleepWhilePaused                  bool
	MaxBufferSize                     int
	BackfillsPerIteration             int
	AllowZeroSleep                    bool
	ReuseTimer                        bool
	NextTimeCacheV2Size               int
	Version                           SchedulerWorkflowVersion
}

func TestClampVersion(t *testing.T) {
	v := SchedulerWorkflowVersion(TriggerImmediatelyTimestamp) // the current recorded version
	for _, c := range []struct {
		why     string
		ceiling int
		want    SchedulerWorkflowVersion
	}{
		{"negative is treated as unset, no clamp", -1, TriggerImmediatelyTimestamp},
		{"zero is unset, no clamp", 0, TriggerImmediatelyTimestamp},
		{"below current clamps down", 1, 1},
		{"clamp below the CHASM gate", int(UpdateFromPrevious), UpdateFromPrevious},
		{"equal to current is a no-op", int(TriggerImmediatelyTimestamp), TriggerImmediatelyTimestamp},
		{"above current is a no-op", int(TriggerImmediatelyTimestamp) + 1, TriggerImmediatelyTimestamp},
	} {
		require.Equalf(t, c.want, clampVersion(v, c.ceiling), "%s (ceiling=%d)", c.why, c.ceiling)
	}
}

// TestTweakablePoliciesJSONCompatibleWithLegacyLayout pins the wire shape of the "tweakables"
// MutableSideEffect marker against the legacy (older rollback peer) struct layout.
func TestTweakablePoliciesJSONCompatibleWithLegacyLayout(t *testing.T) {
	p := CurrentTweakablePolicies
	dc := converter.GetDefaultDataConverter()
	pl, err := dc.ToPayload(p)
	require.NoError(t, err)

	var old legacyTweakablePolicies
	require.NoError(t, dc.FromPayload(pl, &old))
	requireSharedFieldsEqual(t, p, old)
}

// TestTweakablePoliciesZeroFillNewFieldsOnFailback pins the reverse decode direction: after
// failover plus rollback an older peer records the legacy tweakables layout; when the namespace
// later fails back, this binary replays those markers and the newer fields decode as zero
// values, which must be safe.
func TestTweakablePoliciesZeroFillNewFieldsOnFailback(t *testing.T) {
	// Non-default values for every legacy field so each one provably round-trips.
	old := legacyTweakablePolicies{
		DefaultCatchupWindow:              100 * time.Hour,
		MinCatchupWindow:                  5 * time.Second,
		RetentionTime:                     10 * 24 * time.Hour,
		CanceledTerminatedCountAsFailures: true,
		AlwaysAppendTimestamp:             true,
		FutureActionCount:                 15,
		RecentActionCount:                 12,
		FutureActionCountForList:          8,
		RecentActionCountForList:          6,
		IterationsBeforeContinueAsNew:     100,
		SleepWhilePaused:                  true,
		MaxBufferSize:                     500,
		BackfillsPerIteration:             5,
		AllowZeroSleep:                    true,
		ReuseTimer:                        true,
		NextTimeCacheV2Size:               20,
		Version:                           DontTrackOverlapping,
	}

	dc := converter.GetDefaultDataConverter()
	pl, err := dc.ToPayload(old)
	require.NoError(t, err)

	var current TweakablePolicies
	require.NoError(t, dc.FromPayload(pl, &current))

	// Newer fields zero-fill. Safe today: SpecFieldLengthLimit is read only behind the
	// LimitMemoSpecSize(11) gate, unreachable at the older recorded version, and an absent
	// EnableCHASMMigration leaves migration disabled.
	require.Zero(t, current.SpecFieldLengthLimit)
	require.False(t, current.EnableCHASMMigration)

	requireSharedFieldsEqual(t, current, old)
}

// requireSharedFieldsEqual asserts the fields shared between the legacy layout and the current
// TweakablePolicies match, pinning the cross-version wire shape in one place.
func requireSharedFieldsEqual(t *testing.T, cur TweakablePolicies, old legacyTweakablePolicies) {
	t.Helper()
	require.Equal(t, old.DefaultCatchupWindow, cur.DefaultCatchupWindow)
	require.Equal(t, old.MinCatchupWindow, cur.MinCatchupWindow)
	require.Equal(t, old.RetentionTime, cur.RetentionTime)
	require.Equal(t, old.CanceledTerminatedCountAsFailures, cur.CanceledTerminatedCountAsFailures)
	require.Equal(t, old.AlwaysAppendTimestamp, cur.AlwaysAppendTimestamp)
	require.Equal(t, old.FutureActionCount, cur.FutureActionCount)
	require.Equal(t, old.RecentActionCount, cur.RecentActionCount)
	require.Equal(t, old.FutureActionCountForList, cur.FutureActionCountForList)
	require.Equal(t, old.RecentActionCountForList, cur.RecentActionCountForList)
	require.Equal(t, old.IterationsBeforeContinueAsNew, cur.IterationsBeforeContinueAsNew)
	require.Equal(t, old.SleepWhilePaused, cur.SleepWhilePaused)
	require.Equal(t, old.MaxBufferSize, cur.MaxBufferSize)
	require.Equal(t, old.BackfillsPerIteration, cur.BackfillsPerIteration)
	require.Equal(t, old.AllowZeroSleep, cur.AllowZeroSleep)
	require.Equal(t, old.ReuseTimer, cur.ReuseTimer)
	require.Equal(t, old.NextTimeCacheV2Size, cur.NextTimeCacheV2Size)
	require.Equal(t, old.Version, cur.Version)
}
