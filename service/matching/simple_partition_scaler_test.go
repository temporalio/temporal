package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/number"
)

// encodeCounts builds a Compact8-encoded backlog-count slice from raw values,
// matching the on-the-wire form updateBacklogTarget consumes.
func encodeCounts(values ...int64) []byte {
	b := make([]byte, len(values))
	for i, v := range values {
		b[i] = number.EncodeCompact8(v)
	}
	return b
}

// TestUpdateBacklogTargetDisabled verifies that backlog-based scaling is off
// (returns 0, no bits touched) unless both BacklogBase and BacklogReset are set.
func TestUpdateBacklogTargetDisabled(t *testing.T) {
	t.Parallel()
	cases := []dynamicconfig.SimplePartitionScalerSettings{
		{},                                  // both zero
		{BacklogBase: 300},                  // reset unset
		{BacklogReset: 100},                 // base unset
		{BacklogBase: 300, BacklogReset: 0}, // reset explicitly zero
	}
	for _, cfg := range cases {
		var bs bitSet
		counts := encodeCounts(1000, 1000, 1000)
		target := updateBacklogTarget(cfg, counts, &bs)
		require.Zero(t, target)
		require.Zero(t, bs.len(), "bitset must not be modified while disabled")
	}
}

// TestUpdateBacklogTargetSetsBitsAboveBase verifies that partitions whose
// backlog exceeds BacklogBase count toward the target, and the corresponding
// bits are recorded in the private bitset.
func TestUpdateBacklogTargetSetsBitsAboveBase(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{BacklogReset: 100, BacklogBase: 300}

	var bs bitSet
	// p0 well above base, p1 below reset, p2 above base.
	counts := encodeCounts(500, 32, 500)
	target := updateBacklogTarget(cfg, counts, &bs)

	require.Equal(t, 2, target, "two partitions above base should count toward target")
	require.True(t, bs.get(0))
	require.False(t, bs.get(1))
	require.True(t, bs.get(2))
}

// TestUpdateBacklogTargetHysteresis verifies the dead zone between BacklogReset
// and BacklogBase: a set bit stays set and a clear bit stays clear when the
// count lands between the two thresholds.
func TestUpdateBacklogTargetHysteresis(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{BacklogReset: 100, BacklogBase: 300}

	// p0 starts set, p1 starts clear. Both get a count of ~200 (between
	// reset=100 and base=300), so neither should flip.
	var bs bitSet
	bs = bs.set(0)
	midCount := number.DecodeCompact8(number.EncodeCompact8(200))
	require.Greater(t, midCount, int64(100))
	require.Less(t, midCount, int64(300))

	counts := encodeCounts(200, 200)
	target := updateBacklogTarget(cfg, counts, &bs)

	require.Equal(t, 1, target, "only the already-set partition counts")
	require.True(t, bs.get(0), "set bit stays set in dead zone")
	require.False(t, bs.get(1), "clear bit stays clear in dead zone")
}

// TestUpdateBacklogTargetClearsBelowReset verifies that a previously-set bit is
// cleared once its backlog drops below BacklogReset.
func TestUpdateBacklogTargetClearsBelowReset(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{BacklogReset: 100, BacklogBase: 300}

	var bs bitSet
	bs = bs.set(0).set(1)

	// p0 drops below reset (cleared), p1 stays in the dead zone (kept).
	counts := encodeCounts(32, 200)
	target := updateBacklogTarget(cfg, counts, &bs)

	require.Equal(t, 1, target)
	require.False(t, bs.get(0), "bit cleared once below reset")
	require.True(t, bs.get(1), "bit in dead zone retained")
}

// TestOnTasksFixedIncludesBacklogCap verifies the fixed-target fast path now
// propagates BacklogCap into the decision.
func TestOnTasksFixedIncludesBacklogCap(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{
		Enabled:    true,
		Fixed:      2,
		BacklogCap: 1000,
	}
	scaler := newSimplePartitionScaler(
		dynamicconfig.GetTypedPropertyFn(cfg),
		nil, // time source unused on the fixed path
	)
	decision := scaler.OnTasks(PartitionScalerInput{CurrentTarget: 1})
	require.Equal(t, 2, decision.NewTarget)
	require.Equal(t, 1000, decision.BacklogCap)
}

// TestOnTasksFloorsAddTargetAtOne verifies that with no rate windows configured
// the add-based target is floored at 1 (never 0, which would disable scaling).
// This baseline is what lets backlog-based scaling grow.
func TestOnTasksFloorsAddTargetAtOne(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{Enabled: true}
	scaler := newSimplePartitionScaler(dynamicconfig.GetTypedPropertyFn(cfg), nil)

	decision := scaler.OnTasks(PartitionScalerInput{CurrentTarget: 0})
	require.Equal(t, 1, decision.NewTarget, "add baseline must floor at 1, not disable scaling")
}

// TestOnTasksBacklogScalesUpAndDown verifies that with no rate windows, backlog
// pressure grows the target one partition at a time (baseline 1 + hot count) and
// shrinks back to the baseline once partitions drain below BacklogReset.
func TestOnTasksBacklogScalesUpAndDown(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{
		Enabled:      true,
		BacklogReset: 100,
		BacklogBase:  300,
		BacklogCap:   1000,
		Max:          4,
	}
	scaler := newSimplePartitionScaler(dynamicconfig.GetTypedPropertyFn(cfg), nil)

	// One partition, hot: baseline 1 + 1 hot = 2.
	d := scaler.OnTasks(PartitionScalerInput{CurrentTarget: 1, BacklogCounts: encodeCounts(500)})
	require.Equal(t, 2, d.NewTarget)

	// Two partitions, both hot: baseline 1 + 2 hot = 3.
	d = scaler.OnTasks(PartitionScalerInput{
		CurrentTarget: 2,
		BacklogCounts: encodeCounts(500, 500),
		PrivateState:  d.PrivateState,
	})
	require.Equal(t, 3, d.NewTarget)

	// A newly-opened partition that is not yet hot does not add more capacity:
	// baseline 1 + 2 hot = 3 (unchanged).
	d = scaler.OnTasks(PartitionScalerInput{
		CurrentTarget: 3,
		BacklogCounts: encodeCounts(500, 500, 32),
		PrivateState:  d.PrivateState,
	})
	require.Equal(t, 3, d.NewTarget)

	// All drain below reset: bits clear, target falls back to the baseline of 1.
	d = scaler.OnTasks(PartitionScalerInput{
		CurrentTarget: 3,
		BacklogCounts: encodeCounts(32, 32, 32),
		PrivateState:  d.PrivateState,
	})
	require.Equal(t, 1, d.NewTarget)
}
