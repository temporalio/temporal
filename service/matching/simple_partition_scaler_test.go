package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/number"
)

// TestSimplePartitionScalerEnabledDoesNotPanic when the scaler is enabled with any
// Up/Down window, OnTasks lazily creates trackers by writing to the trackers map.
// Confirm that when SimplePartitionScaler is enabled with non-empty Ups and Downs,
// there is no nil-map panic.
func TestSimplePartitionScalerEnabledDoesNotPanic(t *testing.T) {
	t.Parallel()

	scaler := newSimplePartitionScaler(
		dynamicconfig.GetTypedPropertyFn(dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true,
			Ups: []dynamicconfig.SimplePartitionScalerThreshold{
				{Window: time.Second, TargetRate: 100},
			},
			Downs: []dynamicconfig.SimplePartitionScalerThreshold{
				{Window: time.Second, TargetRate: 100},
			},
		}),
		nil,
		clock.NewEventTimeSource(),
	)

	// The first call reaches getTracker. Must report no change because no full
	// window has elapsed yet.
	var dec PartitionScalerDecision
	require.NotPanics(t, func() {
		dec = scaler.OnTasks(PartitionScalerInput{NumTasks: 1, CurrentTarget: 1})
	})
	require.True(t, dec.NoChange, "first call before a full window should not change the target")
}

// encodeCounts builds a Compact8-encoded backlog-count slice from raw values,
// matching the on-the-wire form updateBacklogTarget consumes.
func encodeCounts(values ...int64) []byte {
	b := make([]byte, len(values))
	for i, v := range values {
		b[i] = number.EncodeCompact8(v)
	}
	return b
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
	bs := bitSet(nil).set(0)
	counts := encodeCounts(200, 200)
	require.Greater(t, number.DecodeCompact8(counts[0]), int64(100), "quantization moved too much")
	require.Less(t, number.DecodeCompact8(counts[0]), int64(300), "quantization moved too much")
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

	bs := bitSet(nil).set(0).set(1)

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
		nil, // no legacy count
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
	scaler := newSimplePartitionScaler(dynamicconfig.GetTypedPropertyFn(cfg), nil, nil)

	decision := scaler.OnTasks(PartitionScalerInput{CurrentTarget: 0})
	require.Equal(t, 1, decision.NewTarget, "add baseline must floor at 1, not disable scaling")
}

// TestOnTasksBacklogScalesUpAndDown verifies that with no rate windows, backlog
// pressure grows the target one partition at a time (baseline 1 + occupied count)
// and shrinks back to the baseline once partitions drain below BacklogReset.
func TestOnTasksBacklogScalesUpAndDown(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{
		Enabled:      true,
		BacklogReset: 100,
		BacklogBase:  300,
		BacklogCap:   1000,
		Max:          4,
	}
	scaler := newSimplePartitionScaler(dynamicconfig.GetTypedPropertyFn(cfg), nil, nil)

	// One partition, occupied: baseline 1 + 1 occupied = 2.
	d := scaler.OnTasks(PartitionScalerInput{CurrentTarget: 1, BacklogCounts: encodeCounts(500)})
	require.Equal(t, 2, d.NewTarget)

	// Two partitions, both occupied: baseline 1 + 2 occupied = 3.
	d = scaler.OnTasks(PartitionScalerInput{
		CurrentTarget: 2,
		BacklogCounts: encodeCounts(500, 500),
		PrivateState:  d.PrivateState,
	})
	require.Equal(t, 3, d.NewTarget)

	// A newly-opened partition that is not yet occupied does not add more capacity:
	// baseline 1 + 2 occupied = 3 (unchanged).
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

// TestOnTasksLegacyMultiples covers the *AsMultipleOfLegacy settings and how they combine
// with the explicit Fixed/Min/Max: an explicit Fixed wins over the derived one, while derived
// Min/Max apply in addition to explicit ones, so the more restrictive of the pair wins.
//
// Each case uses no Ups/Downs and CurrentTarget 1, so the pre-clamp target is
// 1 (add baseline) + the number of occupied partitions.
func TestOnTasksLegacyMultiples(t *testing.T) {
	t.Parallel()

	// backlog knobs shared by the non-fixed cases, so backlog counts above 300 occupy a
	// partition and add one to the target
	const backlogReset, backlogBase = 100, 300

	for _, tc := range []struct {
		name        string
		cfg         dynamicconfig.SimplePartitionScalerSettings
		legacyCount func() int
		counts      []int64
		expected    int
	}{{
		name: "derived max clamps",
		cfg: dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true, BacklogReset: backlogReset, BacklogBase: backlogBase,
			MaxAsMultipleOfLegacy: 1,
		},
		legacyCount: func() int { return 2 },
		counts:      []int64{500, 500, 500}, // pre-clamp 1+3 = 4
		expected:    2,                      // capped at the legacy count: the rollback-safety case
	}, {
		name: "derived min raises",
		cfg: dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true, BacklogReset: backlogReset, BacklogBase: backlogBase,
			MinAsMultipleOfLegacy: 2,
		},
		legacyCount: func() int { return 2 },
		counts:      nil, // pre-clamp 1
		expected:    4,
	}, {
		name: "derived min more restrictive than explicit",
		cfg: dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true, BacklogReset: backlogReset, BacklogBase: backlogBase,
			Min: 2, MinAsMultipleOfLegacy: 1,
		},
		legacyCount: func() int { return 4 },
		counts:      nil, // pre-clamp 1
		expected:    4,   // derived min 4 beats explicit min 2
	}, {
		name: "explicit min more restrictive than derived",
		cfg: dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true, BacklogReset: backlogReset, BacklogBase: backlogBase,
			Min: 6, MinAsMultipleOfLegacy: 1,
		},
		legacyCount: func() int { return 4 },
		counts:      nil, // pre-clamp 1
		expected:    6,   // explicit min 6 beats derived min 4
	}, {
		name: "derived max more restrictive than explicit",
		cfg: dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true, BacklogReset: backlogReset, BacklogBase: backlogBase,
			Max: 100, MaxAsMultipleOfLegacy: 1,
		},
		legacyCount: func() int { return 2 },
		counts:      []int64{500, 500, 500}, // pre-clamp 1+3 = 4
		expected:    2,                      // derived max 2 beats explicit max 100
	}, {
		name: "explicit max more restrictive than derived",
		cfg: dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true, BacklogReset: backlogReset, BacklogBase: backlogBase,
			Max: 2, MaxAsMultipleOfLegacy: 4,
		},
		legacyCount: func() int { return 2 },
		counts:      []int64{500, 500, 500}, // pre-clamp 1+3 = 4
		expected:    2,                      // explicit max 2 beats derived max 8
	}, {
		name: "multiple rounds to nearest",
		cfg: dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true, BacklogReset: backlogReset, BacklogBase: backlogBase,
			MaxAsMultipleOfLegacy: 1.5,
		},
		legacyCount: func() int { return 3 },
		counts:      []int64{500, 500, 500, 500, 500, 500}, // pre-clamp 1+6 = 7
		expected:    5,                                     // 1.5*3 = 4.5 rounds to 5
	}, {
		name: "small multiple still yields at least one",
		cfg: dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true, BacklogReset: backlogReset, BacklogBase: backlogBase,
			MaxAsMultipleOfLegacy: 0.1,
		},
		legacyCount: func() int { return 2 },
		counts:      []int64{500}, // pre-clamp 1+1 = 2
		expected:    1,            // 0.1*2 = 0.2 would round down to 0
	}, {
		name: "nil legacy count disables derivation",
		cfg: dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true, BacklogReset: backlogReset, BacklogBase: backlogBase,
			MaxAsMultipleOfLegacy: 1,
		},
		legacyCount: nil,
		counts:      []int64{500, 500, 500}, // pre-clamp 1+3 = 4
		expected:    4,                      // unclamped
	}, {
		name: "max wins over a conflicting min",
		cfg: dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true, BacklogReset: backlogReset, BacklogBase: backlogBase,
			Max: 4, MinAsMultipleOfLegacy: 2,
		},
		legacyCount: func() int { return 4 },
		counts:      nil, // pre-clamp 1
		expected:    4,   // derived min 8 raises it, then explicit max 4 pulls it back down
	}, {
		name:        "derived fixed",
		cfg:         dynamicconfig.SimplePartitionScalerSettings{Enabled: true, FixedAsMultipleOfLegacy: 2},
		legacyCount: func() int { return 4 },
		expected:    8,
	}, {
		name:        "explicit fixed wins over derived",
		cfg:         dynamicconfig.SimplePartitionScalerSettings{Enabled: true, Fixed: 3, FixedAsMultipleOfLegacy: 2},
		legacyCount: func() int { return 8 },
		expected:    3,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			scaler := newSimplePartitionScaler(
				dynamicconfig.GetTypedPropertyFn(tc.cfg),
				tc.legacyCount,
				nil, // time source unused with no Ups/Downs
			)
			d := scaler.OnTasks(PartitionScalerInput{
				CurrentTarget: 1,
				BacklogCounts: encodeCounts(tc.counts...),
			})
			require.Equal(t, tc.expected, d.NewTarget)
		})
	}
}

// TestOnTasksFixedFromLegacyIncludesBacklogCap verifies the derived-Fixed fast path
// propagates BacklogCap, like the explicit-Fixed path does.
func TestOnTasksFixedFromLegacyIncludesBacklogCap(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{
		Enabled:                 true,
		FixedAsMultipleOfLegacy: 2,
		BacklogCap:              1000,
	}
	scaler := newSimplePartitionScaler(
		dynamicconfig.GetTypedPropertyFn(cfg),
		func() int { return 4 },
		nil, // time source unused on the fixed path
	)
	d := scaler.OnTasks(PartitionScalerInput{CurrentTarget: 1})
	require.Equal(t, 8, d.NewTarget)
	require.Equal(t, 1000, d.BacklogCap)
}
