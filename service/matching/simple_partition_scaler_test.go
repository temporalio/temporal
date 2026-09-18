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
		nil, // no old count
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

// TestBoundsFromOldCount verifies how the *AsMultipleOfOldCount settings combine with the
// explicit Fixed/Min/Max: an explicit Fixed wins, while derived Min/Max apply in addition to
// explicit ones (the more restrictive wins).
func TestBoundsFromOldCount(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                     string
		cfg                      dynamicconfig.SimplePartitionScalerSettings
		oldCount                 int
		fixed, minTarget, maxTgt int
	}{{
		name: "no multiples set",
		cfg:  dynamicconfig.SimplePartitionScalerSettings{Fixed: 3, Min: 2, Max: 8},
		// oldCount unused
		fixed: 3, minTarget: 2, maxTgt: 8,
	}, {
		name:     "derived only",
		cfg:      dynamicconfig.SimplePartitionScalerSettings{MinAsMultipleOfOldCount: 0.5, MaxAsMultipleOfOldCount: 1},
		oldCount: 8,
		fixed:    0, minTarget: 4, maxTgt: 8,
	}, {
		name:     "explicit fixed wins over derived",
		cfg:      dynamicconfig.SimplePartitionScalerSettings{Fixed: 3, FixedAsMultipleOfOldCount: 2},
		oldCount: 8,
		fixed:    3,
	}, {
		name:     "derived fixed used when explicit is zero",
		cfg:      dynamicconfig.SimplePartitionScalerSettings{FixedAsMultipleOfOldCount: 2},
		oldCount: 3,
		fixed:    6,
	}, {
		name: "derived min/max are more restrictive",
		cfg: dynamicconfig.SimplePartitionScalerSettings{
			Min: 2, Max: 100,
			MinAsMultipleOfOldCount: 1, MaxAsMultipleOfOldCount: 1,
		},
		oldCount: 4,
		// derived min 4 > explicit 2, derived max 4 < explicit 100
		minTarget: 4, maxTgt: 4,
	}, {
		name: "explicit min/max are more restrictive",
		cfg: dynamicconfig.SimplePartitionScalerSettings{
			Min: 6, Max: 8,
			MinAsMultipleOfOldCount: 1, MaxAsMultipleOfOldCount: 4,
		},
		oldCount: 4,
		// derived min 4 < explicit 6, derived max 16 > explicit 8
		minTarget: 6, maxTgt: 8,
	}, {
		name:     "rounds to nearest",
		cfg:      dynamicconfig.SimplePartitionScalerSettings{MaxAsMultipleOfOldCount: 1.5},
		oldCount: 3, // 4.5 rounds to 5
		maxTgt:   5,
	}, {
		name:     "small multiple still yields at least one",
		cfg:      dynamicconfig.SimplePartitionScalerSettings{MaxAsMultipleOfOldCount: 0.1},
		oldCount: 2, // 0.2 would round to 0
		maxTgt:   1,
	}, {
		name:      "zero old count disables derivation",
		cfg:       dynamicconfig.SimplePartitionScalerSettings{Min: 2, MaxAsMultipleOfOldCount: 1},
		oldCount:  0,
		minTarget: 2,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			oldCount := func() int { return tc.oldCount }
			s := newSimplePartitionScaler(dynamicconfig.GetTypedPropertyFn(tc.cfg), oldCount, nil)
			fixed, minTarget, maxTarget := s.bounds(tc.cfg)
			require.Equal(t, tc.fixed, fixed, "fixed")
			require.Equal(t, tc.minTarget, minTarget, "min")
			require.Equal(t, tc.maxTgt, maxTarget, "max")
		})
	}
}

// TestOnTasksClampsToMaxFromOldCount verifies the derived Max actually clamps the decision,
// which is the rollback-safety case: MaxAsMultipleOfOldCount=1 keeps the scaler from ever
// exceeding the static partition count.
func TestOnTasksClampsToMaxFromOldCount(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{
		Enabled:                 true,
		BacklogReset:            100,
		BacklogBase:             300,
		MaxAsMultipleOfOldCount: 1,
	}
	scaler := newSimplePartitionScaler(
		dynamicconfig.GetTypedPropertyFn(cfg),
		func() int { return 2 },
		nil,
	)

	// Baseline 1 + 3 occupied partitions = 4, but the old count of 2 caps it.
	d := scaler.OnTasks(PartitionScalerInput{
		CurrentTarget: 3,
		BacklogCounts: encodeCounts(500, 500, 500),
	})
	require.Equal(t, 2, d.NewTarget)
}

// TestOnTasksFixedFromOldCount verifies the derived Fixed takes the fast path.
func TestOnTasksFixedFromOldCount(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{
		Enabled:                   true,
		FixedAsMultipleOfOldCount: 2,
		BacklogCap:                1000,
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
