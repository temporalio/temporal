package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
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
