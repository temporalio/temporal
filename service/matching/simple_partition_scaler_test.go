package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
)

// TestSimplePartitionScalerEnabledDoesNotPanic is the regression test for the
// nil-map panic in getTracker: when the scaler is enabled with any Up/Down
// window, OnTasks lazily creates trackers by writing to the trackers map. If
// that map is never initialized in the constructor, this write panics with
// "assignment to entry in nil map" and crash-loops the matching service.
//
// The existing scale_manager_test.go only drives a MockPartitionScaler, so it
// never exercised this real code path.
func TestSimplePartitionScalerEnabledDoesNotPanic(t *testing.T) {
	t.Parallel()

	scaler := newSimplePartitionScaler(
		dynamicconfig.GetTypedPropertyFn(dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true,
			Ups: []dynamicconfig.SimplePartitionScalerThreshold{
				{Window: time.Second, TargetRate: 100},
			},
		}),
		clock.NewEventTimeSource(),
	)

	// The first call reaches getTracker (the map write that used to panic) and,
	// because no full window has elapsed yet, must report no change.
	var dec PartitionScalerDecision
	require.NotPanics(t, func() {
		dec = scaler.OnTasks(PartitionScalerInput{NumTasks: 1, CurrentTarget: 1})
	})
	require.True(t, dec.NoChange, "first call before a full window should not change the target")
}
