package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/clock"
)

const testShareBand = 1.2

func report(target int32) *taskqueuepb.PollerScalingInfo {
	return &taskqueuepb.PollerScalingInfo{PollerTarget: target}
}

func newTestShareTracker(ttl time.Duration) (*pollerShareTracker, *clock.EventTimeSource) {
	ts := clock.NewEventTimeSource()
	ts.Update(time.Unix(0, 0))
	return newPollerShareTracker(ttl, ts), ts
}

func TestPollerShareTrackerClassify(t *testing.T) {
	t.Parallel()

	// Fleet mean is 10. The fair zone at band 1.2 is (8.33, 12].
	cases := []struct {
		name    string
		targets map[string]int32
		subject string
		want    shareVerdict
	}{
		{"over share", map[string]int32{"a": 20, "b": 5, "c": 5}, "a", shareOver},
		{"under share", map[string]int32{"a": 20, "b": 5, "c": 5}, "b", shareUnder},
		{"even fleet is fair", map[string]int32{"a": 10, "b": 10, "c": 10}, "a", shareFair},
		{"inside the band is fair", map[string]int32{"a": 11, "b": 10, "c": 9}, "a", shareFair},
		{"unreported worker is unknown", map[string]int32{"a": 10, "b": 10}, "ghost", shareUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tr, _ := newTestShareTracker(time.Minute)
			for key, target := range tc.targets {
				tr.record(key, report(target))
			}
			require.Equal(t, tc.want, tr.classify(tc.subject, testShareBand))
		})
	}
}

// A worker with no peer to compare against must never be denied pollers, or a single-worker
// fleet could never grow.
func TestPollerShareTrackerLoneWorkerIsUnknown(t *testing.T) {
	t.Parallel()
	tr, _ := newTestShareTracker(time.Minute)
	tr.record("a", report(100))
	require.Equal(t, shareUnknown, tr.classify("a", testShareBand))

	tr.record("b", report(1))
	require.Equal(t, shareOver, tr.classify("a", testShareBand))
}

func TestPollerShareTrackerDisabledByBand(t *testing.T) {
	t.Parallel()
	tr, _ := newTestShareTracker(time.Minute)
	tr.record("a", report(100))
	tr.record("b", report(1))

	// Wildly imbalanced, but a band at or below 1 is the off switch.
	for _, band := range []float64{0, 1} {
		require.Equal(t, shareUnknown, tr.classify("a", band))
		require.Equal(t, shareUnknown, tr.classify("b", band))
	}
}

// Workers that report nothing are invisible both as a subject of fairness and as a
// contributor to the fleet mean -- otherwise an SDK that doesn't report would drag the mean
// to zero and make every reporting worker look over-share.
func TestPollerShareTrackerIgnoresNonReporters(t *testing.T) {
	t.Parallel()
	tr, _ := newTestShareTracker(time.Minute)
	tr.record("a", report(10))
	tr.record("b", report(10))
	tr.record("silent", nil)
	tr.record("zero", report(0))
	tr.record("", report(10))

	require.Equal(t, shareFair, tr.classify("a", testShareBand))
	require.Equal(t, shareUnknown, tr.classify("silent", testShareBand))
	require.Equal(t, shareUnknown, tr.classify("", testShareBand))
}

// Without eviction a departed worker keeps inflating the fleet mean for a full TTL, which
// reads as every survivor being under-share and suppresses the scale-downs that should
// follow the departure.
func TestPollerShareTrackerForget(t *testing.T) {
	t.Parallel()
	tr, _ := newTestShareTracker(time.Minute)
	tr.record("big", report(30))
	tr.record("a", report(5))
	tr.record("b", report(5))
	require.Equal(t, shareUnder, tr.classify("a", testShareBand))

	tr.forget("big")
	require.Equal(t, shareFair, tr.classify("a", testShareBand))
}

// Covers crashed pods and killed containers, where forget never runs.
func TestPollerShareTrackerTTLSweep(t *testing.T) {
	t.Parallel()
	tr, ts := newTestShareTracker(time.Minute)
	tr.record("big", report(30))
	tr.record("a", report(5))
	tr.record("b", report(5))
	require.Equal(t, shareUnder, tr.classify("a", testShareBand))

	// "big" goes silent; the others keep polling.
	ts.Update(time.Unix(0, 0).Add(90 * time.Second))
	tr.record("a", report(5))
	tr.record("b", report(5))

	require.Equal(t, shareFair, tr.classify("a", testShareBand))
	require.Equal(t, shareUnknown, tr.classify("big", testShareBand))
}

func TestPollerShareTrackerNilIsInert(t *testing.T) {
	t.Parallel()
	var tr *pollerShareTracker
	require.NotPanics(t, func() {
		tr.record("a", report(10))
		tr.forget("a")
		require.Equal(t, shareUnknown, tr.classify("a", testShareBand))
	})
}
