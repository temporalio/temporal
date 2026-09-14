package activity

// Unpausing inside a dispatch-delay window — a start delay or a retry backoff — honors the time
// that remains. Reset discards the remaining delay, because it starts the activity over as if on
// its first attempt.

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/activity/model"
)

// dispatchDelayWindow is a configuration plus the events that leave an activity inside the
// dispatch-delay window it names.
type dispatchDelayWindow struct {
	name   string
	cfg    model.Config
	prefix []model.Event
}

var dispatchDelayWindows = []dispatchDelayWindow{
	// Fail the first attempt retryably, so the next one is waiting out a retry backoff.
	{"RetryBackoff", model.Config{MaxAttempts: 3}, []model.Event{model.Poll, model.FailRetryably}},
	// A start delay holds the very first dispatch, so there is nothing to drive first.
	{"StartDelay", model.Config{MaxAttempts: 3, HasStartDelay: true}, nil},
}

func TestRemainingDispatchDelay_Reset(t *testing.T) {
	for _, window := range dispatchDelayWindows {
		t.Run(window.name, func(t *testing.T) {
			d := newDriver(t, window.cfg)
			a := d.start()
			for _, e := range append(append([]model.Event{}, window.prefix...), model.Reset) {
				require.NoError(t, a.realize(e), "%s", e)
			}
			switch window.name {
			case "RetryBackoff":
				require.True(t, a.dispatchable(), "reset must not honor the remaining retry backoff")
			case "StartDelay":
				require.False(t, a.dispatchable(), "reset must honor the remaining start delay")
			default:
				panic("invalid test case")
			}
		})
	}
}

func TestRemainingDispatchDelay_Unpause(t *testing.T) {
	for _, window := range dispatchDelayWindows {
		t.Run(window.name, func(t *testing.T) {
			d := newDriver(t, window.cfg)
			a := d.start()
			for _, e := range append(append([]model.Event{}, window.prefix...), model.Pause, model.Unpause) {
				require.NoError(t, a.realize(e), "%s", e)
			}
			require.False(t, a.dispatchable(), "unpause must honor the remaining dispatch delay")
		})
	}
}
