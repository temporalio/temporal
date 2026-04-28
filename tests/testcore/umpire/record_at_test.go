package umpire

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type clockTestFact string

func (f clockTestFact) Key() string { return string(f) }

// Record.At is stamped from the configured clock, and that same clock is
// surfaced to rules via RuleContext.Now — the seam time-bounded liveness rules
// use to fail overdue conditions mid-run.
func TestRecordAtAndRuleContextNow(t *testing.T) {
	u := &Umpire{}
	fixed := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	u.SetClock(func() time.Time { return fixed })

	rec := u.Record(clockTestFact("op-1"))
	require.Equal(t, fixed, rec.At, "Record.At should be stamped from the umpire clock")

	var seenNow time.Time
	var seenAt time.Time
	u.AddRule(LivenessRule{
		Name: "clock-probe",
		Check: func(ctx *RuleContext, history []*Record, _ bool) {
			seenNow = ctx.Now()
			if len(history) > 0 {
				seenAt = history[0].At
			}
		},
	})
	require.Empty(t, u.CheckRules(false))
	require.Equal(t, fixed, seenNow, "RuleContext.Now should report the clock time")
	require.Equal(t, fixed, seenAt, "rules observe Record.At")
}

// A zero-value Umpire (no SetClock) must still stamp a sane wall-clock time.
func TestRecordAtDefaultsToWallClock(t *testing.T) {
	u := &Umpire{}
	before := time.Now()
	rec := u.Record(clockTestFact("op-2"))
	after := time.Now()
	require.False(t, rec.At.Before(before))
	require.False(t, rec.At.After(after))
}
