package schedulertest

import (
	"fmt"
)

// Violation describes a single broken invariant.
type Violation struct {
	Name    string
	Message string
}

func (v Violation) String() string {
	return fmt.Sprintf("[%s] %s", v.Name, v.Message)
}

// CheckInvariants evaluates the scheduler invariants against a settled state
// (cur). When prev is non-nil it also checks cross-step invariants (monotonicity,
// closed-is-terminal). It returns all violations found; an empty slice means the
// state is healthy.
//
// The invariants encode the legitimate-quiescence rules from Scheduler.isHeldOpen
// and Scheduler.getIdleExpiration: a non-closed schedule may have no pending task
// only when it is held open (paused or draining a backfill). Any other taskless,
// non-closed state is the "stuck" bug class.
func CheckInvariants(prev *Snapshot, cur Snapshot) []Violation {
	var violations []Violation
	add := func(name, format string, args ...any) {
		violations = append(violations, Violation{Name: name, Message: fmt.Sprintf(format, args...)})
	}

	// (1) No-stuck (primary): a live schedule must always have a way to make
	// progress. The only taskless, non-closed state allowed is "held open"
	// (paused or pending backfill). When idle is armed there is a pending idle
	// task, so HasPendingTask covers that case.
	if !cur.Closed && !cur.HasPendingTask && !cur.IsHeldOpen {
		add("no-stuck",
			"scheduler has no pending task, is not closed, and is not held open "+
				"(paused=%v backfillers=%d) at now=%s — stuck",
			cur.Paused, cur.BackfillerCount, cur.Now.Format("2006-01-02T15:04:05"))
	}

	// (2) Idle correctness: a held-open schedule must never have an idle-close
	// armed; idle close would race the customer's intent to resume/drain.
	if cur.IsHeldOpen && !cur.IdleCloseTime.IsZero() {
		add("idle-not-while-held-open",
			"scheduler is held open (paused=%v backfillers=%d) but has idle close armed at %s",
			cur.Paused, cur.BackfillerCount, cur.IdleCloseTime.Format("2006-01-02T15:04:05"))
	}

	if prev != nil {
		// (3) Closed is terminal: a closed schedule must never reopen.
		if prev.Closed && !cur.Closed {
			add("closed-terminal", "scheduler reopened after being closed")
		}

		// (4) High-water-mark monotonicity: generator/invoker HWMs never regress.
		if cur.GeneratorLPT.Before(prev.GeneratorLPT) {
			add("hwm-monotonic-generator",
				"generator high-water mark went backward: %s -> %s",
				prev.GeneratorLPT.Format("2006-01-02T15:04:05"),
				cur.GeneratorLPT.Format("2006-01-02T15:04:05"))
		}
		if cur.InvokerLPT.Before(prev.InvokerLPT) {
			add("hwm-monotonic-invoker",
				"invoker high-water mark went backward: %s -> %s",
				prev.InvokerLPT.Format("2006-01-02T15:04:05"),
				cur.InvokerLPT.Format("2006-01-02T15:04:05"))
		}
	}

	return violations
}

// fataler is the subset of testing.TB the invariant assertion helper needs.
type fataler interface {
	Helper()
	Fatalf(format string, args ...any)
}

// CheckInvariantsHook returns an AfterStep hook that captures a snapshot after
// every step and fails the test on the first invariant violation. It threads the
// previous snapshot so cross-step invariants are checked. This is the normal way
// invariant checking is wired into a Driver:
//
//	d := NewDriver(t)
//	d.AfterStep = CheckInvariantsHook(t)
func CheckInvariantsHook(t fataler) func(d *Driver) {
	var prev *Snapshot
	return func(d *Driver) {
		t.Helper()
		cur := d.Snapshot()
		if violations := CheckInvariants(prev, cur); len(violations) > 0 {
			t.Fatalf("scheduler invariant violation at step (now=%s): %s",
				cur.Now.Format("2006-01-02T15:04:05"), violations[0])
		}
		snap := cur
		prev = &snap
	}
}
