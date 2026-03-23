package parallelsuite

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// guardT is a [require.TestingT] wrapper that detects mixing of assertions and Run.
//
// Before markHasSubtests: tracks assertion usage via Helper() (sets asserted flag).
// After markHasSubtests: panics on any assertion via Helper()/Errorf()/FailNow().
type guardT struct {
	*testing.T
	name        string
	asserted    atomic.Bool
	hasSubtests atomic.Bool
}

func (g *guardT) Helper() {
	if g.hasSubtests.Load() {
		panic(fmt.Sprintf(
			"parallelsuite: assertion called on %q after Run() was called; "+
				"a test must either use assertions OR call Run(), not both — "+
				"use the callback parameter's assertions inside Run() instead",
			g.name,
		))
	}
	g.asserted.Store(true)
	g.T.Helper()
}

func (g *guardT) Errorf(format string, args ...any) {
	if g.hasSubtests.Load() {
		g.Helper() // panics with clear message
	}
	g.T.Errorf(format, args...)
}

func (g *guardT) FailNow() {
	if g.hasSubtests.Load() {
		g.Helper() // panics with clear message
	}
	g.T.FailNow()
}

func (g *guardT) markHasSubtests() {
	if g.hasSubtests.Swap(true) {
		return
	}
	if g.asserted.Load() {
		panic(fmt.Sprintf(
			"parallelsuite: Run() called on %q after assertions were already used; "+
				"a test must either use assertions OR call Run(), not both",
			g.name,
		))
	}
}
