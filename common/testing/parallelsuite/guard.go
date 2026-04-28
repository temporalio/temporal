package parallelsuite

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// guardT is a [require.TestingT] wrapper that prevents assertions after Run() is called.
//
// Assertions before Run() are allowed; after Run() any assertion panics.
type guardT struct {
	*testing.T
	name        string
	hasSubtests atomic.Bool
}

func (g *guardT) Helper() {
	if g.hasSubtests.Load() {
		panic(fmt.Sprintf(
			"parallelsuite: assertion called on %q after Run() was called; "+
				"use the callback parameter's assertions inside Run() instead",
			g.name,
		))
	}
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
	g.hasSubtests.Store(true)
}
