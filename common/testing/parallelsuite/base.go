package parallelsuite

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
)

// assertions bundles require-style (fail-fast) assertions, proto assertions, and
// history assertions. It is embedded by both [Suite] and [Suite] to provide an
// identical assertion surface.
//
// It enforces a strict rule: a test method (or subtest) must either use assertions
// directly OR create subtests via Run — not both. Mixing is detected in both
// directions via a shared [guard].
type base struct {
	*require.Assertions
	protorequire.ProtoAssertions
	historyrequire.HistoryRequire

	guardT guardT
}

func (b *base) initBase(t *testing.T) {
	g := &b.guardT
	g.name = t.Name()
	g.T = t
	g.asserted.Store(false)
	g.hasSubtests.Store(false)
	b.Assertions = require.New(g)
	b.ProtoAssertions = protorequire.New(g)
	b.HistoryRequire = historyrequire.New(g)
}

// T returns the *testing.T, panicking if the guard has been sealed.
func (b *base) T() *testing.T {
	if b.guardT.hasSubtests.Load() {
		panic("parallelsuite: do not call T() after Run(); use the subtest callback's parameter instead")
	}
	return b.guardT.T
}

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
