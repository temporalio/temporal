package stamp

import (
	"testing"

	"go.temporal.io/server/common/log"
)

type (
	Suite struct {
		TB            testing.TB
		newLoggerFunc func(testing.TB) log.Logger
	}
)

func NewSuite(
	tb testing.TB,
	newLoggerFn func(testing.TB) log.Logger,
) *Suite {
	if t, ok := tb.(*testing.T); ok {
		t.Parallel() // enforce parallel execution
	}
	return &Suite{
		TB:            tb,
		newLoggerFunc: newLoggerFn,
	}
}

func (s *Suite) Run(
	t *testing.T,
	testFn func(*Scenario),
	opts ...ScenarioOption,
) {
	t.Parallel() // enforce parallel execution
	scenario := newScenario(t, s.newLoggerFunc, opts...)
	scenario.Run(testFn)
}

func (s *Suite) Fuzz(
	t *testing.T,
	seed int,
	fuzzFn func(s *Scenario),
	opts ...ScenarioOption,
) {
	opts = append(opts,
		allowRandomOption{},
		WithSeed(seed))
	s.Run(t, fuzzFn, opts...)
}
