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

func (s *Suite) NewScenario(
	t *testing.T,
	opts ...ScenarioOption,
) *Scenario {
	t.Parallel() // enforce parallel execution
	if _, ok := s.TB.(*testing.F); ok {
		// allow randomness in fuzz tests
		opts = append(opts, allowRandomOption{})
	}
	return newScenario(t, s.newLoggerFunc(t), opts...)
}

func (s *Suite) RunScenarioMacro(
	t *testing.T,
	fn func(scenario *MacroScenario),
	opts ...ScenarioOption,
) {
	t.Parallel() // enforce parallel execution
	newScenarioMacro(t, s.newLoggerFunc, fn, append(opts)...).Run()
}
