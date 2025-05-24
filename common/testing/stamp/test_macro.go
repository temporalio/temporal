package stamp

//import (
//	"testing"
//
//	"go.temporal.io/server/common/log"
//)
//
//type (
//	ScenarioMacro struct {
//		t          *testing.T
//		loggerFunc func(testing.TB) log.Logger
//		fn         func(*MacroScenario)
//		opts       []ScenarioOption
//		genCache   map[string]any
//	}
//	MacroScenario struct {
//		*Scenario
//		parent         *ScenarioMacro
//		explorationRun bool
//	}
//	LabeledFunction struct {
//		Label string
//		Fn    func()
//	}
//)
//
//func newScenarioMacro(
//	t *testing.T,
//	loggerFunc func(testing.TB) log.Logger,
//	fn func(*MacroScenario),
//	opts ...ScenarioOption,
//) *ScenarioMacro {
//	return &ScenarioMacro{
//		t:          t,
//		loggerFunc: loggerFunc,
//		fn:         fn,
//		opts:       opts,
//		genCache:   map[string]any{},
//	}
//}
//
//func (sm *ScenarioMacro) newScenario(t *testing.T) *MacroScenario {
//	s := newScenario(t, sm.loggerFunc(t), sm.opts...)
//	return &MacroScenario{
//		Scenario: s,
//		parent:   sm,
//	}
//}
//
