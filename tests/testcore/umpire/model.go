package umpire

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// Model is a generic, embeddable base for property-based test models using rapid.
// The type parameter M is the concrete model type that embeds Model.
//
// M provides actions, cleanup, and the invariant checker. CheckInvariant is
// registered as the unnamed "" action in rapid.T.Repeat.
//
// Actions access the current test wrapper via m.T().
type Model[M ModelBehavior] struct {
	*require.Assertions
	Umpire *Umpire
	self   M
	t      *T
}

// Action describes a state-machine action and its optional precondition.
type Action struct {
	Name    string
	Enabled func() bool
	Run     func(*T)
}

type ModelBehavior interface {
	Actions() []Action
	CheckInvariant(*T)
	Cleanup(*T)
}

// Composite aggregates multiple model behaviors into one runnable model.
type Composite struct {
	components []ModelBehavior
}

// NewComposite creates a model behavior from multiple independently-owned components.
func NewComposite(components ...ModelBehavior) *Composite {
	return &Composite{components: components}
}

func (c *Composite) Actions() []Action {
	var actions []Action
	for _, component := range c.components {
		actions = append(actions, component.Actions()...)
	}
	return actions
}

func (c *Composite) CheckInvariant(t *T) {
	for _, component := range c.components {
		component.CheckInvariant(t)
	}
}

func (c *Composite) Cleanup(t *T) {
	for i := len(c.components) - 1; i >= 0; i-- {
		c.components[i].Cleanup(t)
	}
}

func (c *Composite) Rules() *RuleSet {
	merged := &RuleSet{}
	for _, component := range c.components {
		provider, ok := component.(ruleProvider)
		if !ok {
			continue
		}
		merged.Merge(provider.Rules())
	}
	return merged
}

type ruleProvider interface {
	Rules() *RuleSet
}

type requestMutationTSetter interface {
	SetRequestMutationT(*T)
}

// NewModel creates a Model backed by the given umpire.
// self is a reference to the embedding struct.
func NewModel[M ModelBehavior](u *Umpire, self M) Model[M] {
	return Model[M]{Umpire: u, self: self}
}

// T returns the current test wrapper for the active action.
func (m *Model[M]) T() *T {
	return m.t
}

// CheckRules runs all registered umpire rules and fails on any violations.
func (m *Model[M]) CheckRules() {
	m.checkRules(m.t, false)
}

func (m *Model[M]) checkRules(t *T, final bool) {
	t.Helper()
	violations := m.Umpire.CheckRules(final)
	if len(violations) != 0 {
		for _, violation := range violations {
			t.Logf("violation: %s", violation)
		}
		if summary := formatCoverageSummary(m.Umpire.CoverageSummary()); summary != "" {
			t.Logf("%s", summary)
		}
		t.Fatalf("property violations: %v", violations)
	}
}

// Run executes the concrete model's actions via t.Repeat. Cleanup is called
// via defer after each rapid iteration. Umpire.Reset is called automatically
// before Cleanup.
//
// Optional methods on M:
//   - Rules() *RuleSet — provides rules registered before the first action
func (m *Model[M]) Run(t *T) {
	t.Helper()
	actions, err := m.actions()
	if err != nil {
		t.Fatalf("%v", err)
	}
	m.Umpire.Reset()
	m.Umpire.startCoverageRun(t.coverage)
	m.setT(t)
	m.registerRules()
	defer func() {
		m.setT(t)
		m.clearRequestMutationT()
		m.self.Cleanup(t)
		m.Umpire.Reset()
	}()
	t.raw.Repeat(m.rapidActions(actions))
	m.clearRequestMutationT()
	m.checkRules(t, true)
}

func (m *Model[M]) registerRules() {
	provider, ok := any(m.self).(ruleProvider)
	if !ok {
		return
	}
	provider.Rules().Attach(m.Umpire)
}

func (m *Model[M]) actions() (map[string]Action, error) {
	actions := make(map[string]Action)
	for _, action := range m.self.Actions() {
		if err := addAction(actions, action); err != nil {
			return nil, fmt.Errorf("model %T: %w", m.self, err)
		}
	}
	actions[""] = Action{
		Name: "",
		Run:  m.self.CheckInvariant,
	}
	return actions, nil
}

func (m *Model[M]) setT(t *T) {
	m.t = t
	m.Assertions = t.Assertions
	if mutations, ok := any(m.self).(requestMutationTSetter); ok {
		mutations.SetRequestMutationT(t)
	}
}

func (m *Model[M]) clearRequestMutationT() {
	if mutations, ok := any(m.self).(requestMutationTSetter); ok {
		mutations.SetRequestMutationT(nil)
	}
}

func addAction(actions map[string]Action, action Action) error {
	if action.Name == "" {
		return errors.New("action must have a name")
	}
	if action.Run == nil {
		return fmt.Errorf("action %s must have a Run function", action.Name)
	}
	if _, ok := actions[action.Name]; ok {
		return fmt.Errorf("duplicate action %s", action.Name)
	}
	actions[action.Name] = action
	return nil
}

func (m *Model[M]) rapidActions(actions map[string]Action) map[string]func(*rapid.T) {
	out := make(map[string]func(*rapid.T), len(actions))
	for name, action := range actions {
		name := name
		action := action
		out[name] = func(rt *rapid.T) {
			mt := newT(rt)
			m.setT(mt)
			if name == "" {
				m.clearRequestMutationT()
				mt.Logf("action begin: CheckInvariant")
				defer mt.Logf("action end: CheckInvariant")
				action.Run(mt)
				m.checkRules(mt, false)
				return
			}
			mt.Logf("action begin: %s", name)
			defer mt.Logf("action end: %s", name)
			if action.Enabled != nil && !action.Enabled() {
				mt.Logf("action skip: %s disabled", name)
				mt.Skipf("%s disabled", name)
			}
			action.Run(mt)
		}
	}
	return out
}

// Check runs a property-based test. The testFn receives a test context
// and should set up the model and call m.Run(t).
func Check(t *testing.T, testFn func(t *T)) {
	t.Helper()
	coverage := newCoverageCollector()
	defer func() {
		if summary := formatCoverageSummary(coverage.summary()); summary != "" {
			t.Logf("[umpire] %s", summary)
		}
	}()
	rapid.Check(t, func(rt *rapid.T) {
		testFn(newT(rt, coverage))
	})
}

// Draw randomly selects an element from the slice using the rapid generator.
// Skips the test step if the slice is empty.
func Draw[V any](t *T, label string, items []V) V {
	t.Helper()
	if len(items) == 0 {
		t.Skip("no items for " + label)
	}
	return rapid.SampledFrom(items).Draw(t.raw, label)
}

// drawGen is the internal escape hatch used by umpire helpers (e.g.
// RequestMutations) that need to draw from a rapid generator. It is
// deliberately unexported so callers outside this package work in terms of
// Draw and named umpire helpers, keeping rapid out of the public API.
func drawGen[V any](t *T, label string, gen *rapid.Generator[V]) V {
	t.Helper()
	return gen.Draw(t.raw, label)
}
