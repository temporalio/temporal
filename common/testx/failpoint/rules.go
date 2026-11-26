package failpoint

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
)

// Rule declares a failpoint, for example in a configuration file. Rules name errors from the
// catalog (see RegisterError), so they can be loaded from data.
type Rule struct {
	// Key and Scope name the failpoint, as the name of a key and ForScope do.
	Key   string
	Scope string
	// Action is Return, Panic, Block, Pause or On.
	Action Action
	// Error names a registered error. Return returns it and Panic panics with it. It is required
	// for Return and Panic, and not allowed for the other actions.
	Error string
	// Message is passed to the constructor of Error.
	Message string
	// Delay is how long Pause sleeps. It is required for Pause, and not allowed for the other
	// actions.
	Delay time.Duration
	// Trigger limits which executions fire. For a rule that fires once, set Trigger.MaxFires to 1.
	Trigger Trigger
}

func (r Rule) key() keyID {
	return keyID{name: r.Key, scope: r.Scope}
}

// Validate reports whether the rule can be applied. It runs the constructor of the error of the rule
// to check that the failpoint of the key can return it.
func (r Rule) Validate() error {
	_, err := r.prepare()
	return err
}

// preparedRule is a valid rule with what Apply needs to install it, so that nothing can fail after
// Apply starts to change the registry.
type preparedRule struct {
	rule   Rule
	kind   keyKind
	action ruleAction
}

// prepare validates the rule and resolves its action configuration. The error constructors must
// not run under the registry's lock, so Apply prepares the rules before it takes it.
func (r Rule) prepare() (preparedRule, error) {
	kind, err := r.validate()
	if err != nil {
		return preparedRule{}, fmt.Errorf("failpoint rule %s: %w", r.key(), err)
	}
	a := r.resolve()
	if r.Action == ActionReturn && !kind.accepts(a.err) {
		return preparedRule{}, fmt.Errorf("failpoint rule %s: key %q can't return error %q: its value is not of the type of %T",
			r.key(), r.Key, r.Error, a.err)
	}
	return preparedRule{rule: r, kind: kind, action: a}, nil
}

func (r Rule) validate() (keyKind, error) {
	if r.Key == "" {
		return keyKind{}, errors.New("key must be set")
	}
	kind, err := lookupKind(r.Key)
	if err != nil {
		return keyKind{}, err
	}
	return kind, r.validateFields()
}

// validateFields checks the parts of the rule other than its key.
func (r Rule) validateFields() error {
	switch r.Action {
	case ActionReturn, ActionPanic:
		if r.Error == "" {
			return fmt.Errorf("error must be set for action %s", r.Action)
		}
		if _, ok := LookupError(r.Error); !ok {
			return fmt.Errorf("unknown error %q, valid errors are %s", r.Error, strings.Join(ErrorNames(), ", "))
		}
	case ActionPause:
		if r.Delay <= 0 {
			return errors.New("delay must be positive for action Pause")
		}
	case ActionBlock, ActionOn:
	default:
		return fmt.Errorf("unsupported action %s, valid actions are Return, Panic, Block, Pause and On", r.Action)
	}
	switch {
	case r.Error != "" && r.Action != ActionReturn && r.Action != ActionPanic:
		return fmt.Errorf("error is not allowed for action %s", r.Action)
	case r.Message != "" && r.Error == "":
		return errors.New("message is only allowed with an error")
	case r.Delay != 0 && r.Action != ActionPause:
		return fmt.Errorf("delay is not allowed for action %s", r.Action)
	}
	return validateTrigger(r.Trigger)
}

// ValidateRules reports whether Apply accepts rules: each rule is valid and no two rules have the
// same key and scope.
func ValidateRules(rules []Rule) error {
	_, err := prepareRules(rules)
	return err
}

// prepareRules prepares each rule, and checks that no two rules have the same key and scope.
func prepareRules(rules []Rule) ([]preparedRule, error) {
	prepared := make([]preparedRule, 0, len(rules))
	keys := make(map[keyID]bool, len(rules))
	for _, r := range rules {
		p, err := r.prepare()
		if err != nil {
			return nil, err
		}
		if keys[r.key()] {
			return nil, fmt.Errorf("duplicate failpoint rule for %s", r.key())
		}
		keys[r.key()] = true
		prepared = append(prepared, p)
	}
	return prepared, nil
}

func validateTrigger(t Trigger) error {
	switch {
	case math.IsNaN(t.Probability) || t.Probability < 0 || t.Probability > 1:
		return fmt.Errorf("trigger probability %v is not in [0, 1]", t.Probability)
	case t.SkipFirst < 0:
		return fmt.Errorf("trigger skip first %d is negative", t.SkipFirst)
	case t.MaxFires < 0:
		return fmt.Errorf("trigger max fires %d is negative", t.MaxFires)
	}
	return nil
}

// ruleAction is the action configuration that a rule declares.
type ruleAction struct {
	action Action
	// err is what Return returns and Panic panics with.
	err      error
	pauseDur time.Duration
}

// resolve returns the action configuration that the rule declares, and runs the constructor of
// its error. The rule must be valid.
func (r Rule) resolve() ruleAction {
	a := ruleAction{action: r.Action, pauseDur: r.Delay}
	if r.Error != "" {
		newError, _ := LookupError(r.Error)
		a.err = newError(r.Message)
	}
	return a
}

// ruleTarget is a failpoint that a RuleSet creates and configures.
type ruleTarget interface {
	entry
	// installRule replaces the configuration of the failpoint with what a rule declares, and
	// returns the seed of its trigger.
	installRule(a ruleAction, trigger Trigger) int64
}

func (f *Failpoint[T]) installRule(a ruleAction, trigger Trigger) int64 {
	s := settings[T]{action: a.action, pauseDur: a.pauseDur}
	switch a.action {
	case ActionReturn:
		// prepare checked that the failpoint accepts the error.
		if v, ok := any(a.err).(T); ok {
			s.value = v
		}
	case ActionPanic:
		s.panicValue = a.err
	default:
	}
	return f.install(s, trigger)
}

// errRuleSetClosed is returned by Apply after Close.
var errRuleSetClosed = errors.New("failpoint rule set is closed")

// RuleSet keeps the failpoints of a registry in line with a list of rules. It only changes and
// removes the failpoints that it created itself, and never replaces a failpoint that other code
// registered, such as a test.
type RuleSet struct {
	registry *Registry

	mu     sync.Mutex
	closed bool
	owned  map[keyID]ownedFailpoint
}

// ownedFailpoint is a failpoint that a RuleSet created, and the rule that it applied to it.
type ownedFailpoint struct {
	rule Rule
	fp   ruleTarget
}

// NewRuleSet returns a rule set that manages failpoints in registry.
func NewRuleSet(registry *Registry) *RuleSet {
	return &RuleSet{registry: registry, owned: make(map[keyID]ownedFailpoint)}
}

// AppliedRule is a rule that Apply installed or changed.
type AppliedRule struct {
	// Key and Scope are those of the rule.
	Key   string
	Scope string
	// Seed is the seed of the trigger's random numbers, see Failpoint.Seed. For a rule with a
	// probability but no seed of its own it is the one that Apply picked, so a caller can log it
	// and a later run can repeat the sequence of fires by setting Trigger.Seed. It is zero for a
	// rule without a probability.
	Seed int64
}

// Apply makes the failpoints of the set match rules. It creates the failpoints of new rules,
// reconfigures those whose rule changed, and removes those whose rule is no longer listed. A
// failpoint whose rule is unchanged keeps its state, such as its counts and held blocks. To turn a
// failpoint off, remove its rule from the list. A failpoint of the set that other
// code removed is created again.
//
// Apply returns the rules that it installed or changed, in the order of rules: those that created or
// reconfigured a failpoint, not those that were unchanged. It returns
// an error and changes nothing if a rule is invalid. If other code registered a
// failpoint under the key of a rule, Apply skips that rule, applies the others, and returns an
// error that lists the skipped keys; the next Apply tries them again. A failpoint of the set that
// other code removed or replaced is disabled in either case, so it releases the executions it
// holds.
func (s *RuleSet) Apply(rules []Rule) ([]AppliedRule, error) {
	// The error constructors must not run under the registry's lock.
	prepared, err := prepareRules(rules)
	if err != nil {
		return nil, err
	}
	keys := make(map[keyID]bool, len(rules))
	for _, r := range rules {
		keys[r.key()] = true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errRuleSetClosed
	}

	var applied []AppliedRule
	// Atomically serializes Apply with the multi-step changes of other code. update serializes it
	// with every other change to the registry.
	s.registry.Atomically(func() {
		s.registry.update(func(failpoints map[keyID]entry) {
			applied, err = s.reconcileLocked(failpoints, rules, keys, prepared)
		})
	})
	return applied, err
}

func (s *RuleSet) reconcileLocked(
	failpoints map[keyID]entry,
	rules []Rule,
	keys map[keyID]bool,
	prepared []preparedRule,
) ([]AppliedRule, error) {
	s.forgetDetachedLocked(failpoints)
	collisions := s.collisionsLocked(failpoints, rules)
	for key, owned := range s.owned {
		if !keys[key] {
			s.removeLocked(failpoints, key, owned)
		}
	}
	var applied []AppliedRule
	for _, p := range prepared {
		if slices.Contains(collisions, p.rule.key()) {
			continue
		}
		if a, ok := s.applyLocked(failpoints, p); ok {
			applied = append(applied, a)
		}
	}
	if len(collisions) > 0 {
		return applied, fmt.Errorf("failpoint rules for keys of failpoints that the rule set didn't create, skipped: %v", collisions)
	}
	return applied, nil
}

// forgetDetachedLocked disables and forgets the failpoints of the set that other code removed from
// failpoints or replaced. The caller must hold s.mu.
func (s *RuleSet) forgetDetachedLocked(failpoints map[keyID]entry) {
	for key, owned := range s.owned {
		if fp, ok := failpoints[key]; !ok || fp != entry(owned.fp) {
			delete(s.owned, key)
			owned.fp.Disable()
		}
	}
}

// collisionsLocked returns the keys of rules that other code holds. The caller must hold s.mu, and
// the set must own every failpoint it remembers.
func (s *RuleSet) collisionsLocked(failpoints map[keyID]entry, rules []Rule) []keyID {
	var collisions []keyID
	for _, r := range rules {
		key := r.key()
		if _, owned := s.owned[key]; owned {
			continue
		}
		if _, ok := failpoints[key]; ok {
			collisions = append(collisions, key)
		}
	}
	return collisions
}

// applyLocked applies a prepared rule, and reports whether it installed or changed the failpoint.
// The caller must hold s.mu, and the key of the rule must be free or owned.
func (s *RuleSet) applyLocked(failpoints map[keyID]entry, p preparedRule) (AppliedRule, bool) {
	r, a := p.rule, p.action
	key := r.key()
	applied := AppliedRule{Key: r.Key, Scope: r.Scope}
	if owned, ok := s.owned[key]; ok {
		changed := owned.rule != r
		if changed {
			applied.Seed = owned.fp.installRule(a, r.Trigger)
		}
		s.owned[key] = ownedFailpoint{rule: r, fp: owned.fp}
		return applied, changed
	}
	fp := p.kind.newFailpoint(key)
	applied.Seed = fp.installRule(a, r.Trigger)
	failpoints[key] = fp
	s.owned[key] = ownedFailpoint{rule: r, fp: fp}
	return applied, true
}

// removeLocked removes a failpoint of the set. The caller must hold s.mu.
func (s *RuleSet) removeLocked(failpoints map[keyID]entry, key keyID, owned ownedFailpoint) {
	delete(s.owned, key)
	if failpoints[key] == entry(owned.fp) {
		delete(failpoints, key)
	}
	owned.fp.Disable()
}

// Close removes the failpoints of the set. Apply fails after Close.
func (s *RuleSet) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.registry.Atomically(func() {
		s.registry.update(func(failpoints map[keyID]entry) {
			for key, owned := range s.owned {
				s.removeLocked(failpoints, key, owned)
			}
		})
	})
}
