package failpoint

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// Rule declares a failpoint, for example in a configuration file. Rules name errors and behaviors
// from the catalogs (see RegisterError and RegisterBehavior), so they can be loaded from data.
type Rule struct {
	// Key and Scope name the failpoint, as KeyFromName does.
	Key   string
	Scope string
	// Action is Return, Panic, Block, Pause, Call or On.
	Action Action
	// Error names a registered error. Return returns it and Panic panics with it. It is required
	// for Return and Panic, and not allowed for the other actions.
	Error string
	// Message is passed to the constructor of Error.
	Message string
	// Behavior names a registered behavior that Call calls. It is required for Call, and not
	// allowed for the other actions.
	Behavior string
	// Delay is how long Pause sleeps. It is required for Pause, and not allowed for the other
	// actions.
	Delay time.Duration
	// Recurrence defaults to Always.
	Recurrence Recurrence
	Trigger    Trigger
	// Disabled keeps the failpoint configured but suspended, see Failpoint.SetEnabled.
	Disabled bool
}

func (r Rule) key() Key {
	return KeyFromName(r.Key, r.Scope)
}

// Validate reports whether the rule can be applied.
func (r Rule) Validate() error {
	if err := r.validate(); err != nil {
		return fmt.Errorf("failpoint rule %s: %w", r.key(), err)
	}
	return nil
}

func (r Rule) validate() error {
	if r.Key == "" {
		return errors.New("key must be set")
	}
	switch r.Action {
	case Return, Panic:
		if r.Error == "" {
			return fmt.Errorf("error must be set for action %s", r.Action)
		}
		if _, ok := LookupError(r.Error); !ok {
			return fmt.Errorf("unknown error %q", r.Error)
		}
	case Call:
		if r.Behavior == "" {
			return errors.New("behavior must be set for action Call")
		}
		if _, ok := LookupBehavior(r.Behavior); !ok {
			return fmt.Errorf("unknown behavior %q", r.Behavior)
		}
	case Pause:
		if r.Delay <= 0 {
			return errors.New("delay must be positive for action Pause")
		}
	case Block, On:
	default:
		return fmt.Errorf("unsupported action %s", r.Action)
	}
	switch {
	case r.Error != "" && r.Action != Return && r.Action != Panic:
		return fmt.Errorf("error is not allowed for action %s", r.Action)
	case r.Message != "" && r.Error == "":
		return errors.New("message is only allowed with an error")
	case r.Behavior != "" && r.Action != Call:
		return fmt.Errorf("behavior is not allowed for action %s", r.Action)
	case r.Delay != 0 && r.Action != Pause:
		return fmt.Errorf("delay is not allowed for action %s", r.Action)
	}
	switch r.Recurrence {
	case RecurrenceUnspecified, Once, Always:
	default:
		return fmt.Errorf("unsupported recurrence %d", r.Recurrence)
	}
	return validateTrigger(r.Trigger)
}

// ValidateRules reports whether Apply accepts rules: each rule is valid and no two rules have the
// same key and scope.
func ValidateRules(rules []Rule) error {
	keys := make(map[Key]bool, len(rules))
	for _, r := range rules {
		if err := r.Validate(); err != nil {
			return err
		}
		if keys[r.key()] {
			return fmt.Errorf("duplicate failpoint rule for %s", r.key())
		}
		keys[r.key()] = true
	}
	return nil
}

func validateTrigger(t Trigger) error {
	switch {
	case math.IsNaN(t.Probability) || t.Probability < 0 || t.Probability > 1:
		return fmt.Errorf("trigger probability %v is not in [0, 1]", t.Probability)
	case t.After < 0:
		return fmt.Errorf("trigger after %d is negative", t.After)
	case t.Max < 0:
		return fmt.Errorf("trigger max %d is negative", t.Max)
	}
	return nil
}

// settings returns the failpoint configuration that the rule declares. The rule must be valid.
func (r Rule) settings() settings[error] {
	c := settings[error]{
		action:  r.Action,
		times:   cmp.Or(r.Recurrence, Always),
		trigger: r.Trigger,
		enabled: !r.Disabled,
	}
	switch r.Action {
	case Return:
		newError, _ := LookupError(r.Error)
		c.value = newError(r.Message)
	case Panic:
		newError, _ := LookupError(r.Error)
		c.panicValue = newError(r.Message)
	case Pause:
		c.pauseDur = r.Delay
	case Call:
		behavior, _ := LookupBehavior(r.Behavior)
		c.callback = func(any) error { return behavior() }
	case Block, On:
	default:
		//nolint:forbidigo // an unsupported rule action is a programming error
		panic(fmt.Sprintf("unsupported failpoint rule action: %s", r.Action))
	}
	return c
}

// sameConfiguration reports whether r and other configure a failpoint the same way, apart from
// whether it is suspended.
func (r Rule) sameConfiguration(other Rule) bool {
	other.Disabled = r.Disabled
	return r == other
}

// errRuleSetClosed is returned by Apply after Close.
var errRuleSetClosed = errors.New("failpoint rule set is closed")

// RuleSet keeps the failpoints of a registry in line with a list of rules. It only changes and
// removes the failpoints that it created itself, and never replaces a failpoint that other code
// registered, such as a test. The failpoints it creates report its source as their Owner.
type RuleSet struct {
	registry Registry
	source   string

	mu     sync.Mutex
	closed bool
	owned  map[Key]ownedFailpoint
}

// ownedFailpoint is a failpoint that a RuleSet created, and the rule that it applied to it.
type ownedFailpoint struct {
	rule Rule
	fp   *Failpoint[error]
}

// NewRuleSet returns a rule set that manages failpoints in registry. source describes where the
// rules come from, such as a configuration setting; it is the Owner of the set's failpoints. It
// panics if source is empty, because other code tells the set's failpoints apart by their Owner.
func NewRuleSet(registry Registry, source string) *RuleSet {
	if source == "" {
		//nolint:forbidigo // a missing source is a programming error
		panic("failpoint: NewRuleSet requires a source")
	}
	return &RuleSet{registry: registry, source: source, owned: make(map[Key]ownedFailpoint)}
}

// Apply makes the failpoints of the set match rules. It creates the failpoints of new rules,
// reconfigures those whose rule changed, and removes those whose rule is no longer listed. A
// failpoint whose rule is unchanged keeps its state, such as its counts and held blocks; changing
// only Disabled suspends or resumes it without resetting it. A failpoint of the set that other
// code removed is created again.
//
// Apply returns an error and changes nothing if a rule is invalid, or if other code registered a
// failpoint under the key of a rule. A failpoint of the set that other code removed or replaced
// is disabled in either case, so it releases the executions it holds.
func (s *RuleSet) Apply(rules []Rule) error {
	if err := ValidateRules(rules); err != nil {
		return err
	}
	keys := make(map[Key]bool, len(rules))
	// The settings call the error constructors, which must not run under the registry's lock.
	resolved := make([]settings[error], len(rules))
	for i, r := range rules {
		keys[r.key()] = true
		resolved[i] = r.settings()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errRuleSetClosed
	}

	var err error
	// Configure serializes Apply with the multi-step changes of other code, such as a
	// FailpointService, and update with every other change to the registry.
	s.registry.Configure(func() {
		s.registry.update(func(failpoints map[Key]any) {
			s.forgetDetachedLocked(failpoints)
			if err = s.checkCollisionsLocked(failpoints, rules); err != nil {
				return
			}
			for key, owned := range s.owned {
				if !keys[key] {
					s.removeLocked(failpoints, key, owned)
				}
			}
			for i, r := range rules {
				s.applyLocked(failpoints, r, resolved[i])
			}
		})
	})
	return err
}

// forgetDetachedLocked disables and forgets the failpoints of the set that other code removed from
// failpoints or replaced. The caller must hold s.mu.
func (s *RuleSet) forgetDetachedLocked(failpoints map[Key]any) {
	for key, owned := range s.owned {
		if fp, ok := failpoints[key]; !ok || fp != any(owned.fp) {
			delete(s.owned, key)
			owned.fp.Disable()
		}
	}
}

// checkCollisionsLocked returns an error that lists the keys of rules that other code holds. The
// caller must hold s.mu, and the set must own every failpoint it remembers.
func (s *RuleSet) checkCollisionsLocked(failpoints map[Key]any, rules []Rule) error {
	var collisions []Key
	for _, r := range rules {
		key := r.key()
		if _, owned := s.owned[key]; owned {
			continue
		}
		if _, ok := failpoints[key]; ok {
			collisions = append(collisions, key)
		}
	}
	if len(collisions) > 0 {
		return fmt.Errorf("failpoint rules for keys of failpoints that the rule set didn't create: %v", collisions)
	}
	return nil
}

// applyLocked applies r, whose settings are c. The caller must hold s.mu, and the key of r must be
// free or owned.
func (s *RuleSet) applyLocked(failpoints map[Key]any, r Rule, c settings[error]) {
	key := r.key()
	if owned, ok := s.owned[key]; ok {
		if !owned.rule.sameConfiguration(r) {
			owned.fp.install(c)
		} else if owned.rule.Disabled != r.Disabled {
			owned.fp.SetEnabled(!r.Disabled)
		}
		s.owned[key] = ownedFailpoint{rule: r, fp: owned.fp}
		return
	}
	fp := NewOwnedFailpoint[error](key, s.source)
	fp.install(c)
	failpoints[key] = fp
	s.owned[key] = ownedFailpoint{rule: r, fp: fp}
}

// removeLocked removes a failpoint of the set. The caller must hold s.mu.
func (s *RuleSet) removeLocked(failpoints map[Key]any, key Key, owned ownedFailpoint) {
	delete(s.owned, key)
	if failpoints[key] == any(owned.fp) {
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
	s.registry.Configure(func() {
		s.registry.update(func(failpoints map[Key]any) {
			for key, owned := range s.owned {
				s.removeLocked(failpoints, key, owned)
			}
		})
	})
}
