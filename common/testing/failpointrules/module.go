package failpointrules

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/testx/failpoint"
	"go.uber.org/fx"
)

// Setting holds the failpoint rules of the server. Its conversion rejects an invalid value, so
// Setting.Validate does. The module reads the raw value from dynamicconfig.Client directly so it
// can fail startup on an invalid initial value and keep previous rules on an invalid update.
var Setting = dynamicconfig.NewGlobalTypedSettingWithConverter(
	"testing.failpointRules",
	ConvertRules,
	[]failpoint.Rule(nil),
	`testing.failpointRules is a list of failpoint rules that servers with failpoints enabled apply
to the failpoints of the server, see failpoint.Rule. A server doesn't start with an invalid value,
and keeps the previous rules if the value becomes invalid.`,
)

// Module applies Setting to the failpoints of the process, if the startup switch enabled
// failpoints; without them it installs nothing and subscribes to nothing. It belongs in the graph of the
// server process, not of each service, so that one rule set manages the shared registry. It
// starts before the services when it comes before the server's lifecycle hook.
var Module = fx.Options(
	fx.Provide(newApplierFromParams),
	fx.Invoke(func(lc fx.Lifecycle, s *applier) {
		if s != nil {
			lc.Append(fx.Hook{OnStart: s.start, OnStop: s.stop})
		}
	}),
)

// ReleaseModule removes the failpoints of the rules when the server stops. It belongs after the
// server's lifecycle hook, so fx runs it before the services stop, and a block of a rule doesn't
// hold up their shutdown.
var ReleaseModule = fx.Invoke(func(lc fx.Lifecycle, s *applier) {
	if s != nil {
		lc.Append(fx.StopHook(s.stop))
	}
})

type params struct {
	fx.In
	FailpointSource *failpoint.Registry
	Client          dynamicconfig.Client
	Logger          log.Logger
}

// newApplierFromParams returns nil without a failpoint registry.
func newApplierFromParams(p params) *applier {
	if p.FailpointSource == nil {
		return nil
	}
	return newApplier(p.FailpointSource, p.Client, p.Logger)
}

// applier applies the value of Setting to a rule set.
type applier struct {
	set    *failpoint.RuleSet
	client dynamicconfig.Client
	logger log.Logger

	mu      sync.Mutex
	stopped bool
	cancel  func()
	// installed are the rules whose failpoints the rule set holds after the last change, to log the
	// ones that a change removes. A rule that the rule set skipped is not in it.
	installed map[ruleID]bool
}

func newApplier(registry *failpoint.Registry, client dynamicconfig.Client, logger log.Logger) *applier {
	return &applier{
		set:    failpoint.NewRuleSet(registry),
		client: client,
		logger: logger,
	}
}

func (s *applier) getRules() ([]failpoint.Rule, error) {
	for _, cv := range s.client.GetValue(Setting.Key()) {
		if cv.Constraints == (dynamicconfig.Constraints{}) {
			return ConvertRules(cv.Value)
		}
	}
	return nil, nil
}

// start applies the value of the setting. It fails if the value is invalid, so the server doesn't
// start without the rules that it was asked to run.
func (s *applier) start(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if notifying, ok := s.client.(dynamicconfig.NotifyingClient); ok {
		s.cancel = notifying.Subscribe(func(changed map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue) {
			if _, ok := changed[Setting.Key()]; ok {
				go s.update()
			}
		})
	}
	rules, err := s.getRules()
	if err != nil {
		if s.cancel != nil {
			s.cancel()
			s.cancel = nil
		}
		return invalidValueError(err)
	}
	s.apply(rules)
	return nil
}

// update applies the current value of the setting, whichever change triggered it: callbacks can
// arrive out of order. If the value is invalid, update keeps the previous rules.
func (s *applier) update() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	rules, err := s.getRules()
	if err != nil {
		s.logger.Error("Keeping the previous failpoint rules", tag.Error(invalidValueError(err)))
		return
	}
	s.apply(rules)
}

// ruleID identifies the failpoint of a rule.
type ruleID struct{ key, scope string }

// apply applies valid rules. The rule set skips the rules for keys that other code holds, such as a
// failpoint that a test set, and applies the others. It tries the skipped rules again at the next
// change of the setting. The skipped keys are logged. So are the rules that were applied, with the
// seed of a probability that picked one, and the rules that the change removed.
func (s *applier) apply(rules []failpoint.Rule) {
	applied, err := s.set.Apply(rules)
	// The rule set keeps the failpoint of a rule that was installed and is still listed, and it
	// reports the other rules that it installed. A rule that it skipped is in neither.
	installed := make(map[ruleID]bool, len(rules))
	for _, a := range applied {
		installed[ruleID{a.Key, a.Scope}] = true
		tags := []tag.Tag{tag.NewStringTag("failpoint-key", a.Key), tag.NewStringTag("failpoint-scope", a.Scope)}
		if a.Seed != 0 {
			tags = append(tags, tag.NewInt64("failpoint-seed", a.Seed))
		}
		s.logger.Info("Applied failpoint rule", tags...)
	}
	for _, r := range rules {
		if id := (ruleID{r.Key, r.Scope}); s.installed[id] {
			installed[id] = true
		}
	}
	if err != nil {
		s.logger.Warn("Skipped failpoint rules for keys of other failpoints", tag.Key(Setting.Key().String()), tag.Error(err))
	}
	s.logRemoved(rules)
	s.installed = installed
}

// logRemoved logs the installed rules of the previous change that rules no longer has.
func (s *applier) logRemoved(rules []failpoint.Rule) {
	kept := make(map[ruleID]bool, len(rules))
	for _, r := range rules {
		kept[ruleID{r.Key, r.Scope}] = true
	}
	removed := slices.DeleteFunc(slices.Collect(maps.Keys(s.installed)), func(id ruleID) bool { return kept[id] })
	slices.SortFunc(removed, func(a, b ruleID) int { return cmp.Or(strings.Compare(a.key, b.key), strings.Compare(a.scope, b.scope)) })
	for _, id := range removed {
		s.logger.Info("Removed failpoint rule", tag.NewStringTag("failpoint-key", id.key), tag.NewStringTag("failpoint-scope", id.scope))
	}
}

func (s *applier) stop(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil
	}
	s.stopped = true
	if s.cancel != nil {
		s.cancel()
	}
	s.set.Close()
	return nil
}

func invalidValueError(err error) error {
	return fmt.Errorf("invalid dynamic config setting %s: %w", Setting.Key(), err)
}
