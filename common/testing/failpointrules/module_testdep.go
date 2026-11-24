//go:build test_dep

package failpointrules

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/testx/failpoint"
	"go.uber.org/fx"
)

// rulesValue is the value of Setting. Its conversion never fails, so an invalid value reaches the
// subscriber in err, instead of dynamic config replacing it with the default.
type rulesValue struct {
	rules []failpoint.Rule
	err   error
}

// Setting holds the failpoint rules of the server. An invalid value stops the server.
var Setting = dynamicconfig.NewGlobalTypedSettingWithConverter(
	"testing.failpointRules",
	func(value any) (rulesValue, error) {
		rules, err := ConvertRules(value)
		return rulesValue{rules: rules, err: err}, nil
	},
	rulesValue{},
	`testing.failpointRules is a list of failpoint rules that test builds apply to the failpoints
of the server, see failpoint.Rule. An invalid value stops the server.`,
)

// Module applies Setting to the failpoints of the test hooks. It belongs in the graph of the
// server process, not of each service, so that one rule set manages the shared registry. It
// starts before the services when it comes before the server's lifecycle hook.
var Module = fx.Options(
	fx.Provide(newSourceFromParams),
	fx.Invoke(func(lc fx.Lifecycle, s *source) {
		if s != nil {
			lc.Append(fx.Hook{OnStart: s.start, OnStop: s.stop})
		}
	}),
)

// ReleaseModule removes the failpoints of the rules when the server stops. It belongs after the
// server's lifecycle hook, so fx runs it before the services stop, and a block of a rule doesn't
// hold up their shutdown.
var ReleaseModule = fx.Invoke(func(lc fx.Lifecycle, s *source) {
	if s != nil {
		lc.Append(fx.StopHook(s.stop))
	}
})

type params struct {
	fx.In
	TestHooks  testhooks.TestHooks
	Collection *dynamicconfig.Collection
	Logger     log.Logger
}

// newSourceFromParams returns nil without a failpoint registry.
func newSourceFromParams(p params) *source {
	registry := p.TestHooks.Failpoints()
	if registry == nil {
		return nil
	}
	return newSource(registry, p.Collection, p.Logger, func(err error) {
		p.Logger.Fatal("Invalid failpoint rules", tag.Key(Setting.Key().String()), tag.Error(err))
	})
}

// source applies the value of Setting to a rule set.
type source struct {
	set        *failpoint.RuleSet
	collection *dynamicconfig.Collection
	logger     log.Logger
	// fatal handles an invalid value after start.
	fatal func(error)

	mu      sync.Mutex
	stopped bool
	cancel  func()
}

func newSource(registry failpoint.Registry, collection *dynamicconfig.Collection, logger log.Logger, fatal func(error)) *source {
	return &source{
		set:        failpoint.NewRuleSet(registry, "dynamic config setting "+Setting.Key().String()),
		collection: collection,
		logger:     logger,
		fatal:      fatal,
	}
}

func (s *source) start(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, cancel := Setting.Subscribe(s.collection)(s.update)
	s.cancel = cancel
	if value.err != nil {
		// fx doesn't stop a hook that failed to start.
		s.stopped = true
		cancel()
		return invalidValueError(value.err)
	}
	s.apply(value.rules)
	return nil
}

// update handles a change of the value to delivered. An invalid delivered value is fatal, even if
// a later change replaced it. Callbacks can arrive out of order, so update applies the current
// value instead of delivered.
func (s *source) update(delivered rulesValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	if delivered.err != nil {
		s.fatal(invalidValueError(delivered.err))
		return
	}
	value := Setting.Get(s.collection)()
	if value.err != nil {
		s.fatal(invalidValueError(value.err))
		return
	}
	s.apply(value.rules)
}

// apply applies valid rules. The rule set only fails for keys that other code holds, such as a
// failpoint that a test set. It then keeps the previous rules, and the error is logged.
func (s *source) apply(rules []failpoint.Rule) {
	if err := s.set.Apply(rules); err != nil {
		s.logger.Error("Failed to apply some failpoint rules", tag.Key(Setting.Key().String()), tag.Error(err))
	}
}

func (s *source) stop(context.Context) error {
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
