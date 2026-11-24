package buggify

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/testx/failpoint"
	fpbuggify "go.temporal.io/testx/failpoint/buggify"
	"go.uber.org/fx"
)

// settingValue is the value of Setting. Dynamic config converts the setting on every load, also
// while failpoints are not enabled, so the conversion only keeps the raw value and never fails.
// The module, which only runs with failpoints enabled, validates it with parsed, so an invalid
// value reaches the subscriber in err, instead of dynamic config replacing it with the default.
type settingValue struct {
	raw   any
	value Value
	err   error
}

// parsed returns v with the value and the error of its raw value.
func (v settingValue) parsed() settingValue {
	v.value, v.err = ConvertValue(v.raw)
	return v
}

// Setting configures BUGGIFY for the server, see Value. An invalid value stops the server.
var Setting = dynamicconfig.NewGlobalTypedSettingWithConverter(
	SettingKey,
	func(value any) (settingValue, error) {
		return settingValue{raw: value}, nil
	},
	settingValue{},
	`testing.buggify configures BUGGIFY, seeded random failpoint activation, if failpoints are enabled: a map
with seed (0: FAILPOINT_BUGGIFY_SEED), disabled, activation, fire, error,
prefixes (prefix, activation, fire, error), include and exclude, which override the server's
defaults. Settings marked as buggifiable are randomized from the seed at startup. An invalid
value stops the server.`,
)

// Module applies Setting, and the seed environment variables, to the failpoint registry of the
// process, and logs the seed and the BUGGIFY activation decisions. It belongs in the graph of
// the server process, before the server's lifecycle hook, so BUGGIFY is configured before the
// services start.
var Module = fx.Options(
	fx.Provide(newSourceFromParams),
	fx.Invoke(func(lc fx.Lifecycle, s *source) {
		if s != nil {
			lc.Append(fx.Hook{OnStart: s.start, OnStop: s.stop})
		}
	}),
)

// WrapDynamicConfigClient resolves the BUGGIFY configuration of the server once, from Setting, as
// client returns it, and the environment. If failpoints are not enabled, it returns client as it
// is, and BUGGIFY is never configured. If it enables BUGGIFY, it returns client with the
// settings marked with dynamicconfig.Buggifiable randomized: the values are chosen from the seed
// and logged. Values that client sets without constraints take precedence. It also returns the
// configuration, which Module applies at startup. It returns an error if Setting or the
// environment is invalid.
func WrapDynamicConfigClient(client dynamicconfig.Client, logger log.Logger, failpointsEnabled bool) (dynamicconfig.Client, Startup, error) {
	if !failpointsEnabled {
		return client, Startup{}, nil
	}
	return wrapDynamicConfigClient(client, logger, os.LookupEnv)
}

func wrapDynamicConfigClient(client dynamicconfig.Client, logger log.Logger, lookup func(string) (string, bool)) (dynamicconfig.Client, Startup, error) {
	env, err := ReadEnv(lookup)
	if err != nil {
		return nil, Startup{}, err
	}
	value, err := ConvertValue(globalValue(client.GetValue(Setting.Key())))
	if err != nil {
		return nil, Startup{}, invalidValueError(err)
	}
	config, seedSource, err := Resolve(value, env)
	if err != nil {
		return nil, Startup{}, err
	}
	startup := Startup{Resolved: true, Value: value, Env: env, Config: config, SeedSource: seedSource}
	if !config.Enabled() {
		return client, startup, nil
	}
	knobs := ChooseKnobs(config, dynamicconfig.BuggifiableSettings())
	keys := slices.SortedFunc(maps.Keys(knobs), func(a, b dynamicconfig.Key) int {
		return strings.Compare(a.String(), b.String())
	})
	for _, key := range keys {
		logger.Info("BUGGIFY randomized dynamic config setting",
			tag.Key(key.String()), tag.Value(knobs[key]), tag.NewUInt64("buggify-seed", config.Seed))
	}
	return NewKnobClient(client, knobs), startup, nil
}

// globalValue returns the value without constraints, as dynamic config resolves a global setting,
// or nil.
func globalValue(cvs []dynamicconfig.ConstrainedValue) any {
	for _, cv := range cvs {
		if cv.Constraints == (dynamicconfig.Constraints{}) {
			return cv.Value
		}
	}
	return nil
}

type params struct {
	fx.In
	Failpoints *failpoints.Source
	Collection *dynamicconfig.Collection
	Logger     log.Logger
	// Startup is set by servers that resolved BUGGIFY with WrapDynamicConfigClient.
	Startup Startup `optional:"true"`
}

// newSourceFromParams returns nil without a failpoint registry.
func newSourceFromParams(p params) *source {
	registry := p.Failpoints.Failpoints()
	if registry == nil {
		return nil
	}
	var gomad io.Writer
	if os.Getenv(GomadEventsEnv) != "" {
		gomad = os.Stdout
	}
	s := newSource(registry, p.Collection, p.Logger, os.LookupEnv, gomad, func(err error) {
		p.Logger.Fatal("Invalid BUGGIFY configuration", tag.Key(SettingKey), tag.Error(err))
	})
	s.startup = p.Startup
	return s
}

// source applies the value of Setting to a registry.
type source struct {
	registry failpoint.Registry
	// buggify is the BUGGIFY of registry, installed when the source is created. If other code
	// replaces it as the registry's fallback, the source leaves the replacement alone.
	buggify    *fpbuggify.Buggify
	collection *dynamicconfig.Collection
	logger     log.Logger
	// lookup reads the environment at start, unless startup has it, into env.
	lookup func(string) (string, bool)
	env    Env
	// gomad receives the BUGGIFY events as gomad event lines, if not nil.
	gomad io.Writer
	// fatal handles an invalid value after start.
	fatal func(error)
	// startup is the configuration to start with, if resolved. Later changes of Setting apply.
	startup Startup

	mu          sync.Mutex
	stopped     bool
	cancel      func()
	unsubscribe func()
}

func newSource(
	registry failpoint.Registry,
	collection *dynamicconfig.Collection,
	logger log.Logger,
	lookup func(string) (string, bool),
	gomad io.Writer,
	fatal func(error),
) *source {
	return &source{
		registry:   registry,
		buggify:    fpbuggify.For(registry),
		collection: collection,
		logger:     logger,
		lookup:     lookup,
		gomad:      gomad,
		fatal:      fatal,
	}
}

func (s *source) start(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unsubscribe = s.registry.Subscribe(s.report)
	value, cancel := Setting.Subscribe(s.collection)(s.update)
	s.cancel = cancel
	value = value.parsed()
	var err error
	if s.startup.Resolved {
		s.env = s.startup.Env
	} else {
		s.env, err = ReadEnv(s.lookup)
	}
	switch {
	case err != nil:
	case s.startup.Resolved && value.err == nil && reflect.DeepEqual(value.value, s.startup.Value):
		// The randomized settings were chosen with this configuration.
		err = s.setLocked(s.startup.Config, s.startup.SeedSource)
	default:
		// Without a startup configuration, or if the setting changed since the server resolved
		// it, the current value applies, as a later change would.
		err = s.applyLocked(value)
	}
	if err != nil {
		// fx doesn't stop a hook that failed to start.
		s.stopLocked()
		return err
	}
	return nil
}

// update handles a change of the value to delivered. An invalid delivered value is fatal, even if
// a later change replaced it. Callbacks can arrive out of order, so update applies the current
// value instead of delivered.
func (s *source) update(delivered settingValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	delivered = delivered.parsed()
	if delivered.err != nil {
		s.fatal(invalidValueError(delivered.err))
		return
	}
	// The delivered value is validated even if a later change already replaced it.
	if _, _, err := Resolve(delivered.value, s.env); err != nil {
		s.fatal(err)
		return
	}
	if err := s.applyLocked(Setting.Get(s.collection)().parsed()); err != nil {
		s.fatal(err)
	}
}

func (s *source) applyLocked(value settingValue) error {
	if value.err != nil {
		return invalidValueError(value.err)
	}
	config, seedSource, err := Resolve(value.value, s.env)
	if err != nil {
		return err
	}
	return s.setLocked(config, seedSource)
}

func (s *source) setLocked(config fpbuggify.Config, seedSource string) error {
	previous := s.buggify.Config()
	if err := s.buggify.Set(config); errors.Is(err, fpbuggify.ErrNotInstalled) {
		s.logger.Warn("BUGGIFY is no longer the fallback of the failpoint registry; its configuration has no effect",
			tag.NewUInt64("buggify-seed", config.Seed))
		return nil
	} else if err != nil {
		return err
	}
	switch {
	case config.Seed == previous.Seed:
	case config.Enabled():
		s.logger.Info(fmt.Sprintf("BUGGIFY enabled; reproduce with %s=%d", fpbuggify.SeedEnv, config.Seed),
			tag.NewUInt64("buggify-seed", config.Seed), tag.NewStringTag("buggify-seed-source", seedSource))
	default:
		s.logger.Info("BUGGIFY disabled")
	}
	return nil
}

// report logs a BUGGIFY activation decision, and passes BUGGIFY events to gomad.
func (s *source) report(e failpoint.Event) {
	if e.Kind == failpoint.FallbackActivationEvent {
		if e.Activated {
			s.logger.Info("BUGGIFY activated failpoint", tag.Key(e.Key.Name()))
		} else {
			s.logger.Debug("BUGGIFY did not activate failpoint", tag.Key(e.Key.Name()))
		}
	}
	if s.gomad != nil {
		if line, ok := GomadLine(e); ok {
			_, _ = fmt.Fprintln(s.gomad, line)
		}
	}
}

// stop disables BUGGIFY.
func (s *source) stop(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopLocked()
	return nil
}

func (s *source) stopLocked() {
	if s.stopped {
		return
	}
	s.stopped = true
	if s.cancel != nil {
		s.cancel()
	}
	if s.unsubscribe != nil {
		s.unsubscribe()
	}
	// Disabling a BUGGIFY that is no longer installed only returns ErrNotInstalled.
	_ = s.buggify.Set(fpbuggify.Config{})
}

func invalidValueError(err error) error {
	return fmt.Errorf("invalid dynamic config setting %s: %w", SettingKey, err)
}
