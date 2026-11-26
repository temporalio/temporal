package serverbuggify

import (
	"context"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
	"go.uber.org/fx"
)

// Module applies the failpoints.buggify static config to the failpoint registry of the process,
// and logs the seed and the BUGGIFY activation decisions. It belongs in the graph of
// the server process, before the server's lifecycle hook, so BUGGIFY is configured before the
// services start.
var Module = fx.Options(
	fx.Provide(newApplierFromParams),
	fx.Invoke(func(lc fx.Lifecycle, s *applier) {
		if s != nil {
			lc.Append(fx.Hook{OnStart: s.start, OnStop: s.stop})
		}
	}),
)

// RandomizeSettings resolves the BUGGIFY configuration of the server from the failpoints.buggify
// block of the static config. If failpoints are not enabled, it returns client as it is, and BUGGIFY
// is never configured, whatever the block says. If it enables BUGGIFY, it returns client with the
// settings of BuggifiableSettings randomized: the values are chosen from the seed and logged, for
// the settings that an entry of the block's keys activates by the name SettingPrefix+key. Values
// that client sets without constraints take precedence. It also returns the static configuration
// that it read, which Module configures BUGGIFY from at startup. It returns an error if the block
// is invalid.
func RandomizeSettings(
	client dynamicconfig.Client,
	logger log.Logger,
	failpointsEnabled bool,
	buggifyConfig config.FailpointsBuggify,
) (dynamicconfig.Client, StaticConfig, error) {
	if !failpointsEnabled {
		return client, StaticConfig{}, nil
	}
	return randomizeSettings(client, logger, buggifyConfig)
}

func randomizeSettings(client dynamicconfig.Client, logger log.Logger, buggifyConfig config.FailpointsBuggify) (dynamicconfig.Client, StaticConfig, error) {
	static, err := ReadStatic(buggifyConfig)
	if err != nil {
		return nil, StaticConfig{}, err
	}
	if !static.Config.Enabled() {
		return client, static, nil
	}
	chosen := ChooseSettings(static.Config, BuggifiableSettings())
	keys := slices.SortedFunc(maps.Keys(chosen), func(a, b dynamicconfig.Key) int {
		return strings.Compare(a.String(), b.String())
	})
	for _, key := range keys {
		logger.Info("BUGGIFY randomized dynamic config setting",
			tag.Key(key.String()), tag.Value(chosen[key]), tag.NewUInt64("buggify-seed", static.Config.Seed))
	}
	return NewRandomizingClient(client, chosen), static, nil
}

type params struct {
	fx.In
	FailpointSource *failpoint.Registry
	Logger          log.Logger
	// Static is the static config that servers read with RandomizeSettings.
	Static StaticConfig `optional:"true"`
}

// newApplierFromParams returns nil without a failpoint registry.
func newApplierFromParams(p params) *applier {
	if p.FailpointSource == nil {
		return nil
	}
	var trace io.Writer
	if p.Static.Trace {
		trace = os.Stdout
	}
	return newApplier(p.FailpointSource, p.Logger, p.Static, trace)
}

// applier applies the static configuration to a registry.
type applier struct {
	registry *failpoint.Registry
	// buggify is the BUGGIFY policy of registry.
	buggify *buggify.Buggify
	logger  log.Logger
	// static is the BUGGIFY configuration of the static config.
	static StaticConfig
	// trace receives a trace line for each BUGGIFY decision, if not nil, see TraceLine.
	trace io.Writer

	mu      sync.Mutex
	stopped bool
}

func newApplier(
	registry *failpoint.Registry,
	logger log.Logger,
	static StaticConfig,
	trace io.Writer,
) *applier {
	return &applier{
		registry: registry,
		buggify:  buggify.New(registry),
		logger:   logger,
		static:   static,
		trace:    trace,
	}
}

// start applies the static configuration. It fails if the configuration is invalid, so the server
// doesn't start without the configuration that it was asked to run.
func (s *applier) start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buggify.OnDecision(s.report)
	if err := s.setLocked(s.static.Config); err != nil {
		// fx doesn't stop a hook that failed to start.
		s.stopLocked(ctx)
		return err
	}
	return nil
}

func (s *applier) setLocked(cfg buggify.Config) error {
	previous := s.buggify.Config()
	if err := s.buggify.Set(cfg); err != nil {
		return err
	}
	switch {
	case cfg.Seed == previous.Seed:
	case cfg.Enabled():
		s.logger.Info("BUGGIFY enabled", tag.NewUInt64("buggify-seed", cfg.Seed))
	default:
		s.logger.Info("BUGGIFY disabled")
	}
	return nil
}

// report logs a BUGGIFY decision, and traces it if the applier has a trace writer. BUGGIFY delivers events
// from the goroutine of any execution, one at a time, and possibly after the execution that
// decided has returned, so report only logs and writes.
func (s *applier) report(e buggify.Event) {
	switch e.Kind {
	case buggify.EventActivation:
		if e.Activated {
			s.logger.Info("BUGGIFY activated failpoint", tag.Key(e.Name))
		} else {
			s.logger.Debug("BUGGIFY did not activate failpoint", tag.Key(e.Name))
		}
	case buggify.EventScopeLimit:
		s.logger.Warn("BUGGIFY keeps no more execution counters per scope; which executions fire is no longer reproducible")
	default:
		// The other events are only traced.
	}
	if s.trace != nil {
		if line, ok := TraceLine(e); ok {
			_, _ = fmt.Fprintln(s.trace, line)
		}
	}
}

// stop disables BUGGIFY. It first writes the trace lines of the decisions that BUGGIFY has already
// made, for as long as ctx allows.
func (s *applier) stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopLocked(ctx)
	return nil
}

func (s *applier) stopLocked(ctx context.Context) {
	if s.stopped {
		return
	}
	s.stopped = true
	// Config{} is valid, so Set can't fail. No decision is made after it, and the observer receives
	// the events of the decisions before it.
	_ = s.buggify.Set(buggify.Config{})
	if err := s.buggify.StopObserving(ctx); err != nil {
		s.logger.Warn("BUGGIFY dropped events at shutdown", tag.Error(err))
	}
}
