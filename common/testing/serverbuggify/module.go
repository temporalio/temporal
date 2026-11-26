// Package serverbuggify configures BUGGIFY, seeded random failpoint activation (see
// go.temporal.io/testx/failpoint/buggify), for a server with failpoints enabled, from the
// testing.buggify dynamic config setting.
package serverbuggify

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/mitchellh/mapstructure"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	_ "go.temporal.io/server/common/testing/failpointerrors" // the errors that the keys name
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
	"go.uber.org/fx"
)

// Config is the value of Setting. Without a Seed, which is also the value of a removed setting,
// BUGGIFY is off.
type Config struct {
	Seed uint64
	// Keys override the default keys: an entry with the Match of a default merges into it, field by
	// field (a field that it doesn't set keeps the default's), and the default keeps its position.
	// An entry with another Match comes before all the defaults, in order, and an unset probability
	// of it is 0. Without the defaults, the entries are the keys.
	Keys []Key
	// Defaults is whether the default keys apply, true if unset.
	Defaults *bool
	// Trace prints a line per BUGGIFY decision to the standard output, see buggify.TraceTo.
	Trace bool
}

// Key is an entry of Config.Keys, see buggify.Key.
type Key struct {
	Match                 string
	ActivationProbability *float64
	FireProbability       *float64
	Error                 string
}

// defaultKeys fail the gRPC and HTTP calls between services with Unavailable, and make matching
// skip sync matches. No persistence, response or health key fails: the entry of the health service
// comes before the gRPC entry that would match it. The last match is matching.SkipSyncMatchKey.
var defaultKeys = []Key{
	{Match: "grpc:/grpc.health.v1.Health/*", ActivationProbability: new(0.0), FireProbability: new(0.0)},
	{Match: "grpc:*", ActivationProbability: new(0.1), FireProbability: new(0.01), Error: "Unavailable"},
	{Match: "http:*", ActivationProbability: new(0.1), FireProbability: new(0.01), Error: "Unavailable"},
	{Match: "matching.skipSyncMatch", ActivationProbability: new(0.25), FireProbability: new(0.25)},
}

// Setting holds the BUGGIFY configuration of the server, see Config. Its conversion keeps the raw
// value: the module decodes it, so it can fail startup on an invalid initial value and keep the
// previous configuration on an invalid update.
var Setting = dynamicconfig.NewGlobalTypedSettingWithConverter(
	"testing.buggify",
	func(v any) (any, error) { return v, nil },
	any(nil),
	`testing.buggify configures BUGGIFY, seeded random failpoint activation, for servers with failpoints
enabled: seed, keys (match, activationProbability, fireProbability, error), defaults and trace. A server
doesn't start with an invalid value, and keeps the previous configuration if the value becomes invalid.`,
)

// decode reads a raw value strictly, and merges its keys with the defaults. It returns an error
// for any value: the strict decoding panics for some, for example a nested map with a key that is
// not a string, which YAML can produce.
func decode(raw any) (_ Config, _ buggify.Config, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("invalid value: %v", r)
		}
	}()
	var c Config
	d, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{ErrorUnused: true, Result: &c, MatchName: strings.EqualFold})
	if err == nil && raw != nil {
		err = d.Decode(raw)
	}
	if err != nil {
		return c, buggify.Config{}, err
	}
	defaults := slices.Clone(defaultKeys)
	if c.Defaults != nil && !*c.Defaults {
		defaults = nil
	}
	var keys []Key
	merged := map[string]bool{}
	for _, k := range c.Keys {
		i := slices.IndexFunc(defaults, func(d Key) bool { return d.Match == k.Match })
		if i < 0 || merged[k.Match] { // A repeated match stays a duplicate, which Validate rejects.
			keys = append(keys, k)
			continue
		}
		merged[k.Match] = true
		d := &defaults[i]
		d.ActivationProbability = cmp.Or(k.ActivationProbability, d.ActivationProbability)
		d.FireProbability = cmp.Or(k.FireProbability, d.FireProbability)
		d.Error = cmp.Or(k.Error, d.Error)
	}
	out := buggify.Config{Seed: c.Seed}
	for _, k := range append(keys, defaults...) {
		out.Keys = append(out.Keys, buggify.Key{
			Match: k.Match, Error: k.Error,
			ActivationProbability: *cmp.Or(k.ActivationProbability, new(0.0)), FireProbability: *cmp.Or(k.FireProbability, new(0.0)),
		})
	}
	return c, out, out.Validate()
}

// Module applies Setting to BUGGIFY of the failpoint registry of the process, if the startup switch
// enabled failpoints; without them it installs nothing and subscribes to nothing. It belongs in the
// graph of the server process, before the server's lifecycle hook, so BUGGIFY is configured before
// the services start.
var Module = fx.Options(
	fx.Provide(func(r *failpoint.Registry, c dynamicconfig.Client, l log.Logger) *applier {
		if r == nil {
			return nil
		}
		return &applier{buggify: buggify.New(r), client: c, logger: l}
	}),
	fx.Invoke(func(lc fx.Lifecycle, a *applier) {
		if a != nil {
			lc.Append(fx.Hook{OnStart: a.start, OnStop: a.stop})
		}
	}),
)

type applier struct {
	buggify *buggify.Buggify
	client  dynamicconfig.Client
	logger  log.Logger

	mu      sync.Mutex
	stopped bool
	cancel  func()
	// tracing is whether the applier attached the trace observer, so that it leaves an observer
	// that someone else installed alone.
	tracing bool
}

// start applies the setting, and fails if it is invalid, so the server doesn't start without the
// configuration that it was asked to run.
func (a *applier) start(ctx context.Context) error {
	if n, ok := a.client.(dynamicconfig.NotifyingClient); ok {
		a.cancel = n.Subscribe(func(changed map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue) {
			if _, ok := changed[Setting.Key()]; ok {
				go a.update()
			}
		})
	}
	if err := a.apply(); err != nil {
		_ = a.stop(ctx) // fx doesn't stop a hook that failed to start.
		return fmt.Errorf("invalid dynamic config setting %s: %w", Setting.Key(), err)
	}
	return nil
}

// update applies the setting after a change, and keeps the previous configuration if it is invalid.
func (a *applier) update() {
	if err := a.apply(); err != nil {
		a.logger.Error("Keeping the previous BUGGIFY configuration", tag.Error(err))
	}
}

// apply sets BUGGIFY from the value of the setting, unless the applier stopped.
func (a *applier) apply() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	var raw any
	for _, cv := range a.client.GetValue(Setting.Key()) {
		if cv.Constraints == (dynamicconfig.Constraints{}) {
			raw = cv.Value
			break
		}
	}
	c, cfg, err := decode(raw)
	if err != nil || a.stopped {
		return err
	}
	previous := a.buggify.Config().Seed
	if err := a.buggify.Set(cfg); err != nil {
		return err
	}
	switch {
	case c.Trace && !a.tracing:
		a.buggify.OnDecision(buggify.TraceTo(os.Stdout))
		a.tracing = true
	case !c.Trace && a.tracing:
		a.buggify.OnDecision(nil)
		a.tracing = false
	default:
	}
	if msg := "BUGGIFY disabled"; cfg.Seed != previous {
		if cfg.Enabled() {
			msg = "BUGGIFY enabled"
		}
		a.logger.Info(msg, tag.NewUInt64("buggify-seed", cfg.Seed))
	}
	return nil
}

// stop disables BUGGIFY, after it has written the trace lines of the decisions that it made.
func (a *applier) stop(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.stopped {
		a.stopped = true
		if a.cancel != nil {
			a.cancel()
		}
		_ = a.buggify.Set(buggify.Config{}) // The zero value is valid.
		_ = a.buggify.StopObserving(ctx)
	}
	return nil
}
