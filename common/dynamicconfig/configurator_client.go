package dynamicconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/dynamicconfig/configurator"
	"go.temporal.io/server/common/dynamicconfig/configurator/types"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/util"
	"gopkg.in/yaml.v3"
)

// ExpressionFilePollInterval is how often the expression config file is stat'd for changes.
// It matches the minimum poll interval of the file based client.
const ExpressionFilePollInterval = 5 * time.Second

// AmbientConstraints describes the process-scoped dimensions that expressions can match on.
//
// These are exactly the dimensions the Constraints struct cannot express: adding one today
// means a new field, a new precedence order in cmd/tools/gendynamicconfig, and regenerating
// setting_gen.go. Here they are configuration.
type AmbientConstraints struct {
	// Environment, e.g. "production" or "staging".
	Environment string
	// AvailabilityZone or region, e.g. "us-west-2".
	AvailabilityZone string
	// ClusterName as configured in clusterMetadata.
	ClusterName string
	// ServiceName, e.g. "frontend", "history", "matching", "worker". Usually empty: a single
	// binary serves several services from one client, so this is only set when a process
	// hosts exactly one.
	ServiceName string
	// Custom carries operator-defined dimensions verbatim.
	Custom map[string]any
}

func (a AmbientConstraints) asMap() map[string]any {
	m := make(map[string]any, len(a.Custom)+6)
	maps.Copy(m, a.Custom)
	putNonEmpty(m, "env", a.Environment)
	putNonEmpty(m, "zone", a.AvailabilityZone)
	putNonEmpty(m, "cluster", a.ClusterName)
	putNonEmpty(m, "service", a.ServiceName)
	putNonEmpty(m, "serverVersion", headers.ServerVersion)
	if host, err := os.Hostname(); err == nil {
		putNonEmpty(m, "host", host)
	}
	return m
}

func putNonEmpty(m map[string]any, key, value string) {
	if value != "" {
		m[key] = value
	}
}

// ConfiguratorClient is a dynamicconfig.Client that resolves values by evaluating constraint
// expressions (see common/dynamicconfig/configurator/README.md) against the deployment
// dimensions of the running process.
//
// It layers over an inner Client, normally the file based one. A key present in the
// expression file is served entirely from there; every other key is delegated untouched. The
// two are not merged for a single key, so each key is configured in exactly one place.
//
// # How it fits the Client contract
//
// Expressions are evaluated once per key at load time, and the result is published as a
// single ConstrainedValue with *empty* Constraints. Every precedence order ends in the empty
// Constraints (cmd/tools/gendynamicconfig/main.go), so that one value is matched by every
// setting at every precedence, and nothing needs to be enumerated. GetValue is then a map
// lookup returning a stable slice, so reads cost what they cost today and Collection's
// conversion cache still hits.
//
// # What it cannot do
//
// GetValue is handed a Key and nothing else: it is not told the namespace, task queue, or
// shard of the call. An expression referencing those dimensions therefore has nothing to
// match against and falls through to its default. Expressions can constrain by *where the
// server is*, not by *what is being asked of it*.
//
// Note also that an unconstrained value loses to a more specific constrained default
// (findAndResolveWithConstrainedDefaults), exactly as an unconstrained value in the dynamic
// config file does today.
type ConfiguratorClient struct {
	logger  log.Logger
	inner   Client
	ambient map[string]any

	// values is replaced wholesale on reload; it is never mutated in place, both because
	// readers are lock-free and because the library's LoadKey is not safe to call
	// concurrently with Eval.
	values atomic.Pointer[ConfigValueMap]

	cancelInnerSubscription func()

	NotifyingClientImpl
}

var _ Client = (*ConfiguratorClient)(nil)
var _ NotifyingClient = (*ConfiguratorClient)(nil)

// NewConfiguratorClient returns a client with no expression configuration loaded, which
// therefore delegates everything to inner. inner may be nil, in which case keys that the
// expression file does not configure have no values and settings use their compiled-in
// defaults.
func NewConfiguratorClient(ambient AmbientConstraints, inner Client, logger log.Logger) *ConfiguratorClient {
	c := &ConfiguratorClient{
		logger:              logger,
		inner:               inner,
		ambient:             ambient.asMap(),
		NotifyingClientImpl: NewNotifyingClientImpl(),
	}
	c.values.Store(&ConfigValueMap{})

	// Forward the inner client's updates, so subscribers still see changes to the keys we
	// are not overriding.
	if notifying, ok := inner.(NotifyingClient); ok {
		c.cancelInnerSubscription = notifying.Subscribe(c.innerKeysChanged)
	}
	return c
}

// Stop releases the subscription to the inner client.
func (c *ConfiguratorClient) Stop() {
	if c.cancelInnerSubscription != nil {
		c.cancelInnerSubscription()
	}
}

// GetValue implements Client.
func (c *ConfiguratorClient) GetValue(key Key) []ConstrainedValue {
	if cvs, ok := (*c.values.Load())[key]; ok {
		return cvs
	}
	if c.inner == nil {
		return nil
	}
	return c.inner.GetValue(key)
}

// innerKeysChanged republishes an update from the inner client, substituting the effective
// value so that a key we are overriding is not reported as having changed to the inner
// client's value.
func (c *ConfiguratorClient) innerKeysChanged(changed map[Key][]ConstrainedValue) {
	overrides := *c.values.Load()
	effective := make(map[Key][]ConstrainedValue, len(changed))
	for key := range changed {
		if cvs, ok := overrides[key]; ok {
			effective[key] = cvs
		} else {
			effective[key] = changed[key]
		}
	}
	c.PublishUpdates(effective)
}

// yamlExprEntry is the on-disk form of one setting's expression configuration. It matches the
// schema of the upstream configurator library:
//
//	history.persistenceMaxQPS:
//	  defaultValue: 9000
//	  overrides:
//	    - matchString: '"zone" = "us-west-2" and "env" = "production"'
//	      matchResult: 18000
type yamlExprEntry struct {
	DefaultValue any `yaml:"defaultValue" json:"defaultValue"`
	Overrides    []struct {
		MatchString string `yaml:"matchString" json:"matchString"`
		MatchResult any    `yaml:"matchResult" json:"matchResult"`
	} `yaml:"overrides" json:"overrides"`
}

// LoadFile parses contents, evaluates every key against this process's ambient constraints,
// and atomically installs the results. On any error the previous values are left in place, so
// a bad edit degrades to the last good state rather than to compiled-in defaults.
func (c *ConfiguratorClient) LoadFile(contents []byte) error {
	var entries map[string]yamlExprEntry
	if err := yaml.Unmarshal(contents, &entries); err != nil {
		return fmt.Errorf("decoding expression config: %w", err)
	}

	cfg := configurator.New[int]()
	outcomes := make(map[Key][]any, len(entries))
	names := make(map[Key]string, len(entries))

	var errs []error
	for name, entry := range entries {
		key := MakeKey(name)
		vals, blob, err := c.parseEntry(key, entry)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		// LoadKey also parses every matchString, so a malformed expression fails here rather
		// than silently never matching at runtime.
		if err := cfg.LoadKey(key.String(), blob); err != nil {
			errs = append(errs, fmt.Errorf("key %q: %w", name, err))
			continue
		}
		outcomes[key], names[key] = vals, key.String()
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	values := make(ConfigValueMap, len(outcomes))
	for key, vals := range outcomes {
		idx, err := cfg.Eval(context.Background(), names[key], c.ambient)
		if err != nil {
			errs = append(errs, fmt.Errorf("key %q: %w", key, err))
			continue
		}
		if idx < 0 || idx >= len(vals) {
			// Not reachable: indexes are generated alongside vals in parseEntry.
			errs = append(errs, fmt.Errorf("key %q: expression produced out-of-range outcome %d", key, idx))
			continue
		}
		values[key] = []ConstrainedValue{{Value: vals[idx]}}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	old := c.values.Swap(&values)
	if changed := c.changedSince(*old, values); len(changed) > 0 {
		c.PublishUpdates(changed)
	}
	return nil
}

// changedSince reports the keys whose effective value differs between two generations,
// including keys added and keys removed. It must be called after the swap, because a key
// that is no longer overridden reports the *inner* client's value: publishing nil would
// tell subscribers the key has no value at all and drop them to the compiled-in default.
func (c *ConfiguratorClient) changedSince(prev, next ConfigValueMap) map[Key][]ConstrainedValue {
	changed := make(map[Key][]ConstrainedValue)
	for key, cvs := range next {
		before, ok := prev[key]
		if !ok || !sameValues(before, cvs) {
			changed[key] = cvs
		}
	}
	for key := range prev {
		if _, ok := next[key]; !ok {
			changed[key] = c.GetValue(key)
		}
	}
	return changed
}

// parseEntry converts one YAML entry into the table of possible values plus the JSON blob
// handed to the library.
//
// The library is asked to resolve an *index* into that table rather than a value, so that
// values are decoded once here, using Temporal's own YAML conventions and validated against
// the settings registry, instead of being JSON-decoded by the library on every evaluation.
func (c *ConfiguratorClient) parseEntry(key Key, entry yamlExprEntry) ([]any, []byte, error) {
	setting := queryRegistry(key)
	if setting == nil {
		c.logger.Warn("Expression config contains unregistered dynamic config key",
			tag.Key(key.String()))
	}

	outcome := func(v any, what string) (any, error) {
		// yaml decodes nested maps as map[any]any; dynamic config values need string keys.
		converted, err := convertKeyTypeToString(v)
		if err != nil {
			return nil, fmt.Errorf("key %q %s: %w", key, what, err)
		}
		if setting != nil {
			if valErr := setting.Validate(converted); valErr != nil {
				return nil, fmt.Errorf("key %q %s: %w", key, what, valErr)
			}
		}
		return converted, nil
	}

	def, err := outcome(entry.DefaultValue, "defaultValue")
	if err != nil {
		return nil, nil, err
	}

	vals := make([]any, 0, len(entry.Overrides)+1)
	vals = append(vals, def)

	libraryCfg := types.Config{DefaultValue: json.RawMessage("0")}
	for i, o := range entry.Overrides {
		v, err := outcome(o.MatchResult, fmt.Sprintf("override %d (%q)", i, o.MatchString))
		if err != nil {
			return nil, nil, err
		}
		vals = append(vals, v)
		libraryCfg.Overrides = append(libraryCfg.Overrides, types.Override{
			MatchString: o.MatchString,
			// index into vals; 0 is the default, so overrides start at 1
			MatchResult: json.RawMessage(strconv.Itoa(len(vals) - 1)),
		})
	}

	blob, err := json.Marshal(libraryCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("key %q: %w", key, err)
	}
	return vals, blob, nil
}

func sameValues(a, b []ConstrainedValue) bool {
	if len(a) != len(b) || len(a) != 1 {
		return false
	}
	// Values come from YAML and are comparable scalars in every case that matters; anything
	// else conservatively reports "changed".
	return a[0].Constraints == b[0].Constraints && a[0].Value == b[0].Value
}

// LoadFileFrom reads path and loads it.
func (c *ConfiguratorClient) LoadFileFrom(path string) error {
	contents, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading expression config %s: %w", path, err)
	}
	return c.LoadFile(contents)
}

// Watch polls path and reloads it whenever its modification time advances past lastMod. It
// returns when ctx is cancelled. Reload failures are logged and retried on the next change.
func (c *ConfiguratorClient) Watch(ctx context.Context, path string, interval time.Duration, lastMod time.Time) error {
	for ctx.Err() == nil {
		_ = util.InterruptibleSleep(ctx, interval)
		info, err := os.Stat(path)
		if err != nil {
			c.logger.Warn("Failed to stat expression config", tag.NewStringTag("path", path), tag.Error(err))
			continue
		}
		if !info.ModTime().After(lastMod) {
			continue
		}
		lastMod = info.ModTime()
		if err := c.LoadFileFrom(path); err != nil {
			c.logger.Error("Failed to reload expression config, keeping previous values",
				tag.NewStringTag("path", path), tag.Error(err))
		} else {
			c.logger.Info("Reloaded expression config", tag.NewStringTag("path", path))
		}
	}
	return ctx.Err()
}

// StartWatching begins watching path in the background. The returned func stops it and is
// safe to call more than once.
func (c *ConfiguratorClient) StartWatching(path string, interval time.Duration) func() {
	// Stat before returning, so the baseline is the file as it was at the caller's initial
	// load rather than whenever the goroutine happens to be scheduled.
	var lastMod time.Time
	if info, err := os.Stat(path); err == nil {
		lastMod = info.ModTime()
	}

	var g goro.Group
	g.Go(func(ctx context.Context) error {
		return c.Watch(ctx, path, interval, lastMod)
	})

	var once sync.Once
	return func() {
		once.Do(func() {
			g.Cancel()
			g.Wait()
		})
	}
}
