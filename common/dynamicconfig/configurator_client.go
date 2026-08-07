package dynamicconfig

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
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

// constraintKeysDirective is a reserved top-level key in the expression file, holding the
// list of caller-supplied dimensions its expressions are allowed to reference. Every dynamic
// config key contains a dot, so this cannot collide with a setting name.
const constraintKeysDirective = "constraintKeys"

// AmbientConstraints describes the process-scoped dimensions that expressions can match on.
//
// These are exactly the dimensions the Constraints struct cannot express: adding one today
// means a new field, a new precedence order in cmd/tools/gendynamicconfig, and regenerating
// setting_gen.go. Here they are configuration.
//
// They are held by the client and merged into every evaluation, so a caller that supplies
// nothing at all still gets them.
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

func (a AmbientConstraints) asMap() types.Constraints {
	m := make(types.Constraints, len(a.Custom)+6)
	maps.Copy(m, a.Custom)
	putNonEmpty(m, CKEnvironment, a.Environment)
	putNonEmpty(m, CKZone, a.AvailabilityZone)
	putNonEmpty(m, CKCluster, a.ClusterName)
	putNonEmpty(m, CKService, a.ServiceName)
	putNonEmpty(m, CKServerVersion, headers.ServerVersion)
	if host, err := os.Hostname(); err == nil {
		putNonEmpty(m, CKHost, host)
	}
	return m
}

// ambientKeyNames is the set of keys this process can supply by itself, which determines
// whether an entry can be resolved once at load or has to be evaluated on every read.
func (a AmbientConstraints) ambientKeyNames() map[string]struct{} {
	names := map[string]struct{}{
		CKEnvironment: {}, CKZone: {}, CKCluster: {}, CKService: {}, CKHost: {}, CKServerVersion: {},
	}
	for k := range a.Custom {
		names[k] = struct{}{}
	}
	return names
}

func putNonEmpty(m types.Constraints, key, value string) {
	if value != "" {
		m[key] = value
	}
}

// ConfiguratorClient resolves dynamic config by evaluating constraint expressions (see
// common/dynamicconfig/configurator/README.md).
//
// It serves two seams:
//
//   - As a Client, for the ordinary Get accessors. Entries are resolved against this
//     process's ambient constraints and published as a single ConstrainedValue with empty
//     Constraints. Every precedence order ends in the empty Constraints
//     (cmd/tools/gendynamicconfig/main.go), so that one value is matched by every setting at
//     every precedence, and nothing needs enumerating. Reads cost what they cost today.
//   - As an Evaluator, for the GetC accessors, which hand it a caller-supplied
//     ConstraintsMap. Entries whose expressions reference anything the caller could supply
//     are evaluated per read, over a layered view of caller constraints and ambient ones.
//
// Entries whose expressions only reference ambient keys never take the second path: their
// value cannot depend on the caller, so it is computed once at load.
//
// It layers over an inner Client, normally the file based one. A key present in the
// expression file is served entirely from there; every other key is delegated untouched.
type ConfiguratorClient struct {
	logger  log.Logger
	inner   Client
	ambient types.Constraints
	// vocabulary is the set of constraint keys an expression may reference. Anything else is
	// rejected at load, so a typo fails loudly rather than silently matching nothing.
	vocabulary map[string]struct{}
	// ambientKeys is the subset of vocabulary this process supplies itself.
	ambientKeys map[string]struct{}

	// snapshot is replaced wholesale on reload; never mutated in place, both because readers
	// are lock-free and because the library's LoadKey is not safe to call concurrently with
	// Eval.
	snapshot atomic.Pointer[exprSnapshot]

	cancelInnerSubscription func()
	errCount                atomic.Int64

	NotifyingClientImpl
}

type (
	// exprSnapshot is one immutable generation of the expression configuration.
	exprSnapshot struct {
		// cfg carries values in the same shape Client.GetValue returns them — a slice of one
		// unconstrained ConstrainedValue — so an expression result can be fed to the same
		// matching machinery, which is what keeps GetC in step with Get for settings that
		// have constrained defaults.
		//
		// The library treats values as opaque, so they are decoded once here from YAML using
		// Temporal's own conventions and validated against the settings registry; nothing is
		// unmarshalled on the read path. The slices are owned by cfg and stable for the life
		// of the snapshot, which lets Collection cache conversions against pointers into them.
		cfg     configurator.Configurator[[]ConstrainedValue]
		entries map[Key]*exprEntry
	}

	exprEntry struct {
		name string
		// referenced is the sorted set of constraint keys this entry's expressions test.
		referenced []string
		// ambientOnly is true when every referenced key is one this process supplies itself.
		// Such an entry is resolved at load — unless a caller explicitly supplies one of the
		// referenced keys, which shadows the ambient value and forces a re-evaluation.
		ambientOnly bool
		// resolved is the ambient-only resolution, served through GetValue. Also the answer
		// for any caller that supplies nothing.
		resolved []ConstrainedValue
	}
)

var (
	_ Client    = (*ConfiguratorClient)(nil)
	_ Evaluator = (*ConfiguratorClient)(nil)
)

// NewConfiguratorClient returns a client with no expression configuration loaded, which
// therefore delegates everything to inner. inner may be nil, in which case keys the
// expression file does not configure have no values and settings use their compiled-in
// defaults.
func NewConfiguratorClient(ambient AmbientConstraints, inner Client, logger log.Logger) *ConfiguratorClient {
	ambientKeys := ambient.ambientKeyNames()
	vocabulary := make(map[string]struct{}, len(builtinConstraintKeys)+len(ambientKeys))
	maps.Copy(vocabulary, builtinConstraintKeys)
	maps.Copy(vocabulary, ambientKeys)

	c := &ConfiguratorClient{
		logger:              logger,
		inner:               inner,
		ambient:             ambient.asMap(),
		vocabulary:          vocabulary,
		ambientKeys:         ambientKeys,
		NotifyingClientImpl: NewNotifyingClientImpl(),
	}
	c.errCount.Store(-1)
	c.snapshot.Store(&exprSnapshot{entries: map[Key]*exprEntry{}})

	// Forward the inner client's updates, so subscribers still see changes to keys we are not
	// overriding.
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

// GetValue implements Client. It returns the ambient-only resolution, which is the right
// answer for any caller that supplies no constraints of its own — including every subscriber,
// since Subscribe has no way to pass them.
func (c *ConfiguratorClient) GetValue(key Key) []ConstrainedValue {
	if e, ok := c.snapshot.Load().entries[key]; ok {
		return e.resolved
	}
	if c.inner == nil {
		return nil
	}
	return c.inner.GetValue(key)
}

// Eval implements Evaluator, resolving key against the caller's constraints layered over this
// process's ambient ones. Returns nil when the key is not expression-configured, or when its
// value cannot depend on the caller, in which case the Client path applies.
func (c *ConfiguratorClient) Eval(key Key, cm ConstraintsMap) []ConstrainedValue {
	snap := c.snapshot.Load()
	e, ok := snap.entries[key]
	if !ok {
		return nil
	}
	if e.canUseResolved(cm) {
		return e.resolved
	}

	// A layered view rather than a merged copy, so this allocates nothing: the pooled
	// layeredLookup is a pointer, so boxing it into the interface is free.
	l := lookupPool.Get().(*layeredLookup) //nolint:revive // unchecked-type-assertion
	l.caller, l.ambient = cm, c.ambient
	cvs, err := snap.cfg.Eval(context.Background(), e.name, l)
	l.caller, l.ambient = nil, nil
	lookupPool.Put(l)

	if err != nil {
		if c.throttleLog() {
			c.logger.Warn("Failed to evaluate expression config, falling back to file config",
				tag.Key(key.String()), tag.Error(err))
		}
		return nil
	}
	return cvs
}

// canUseResolved reports whether the value resolved at load is still correct for this call,
// which spares a full evaluation. It is, when the caller supplied nothing, or when the entry
// depends only on dimensions this process supplies and the caller has not overridden any of
// them. The scan is over the entry's referenced keys, typically one or two, rather than over
// the caller's map.
func (e *exprEntry) canUseResolved(cm ConstraintsMap) bool {
	if len(cm) == 0 {
		return true
	}
	if !e.ambientOnly {
		return false
	}
	for _, k := range e.referenced {
		if _, shadowed := cm[k]; shadowed {
			return false
		}
	}
	return true
}

// layeredLookup presents caller-supplied and ambient constraints as a single view, with the
// caller's taking precedence. Pooled, so evaluation never allocates.
type layeredLookup struct {
	caller  ConstraintsMap
	ambient types.Constraints
}

func (l *layeredLookup) Get(key string) (any, bool) {
	if v, ok := l.caller[key]; ok {
		return v, true
	}
	v, ok := l.ambient[key]
	return v, ok
}

var lookupPool = sync.Pool{New: func() any { return new(layeredLookup) }}

func (c *ConfiguratorClient) throttleLog() bool {
	n := c.errCount.Add(1)
	return n < errCountLogThreshold || n%errCountLogThreshold == 0
}

// innerKeysChanged republishes an update from the inner client, substituting the effective
// value so that a key we are overriding is not reported as having changed to the inner
// client's value.
func (c *ConfiguratorClient) innerKeysChanged(changed map[Key][]ConstrainedValue) {
	entries := c.snapshot.Load().entries
	effective := make(map[Key][]ConstrainedValue, len(changed))
	for key := range changed {
		if e, ok := entries[key]; ok {
			effective[key] = e.resolved
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

// LoadFile parses contents, resolves every entry against this process's ambient constraints,
// and atomically installs the result. On any error the previous values are left in place, so
// a bad edit degrades to the last good state rather than to compiled-in defaults.
func (c *ConfiguratorClient) LoadFile(contents []byte) error {
	entries, vocabulary, err := c.decodeFile(contents)
	if err != nil {
		return err
	}

	snap := &exprSnapshot{
		cfg:     configurator.New[[]ConstrainedValue](),
		entries: make(map[Key]*exprEntry, len(entries)),
	}

	var errs []error
	for name, entry := range entries {
		key := MakeKey(name)
		e, cfg, err := c.parseEntry(key, entry)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		// Load parses every matchString, so a malformed expression fails here rather than
		// silently never matching at runtime.
		if err := snap.cfg.Load(e.name, cfg); err != nil {
			errs = append(errs, fmt.Errorf("key %q: %w", name, err))
			continue
		}
		snap.entries[key] = e
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	// Now that expressions are parsed, check the vocabulary and work out which entries can be
	// resolved once here rather than on every read.
	for key, e := range snap.entries {
		if err := c.classifyAndResolve(snap, key, e, vocabulary); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	old := c.snapshot.Swap(snap)
	if changed := c.changedSince(old, snap); len(changed) > 0 {
		c.PublishUpdates(changed)
	}
	return nil
}

// decodeFile splits the file into its entries and the vocabulary its expressions may draw on.
//
// The vocabulary cannot be derived from what this process knows: the point of the
// ConstraintsMap accessors is that a call site can invent a dimension, and the client has no
// way to learn about it. So the file declares the dimensions its expressions use, and
// anything outside that set is a typo.
func (c *ConfiguratorClient) decodeFile(contents []byte) (map[string]yamlExprEntry, map[string]struct{}, error) {
	var raw map[string]yaml.Node
	if err := yaml.Unmarshal(contents, &raw); err != nil {
		return nil, nil, fmt.Errorf("decoding expression config: %w", err)
	}

	vocabulary := maps.Clone(c.vocabulary)
	if node, ok := raw[constraintKeysDirective]; ok {
		var declared []string
		if err := node.Decode(&declared); err != nil {
			return nil, nil, fmt.Errorf("decoding %s: %w", constraintKeysDirective, err)
		}
		for _, k := range declared {
			vocabulary[k] = struct{}{}
		}
		delete(raw, constraintKeysDirective)
	}

	entries := make(map[string]yamlExprEntry, len(raw))
	for name, node := range raw {
		var entry yamlExprEntry
		if err := node.Decode(&entry); err != nil {
			return nil, nil, fmt.Errorf("decoding entry %q: %w", name, err)
		}
		entries[name] = entry
	}
	return entries, vocabulary, nil
}

// classifyAndResolve records which constraint keys an entry tests, rejects any that are not
// in the vocabulary, and resolves the entry against ambient constraints so that callers
// supplying nothing — including every subscriber — have an answer without evaluating.
func (c *ConfiguratorClient) classifyAndResolve(
	snap *exprSnapshot,
	key Key,
	e *exprEntry,
	vocabulary map[string]struct{},
) error {
	referenced, ok := snap.cfg.ReferencedKeys(e.name)
	if !ok {
		return nil
	}

	var unknown []string
	e.referenced = referenced
	e.ambientOnly = true
	for _, k := range referenced {
		if _, isVocab := vocabulary[k]; !isVocab {
			unknown = append(unknown, k)
		}
		if _, isAmbient := c.ambientKeys[k]; !isAmbient {
			e.ambientOnly = false
		}
	}
	if len(unknown) > 0 {
		return fmt.Errorf(
			"key %q references unknown constraint %v; declare it under %s or expressionConstraints, or fix the spelling",
			key, unknown, constraintKeysDirective)
	}

	// A single value with no constraints: every precedence order ends in {}, so this matches
	// whatever the caller asks for. The slice is the library's own, so it stays stable.
	resolved, err := snap.cfg.Eval(context.Background(), e.name, c.ambient)
	if err != nil {
		return fmt.Errorf("key %q: %w", key, err)
	}
	e.resolved = resolved
	return nil
}

// parseEntry converts one YAML entry into the config handed to the library.
//
// Values are decoded here rather than by the library: they go through Temporal's own YAML
// conventions, which keeps an int an int, and are validated against the settings registry so
// a type error fails the load instead of silently yielding the default at read time. The
// library carries the resulting *ConstrainedValue through untouched.
func (c *ConfiguratorClient) parseEntry(
	key Key,
	entry yamlExprEntry,
) (*exprEntry, configurator.Config[[]ConstrainedValue], error) {
	var empty configurator.Config[[]ConstrainedValue]
	setting := queryRegistry(key)
	if setting == nil {
		c.logger.Warn("Expression config contains unregistered dynamic config key",
			tag.Key(key.String()))
	}

	outcome := func(v any, what string) ([]ConstrainedValue, error) {
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
		return []ConstrainedValue{{Value: converted}}, nil
	}

	def, err := outcome(entry.DefaultValue, "defaultValue")
	if err != nil {
		return nil, empty, err
	}

	cfg := configurator.Config[[]ConstrainedValue]{
		DefaultValue: def,
		Overrides:    make([]configurator.Override[[]ConstrainedValue], 0, len(entry.Overrides)),
	}
	for i, o := range entry.Overrides {
		cv, err := outcome(o.MatchResult, fmt.Sprintf("override %d (%q)", i, o.MatchString))
		if err != nil {
			return nil, empty, err
		}
		cfg.Overrides = append(cfg.Overrides, configurator.Override[[]ConstrainedValue]{
			MatchString: o.MatchString,
			MatchResult: cv,
		})
	}

	return &exprEntry{name: key.String()}, cfg, nil
}

// changedSince reports the keys whose effective value differs between two generations,
// including keys added and removed. It must be called after the swap: a key that is no longer
// overridden reports the *inner* client's value, because publishing nil would tell
// subscribers the key has no value at all and drop them to the compiled-in default.
func (c *ConfiguratorClient) changedSince(prev, next *exprSnapshot) map[Key][]ConstrainedValue {
	changed := make(map[Key][]ConstrainedValue)
	for key, e := range next.entries {
		before, ok := prev.entries[key]
		if !ok || !sameValues(before.resolved, e.resolved) {
			changed[key] = e.resolved
		}
	}
	for key := range prev.entries {
		if _, ok := next.entries[key]; !ok {
			changed[key] = c.GetValue(key)
		}
	}
	return changed
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

// StartWatching begins watching path in the background. The returned func stops it and is safe
// to call more than once.
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

// ExpressionKeys returns the configured keys, sorted. For diagnostics and tests.
func (c *ConfiguratorClient) ExpressionKeys() []string {
	entries := c.snapshot.Load().entries
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k.String())
	}
	slices.Sort(keys)
	return keys
}
