package failpoint

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

// BuggifyConfig configures BUGGIFY, FoundationDB-style randomized fault activation. With a seed,
// each key name that the code under test executes, and that has no failpoint registered, is
// activated for the whole run with a probability. Each execution of an activated key then fires
// with another probability. Both decisions are pure functions of the seed, the key name and, for
// firing, the number of the execution of that name, so the same seed activates the same keys.
//
// The zero value disables BUGGIFY.
type BuggifyConfig struct {
	// Seed seeds every decision. Zero disables BUGGIFY.
	Seed uint64
	// Activation and Fire are the probabilities, in [0, 1], that a key that matches no prefix is
	// activated for the run, and that an execution of an activated key fires.
	Activation float64
	Fire       float64
	// Error names a registered error (see RegisterError) that Execute returns when a key that
	// matches no prefix fires. Without it, such keys only fire at Buggify call sites.
	Error string
	// Prefixes override the probabilities and the error for the key names they prefix. The
	// longest prefix that matches applies.
	Prefixes []BuggifyPrefix
	// Include and Exclude select the key names that BUGGIFY applies to, as patterns in which "*"
	// matches any text. An empty Include selects every name; Exclude wins over Include.
	Include []string
	Exclude []string
}

// BuggifyPrefix overrides the BUGGIFY probabilities and error for key names with a prefix.
type BuggifyPrefix struct {
	Prefix     string
	Activation float64
	Fire       float64
	// Error names a registered error that Execute returns when a key fires. Without it, the keys
	// only fire at Buggify call sites.
	Error string
}

// clone returns a copy of c that shares no slices with it.
func (c BuggifyConfig) clone() BuggifyConfig {
	c.Prefixes = slices.Clone(c.Prefixes)
	c.Include = slices.Clone(c.Include)
	c.Exclude = slices.Clone(c.Exclude)
	return c
}

// Enabled reports whether the configuration enables BUGGIFY.
func (c BuggifyConfig) Enabled() bool {
	return c.Seed != 0
}

// Validate reports whether SetBuggify accepts the configuration.
func (c BuggifyConfig) Validate() error {
	if err := validateBuggifyParams("", c.Activation, c.Fire, c.Error); err != nil {
		return err
	}
	prefixes := make(map[string]bool, len(c.Prefixes))
	for _, p := range c.Prefixes {
		if p.Prefix == "" {
			return errors.New("buggify prefix must not be empty")
		}
		if prefixes[p.Prefix] {
			return fmt.Errorf("duplicate buggify prefix %q", p.Prefix)
		}
		prefixes[p.Prefix] = true
		if err := validateBuggifyParams(p.Prefix, p.Activation, p.Fire, p.Error); err != nil {
			return err
		}
	}
	if slices.Contains(c.Include, "") || slices.Contains(c.Exclude, "") {
		return errors.New("buggify include and exclude patterns must not be empty")
	}
	return nil
}

func validateBuggifyParams(prefix string, activation, fire float64, errorName string) error {
	for _, p := range []float64{activation, fire} {
		if math.IsNaN(p) || p < 0 || p > 1 {
			return fmt.Errorf("buggify probability %v for prefix %q is not in [0, 1]", p, prefix)
		}
	}
	if errorName != "" {
		if _, ok := LookupError(errorName); !ok {
			return fmt.Errorf("unknown error %q for buggify prefix %q", errorName, prefix)
		}
	}
	return nil
}

// Draw returns a number in [0, 1) that is a pure function of the seed, name and n. BUGGIFY
// activates a key name if Draw(name, 0) is below its activation probability, and fires the n-th
// execution of the name in a scope if DrawScoped(name, scope, n) is below its fire probability.
// Code that makes other seeded choices, such as picking a configuration value, uses it to stay
// reproducible with the same seed.
func (c BuggifyConfig) Draw(name string, n uint64) float64 {
	x := mix64(c.Seed ^ mix64(fnv64a(name)) ^ mix64(n+0x9e3779b97f4a7c15))
	return float64(x>>11) / (1 << 53)
}

// DrawScoped is Draw for the executions of a name in a scope, which BUGGIFY numbers separately for
// each scope, so the executions in one scope don't change which executions fire in another.
// Without a scope, it is Draw.
func (c BuggifyConfig) DrawScoped(name, scope string, n uint64) float64 {
	if scope == "" {
		return c.Draw(name, n)
	}
	x := mix64(c.Seed ^ mix64(fnv64a(name)) ^ mix64(fnv64a(scope)^0x6a09e667f3bcc909) ^ mix64(n+0x9e3779b97f4a7c15))
	return float64(x>>11) / (1 << 53)
}

// Activated reports whether BUGGIFY activates the key name for the run. It is false for names
// that the configuration doesn't select, and if BUGGIFY is disabled.
func (c BuggifyConfig) Activated(name string) bool {
	params, ok := c.params(name)
	return ok && c.Draw(name, 0) < params.Activation
}

// params returns the probabilities and error of a key name, and whether BUGGIFY selects it.
func (c BuggifyConfig) params(name string) (BuggifyPrefix, bool) {
	if !c.Enabled() || !c.selects(name) {
		return BuggifyPrefix{}, false
	}
	params := BuggifyPrefix{Activation: c.Activation, Fire: c.Fire, Error: c.Error}
	for _, p := range c.Prefixes {
		if strings.HasPrefix(name, p.Prefix) && len(p.Prefix) > len(params.Prefix) {
			params = p
		}
	}
	return params, true
}

func (c BuggifyConfig) selects(name string) bool {
	for _, pattern := range c.Exclude {
		if matchPattern(pattern, name) {
			return false
		}
	}
	if len(c.Include) == 0 {
		return true
	}
	for _, pattern := range c.Include {
		if matchPattern(pattern, name) {
			return true
		}
	}
	return false
}

// matchPattern reports whether name matches pattern, in which "*" matches any text, including
// none.
func matchPattern(pattern, name string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == name
	}
	if !strings.HasPrefix(name, parts[0]) {
		return false
	}
	name = name[len(parts[0]):]
	last := parts[len(parts)-1]
	for _, part := range parts[1 : len(parts)-1] {
		i := strings.Index(name, part)
		if i < 0 {
			return false
		}
		name = name[i+len(part):]
	}
	return strings.HasSuffix(name, last)
}

func fnv64a(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// mix64 is the finalizer of SplitMix64.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// maxBuggifyKeys bounds the selected key names whose BUGGIFY state a registry keeps, since some
// names, such as those of HTTP operations, contain text chosen by callers. Further names are
// never activated. maxBuggifyScopes bounds the execution counters of activated names per scope;
// executions in further scopes never fire. maxUnselectedKeys bounds the cache of names that the
// configuration doesn't select; further names are matched against the configuration on each
// execution.
const (
	maxBuggifyKeys    = 1 << 14
	maxBuggifyScopes  = 1 << 16
	maxUnselectedKeys = 1 << 14
)

// buggifyState is the BUGGIFY state of a registry for one configuration.
type buggifyState struct {
	config BuggifyConfig
	// keys holds a *buggifyKey per selected key name, created on its first execution, and
	// unselected caches the names that the configuration doesn't select. Both are only added to
	// under mu, which keeps their sizes within their bounds.
	// The execution counters of the scopes of activated names (see buggifyKey) are also only
	// added to under mu.
	keys       sync.Map
	unselected sync.Map
	mu         sync.Mutex
	count      int
	scopeCount int
	unselCount int
}

// buggifyKey is the BUGGIFY state of a key name: its activation for the run, how often it was
// executed and fired in all scopes, and, if it is activated, the number of its executions in each
// scope, which numbers the draws that decide whether they fire.
type buggifyKey struct {
	activated bool
	fire      float64
	newError  func(msg string) error
	hits      atomic.Int64
	fires     atomic.Int64
	// scopes holds an *atomic.Uint64 per scope, "" for executions without a scope.
	scopes sync.Map
}

// key returns the state of a key name, or nil if the configuration doesn't select it. On the first
// execution of a selected name it decides the activation and reports it to emit, before any
// other execution can see the state, so the activation event precedes the name's fire events.
func (b *buggifyState) key(name string, emit func(Event)) *buggifyKey {
	if k, ok := b.keys.Load(name); ok {
		return k.(*buggifyKey)
	}
	if _, ok := b.unselected.Load(name); ok {
		return nil
	}
	params, selected := b.config.params(name)

	b.mu.Lock()
	defer b.mu.Unlock()
	if k, ok := b.keys.Load(name); ok {
		return k.(*buggifyKey)
	}
	if !selected {
		if _, ok := b.unselected.Load(name); !ok && b.unselCount < maxUnselectedKeys {
			b.unselected.Store(name, struct{}{})
			b.unselCount++
		}
		return nil
	}
	if b.count >= maxBuggifyKeys {
		return nil
	}
	k := &buggifyKey{activated: b.config.Draw(name, 0) < params.Activation, fire: params.Fire}
	if params.Error != "" {
		k.newError, _ = LookupError(params.Error)
	}
	emit(Event{Kind: BuggifyActivationEvent, Key: Key{name: name}, Activated: k.activated})
	b.keys.Store(name, k)
	b.count++
	return k
}

// fire counts an execution of an activated key in a scope and reports whether it fires with
// probability p, or the key's fire probability if p is negative, and the number of the execution in
// the scope.
func (b *buggifyState) fire(key Key, k *buggifyKey, p float64) (bool, uint64) {
	if p < 0 {
		p = k.fire
	}
	counter := b.scopeCounter(k, key.scope)
	if counter == nil {
		return false, 0
	}
	n := counter.Add(1)
	fired := b.config.DrawScoped(key.name, key.scope, n) < p
	if fired {
		k.fires.Add(1)
	}
	return fired, n
}

// scopeCounter returns the execution counter of an activated key in a scope, or nil if the
// counters are at their bound.
func (b *buggifyState) scopeCounter(k *buggifyKey, scope string) *atomic.Uint64 {
	if c, ok := k.scopes.Load(scope); ok {
		return c.(*atomic.Uint64)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if c, ok := k.scopes.Load(scope); ok {
		return c.(*atomic.Uint64)
	}
	if b.scopeCount >= maxBuggifyScopes {
		return nil
	}
	c := new(atomic.Uint64)
	k.scopes.Store(scope, c)
	b.scopeCount++
	return c
}

// BuggifyKeyStats counts the executions of a key name that BUGGIFY saw, in all scopes.
type BuggifyKeyStats struct {
	Name      string
	Activated bool
	// Hits counts the executions of the name, activated or not, with nothing registered for it,
	// and Fires those that BUGGIFY fired.
	Hits  int64
	Fires int64
}

// stats returns the counters of the names that BUGGIFY selected, sorted by name.
func (b *buggifyState) stats() []BuggifyKeyStats {
	var stats []BuggifyKeyStats
	b.keys.Range(func(name, k any) bool {
		key := k.(*buggifyKey)
		// An execution counts its hit before its fire, so reading fires first keeps fires <= hits.
		fires := key.fires.Load()
		stats = append(stats, BuggifyKeyStats{
			Name:      name.(string),
			Activated: key.activated,
			Hits:      key.hits.Load(),
			Fires:     fires,
		})
		return true
	})
	slices.SortFunc(stats, func(a, b BuggifyKeyStats) int { return strings.Compare(a.Name, b.Name) })
	return stats
}

// lookupRegistered returns the failpoint registered for key or, for a scoped key, for the
// unscoped key.
func lookupRegistered(registry Registry, key Key) (any, bool) {
	return registry.lookup(key)
}

// Armed reports whether src has a registry in which a failpoint is registered or BUGGIFY is
// enabled. Call sites that do work before they execute a failpoint, such as building its key,
// check it first, so they cost a nil check without a registry.
func Armed(src Source) bool {
	registry := src.Failpoints()
	return registry != nil && registry.Armed()
}

// Buggify reports whether the call site for key should take its unusual branch on this execution,
// like FoundationDB's BUGGIFY. Call sites use it for branches that are legal but unlikely, such as
// a smaller batch or an extra retry, never for states that cannot happen in reality.
//
// A failpoint registered for key, or for the unscoped key, decides instead of BUGGIFY: Buggify
// reports whether it fired with a value, as Active does, so a test can force the branch with On or
// suppress it with a disabled failpoint. Without a registered failpoint, Buggify reports whether
// the key is activated for the run and this execution fires (see BuggifyConfig). It costs one
// atomic load while neither a failpoint nor BUGGIFY is enabled.
func Buggify(src Source, key Key) bool {
	return buggify(src, key, -1)
}

// BuggifyWithProb is Buggify with the probability p, in [0, 1], that an execution of an activated
// key fires, instead of the configured one, like FoundationDB's BUGGIFY_WITH_PROB.
func BuggifyWithProb(src Source, key Key, p float64) bool {
	return buggify(src, key, max(p, 0))
}

func buggify(src Source, key Key, p float64) bool {
	registry := src.Failpoints()
	if registry == nil || !registry.Armed() {
		return false
	}
	if fpAny, ok := lookupRegistered(registry, key); ok {
		_, set, _ := execute[any](registry, key, fpAny, nil)
		return set
	}
	b := registry.buggify()
	if b == nil {
		return false
	}
	k := b.key(key.name, registry.emit)
	if k == nil {
		return false
	}
	k.hits.Add(1)
	if !k.activated {
		return false
	}
	fired, n := b.fire(key, k, p)
	if fired {
		registry.emit(Event{Kind: BuggifyFireEvent, Key: key, Action: On, Fired: true, Execution: int64(n)})
	}
	return fired
}

// BuggifyError returns the error that BUGGIFY injects for this execution of key, or nil. It is nil
// if a failpoint is registered for key or any of shadows, or for their unscoped keys, whether or
// not it is enabled, if the key is not activated or the execution doesn't fire, or if the
// configuration names no error for the key. Execute and Checkpoint already include it; call sites
// that execute several keys for one operation, with ExecuteRegistered, call it last with the other
// keys as shadows, so a failpoint registered for any of the keys takes precedence over BUGGIFY.
func BuggifyError(src Source, key Key, shadows ...Key) error {
	registry := src.Failpoints()
	if registry == nil || !registry.Armed() {
		return nil
	}
	// One snapshot, so a change that moves a registration from one key to another can't make
	// both lookups miss.
	if registry.registeredAny(key, shadows) {
		return nil
	}
	return buggifyError(registry, key)
}

// buggifyError is BuggifyError for a key that has no failpoint registered.
func buggifyError(registry Registry, key Key) error {
	b := registry.buggify()
	if b == nil {
		return nil
	}
	k := b.key(key.name, registry.emit)
	if k == nil {
		return nil
	}
	k.hits.Add(1)
	if !k.activated || k.newError == nil {
		return nil
	}
	fired, n := b.fire(key, k, -1)
	if !fired {
		return nil
	}
	registry.emit(Event{Kind: BuggifyFireEvent, Key: key, Action: Return, Fired: true, Execution: int64(n)})
	// Callers can store the message, for example in a proto, so it must be valid UTF-8 even if the
	// key name isn't, such as the name of an HTTP operation with an escaped path.
	return k.newError("BUGGIFY: injected by failpoint " + strings.ToValidUTF8(key.name, "\uFFFD"))
}
