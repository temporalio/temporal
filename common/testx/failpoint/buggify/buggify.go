package buggify

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"go.temporal.io/testx/failpoint"
)

// Buggify is the BUGGIFY fallback of a failpoint registry. Get it with For.
type Buggify struct {
	registry failpoint.Registry

	// mu guards state and suppressed.
	mu sync.Mutex
	// state is the state of the configuration, nil while it is disabled, and suppressed counts the
	// Suppress calls that are not restored.
	state      *state
	suppressed int
	// effective is state while it is not suppressed, else nil, so executions read the
	// configuration and the suppression in one snapshot. It is updated under mu.
	effective atomic.Pointer[state]
}

// For returns the BUGGIFY of registry, and installs it as the registry's fallback first if the
// registry has none. It is disabled until Set enables it. It can be called inside
// Registry.Configure. It panics if the registry has another fallback, rather than replace it.
func For(registry failpoint.Registry) *Buggify {
	installed := registry.InstallFallback(&Buggify{registry: registry})
	b, ok := installed.(*Buggify)
	if !ok {
		panic(fmt.Sprintf("buggify: the failpoint registry has another fallback: %T", installed))
	}
	return b
}

// ErrNotInstalled is returned by Set if b is no longer the fallback of its registry, because other
// code replaced it. The configuration is kept, but has no effect.
var ErrNotInstalled = errors.New("buggify: not the fallback of the failpoint registry")

// Set replaces the configuration, see Config. A configuration with a zero seed disables BUGGIFY.
// It returns an error and changes nothing if the configuration is invalid. Setting a different
// configuration forgets the activation decisions and execution counts of the previous one. It
// returns ErrNotInstalled if b is no longer the registry's fallback.
func (b *Buggify) Set(config Config) error {
	// Buggify keeps its own copy, so the caller can't change it.
	config = config.clone()
	if err := config.Validate(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !config.Enabled() {
		b.state = nil
	} else if b.state == nil || !reflect.DeepEqual(b.state.config, config) {
		b.state = &state{config: config}
	}
	if !b.updateLocked() {
		return ErrNotInstalled
	}
	return nil
}

// Config returns the configuration.
func (b *Buggify) Config() Config {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != nil {
		return b.state.config.clone()
	}
	return Config{}
}

// Suppress turns BUGGIFY off until restore is called, without changing its configuration, for
// tests that assert exact behavior. Calls nest.
func (b *Buggify) Suppress() (restore func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.suppressed++
	b.updateLocked()
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			b.suppressed--
			b.updateLocked()
		})
	}
}

// updateLocked publishes effective, and turns the fallback on while it is set. The caller must
// hold b.mu. Executions check that the fallback is active before they read effective, so while
// the two change, both kinds of call sites, Fires and failpoint.Checkpoint, see the same
// decision. It reports whether b is still the registry's fallback; if not, the registry ignores
// the change.
func (b *Buggify) updateLocked() bool {
	var effective *state
	if b.suppressed == 0 {
		effective = b.state
	}
	b.effective.Store(effective)
	return b.registry.SetFallbackActive(b, effective != nil)
}

// FallbackStats returns the counters of the key names that BUGGIFY saw since its configuration
// was last set, sorted by name. It is empty while BUGGIFY is disabled.
func (b *Buggify) FallbackStats() []failpoint.FallbackKeyStats {
	b.mu.Lock()
	s := b.state
	b.mu.Unlock()
	if s == nil {
		return nil
	}
	return s.stats()
}

// Error returns the error that BUGGIFY injects into this execution of key, which has no
// failpoint registered, or nil. It is nil if the key is not activated or the execution doesn't
// fire, or if the configuration names no error for the key. It implements failpoint.Fallback;
// call sites use failpoint.Checkpoint or failpoint.FallbackError.
func (b *Buggify) Error(key failpoint.Key) error {
	s := b.effective.Load()
	if s == nil {
		return nil
	}
	k := s.key(key.Name(), b.registry.Emit)
	if k == nil {
		return nil
	}
	k.hits.Add(1)
	if !k.activated || k.newError == nil {
		return nil
	}
	fired, n := s.fire(key, k, -1)
	if !fired {
		return nil
	}
	b.registry.Emit(failpoint.Event{Kind: failpoint.FallbackFireEvent, Key: key, Action: failpoint.Return, Fired: true, Execution: int64(n)})
	// Callers can store the message, for example in a proto, so it must be valid UTF-8 even if the
	// key name isn't, such as the name of an HTTP operation with an escaped path.
	return k.newError("BUGGIFY: injected by failpoint " + strings.ToValidUTF8(key.Name(), "�"))
}

// Fires reports whether the call site for key should take its unusual branch on this execution,
// like FoundationDB's BUGGIFY. Call sites use it for branches that are legal but unlikely, such as
// a smaller batch or an extra retry, never for states that cannot happen in reality.
//
// A failpoint registered for key, or for the unscoped key, decides instead of BUGGIFY: Fires
// reports whether it fired with a value, as failpoint.Active does, so a test can force the branch
// with On or suppress it with a disabled failpoint. Without a registered failpoint, Fires reports
// whether the key is activated for the run and this execution fires (see Config). It costs one
// atomic load while neither a failpoint nor BUGGIFY is enabled.
func Fires(src failpoint.Source, key failpoint.Key) bool {
	return fires(src, key, -1)
}

// FiresWithProb is Fires with the probability p, in [0, 1], that an execution of an activated key
// fires, instead of the configured one, like FoundationDB's BUGGIFY_WITH_PROB.
func FiresWithProb(src failpoint.Source, key failpoint.Key, p float64) bool {
	return fires(src, key, max(p, 0))
}

func fires(src failpoint.Source, key failpoint.Key, p float64) bool {
	registry := src.Failpoints()
	if registry == nil || !registry.Armed() {
		return false
	}
	// The registered failpoints and the fallback are those of one registry, even if src returns
	// another one on the next call.
	if active, registered := failpoint.ActiveRegistered(registry, key); registered {
		return active
	}
	// The registry's active fallback, as failpoint.Checkpoint consults it.
	b, ok := registry.ActiveFallback().(*Buggify)
	if !ok {
		return false
	}
	s := b.effective.Load()
	if s == nil {
		return false
	}
	k := s.key(key.Name(), registry.Emit)
	if k == nil {
		return false
	}
	k.hits.Add(1)
	if !k.activated {
		return false
	}
	fired, n := s.fire(key, k, p)
	if fired {
		registry.Emit(failpoint.Event{Kind: failpoint.FallbackFireEvent, Key: key, Action: failpoint.On, Fired: true, Execution: int64(n)})
	}
	return fired
}

// maxKeys bounds the selected key names whose BUGGIFY state a registry keeps, since some
// names, such as those of HTTP operations, contain text chosen by callers. Further names are
// never activated. maxScopes bounds the execution counters of activated names per scope;
// executions in further scopes never fire. maxUnselectedKeys bounds the cache of names that the
// configuration doesn't select; further names are matched against the configuration on each
// execution.
const (
	maxKeys           = 1 << 14
	maxScopes         = 1 << 16
	maxUnselectedKeys = 1 << 14
)

// state is the BUGGIFY state of a registry for one configuration.
type state struct {
	config Config
	// keys holds a *keyState per selected key name, created on its first execution, and
	// unselected caches the names that the configuration doesn't select. Both are only added to
	// under mu, which keeps their sizes within their bounds.
	// The execution counters of the scopes of activated names (see keyState) are also only
	// added to under mu.
	keys       sync.Map
	unselected sync.Map
	mu         sync.Mutex
	count      int
	scopeCount int
	unselCount int
}

// keyState is the BUGGIFY state of a key name: its activation for the run, how often it was
// executed and fired in all scopes, and, if it is activated, the number of its executions in each
// scope, which numbers the draws that decide whether they fire.
type keyState struct {
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
func (b *state) key(name string, emit func(failpoint.Event)) *keyState {
	if k, ok := b.keys.Load(name); ok {
		return k.(*keyState)
	}
	if _, ok := b.unselected.Load(name); ok {
		return nil
	}
	params, selected := b.config.params(name)

	b.mu.Lock()
	defer b.mu.Unlock()
	if k, ok := b.keys.Load(name); ok {
		return k.(*keyState)
	}
	if !selected {
		if _, ok := b.unselected.Load(name); !ok && b.unselCount < maxUnselectedKeys {
			b.unselected.Store(name, struct{}{})
			b.unselCount++
		}
		return nil
	}
	if b.count >= maxKeys {
		return nil
	}
	k := &keyState{activated: b.config.Draw(name, 0) < params.Activation, fire: params.Fire}
	if params.Error != "" {
		k.newError, _ = failpoint.LookupError(params.Error)
	}
	emit(failpoint.Event{Kind: failpoint.FallbackActivationEvent, Key: failpoint.KeyFromName(name, ""), Activated: k.activated})
	b.keys.Store(name, k)
	b.count++
	return k
}

// fire counts an execution of an activated key in a scope and reports whether it fires with
// probability p, or the key's fire probability if p is negative, and the number of the execution in
// the scope.
func (b *state) fire(key failpoint.Key, k *keyState, p float64) (bool, uint64) {
	if p < 0 {
		p = k.fire
	}
	counter := b.scopeCounter(k, key.Scope())
	if counter == nil {
		return false, 0
	}
	n := counter.Add(1)
	fired := b.config.DrawScoped(key.Name(), key.Scope(), n) < p
	if fired {
		k.fires.Add(1)
	}
	return fired, n
}

// scopeCounter returns the execution counter of an activated key in a scope, or nil if the
// counters are at their bound.
func (b *state) scopeCounter(k *keyState, scope string) *atomic.Uint64 {
	if c, ok := k.scopes.Load(scope); ok {
		return c.(*atomic.Uint64)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if c, ok := k.scopes.Load(scope); ok {
		return c.(*atomic.Uint64)
	}
	if b.scopeCount >= maxScopes {
		return nil
	}
	c := new(atomic.Uint64)
	k.scopes.Store(scope, c)
	b.scopeCount++
	return c
}

// stats returns the counters of the names that BUGGIFY selected, sorted by name.
func (b *state) stats() []failpoint.FallbackKeyStats {
	var stats []failpoint.FallbackKeyStats
	b.keys.Range(func(name, k any) bool {
		ks := k.(*keyState)
		// An execution counts its hit before its fire, so reading fires first keeps fires <= hits.
		fires := ks.fires.Load()
		stats = append(stats, failpoint.FallbackKeyStats{
			Name:      name.(string),
			Activated: ks.activated,
			Hits:      ks.hits.Load(),
			Fires:     fires,
		})
		return true
	})
	slices.SortFunc(stats, func(a, b failpoint.FallbackKeyStats) int { return strings.Compare(a.Name, b.Name) })
	return stats
}
