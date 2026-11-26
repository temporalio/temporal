package buggify

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/testx/failpoint"
)

// Buggify implements failpoint.Policy for seeded randomized fault activation. Create one with New
// and enable it with Set.
type Buggify struct {
	registry *failpoint.Registry

	// observer is called with the decisions of BUGGIFY, see OnDecision.
	observer atomic.Pointer[func(Event)]
	// events holds the decisions that the observer has yet to receive.
	events eventQueue

	// counting is set by CountExecutions. Executions count themselves for Stats only while it is set.
	counting atomic.Bool

	// mu serializes Set.
	mu sync.Mutex
	// current is the run state of the configuration, nil while BUGGIFY is disabled. Executions
	// read it without mu.
	current atomic.Pointer[run]
}

// New returns a disabled Buggify bound to registry (which may be nil for standalone use).
// Calling Set with an enabled Config installs it as registry's Policy; disabling it clears the
// registry's Policy.
func New(registry *failpoint.Registry) *Buggify {
	return &Buggify{registry: registry}
}

// Set replaces the configuration, see Config. A configuration with a zero seed disables BUGGIFY.
// It returns an error and changes nothing if the configuration is invalid. Setting a different
// configuration forgets the activation decisions and execution counts of the previous one.
func (b *Buggify) Set(config Config) error {
	// Buggify keeps its own copy, so the caller can't change it.
	config = config.clone()
	if err := config.Validate(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	prev := b.current.Load()
	current := prev
	switch {
	case !config.Enabled():
		current = nil
	case current == nil || !reflect.DeepEqual(current.config, config):
		current = &run{config: config, names: make(map[string]*nameState), counting: &b.counting}
	default:
	}
	b.current.Store(current)
	if b.registry != nil {
		switch {
		case current != nil:
			b.registry.SetPolicy(b)
		case prev != nil:
			b.registry.SetPolicy(nil)
		default:
		}
	}
	return nil
}

// Config returns the configuration.
func (b *Buggify) Config() Config {
	if current := b.current.Load(); current != nil {
		return current.config.clone()
	}
	return Config{}
}

// EventKind is what an Event reports.
type EventKind int32

const (
	// EventActivation reports whether BUGGIFY activated a key name, on the first execution of the
	// name. The name is activated or not for the rest of the run.
	EventActivation EventKind = iota
	// EventFire reports an execution of an activated key that fired.
	EventFire
	// EventScopeLimit reports, once per configuration, that BUGGIFY keeps no more execution
	// counters per scope. Executions in further scopes share one counter per key name, so which of
	// them fire is no longer reproducible.
	EventScopeLimit
)

// Event is a decision of BUGGIFY.
type Event struct {
	Kind EventKind
	// Name is the name of the key. Scope is the scope of the execution of an EventFire, and empty
	// for an EventActivation.
	Name  string
	Scope string
	// Activated is the decision of an EventActivation.
	Activated bool
	// Action is what an EventFire did: ActionReturn if it injected an error and ActionOn if a call
	// site takes its branch.
	Action failpoint.Action
	// ExecutionNumber is the number of the execution of the name in the scope of an EventFire.
	ExecutionNumber int64
}

// OnDecision sets fn as the observer of the decisions of BUGGIFY, or removes the observer if fn is
// nil. A set observer replaces the previous one. Events are delivered in the order that BUGGIFY
// decided them, one at a time, without holding a lock of BUGGIFY, so the EventActivation of a
// name precedes its EventFire events. An execution that decides while another goroutine is
// delivering leaves its events to that goroutine and doesn't wait for them, and fn can execute
// keys, also the one that it handles an event for: the events of those executions are delivered
// after fn returns. So fn runs on a goroutine that executes a key, which can be another than the
// one that the event is about, and an event can be delivered after the execution returned. fn
// should be fast, since executions that deliver events run it.
func (b *Buggify) OnDecision(fn func(Event)) {
	if fn == nil {
		b.observer.Store(nil)
		return
	}
	b.events.open()
	b.observer.Store(&fn)
}

// StopObserving is OnDecision(nil) for a shutdown that must not lose events: it stops queueing
// new events, waits until the observer has received the events that were queued before, and then
// removes the observer. It returns the error of ctx, and removes the observer anyway, if ctx ends
// first, for example because the observer blocks; the events that the observer has not received
// are dropped then. Set an observer with OnDecision to receive events again.
func (b *Buggify) StopObserving(ctx context.Context) error {
	b.events.close()
	// The delivery runs on its own goroutine, so a blocked observer can't hold up the shutdown.
	go b.deliver()
	err := b.events.waitIdle(ctx)
	b.observer.Store(nil)
	return err
}

// emit queues an event for the observer, if there is one. It doesn't call the observer, so it is
// safe to call while holding a lock. A caller that emitted must call deliver, without holding a
// lock, before it returns.
func (b *Buggify) emit(e Event) {
	if b.observer.Load() != nil {
		b.events.push(e)
	}
}

// deliver calls the observer with the queued events, unless another goroutine is already doing
// that, in which case it returns at once and leaves the events to it.
func (b *Buggify) deliver() {
	b.events.drain(func(e Event) {
		if fn := b.observer.Load(); fn != nil {
			(*fn)(e)
		}
	})
}

// eventQueue holds events in the order that they were decided, and lets one goroutine at a time
// deliver them.
type eventQueue struct {
	mu       sync.Mutex
	events   []Event
	draining bool
	// closed is whether push drops events, see Buggify.StopObserving.
	closed bool
	// pending counts the queued events, so executions that decided nothing skip mu.
	pending atomic.Int64
}

func (q *eventQueue) push(e Event) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.events = append(q.events, e)
	q.pending.Add(1)
}

func (q *eventQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
}

func (q *eventQueue) open() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = false
}

// waitIdle returns when no events are queued and no goroutine is delivering them, or with the error
// of ctx if that ends first.
func (q *eventQueue) waitIdle(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		q.mu.Lock()
		idle := len(q.events) == 0 && !q.draining
		q.mu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// drain calls deliver with the queued events, oldest first, including those that others queue
// meanwhile, without holding q.mu. It returns at once if another goroutine, or deliver on this one,
// is draining. If deliver panics, the events that were not delivered stay queued for the next
// drain.
func (q *eventQueue) drain(deliver func(Event)) {
	if q.pending.Load() == 0 {
		return
	}
	q.mu.Lock()
	if q.draining {
		q.mu.Unlock()
		return
	}
	q.draining = true
	q.mu.Unlock()

	finished := false
	defer func() {
		if !finished {
			q.mu.Lock()
			q.draining = false
			q.mu.Unlock()
		}
	}()
	for {
		q.mu.Lock()
		if len(q.events) == 0 {
			q.events = nil
			q.draining = false
			finished = true
			q.mu.Unlock()
			return
		}
		e := q.events[0]
		q.events = q.events[1:]
		q.pending.Add(-1)
		q.mu.Unlock()
		deliver(e)
	}
}

// NameStats counts the executions of a key name that BUGGIFY saw, in all scopes.
type NameStats struct {
	Name string
	// Activated reports whether BUGGIFY acts on the name at all.
	Activated bool
	// Executions counts the executions of the name, activated or not, with nothing registered for
	// it, and Fires those that BUGGIFY fired.
	Executions int64
	Fires      int64
}

// CountExecutions makes BUGGIFY count the executions and fires of each key name for Stats, from now
// on. The counters are off by default, since every execution of a selected name would otherwise add
// to counters that all scopes of the name share. Call it before the traffic that Stats should
// cover.
func (b *Buggify) CountExecutions() {
	b.counting.Store(true)
}

// Stats returns the counters of the key names that the configuration selected, since it was last
// set, sorted by name. The names are listed and their activation is reported while BUGGIFY is
// enabled, but the executions and fires only count from CountExecutions on. It is empty while
// BUGGIFY is disabled.
func (b *Buggify) Stats() []NameStats {
	s := b.current.Load()
	if s == nil {
		return nil
	}
	return s.stats()
}

// Error returns the error that BUGGIFY injects into this execution of name in scope, or nil. It is
// nil if the key is not activated or the execution doesn't fire, or if the configuration names no
// error for the key. It implements failpoint.Policy; call sites use failpoint.CheckError or
// failpoint.ExecuteInOrder.
func (b *Buggify) Error(name, scope string) error {
	s := b.current.Load()
	if s == nil {
		return nil
	}
	defer b.deliver()
	k := s.lookup(name, b.emit)
	if k == nil {
		return nil
	}
	if b.counting.Load() {
		k.executions.Add(1)
	}
	if !k.activated || k.newError == nil {
		return nil
	}
	if !s.fire(b.emit, name, scope, k, failpoint.ActionReturn) {
		return nil
	}
	// Callers can store the message, for example in a proto, so it must be valid UTF-8 even if the
	// key name isn't, such as the name of an HTTP operation with an escaped path.
	return k.newError("BUGGIFY: injected by failpoint " + strings.ToValidUTF8(name, "�"))
}

// Switch reports whether a switch-style call site for name in scope should take its alternate path
// on this execution. It implements failpoint.Policy; call sites use failpoint.ExecuteSwitch or
// failpoint.ExecuteSwitchWithPolicyScope.
func (b *Buggify) Switch(name, scope string) bool {
	s := b.current.Load()
	if s == nil {
		return false
	}
	defer b.deliver()
	k := s.lookup(name, b.emit)
	if k == nil {
		return false
	}
	if b.counting.Load() {
		k.executions.Add(1)
	}
	if !k.activated {
		return false
	}
	return s.fire(b.emit, name, scope, k, failpoint.ActionOn)
}

// maxScopes bounds the execution counters of activated names per scope; executions in further
// scopes share one counter per name. maxUnselectedKeys bounds the cache of names that the
// configuration doesn't select; further names are matched against the configuration on each
// execution. The names of keys are bounded by the call sites, which don't build them from text
// that a caller chooses, such as the path of an HTTP request.
const (
	maxScopes         = 1 << 16
	maxUnselectedKeys = 1 << 14
)

// run is the BUGGIFY state of a registry for one configuration.
type run struct {
	config Config
	// counting is the flag of the Buggify, see CountExecutions.
	counting *atomic.Bool

	// mu guards the fields below and the counters of every nameState.
	mu sync.Mutex
	// names holds the nameState of each key name that the code executed, created on its first
	// execution, and nil for the names that the configuration doesn't select. The number of the
	// latter is bounded by maxUnselectedKeys.
	names      map[string]*nameState
	unselCount int
	scopeCount int
	// limitReported is set once EventScopeLimit was reported.
	limitReported bool
}

// nameState is the BUGGIFY state of a key name: its activation for the run and, if it is
// activated, the number of its executions in each scope, which numbers the draws that decide
// whether they fire.
type nameState struct {
	activated bool
	fire      float64
	newError  func(msg string) error
	// nameMix is the hash of the name that its draws use (see Config.Draw), computed once.
	nameMix uint64
	// executions and fires count the executions of the name in all scopes and those that fired,
	// see NameStats.
	executions atomic.Int64
	fires      atomic.Int64
	// counters holds an executionCounter per scope, "" for executions without a scope. It is
	// guarded by run.mu.
	counters map[string]*executionCounter
	// overflow counts the executions in the scopes that have no counter of their own.
	overflow executionCounter
}

// executionCounter counts the executions of an activated key name in a scope.
type executionCounter struct {
	// mu serializes the executions of the scope, so the number of an execution, the decision
	// whether it fires and the queuing of its EventFire happen in one step, and the events of a
	// scope are queued in the order of their numbers.
	mu sync.Mutex
	n  uint64
	// scopeMix is the hash of the scope that the draws of the executions use, computed once.
	scopeMix uint64
}

// lookup returns the state of a key name, or nil if the configuration doesn't select it. It
// creates the state of a selected name on its first execution, and queues its activation with emit
// before any execution can see the state, so the activation event precedes the name's fire events.
func (s *run) lookup(name string, emit func(Event)) *nameState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if k, ok := s.names[name]; ok {
		return k
	}
	params, selected := s.config.params(name)
	if !selected {
		if s.unselCount < maxUnselectedKeys {
			s.names[name] = nil
			s.unselCount++
		}
		return nil
	}
	nameMix := nameMix(name)
	k := &nameState{
		activated: draw(s.config.Seed, nameMix, 0) < params.ActivationProbability,
		fire:      params.FireProbability,
		nameMix:   nameMix,
		counters:  make(map[string]*executionCounter),
	}
	if params.Error != "" {
		k.newError, _ = failpoint.LookupError(params.Error)
	}
	emit(Event{Kind: EventActivation, Name: name, Activated: k.activated})
	s.names[name] = k
	return k
}

// fire counts an execution of an activated key in a scope and reports whether it fires. If it
// fires, fire queues its EventFire with emit in the same critical section that numbers the
// execution, so the events of a scope are queued in the order of their execution numbers.
func (s *run) fire(emit func(Event), name, scope string, k *nameState, action failpoint.Action) bool {
	counter, scopeMix := s.counter(emit, k, scope)
	counter.mu.Lock()
	defer counter.mu.Unlock()
	counter.n++
	if draw(s.config.Seed, k.nameMix^scopeMix, counter.n) >= k.fire {
		return false
	}
	if s.counting.Load() {
		k.fires.Add(1)
	}
	emit(Event{Kind: EventFire, Name: name, Scope: scope, Action: action, ExecutionNumber: int64(counter.n)})
	return true
}

// counter returns the execution counter of an activated key in a scope, and the hash of the scope.
// Once the counters are at their bound, it returns the shared counter of the name for the scopes
// without one, and reports that to emit.
func (s *run) counter(emit func(Event), k *nameState, scope string) (*executionCounter, uint64) {
	s.mu.Lock()
	if c, ok := k.counters[scope]; ok {
		s.mu.Unlock()
		return c, c.scopeMix
	}
	if s.scopeCount >= maxScopes {
		report := !s.limitReported
		s.limitReported = true
		s.mu.Unlock()
		if report {
			emit(Event{Kind: EventScopeLimit})
		}
		return &k.overflow, scopeMix(scope)
	}
	c := &executionCounter{scopeMix: scopeMix(scope)}
	k.counters[scope] = c
	s.scopeCount++
	s.mu.Unlock()
	return c, c.scopeMix
}

// stats returns the counters of the names that the configuration selected, sorted by name.
func (s *run) stats() []NameStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := make([]NameStats, 0, len(s.names))
	for name, k := range s.names {
		if k == nil {
			continue
		}
		// An execution counts itself before its fire, so reading fires first keeps fires <=
		// executions.
		fires := k.fires.Load()
		stats = append(stats, NameStats{Name: name, Activated: k.activated, Executions: k.executions.Load(), Fires: fires})
	}
	slices.SortFunc(stats, func(a, b NameStats) int { return strings.Compare(a.Name, b.Name) })
	return stats
}
