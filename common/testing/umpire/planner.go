package umpire

// This file is the generic route-finder: a pure function over a Lifecycle that computes a Plan
// (route[s] through the model graph) under Constraints, before driving anything. It lives in
// the framework so any consumer (e.g. the actions-model PlanEdge) can use it without the
// Temporal model registry, which stays in tests/umpirev1/planner (Models/DefaultModels). See
// UMPIRE_PLANNER.md for the developer guide and UMPIRE_DRIVER.md for the broader architecture.

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
)

// RouteMode selects which route(s) to a target the planner emits when the model
// admits more than one — the "mostly deterministic" knob: the target states are
// always guaranteed; the route to them is the free variable.
type RouteMode int

const (
	// Shortest emits one canonical shortest route (deterministic; the default).
	Shortest RouteMode = iota
	// AllRoutes emits every simple route to the target — exhaustive over *how* the
	// target is reached, so route-dependent bugs are caught.
	AllRoutes
	// Random emits one route chosen with the plan's seed (reproducible).
	Random
)

// Constraints carve a sub-graph out of the model: they bound both fixed-target
// planning (shaping which routes are legal) and exploration (the space to walk).
// They are enforced by construction — the planner never emits a route that leaves
// the sub-graph — so a Plan cannot violate them.
type Constraints struct {
	AllowEvents []string // if non-empty, only these events may be used
	DenyEvents  []string // these events may never be used
	DenyStates  []string // routes may never enter these states
	MaxDepth    int      // max route length (0 = bounded by the graph size)
	// Grants is the set of drive-capabilities the environment provides. When
	// non-nil, an edge is traversable only if every capability it Needs (its
	// EdgeRequires trait) is granted — so a route can never require power the
	// environment lacks. nil disables capability filtering (any edge is allowed).
	Grants []Capability
	// Hosting is the execution context this run drives (Standalone or Embedded). An edge
	// restricted to a different hosting (its HostedIn trait) is dropped — e.g. a workflow
	// (Embedded) run can never reach terminated, which is Standalone-only. AnyHosting (the
	// default) disables hosting filtering.
	Hosting Hosting
}

// Plan is produced before anything runs: a concrete, inspectable set of routes
// (event sequences) over the model, validated to reach Target (empty for
// exploration). It is the artifact that makes "plan before run" real — dump it,
// check it, replay it.
type Plan struct {
	Target string     // the target state, or "" for an exploration walk
	Routes [][]string // one or more event sequences; each reaches Target
}

// Reaches reports whether the plan is a fixed-target plan for state.
func (p *Plan) Reaches(state string) bool { return p.Target == state && len(p.Routes) > 0 }

// Steps is the total number of events across all routes.
func (p *Plan) Steps() int {
	n := 0
	for _, r := range p.Routes {
		n += len(r)
	}
	return n
}

func (p *Plan) String() string {
	var b strings.Builder
	if p.Target != "" {
		fmt.Fprintf(&b, "Plan -> %s (%d route(s)):\n", p.Target, len(p.Routes))
	} else {
		fmt.Fprintf(&b, "Exploration plan (%d walk(s)):\n", len(p.Routes))
	}
	for i, r := range p.Routes {
		fmt.Fprintf(&b, "  [%d] %s\n", i, strings.Join(r, " -> "))
	}
	return b.String()
}

// Option tunes planning (currently the RNG seed for Random / Explore).
type Option func(*options)

type options struct{ seed int64 }

// WithSeed sets the RNG seed for Random routing and Explore, making them reproducible.
func WithSeed(s int64) Option { return func(o *options) { o.seed = s } }

func apply(opts []Option) options {
	o := options{seed: 1}
	for _, f := range opts {
		f(&o)
	}
	return o
}

// PlanTo builds a Plan that drives lc's entity from its initial state to target,
// honoring c. It fails fast — before any action — if target is not a state of the
// model, or is unreachable under the constraints. That error is itself a useful
// test primitive: a negative/reachability assertion is just an expected failure here.
func PlanTo(lc *Lifecycle, target string, mode RouteMode, c Constraints, opts ...Option) (*Plan, error) {
	if !lc.Reachable()[target] {
		return nil, fmt.Errorf("planner: %q is not a reachable state of this model", target)
	}
	adj := advanceEdges(lc, c)
	start := lc.Initial()
	depth := c.MaxDepth

	var routes [][]string
	switch mode {
	case Shortest:
		if r, ok := shortestRoute(adj, start, target, depth); ok {
			routes = [][]string{r}
		}
	case AllRoutes:
		routes = allRoutes(adj, start, target, depth)
	case Random:
		if r, ok := randomRoute(adj, start, target, depth, rand.New(rand.NewSource(apply(opts).seed))); ok {
			routes = [][]string{r}
		}
	default:
		return nil, fmt.Errorf("planner: unknown route mode %d", mode)
	}
	if len(routes) == 0 {
		// Distinguish a capability shortfall from a plain constraint dead-end, so a
		// missing environment capability is an explicit, named skip — never silent.
		if miss := missingCapabilities(lc, c, target); len(miss) > 0 {
			return nil, fmt.Errorf("planner: %q is unreachable: route needs drive-capability %v not granted by %v", target, miss, c.Grants)
		}
		if h := blockingHosting(lc, c, target); h != AnyHosting {
			return nil, fmt.Errorf("planner: %q is unreachable: route needs %s hosting, this run drives %s", target, h, c.Hosting)
		}
		return nil, fmt.Errorf("planner: %q is unreachable under the given constraints", target)
	}
	return &Plan{Target: target, Routes: routes}, nil
}

// Explore builds a Plan that walks the constrained sub-graph from the initial
// state — for exercising a problem space rather than a fixed target. The walk
// stays within the constraints by construction; it is reproducible via WithSeed,
// and its realized route can be frozen into a fixed regression Plan (replay).
func Explore(lc *Lifecycle, c Constraints, opts ...Option) *Plan {
	adj := advanceEdges(lc, c)
	rng := rand.New(rand.NewSource(apply(opts).seed))
	depth := c.MaxDepth
	if depth <= 0 {
		depth = 2 * len(lc.States())
	}
	state := lc.Initial()
	var route []string
	for i := 0; i < depth; i++ {
		nbrs := adj[state]
		if len(nbrs) == 0 {
			break // terminal, or a dead-end within the constraints
		}
		e := nbrs[rng.Intn(len(nbrs))]
		route = append(route, e.event)
		state = e.to
	}
	return &Plan{Routes: [][]string{route}}
}

// Step is one step the Driver realizes: an abstract model event (the graph label
// the planner emits) plus optional realization Params the Driver type-switches on.
// Planned routes carry a nil Params — the label is all planning knows; a caller binds
// per-step inputs via Plan.RunWith when a run needs them.
type Step struct {
	Event  string
	Params any
}

// Driver realizes one Step against the system under test, turning intent into real
// traffic (RPCs, worker polls, fault injection). Real drivers are Temporal-specific and
// per-entity; tests supply a fake. This is the seam between the pure planner above and a
// live server.
type Driver interface {
	Do(ctx context.Context, a Step) error
}

// Resetter is an optional Driver capability: when a Plan has several routes
// (AllRoutes), Run calls Reset between them to return the SUT to a fresh start.
type Resetter interface {
	Reset(ctx context.Context) error
}

// Run drives every route in the plan through d, in order, with no per-step params (the
// Driver supplies whatever inputs it needs from its own state). Equivalent to RunWith
// with a nil binder.
func (p *Plan) Run(ctx context.Context, d Driver) error {
	return p.RunWith(ctx, d, nil)
}

// RunWith drives every route through d, binding each step's Step.Params via bind
// (nil bind ⇒ nil Params). For multi-route plans it resets the driver between routes (if
// it is a Resetter). The monitor judges the resulting traffic out of band; Run only drives.
func (p *Plan) RunWith(ctx context.Context, d Driver, bind func(step int, event string) any) error {
	for i, route := range p.Routes {
		if i > 0 {
			if r, ok := d.(Resetter); ok {
				if err := r.Reset(ctx); err != nil {
					return fmt.Errorf("planner: reset before route %d: %w", i, err)
				}
			}
		}
		for step, ev := range route {
			a := Step{Event: ev}
			if bind != nil {
				a.Params = bind(step, ev)
			}
			if err := d.Do(ctx, a); err != nil {
				return fmt.Errorf("planner: route %d event %q: %w", i, ev, err)
			}
		}
	}
	return nil
}

// --- planning internals over the model graph --------------------------------------

type edge struct {
	event string
	to    string
}

// advanceEdges builds the adjacency the planner routes over: the model's direct
// transition edges, filtered by the constraints. It uses Edges (real transitions),
// not Classify's forward-jump interpretation — a plan must drive each real step,
// never "jump" over one. Because denied events/states are dropped here, nothing
// downstream can produce a route that violates the constraints.
func advanceEdges(lc *Lifecycle, c Constraints) map[string][]edge {
	allow := toSet(c.AllowEvents)
	denyEv := toSet(c.DenyEvents)
	denySt := toSet(c.DenyStates)
	grants := capSet(c.Grants)
	adj := map[string][]edge{}
	for _, e := range lc.Edges() {
		if e.To == e.From {
			continue // self-loop: no progress
		}
		if len(allow) > 0 && !allow[e.Event] {
			continue
		}
		if denyEv[e.Event] || denySt[e.To] {
			continue
		}
		if !granted(lc, e.From, e.Event, grants) {
			continue // needs a drive-capability this environment doesn't grant
		}
		if !hostingOK(lc, e.From, e.Event, c.Hosting) {
			continue // restricted to a hosting this run does not drive
		}
		adj[e.From] = append(adj[e.From], edge{event: e.Event, to: e.To})
	}
	for s := range adj {
		sort.Slice(adj[s], func(i, j int) bool { return adj[s][i].event < adj[s][j].event })
	}
	return adj
}

// hostingOK reports whether an edge's hosting restriction is compatible with the run's
// declared hosting. An unrestricted edge (AnyHosting) is always allowed, and a run that
// declares no hosting (AnyHosting) accepts every edge; only a concrete mismatch is dropped.
func hostingOK(lc *Lifecycle, from, event string, runHosting Hosting) bool {
	edgeHosting := lc.EdgeHosting(from, event)
	if edgeHosting == AnyHosting || runHosting == AnyHosting {
		return true
	}
	return edgeHosting == runHosting
}

// granted reports whether every drive-capability the from→event edge requires is in
// the grant set. A nil grant set means capability filtering is off (any edge allowed);
// an edge with no requirement is always allowed.
func granted(lc *Lifecycle, from, event string, grants map[Capability]bool) bool {
	if grants == nil {
		return true
	}
	for _, need := range lc.EdgeRequires(from, event) {
		if !grants[need] {
			return false
		}
	}
	return true
}

func capSet(caps []Capability) map[Capability]bool {
	if caps == nil {
		return nil
	}
	m := make(map[Capability]bool, len(caps))
	for _, c := range caps {
		m[c] = true
	}
	return m
}

// missingCapabilities returns the drive-capabilities that a route to target needs but
// the constraints do not grant — nil when the failure is not capability-related (no
// grant set in play, or the target is unreachable even with every capability).
// blockingHosting returns the hosting a route to target requires when the run's declared
// Hosting is what blocks it (dropping the hosting filter makes the target reachable), or
// AnyHosting when hosting is not the blocker. It mirrors missingCapabilities so a hosting
// shortfall is a named, explicit skip rather than a silent dead-end.
func blockingHosting(lc *Lifecycle, c Constraints, target string) Hosting {
	if c.Hosting == AnyHosting {
		return AnyHosting
	}
	full := c
	full.Hosting = AnyHosting // re-plan ignoring hosting to isolate the shortfall
	adj := advanceEdges(lc, full)
	route, ok := shortestRoute(adj, lc.Initial(), target, c.MaxDepth)
	if !ok {
		return AnyHosting // unreachable even ignoring hosting: not a hosting problem
	}
	dest := edgeTargets(adj)
	state := lc.Initial()
	for _, ev := range route {
		if h := lc.EdgeHosting(state, ev); h != AnyHosting {
			return h
		}
		state = dest[state][ev]
	}
	return AnyHosting
}

func missingCapabilities(lc *Lifecycle, c Constraints, target string) []Capability {
	if c.Grants == nil {
		return nil
	}
	full := c
	full.Grants = nil // re-plan ignoring capabilities to isolate the shortfall
	adj := advanceEdges(lc, full)
	route, ok := shortestRoute(adj, lc.Initial(), target, c.MaxDepth)
	if !ok {
		return nil // unreachable even with full power: not a capability problem
	}
	grants := capSet(c.Grants)
	dest := edgeTargets(adj)
	var missing []Capability
	seen := map[Capability]bool{}
	state := lc.Initial()
	for _, ev := range route {
		for _, need := range lc.EdgeRequires(state, ev) {
			if !grants[need] && !seen[need] {
				seen[need] = true
				missing = append(missing, need)
			}
		}
		state = dest[state][ev]
	}
	return missing
}

func edgeTargets(adj map[string][]edge) map[string]map[string]string {
	m := map[string]map[string]string{}
	for from, es := range adj {
		m[from] = map[string]string{}
		for _, e := range es {
			m[from][e.event] = e.to
		}
	}
	return m
}

func shortestRoute(adj map[string][]edge, start, target string, maxDepth int) ([]string, bool) {
	if start == target {
		return []string{}, true
	}
	type node struct {
		state string
		path  []string
	}
	seen := map[string]bool{start: true}
	queue := []node{{start, nil}}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		if maxDepth > 0 && len(n.path) >= maxDepth {
			continue
		}
		for _, e := range adj[n.state] {
			if seen[e.to] {
				continue
			}
			path := append(append([]string{}, n.path...), e.event)
			if e.to == target {
				return path, true
			}
			seen[e.to] = true
			queue = append(queue, node{e.to, path})
		}
	}
	return nil, false
}

func allRoutes(adj map[string][]edge, start, target string, maxDepth int) [][]string {
	var out [][]string
	var dfs func(state string, path []string, visited map[string]bool)
	dfs = func(state string, path []string, visited map[string]bool) {
		if state == target && len(path) > 0 {
			out = append(out, append([]string{}, path...))
			return
		}
		if maxDepth > 0 && len(path) >= maxDepth {
			return
		}
		for _, e := range adj[state] {
			if visited[e.to] {
				continue
			}
			visited[e.to] = true
			dfs(e.to, append(path, e.event), visited)
			visited[e.to] = false
		}
	}
	dfs(start, nil, map[string]bool{start: true})
	return out
}

func randomRoute(adj map[string][]edge, start, target string, maxDepth int, rng *rand.Rand) ([]string, bool) {
	var dfs func(state string, path []string, visited map[string]bool) ([]string, bool)
	dfs = func(state string, path []string, visited map[string]bool) ([]string, bool) {
		if state == target && len(path) > 0 {
			return append([]string{}, path...), true
		}
		if maxDepth > 0 && len(path) >= maxDepth {
			return nil, false
		}
		nbrs := append([]edge{}, adj[state]...)
		rng.Shuffle(len(nbrs), func(i, j int) { nbrs[i], nbrs[j] = nbrs[j], nbrs[i] })
		for _, e := range nbrs {
			if visited[e.to] {
				continue
			}
			visited[e.to] = true
			if r, ok := dfs(e.to, append(path, e.event), visited); ok {
				return r, true
			}
			visited[e.to] = false
		}
		return nil, false
	}
	return dfs(start, nil, map[string]bool{start: true})
}

func toSet(ss []string) map[string]bool {
	if len(ss) == 0 {
		return nil
	}
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}
	return m
}
