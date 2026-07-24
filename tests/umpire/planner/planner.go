// Package planner is the active side's test-authoring surface: a developer
// describes the entity states they want exercised, and the planner computes a
// Plan (route[s] through the model graph) before driving anything. The planner is
// a pure function over the shared model (umpire.Lifecycle's Reachable/Cells), so a
// test's target reachability, its routes, and its constraints are all checkable
// before a single RPC is sent. Realizing an abstract event as real traffic is the
// Driver seam.
//
// See UMPIRE_PLANNER.md for the developer guide and UMPIRE_DRIVER.md for the broader
// architecture.
package planner

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"go.temporal.io/server/common/testing/umpire"
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
func PlanTo(lc *umpire.Lifecycle, target string, mode RouteMode, c Constraints, opts ...Option) (*Plan, error) {
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
		return nil, fmt.Errorf("planner: %q is unreachable under the given constraints", target)
	}
	return &Plan{Target: target, Routes: routes}, nil
}

// Explore builds a Plan that walks the constrained sub-graph from the initial
// state — for exercising a problem space rather than a fixed target. The walk
// stays within the constraints by construction; it is reproducible via WithSeed,
// and its realized route can be frozen into a fixed regression Plan (replay).
func Explore(lc *umpire.Lifecycle, c Constraints, opts ...Option) *Plan {
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

// Driver realizes one abstract model event against the system under test, turning
// intent into real traffic (RPCs, worker polls, fault injection). Real drivers are
// Temporal-specific and per-entity; tests supply a fake. This is the seam between
// the pure planner above and a live server.
type Driver interface {
	Do(ctx context.Context, event string) error
}

// Resetter is an optional Driver capability: when a Plan has several routes
// (AllRoutes), Run calls Reset between them to return the SUT to a fresh start.
type Resetter interface {
	Reset(ctx context.Context) error
}

// Run drives every route in the plan through d, in order. For multi-route plans it
// resets the driver between routes (if it is a Resetter). The monitor judges the
// resulting traffic out of band; Run only drives.
func (p *Plan) Run(ctx context.Context, d Driver) error {
	for i, route := range p.Routes {
		if i > 0 {
			if r, ok := d.(Resetter); ok {
				if err := r.Reset(ctx); err != nil {
					return fmt.Errorf("planner: reset before route %d: %w", i, err)
				}
			}
		}
		for _, ev := range route {
			if err := d.Do(ctx, ev); err != nil {
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
func advanceEdges(lc *umpire.Lifecycle, c Constraints) map[string][]edge {
	allow := toSet(c.AllowEvents)
	denyEv := toSet(c.DenyEvents)
	denySt := toSet(c.DenyStates)
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
		adj[e.From] = append(adj[e.From], edge{event: e.Event, to: e.To})
	}
	for s := range adj {
		sort.Slice(adj[s], func(i, j int) bool { return adj[s][i].event < adj[s][j].event })
	}
	return adj
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
