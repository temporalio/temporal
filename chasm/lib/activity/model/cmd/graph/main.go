// Command graph enumerates the activity model's reachable state graph: the fixpoint of model.Transition
// from model.Initial(cfg), following non-reject edges, with states identified by model.Fingerprint. An
// inspection tool for the same graph the conformance explorers traverse. It drives no server.
//
// Usage:
//
//	go run ./chasm/lib/activity/model/cmd/graph                       # counts + nodes + edges, onebox configs
//	go run ./chasm/lib/activity/model/cmd/graph -show skeleton        # status-level transition relation
//	go run ./chasm/lib/activity/model/cmd/graph -explorer engine -show counts
//	go run ./chasm/lib/activity/model/cmd/graph -config 1 -show nodes
//
// Flags:
//
//	-explorer  engine | onebox   whose event alphabet and config set to explore (default onebox)
//	-config    N                 restrict to config index N of that explorer's set (default: all)
//	-show      list              comma list of counts,nodes,edges,skeleton (default counts,nodes,edges)
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"go.temporal.io/server/chasm/lib/activity/model"
)

const fingerprintLegend = "fingerprint = Status|min(count,3)|resetKeepPaused|resetHeartbeats|resetRestoreOpts|firstAttemptStarted|dispatchTimeSet|dispatchability"

func main() {
	explorer := flag.String("explorer", "onebox", "whose event alphabet + config set to explore: engine (worker RPCs + timeouts/backoff) or onebox (worker RPCs + operator commands)")
	configIdx := flag.Int("config", -1, "restrict to this config index within the explorer's set (default: all)")
	show := flag.String("show", "counts,nodes,edges", "comma list of: counts,nodes,edges,skeleton")
	flag.Parse()

	cfgs, ok := explorerConfigs(*explorer)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown -explorer %q (want engine or onebox)\n", *explorer)
		os.Exit(2)
	}
	sel := map[string]bool{}
	for s := range strings.SplitSeq(*show, ",") {
		sel[strings.TrimSpace(s)] = true
	}

	fmt.Printf("%s — %s\n", *explorer, fingerprintLegend)

	chosen := allIndices(len(cfgs))
	if *configIdx >= 0 {
		if *configIdx >= len(cfgs) {
			fmt.Fprintf(os.Stderr, "-config %d out of range (%s has %d configs)\n", *configIdx, *explorer, len(cfgs))
			os.Exit(2)
		}
		chosen = []int{*configIdx}
	}

	if sel["skeleton"] {
		selCfgs := make([]model.Config, 0, len(chosen))
		for _, i := range chosen {
			selCfgs = append(selCfgs, cfgs[i])
		}
		printSkeleton(*explorer, selCfgs)
	}

	for _, i := range chosen {
		cfg := cfgs[i]
		g := buildGraph(cfg, eventsFor(*explorer, cfg))
		fmt.Printf("\n===== config[%d] %s =====\n", i, describeConfig(cfg))
		if sel["counts"] {
			fmt.Printf("nodes: %d   non-reject edges: %d\n", len(g.nodes), len(g.edges))
		}
		if sel["nodes"] {
			fmt.Printf("NODES (%d):\n%s\n", len(g.nodes), indent(g.nodes))
		}
		if sel["edges"] {
			fmt.Printf("EDGES (%d):\n%s\n", len(g.edges), indent(g.edges))
		}
	}
}

// graph is the reachable node set (by fingerprint) and the distinct non-reject labeled edges.
type graph struct {
	nodes []string // sorted fingerprints
	edges []string // sorted "from  --label-->  to"
}

// buildGraph does the BFS from Initial(cfg), following non-reject edges to fixpoint. Accepted no-op
// self-loops are edges; rejected calls are not. States are deduped by Fingerprint.
func buildGraph(cfg model.Config, events []model.Event) graph {
	start := model.Initial(cfg)
	startFP := model.Fingerprint(start)
	nodeSet := map[string]bool{startFP: true}
	edgeSet := map[string]bool{}
	visited := map[string]bool{startFP: true}
	frontier := []model.AbstractState{start}
	for len(frontier) > 0 {
		var next []model.AbstractState
		for _, s := range frontier {
			for _, e := range events {
				out := model.Transition(cfg, s, e)
				toFP := model.Fingerprint(out.Next)
				nodeSet[toFP] = true
				if out.Reject != model.NoError {
					continue
				}
				edgeSet[fmt.Sprintf("%s  --%s-->  %s", model.Fingerprint(s), e, toFP)] = true
				if !visited[toFP] {
					visited[toFP] = true
					next = append(next, out.Next)
				}
			}
		}
		frontier = next
	}
	return graph{nodes: sortedKeys(nodeSet), edges: sortedKeys(edgeSet)}
}

// printSkeleton collapses the reachable graphs of the given configs to the status level: the union of
// status --eventType--> destStatus over every non-reject edge, without the fingerprint inflation from
// count buckets, reset flags, and dispatchability.
func printSkeleton(explorer string, cfgs []model.Config) {
	rel := map[string]bool{}
	for _, cfg := range cfgs {
		start := model.Initial(cfg)
		visited := map[string]bool{model.Fingerprint(start): true}
		frontier := []model.AbstractState{start}
		for len(frontier) > 0 {
			var next []model.AbstractState
			for _, s := range frontier {
				for _, e := range eventsFor(explorer, cfg) {
					out := model.Transition(cfg, s, e)
					if out.Reject != model.NoError {
						continue
					}
					self := ""
					if s.Status == out.Next.Status {
						self = "   (self)"
					}
					rel[fmt.Sprintf("%-16s --%-24s--> %-16s%s", s.Status, e.Type, out.Next.Status, self)] = true
					fp := model.Fingerprint(out.Next)
					if !visited[fp] {
						visited[fp] = true
						next = append(next, out.Next)
					}
				}
			}
			frontier = next
		}
	}
	lines := sortedKeys(rel)
	fmt.Printf("\n===== STATUS SKELETON (%d status-level edges, unioned over %d config(s)) =====\n%s\n",
		len(lines), len(cfgs), indent(lines))
}

// eventsFor is the event alphabet an explorer drives for a given config. Mirrors the candidate-event
// sets in chasm/lib/activity/activity_conformance_test.go (engine) and
// tests/activity_standalone_conformance.go (onebox). The model's timeout functions drive to TimedOut
// unconditionally, so the engine's timeout events are gated on config rather than left to fire.
func eventsFor(explorer string, cfg model.Config) []model.Event {
	switch explorer {
	case "engine":
		events := []model.Event{
			{Type: model.PollType}, {Type: model.HeartbeatType}, {Type: model.RespondCompletedType},
			{Type: model.RespondFailedType, Retryable: true}, {Type: model.RespondFailedType, Retryable: false},
			{Type: model.RespondCanceledType}, {Type: model.BackoffElapsesType}, {Type: model.StartToCloseElapsesType},
		}
		if cfg.HasHeartbeat {
			events = append(events, model.Event{Type: model.HeartbeatElapsesType})
		}
		if cfg.HasScheduleToClose {
			events = append(events, model.Event{Type: model.ScheduleToCloseElapsesType})
		}
		return events
	default:
		return oneboxEvents()
	}
}

// oneboxEvents mirrors saaCandidateEvents(): worker RPCs + operator commands, no wall-clock.
func oneboxEvents() []model.Event {
	var out []model.Event
	for _, k := range []model.EventType{model.PollType, model.HeartbeatType, model.RespondCompletedType, model.RespondCanceledType, model.UpdateOptionsType} {
		out = append(out, model.Event{Type: k})
	}
	out = append(out, model.Event{Type: model.UpdateOptionsType, SetsStartDelay: true})
	for _, r := range []bool{false, true} {
		out = append(out, model.Event{Type: model.RespondFailedType, Retryable: r})
	}
	for _, sr := range []bool{false, true} {
		out = append(out,
			model.Event{Type: model.PauseType, SameRequestID: sr},
			model.Event{Type: model.TerminateType, SameRequestID: sr},
			model.Event{Type: model.RequestCancelType, SameRequestID: sr},
		)
	}
	for _, kp := range []bool{false, true} {
		for _, ro := range []bool{false, true} {
			out = append(out, model.Event{Type: model.ResetType, KeepPaused: kp, RestoreOriginal: ro})
		}
	}
	for _, ra := range []bool{false, true} {
		out = append(out, model.Event{Type: model.UnpauseType, ResetAttempts: ra})
	}
	return out
}

// explorerConfigs is the config set each explorer sweeps. Mirrors saaTraversalConfigs (onebox) and the
// configs in activity_conformance_test.go (engine).
func explorerConfigs(explorer string) ([]model.Config, bool) {
	switch explorer {
	case "engine":
		return []model.Config{
			{MaxAttempts: 3},
			{MaxAttempts: 2, HasScheduleToClose: true, HasHeartbeat: true},
		}, true
	case "onebox":
		return []model.Config{
			{},
			{HasScheduleToClose: true, HasScheduleToStart: true, HasHeartbeat: true, MaxAttempts: 3},
			{MaxAttempts: 1},
			{HasStartDelay: true},
			{HasStartDelay: true, HasScheduleToClose: true},
		}, true
	default:
		return nil, false
	}
}

func describeConfig(cfg model.Config) string {
	var parts []string
	add := func(cond bool, name string) {
		if cond {
			parts = append(parts, name)
		}
	}
	add(cfg.HasScheduleToClose, "scheduleToClose")
	add(cfg.HasScheduleToStart, "scheduleToStart")
	add(cfg.HasHeartbeat, "heartbeat")
	add(cfg.HasStartDelay, "startDelay")
	if cfg.MaxAttempts == 0 {
		parts = append(parts, "maxAttempts=unlimited")
	} else {
		parts = append(parts, fmt.Sprintf("maxAttempts=%d", cfg.MaxAttempts))
	}
	return strings.Join(parts, " ")
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func allIndices(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

func indent(xs []string) string {
	return "  " + strings.Join(xs, "\n  ")
}
