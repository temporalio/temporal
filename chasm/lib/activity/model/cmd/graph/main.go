// Command graph enumerates the activity behavior-model's reachable state graph: the fixpoint of
// model.Transition from model.Initial(cfg), following non-reject edges, with states identified by
// model.Fingerprint. It is a development/inspection tool for the same graph the tier-2 and tier-3
// explorers traverse; it drives no server and imports only the model.
//
// Usage:
//
//	go run ./chasm/lib/activity/model/cmd/graph                 # counts + nodes + edges, tier-3 configs
//	go run ./chasm/lib/activity/model/cmd/graph -show skeleton  # status-level transition relation
//	go run ./chasm/lib/activity/model/cmd/graph -tier 2 -show counts
//	go run ./chasm/lib/activity/model/cmd/graph -config 1 -show nodes
//
// Flags:
//
//	-tier   2 | 3        the event alphabet and default config set to explore (default 3)
//	-config N            restrict to config index N of the tier's set (default: all)
//	-show   list         comma list of counts,nodes,edges,skeleton (default counts,nodes,edges)
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
	tier := flag.Int("tier", 3, "event alphabet + config set: 2 (worker RPCs + timeouts/backoff) or 3 (worker RPCs + operator commands)")
	configIdx := flag.Int("config", -1, "restrict to this config index within the tier's set (default: all)")
	show := flag.String("show", "counts,nodes,edges", "comma list of: counts,nodes,edges,skeleton")
	flag.Parse()

	cfgs, ok := tierConfigs(*tier)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown -tier %d (want 2 or 3)\n", *tier)
		os.Exit(2)
	}
	sel := map[string]bool{}
	for s := range strings.SplitSeq(*show, ",") {
		sel[strings.TrimSpace(s)] = true
	}

	fmt.Printf("tier %d — %s\n", *tier, fingerprintLegend)

	chosen := allIndices(len(cfgs))
	if *configIdx >= 0 {
		if *configIdx >= len(cfgs) {
			fmt.Fprintf(os.Stderr, "-config %d out of range (tier %d has %d configs)\n", *configIdx, *tier, len(cfgs))
			os.Exit(2)
		}
		chosen = []int{*configIdx}
	}

	if sel["skeleton"] {
		selCfgs := make([]model.Config, 0, len(chosen))
		for _, i := range chosen {
			selCfgs = append(selCfgs, cfgs[i])
		}
		printSkeleton(*tier, selCfgs)
	}

	for _, i := range chosen {
		cfg := cfgs[i]
		g := buildGraph(cfg, eventsFor(*tier, cfg))
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
				edgeSet[fmt.Sprintf("%s  --%s-->  %s", model.Fingerprint(s), model.EventLabel(e), toFP)] = true
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
// status --eventKind--> destStatus over every non-reject edge. This is the semantic transition
// relation, free of the fingerprint inflation from count buckets, reset flags, and dispatchability.
func printSkeleton(tier int, cfgs []model.Config) {
	rel := map[string]bool{}
	for _, cfg := range cfgs {
		start := model.Initial(cfg)
		visited := map[string]bool{model.Fingerprint(start): true}
		frontier := []model.AbstractState{start}
		for len(frontier) > 0 {
			var next []model.AbstractState
			for _, s := range frontier {
				for _, e := range eventsFor(tier, cfg) {
					out := model.Transition(cfg, s, e)
					if out.Reject != model.NoError {
						continue
					}
					self := ""
					if s.Status == out.Next.Status {
						self = "   (self)"
					}
					rel[fmt.Sprintf("%-16s --%-24s--> %-16s%s", s.Status, model.KindName(e.Kind), out.Next.Status, self)] = true
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

// eventsFor is the event alphabet an explorer tier drives for a given config, mirroring the
// candidate-event sets in the tier-2 (chasm/lib/activity/spec_inprocess_test.go) and tier-3
// (tests/activity_standalone_spec_harness.go) explorers. The tier-2 timeout events must be gated on
// config: the model's timeout functions drive to TimedOut unconditionally, so the explorers exclude
// an unconfigured timeout's event rather than let it fire.
func eventsFor(tier int, cfg model.Config) []model.Event {
	switch tier {
	case 2:
		events := []model.Event{
			{Kind: model.Poll}, {Kind: model.Heartbeat}, {Kind: model.RespondCompleted},
			{Kind: model.RespondFailed, Retryable: true}, {Kind: model.RespondFailed, Retryable: false},
			{Kind: model.RespondCanceled}, {Kind: model.BackoffElapses}, {Kind: model.StartToCloseElapses},
		}
		if cfg.HasHeartbeat {
			events = append(events, model.Event{Kind: model.HeartbeatElapses})
		}
		if cfg.HasScheduleToClose {
			events = append(events, model.Event{Kind: model.ScheduleToCloseElapses})
		}
		return events
	default:
		return tier3Events()
	}
}

// tier3Events mirrors saaCandidateEvents(): worker RPCs + operator commands, no wall-clock.
func tier3Events() []model.Event {
	var out []model.Event
	for _, k := range []model.EventKind{model.Poll, model.Heartbeat, model.RespondCompleted, model.RespondCanceled, model.UpdateOptions} {
		out = append(out, model.Event{Kind: k})
	}
	out = append(out, model.Event{Kind: model.UpdateOptions, SetsStartDelay: true})
	for _, r := range []bool{false, true} {
		out = append(out, model.Event{Kind: model.RespondFailed, Retryable: r})
	}
	for _, sr := range []bool{false, true} {
		out = append(out,
			model.Event{Kind: model.Pause, SameRequestID: sr},
			model.Event{Kind: model.Terminate, SameRequestID: sr},
			model.Event{Kind: model.RequestCancel, SameRequestID: sr},
		)
	}
	for _, kp := range []bool{false, true} {
		for _, ro := range []bool{false, true} {
			out = append(out, model.Event{Kind: model.Reset, KeepPaused: kp, RestoreOriginal: ro})
		}
	}
	for _, ra := range []bool{false, true} {
		out = append(out, model.Event{Kind: model.Unpause, ResetAttempts: ra})
	}
	return out
}

// tierConfigs is the config set each tier's explorer sweeps: tier-3 mirrors saaTraversalConfigs
// (tests/activity_standalone_spec_test.go), tier-2 the in-process configs (spec_inprocess_test.go).
func tierConfigs(tier int) ([]model.Config, bool) {
	switch tier {
	case 2:
		return []model.Config{
			{MaxAttempts: 3},
			{MaxAttempts: 2, HasScheduleToClose: true, HasHeartbeat: true},
		}, true
	case 3:
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
