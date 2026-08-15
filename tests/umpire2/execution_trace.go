package umpire2

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"

	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/protocol"
)

type executionTrace struct {
	mu         sync.Mutex
	recorder   *umpirefw.TraceRecorder
	registry   *umpirefw.ModelState
	relations  *umpirefw.RelationStore
	sequence   uint64
	seen       map[string]struct{}
	active     map[string]map[string][]string
	last       map[string]string
	footprints map[string]umpirefw.CausalFootprint
}

func newExecutionTrace(
	registry *umpirefw.ModelState,
	relations *umpirefw.RelationStore,
	declaredFootprints []protocol.NamedCausalFootprint,
) *executionTrace {
	trace := &executionTrace{
		registry:   registry,
		relations:  relations,
		footprints: make(map[string]umpirefw.CausalFootprint, len(declaredFootprints)),
	}
	for _, declared := range declaredFootprints {
		trace.footprints[declared.Footprint.Action] = declared.Footprint
	}
	trace.reset(nil)
	return trace
}

func (t *executionTrace) setRecorder(recorder *umpirefw.TraceRecorder) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reset(recorder)
}

func (t *executionTrace) reset(recorder *umpirefw.TraceRecorder) {
	t.recorder = recorder
	t.seen = map[string]struct{}{}
	t.active = map[string]map[string][]string{}
	t.last = map[string]string{}
	t.sequence = 0
}

func (t *executionTrace) observeExecution(observed umpirefw.ExecutionObservation) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.recorder == nil {
		return nil
	}
	fields := map[string]string{}
	for key, value := range map[string]string{
		"scope":       observed.Scope,
		"phase":       observed.Phase,
		"outcome":     observed.Outcome,
		"error_class": observed.ErrorClass,
		"checkpoint":  observed.Checkpoint,
	} {
		if value != "" {
			fields[key] = value
		}
	}
	name := observed.Action
	if observed.Kind == umpirefw.ExecutionVerdict {
		name = observed.Property
		fields["pass"] = strconv.FormatBool(observed.Pass)
		fields["violations"] = strconv.Itoa(observed.Violations)
	}
	if name == "" {
		return fmt.Errorf("execution observation %s has no stable name", observed.Kind)
	}
	keyKind := "action"
	traceKind := umpirefw.TraceAction
	var causes []string
	switch observed.Kind {
	case umpirefw.ExecutionVerdict:
		keyKind = "verdict"
		traceKind = umpirefw.TraceVerdict
		if last := t.last[observed.Scope]; last != "" {
			causes = []string{last}
		}
	case umpirefw.ExecutionActionFinish:
		if byAction := t.active[observed.Scope]; byAction != nil {
			windows := byAction[observed.Action]
			if len(windows) != 0 {
				causes = []string{windows[0]}
			}
		}
	default:
	}
	key := t.nextKey(keyKind)
	if err := t.recorder.Record(umpirefw.TraceEvent{Key: key, Kind: traceKind, Name: name, Source: umpirefw.InProcessEvidence, Causes: causes, Fields: fields}); err != nil {
		return err
	}
	switch observed.Kind {
	case umpirefw.ExecutionActionStart:
		if t.active[observed.Scope] == nil {
			t.active[observed.Scope] = map[string][]string{}
		}
		t.active[observed.Scope][observed.Action] = append(t.active[observed.Scope][observed.Action], key)
	case umpirefw.ExecutionActionFinish:
		if byAction := t.active[observed.Scope]; byAction != nil {
			windows := byAction[observed.Action]
			if len(windows) != 0 {
				byAction[observed.Action] = windows[1:]
			}
		}
		t.last[observed.Scope] = key
		if footprint, ok := t.footprints[observed.Action]; ok {
			if err := umpirefw.CompareCausalFootprint(footprint, t.recorder.Snapshot()); err != nil {
				return fmt.Errorf("causal footprint %s: %w", observed.Action, err)
			}
		}
	default:
	}
	return nil
}

func (t *executionTrace) recordFacts(facts []umpirefw.Fact) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.recorder == nil {
		return nil
	}
	roots := map[umpirefw.EntityID]struct{}{}
	var errs []error
	for _, observed := range facts {
		fields := map[string]string{}
		var causes []string
		if path := observed.TargetEntity(); path != nil {
			fields["target"] = umpirefw.EntityPathKey(path)
			roots[path.Root()] = struct{}{}
			causes = t.activeCauses(path.Root().ID)
		}
		if err := t.recorder.Record(umpirefw.TraceEvent{
			Key:    t.nextKey("fact"),
			Kind:   umpirefw.TraceFact,
			Name:   observed.Name(),
			Source: umpirefw.InProcessEvidence,
			Causes: causes,
			Fields: fields,
		}); err != nil {
			errs = append(errs, err)
		}
	}
	for _, edge := range t.relations.Snapshot() {
		if _, scoped := roots[edge.Scope]; !scoped {
			continue
		}
		semanticKey := fmt.Sprintf("relation:%s:%s:%s", edge.Type, edge.Source, edge.Target)
		if _, seen := t.seen[semanticKey]; seen {
			continue
		}
		t.seen[semanticKey] = struct{}{}
		if err := t.recorder.Record(umpirefw.TraceEvent{
			Key:    t.nextKey("relation"),
			Kind:   umpirefw.TraceRelation,
			Name:   string(edge.Type),
			Source: umpirefw.InProcessEvidence,
			Causes: t.activeCauses(edge.Scope.ID),
			Fields: map[string]string{
				"source": edge.Source.String(),
				"target": edge.Target.String(),
			},
		}); err != nil {
			errs = append(errs, err)
		}
	}
	for root := range roots {
		for _, entry := range t.registry.QueryAll(0, &root) {
			lifecycled, ok := entry.Entity.(umpirefw.Lifecycled)
			if !ok {
				continue
			}
			for _, edge := range lifecycled.Lifecycle().VisitedEdges() {
				name := protocol.TransitionCoverageID(entry.Entity.Type(), edge)
				semanticKey := "transition:" + entry.Key + ":" + name
				if _, seen := t.seen[semanticKey]; seen {
					continue
				}
				t.seen[semanticKey] = struct{}{}
				if err := t.recorder.Record(umpirefw.TraceEvent{
					Key:    t.nextKey("transition"),
					Kind:   umpirefw.TraceTransition,
					Name:   name,
					Source: umpirefw.InProcessEvidence,
					Causes: t.activeCauses(root.ID),
					Fields: map[string]string{
						"entity": entry.Key,
					},
				}); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}
	return errors.Join(errs...)
}

func (t *executionTrace) activeCauses(scope string) []string {
	byAction := t.active[scope]
	var causes []string
	for _, windows := range byAction {
		causes = append(causes, windows...)
	}
	slices.Sort(causes)
	return slices.Compact(causes)
}

func (t *executionTrace) nextKey(kind string) string {
	t.sequence++
	return fmt.Sprintf("%s:%d", kind, t.sequence)
}

func (t *executionTrace) purgeScope(scope string) {
	t.mu.Lock()
	delete(t.active, scope)
	delete(t.last, scope)
	t.mu.Unlock()
}
