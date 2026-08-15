package umpire

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

// RuntimeEntityDeclaration registers one entity implementation with a runtime.
type RuntimeEntityDeclaration struct {
	Type EntityType
	New  EntityFactory
}

// RelationMutation is one fact-derived change to runtime relation state.
type RelationMutation struct {
	Edge   RelationEdge
	Remove bool
}

// RelationDeriver translates one observed fact into relation mutations.
type RelationDeriver func(Fact) []RelationMutation

// RuntimeRuleDeclaration registers exactly one safety or liveness rule factory.
type RuntimeRuleDeclaration struct {
	Safety   func() SafetyRule
	Liveness func() LivenessRule
}

// RuntimeDeclaration is the immutable input used to construct a Runtime.
type RuntimeDeclaration struct {
	Facts            []Fact
	Entities         []RuntimeEntityDeclaration
	Relations        []RelationSchema
	RelationDerivers []RelationDeriver
	Rules            []RuntimeRuleDeclaration
}

// Runtime owns fact retention, entity routing, relations, rules, and scoped cleanup.
type Runtime struct {
	factTypes map[string]reflect.Type
	state     *ModelState
	facts     *FactLog
	relations *RelationStore
	rules     *RuleRegistry
	derivers  []RelationDeriver
}

// RuntimeView is a scoped, read-only extension view over runtime-owned state.
type RuntimeView struct {
	runtime *Runtime
	scope   *EntityID
}

// NewRuntime validates a complete declaration before constructing mutable runtime state.
func NewRuntime(declaration RuntimeDeclaration) (runtime *Runtime, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			runtime = nil
			err = fmt.Errorf("runtime: construct declaration: %v", recovered)
		}
	}()
	declaration, err = validateRuntimeDeclaration(declaration)
	if err != nil {
		return nil, err
	}

	state := NewModelState()
	factTypes := make(map[string]reflect.Type, len(declaration.Facts))
	for _, observed := range declaration.Facts {
		state.RegisterFact(observed)
		factTypes[observed.Name()] = reflect.TypeOf(observed)
	}
	for _, entity := range declaration.Entities {
		state.RegisterEntity(entity.New)
	}
	relations, err := NewRelationStore(declaration.Relations...)
	if err != nil {
		return nil, fmt.Errorf("runtime: relations: %w", err)
	}
	rules := NewRuleRegistry()
	names := make([]string, 0, len(declaration.Rules))
	for _, declared := range declaration.Rules {
		if declared.Safety != nil {
			rules.RegisterSafety(declared.Safety)
			names = append(names, declared.Safety().Name())
		} else {
			rules.RegisterLiveness(declared.Liveness)
			names = append(names, declared.Liveness().Name())
		}
	}
	if err := rules.InitRules(state, RuleConfig{Relations: relations}, names...); err != nil {
		return nil, fmt.Errorf("runtime: rules: %w", err)
	}
	return &Runtime{
		factTypes: factTypes,
		state:     state,
		facts:     NewFactLog(),
		relations: relations,
		rules:     rules,
		derivers:  slices.Clone(declaration.RelationDerivers),
	}, nil
}

func validateRuntimeDeclaration(declaration RuntimeDeclaration) (RuntimeDeclaration, error) {
	result := RuntimeDeclaration{
		Facts:            slices.Clone(declaration.Facts),
		Entities:         slices.Clone(declaration.Entities),
		Relations:        slices.Clone(declaration.Relations),
		RelationDerivers: slices.Clone(declaration.RelationDerivers),
		Rules:            slices.Clone(declaration.Rules),
	}
	facts := make(map[string]reflect.Type, len(result.Facts))
	for index, observed := range result.Facts {
		if observed == nil || reflect.TypeOf(observed).Kind() != reflect.Pointer {
			return RuntimeDeclaration{}, fmt.Errorf("runtime: fact %d must be a non-nil pointer", index)
		}
		name := strings.TrimSpace(observed.Name())
		if name == "" || name != reflect.TypeOf(observed).Elem().Name() {
			return RuntimeDeclaration{}, fmt.Errorf("runtime: fact %d has invalid name %q", index, observed.Name())
		}
		if _, duplicate := facts[name]; duplicate {
			return RuntimeDeclaration{}, fmt.Errorf("runtime: duplicate fact %q", name)
		}
		facts[name] = reflect.TypeOf(observed)
	}
	entities := make(map[EntityType]struct{}, len(result.Entities))
	for index, entity := range result.Entities {
		if entity.Type == "" || entity.New == nil {
			return RuntimeDeclaration{}, fmt.Errorf("runtime: entity %d requires type and factory", index)
		}
		probe := entity.New()
		if probe == nil {
			return RuntimeDeclaration{}, fmt.Errorf("runtime: entity %d factory returned nil", index)
		}
		if probe.Type() != entity.Type {
			return RuntimeDeclaration{}, fmt.Errorf("runtime: entity %d factory returned type %q, expected %q", index, probe.Type(), entity.Type)
		}
		if _, duplicate := entities[entity.Type]; duplicate {
			return RuntimeDeclaration{}, fmt.Errorf("runtime: duplicate entity %q", entity.Type)
		}
		entities[entity.Type] = struct{}{}
	}
	if _, err := NewRelationStore(result.Relations...); err != nil {
		return RuntimeDeclaration{}, fmt.Errorf("runtime: relations: %w", err)
	}
	for index, derive := range result.RelationDerivers {
		if derive == nil {
			return RuntimeDeclaration{}, fmt.Errorf("runtime: relation deriver %d is nil", index)
		}
	}
	ruleNames := make(map[string]struct{}, len(result.Rules))
	for index, rule := range result.Rules {
		if (rule.Safety == nil) == (rule.Liveness == nil) {
			return RuntimeDeclaration{}, fmt.Errorf("runtime: rule %d must declare exactly one factory", index)
		}
		var name string
		if rule.Safety != nil {
			if probe := rule.Safety(); probe != nil {
				name = probe.Name()
			}
		} else if probe := rule.Liveness(); probe != nil {
			name = probe.Name()
		}
		if strings.TrimSpace(name) == "" {
			return RuntimeDeclaration{}, fmt.Errorf("runtime: rule %d has an empty name", index)
		}
		if _, duplicate := ruleNames[name]; duplicate {
			return RuntimeDeclaration{}, fmt.Errorf("runtime: duplicate rule %q", name)
		}
		ruleNames[name] = struct{}{}
	}
	return result, nil
}

// Ingest records and routes facts, then applies their relation mutations.
func (r *Runtime) Ingest(ctx context.Context, facts ...Fact) error {
	if r == nil {
		return errors.New("runtime is nil")
	}
	for index, observed := range facts {
		if observed == nil {
			return fmt.Errorf("runtime: fact %d is nil", index)
		}
		registered, ok := r.factTypes[observed.Name()]
		if !ok || reflect.TypeOf(observed) != registered {
			return fmt.Errorf("runtime: fact %d %T is not registered", index, observed)
		}
	}
	if len(facts) == 0 {
		return nil
	}
	r.facts.AddAll(facts)
	modelErr := r.state.RouteFacts(ctx, facts)
	var relationErrs []error
	for _, observed := range facts {
		for _, derive := range r.derivers {
			for _, mutation := range derive(observed) {
				var err error
				if mutation.Remove {
					_, err = r.relations.Remove(mutation.Edge)
				} else {
					_, err = r.relations.Add(mutation.Edge)
				}
				if err == nil {
					continue
				}
				relationErrs = append(relationErrs, fmt.Errorf("runtime: derive relation from %s: %w", observed.Name(), err))
				r.recordRelationConflict(err)
			}
		}
	}
	return errors.Join(modelErr, errors.Join(relationErrs...))
}

func (r *Runtime) recordRelationConflict(err error) {
	var relationErr *RelationError
	if !errors.As(err, &relationErr) || relationErr.Scope.Type == "" || relationErr.Scope.ID == "" {
		return
	}
	key := fmt.Sprintf("%s:%s:%s:%s", relationErr.Type, relationErr.Source, relationErr.Target, relationErr.Reason)
	r.rules.RecordConformance(relationErr.Scope, key, Violation{
		Rule: "Conformance", Message: fmt.Sprintf("relation %s rejected: %s", relationErr.Type, relationErr.Reason),
		Tags: map[string]string{"relation": string(relationErr.Type), "source": relationErr.Source.String(), "target": relationErr.Target.String()},
	})
}

// Check evaluates initialized rules within one scope.
func (r *Runtime) Check(ctx context.Context, scope EntityID, final bool) []Violation {
	return r.rules.Check(ctx, final, &scope)
}

// Snapshot returns a deterministic defensive view of one scope.
func (r *Runtime) Snapshot(scope EntityID) Snapshot {
	entries := r.state.QueryAll(0, &scope)
	slices.SortFunc(entries, func(left, right EntityEntry) int { return cmp.Compare(left.Key, right.Key) })
	entities := make([]EntitySnapshot, 0, len(entries))
	for _, entry := range entries {
		leaf := entry.Key[strings.LastIndex(entry.Key, "@")+1:]
		entity := EntitySnapshot{Key: entry.Key, Type: entry.Entity.Type(), ID: strings.TrimPrefix(leaf, string(entry.Entity.Type())+":")}
		if lifecycled, ok := entry.Entity.(Lifecycled); ok {
			lifecycle := lifecycled.Lifecycle()
			entity.Current = lifecycle.Current()
			entity.Terminal = lifecycle.IsTerminal()
			entity.Disposition = lifecycle.CurrentDisposition()
			entity.Visited = lifecycle.VisitedEdges()
		}
		entities = append(entities, entity)
	}
	observed := r.facts.QueryByID(scope)
	facts := make([]FactSnapshot, len(observed))
	for index, fact := range observed {
		facts[index] = FactSnapshot{Name: fact.Name()}
	}
	relations := slices.DeleteFunc(r.relations.Snapshot(), func(edge RelationEdge) bool { return edge.Scope != scope })
	return Snapshot{Generation: r.state.Generation(), Entities: entities, Facts: facts, Relations: relations}
}

// Purge removes all state retained for one scope.
func (r *Runtime) Purge(scope EntityID) {
	r.state.PurgeScope(scope)
	r.facts.PurgeScope(scope)
	r.relations.PurgeScope(scope)
	r.rules.PurgeScope(scope)
}

// PassedKeys returns entity keys that the named rule evaluated and found healthy.
func (r *Runtime) PassedKeys(ruleName string) []string {
	return r.rules.PassedKeys(ruleName)
}

// RuleStats returns per-rule evaluation statistics.
func (r *Runtime) RuleStats() []RuleStats {
	return r.rules.Stats()
}

// View returns a read-only extension view scoped to one root.
func (r *Runtime) View(scope EntityID) RuntimeView {
	return RuntimeView{runtime: r, scope: &scope}
}

// Entities returns entity entries of one type within the view's scope.
func (v RuntimeView) Entities(entityType EntityType, sinceGeneration uint64) []EntityEntry {
	return v.runtime.state.QueryEntities(entityType, sinceGeneration, v.scope)
}

// AllEntities returns every entity entry within the view's scope.
func (v RuntimeView) AllEntities(sinceGeneration uint64) []EntityEntry {
	return v.runtime.state.QueryAll(sinceGeneration, v.scope)
}

// Facts returns facts in observation order within the view's scope.
func (v RuntimeView) Facts() []Fact {
	if v.scope == nil {
		return v.runtime.facts.All()
	}
	return v.runtime.facts.QueryByID(*v.scope)
}

// FactsByType returns facts with one name within the view's scope.
func (v RuntimeView) FactsByType(name string) []Fact {
	if v.scope == nil {
		return slices.DeleteFunc(v.runtime.facts.All(), func(observed Fact) bool { return observed.Name() != name })
	}
	return v.runtime.facts.QueryByType(*v.scope, name)
}

// Relations returns relation edges within the view's scope.
func (v RuntimeView) Relations() []RelationEdge {
	relations := v.runtime.relations.Snapshot()
	if v.scope == nil {
		return relations
	}
	return slices.DeleteFunc(relations, func(edge RelationEdge) bool { return edge.Scope != *v.scope })
}

// RelationTargets returns targets for one source.
func (v RuntimeView) RelationTargets(relationType RelationType, source EntityID) []EntityID {
	return v.runtime.relations.Targets(relationType, source)
}

// RelationSources returns sources for one target.
func (v RuntimeView) RelationSources(relationType RelationType, target EntityID) []EntityID {
	return v.runtime.relations.Sources(relationType, target)
}

// ArtifactFacts returns normalized JSON evidence for facts within one scope.
func (r *Runtime) ArtifactFacts(scope EntityID) ([]json.RawMessage, error) {
	facts := r.View(scope).Facts()
	result := make([]json.RawMessage, 0, len(facts))
	for _, observed := range facts {
		payload, err := json.Marshal(observed)
		if err != nil {
			return nil, fmt.Errorf("encode observed fact %s: %w", observed.Name(), err)
		}
		encoded, err := json.Marshal(struct {
			Name    string          `json:"name"`
			Target  *EntityPath     `json:"target,omitempty"`
			Payload json.RawMessage `json:"payload"`
		}{Name: observed.Name(), Target: observed.TargetEntity(), Payload: payload})
		if err != nil {
			return nil, fmt.Errorf("encode observed fact artifact %s: %w", observed.Name(), err)
		}
		result = append(result, encoded)
	}
	return result, nil
}
