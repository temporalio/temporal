package protocol

import (
	"errors"
	"fmt"
	"reflect"
	"slices"

	"go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/tests/umpire2/model"
	regressactivity "go.temporal.io/server/tests/umpire2/regress/activity"
	regressnexus "go.temporal.io/server/tests/umpire2/regress/nexus"
)

type VerificationOptions struct {
	DefaultBound int
	Bounds       map[umpire.EntityType]int
}

func (p *Protocol) VerificationModel(options VerificationOptions) (verify.Model, error) {
	if p == nil {
		return verify.Model{}, errors.New("protocol verification: protocol is nil")
	}
	if options.DefaultBound <= 0 {
		return verify.Model{}, errors.New("protocol verification: default bound must be positive")
	}
	result := verify.Model{Version: "umpire2/verification-v1"}
	lifecycles := make(map[umpire.EntityType]*umpire.Lifecycle, len(p.entityOrder))
	for _, entityType := range p.entityOrder {
		bound := options.DefaultBound
		if configured, found := options.Bounds[entityType]; found {
			if configured <= 0 {
				return verify.Model{}, fmt.Errorf("protocol verification: bound for %q must be positive", entityType)
			}
			bound = configured
		}
		entity := verify.EntityType{
			Name:   string(entityType),
			IDs:    verificationIDs(entityType, bound),
			Source: verify.Provenance{Path: "tests/umpire2/model"},
		}
		if lifecycle, found := p.Lifecycle(entityType); found {
			lifecycles[entityType] = lifecycle
			entity.Initial = lifecycle.Initial()
			for _, stateName := range lifecycle.States() {
				state := verify.State{
					Name:         stateName,
					Terminal:     lifecycle.Terminal(stateName),
					MustProgress: lifecycle.StateMustProgress(stateName),
				}
				if disposition := lifecycle.Disposition(stateName); disposition != umpire.Unset {
					state.Disposition = disposition.String()
				}
				entity.States = append(entity.States, state)
				if state.MustProgress {
					result.Properties = append(result.Properties, quiescentProgressProperty(entity.Name, stateName))
				}
			}
		}
		result.Entities = append(result.Entities, entity)
	}
	for _, relation := range p.relations {
		result.Relations = append(result.Relations, verify.Relation{
			Name:              string(relation.Type),
			Source:            string(relation.Source),
			Target:            string(relation.Target),
			SourceCardinality: verificationCardinality(relation.SourceCardinality),
			TargetCardinality: verificationCardinality(relation.TargetCardinality),
			SourceLocation:    verify.Provenance{Path: "tests/umpire2/protocol/default_relations.go"},
		})
	}
	result.Properties = append(result.Properties, NexusActivityLinkConsistencyProperties()...)
	result.Properties = append(result.Properties, NexusActivityStrengtheningProperties()...)
	covered := make(map[ActionKey]struct{}, len(p.actionOrder))
	for _, entry := range p.actionOrder {
		covered[entry.Key] = struct{}{}
		action, err := p.verificationAction(entry, lifecycles)
		if err != nil {
			return verify.Model{}, err
		}
		result.Actions = append(result.Actions, action)
		if entry.Action == nil {
			result.Abstractions = append(result.Abstractions, verify.Abstraction{
				Name:   action.Name,
				Reason: entry.GapReason,
				Source: verify.Provenance{Path: "tests/umpire2/protocol/default.go"},
			})
		}
	}
	for entityType, lifecycle := range lifecycles {
		for _, edge := range lifecycle.Edges() {
			if hasVerificationEntry(covered, entityType, edge.From, edge.Event) {
				continue
			}
			key := ActionKey{Entity: entityType, From: edge.From, Event: edge.Event, Hosting: lifecycle.EdgeHosting(edge.From, edge.Event)}
			action, err := p.verificationAction(ActionCatalogEntry{Key: key, GapReason: "no executable action is declared"}, lifecycles)
			if err != nil {
				return verify.Model{}, err
			}
			result.Actions = append(result.Actions, action)
			result.Abstractions = append(result.Abstractions, verify.Abstraction{
				Name:   action.Name,
				Reason: "no executable action is declared",
				Source: verify.Provenance{Path: "tests/umpire2/model"},
			})
		}
	}
	if err := p.addRegressionVerification(&result, lifecycles); err != nil {
		return verify.Model{}, err
	}
	p.addVerificationInventory(&result)
	addLifecycleRefinements(&result)
	if err := verify.Validate(result); err != nil {
		return verify.Model{}, fmt.Errorf("protocol verification: %w", err)
	}
	return result, nil
}

func (p *Protocol) addRegressionVerification(result *verify.Model, lifecycles map[umpire.EntityType]*umpire.Lifecycle) error {
	if p.regression == nil {
		return nil
	}
	for _, action := range p.regression.Snapshot().Actions {
		if action.Schema.Name != "nexus.start_activity" {
			continue
		}
		verificationAction, err := lowerRegressionStartActivity(action, lifecycles)
		if err != nil {
			return err
		}
		result.Actions = append(result.Actions, verificationAction)
		result.Refinements = append(result.Refinements, verify.Refinement{
			Name:              "regression.nexus.start_activity",
			Action:            verificationAction.Name,
			LifecycleActions:  []string{"NexusOperation.scheduled.succeed.Embedded"},
			RegressionActions: []string{action.Schema.Name},
			Source:            verificationAction.Source,
		})
		return nil
	}
	return fmt.Errorf("protocol verification: selected regression action %q is missing", "nexus.start_activity")
}

func lowerRegressionStartActivity(
	action coreregress.ActionCapability,
	lifecycles map[umpire.EntityType]*umpire.Lifecycle,
) (verify.Action, error) {
	expectedVariables := []coreregress.Variable{
		{Name: "operation", Type: regressnexus.OperationType},
		{Name: "activity", Type: regressactivity.ActivityType, Binding: coreregress.FreshBinding},
	}
	expectedPreconditions := []coreregress.AtomTemplate{
		nexusState("operation", regressnexus.Scheduled),
	}
	expectedEffects := []coreregress.AtomTemplate{
		nexusState("operation", regressnexus.Completed),
		activityState("activity", regressactivity.Completed),
		coreregress.Atom("nexus.linked_to_activity", coreregress.TemplateVar("operation"), coreregress.TemplateVar("activity")),
		coreregress.Atom("activity.linked_to_nexus_operation", coreregress.TemplateVar("activity"), coreregress.TemplateVar("operation")),
	}
	if action.Mode != coreregress.ReactiveAction ||
		!reflect.DeepEqual(action.Variables, expectedVariables) ||
		!reflect.DeepEqual(action.Preconditions, expectedPreconditions) ||
		!reflect.DeepEqual(action.Effects, expectedEffects) {
		return verify.Action{}, fmt.Errorf("protocol verification: regression action %q no longer matches its verification refinement", action.Schema.Name)
	}
	nexusLifecycle := lifecycles[model.NexusOperationType]
	activityLifecycle := lifecycles[model.ActivityType]
	if nexusLifecycle == nil || activityLifecycle == nil {
		return verify.Action{}, fmt.Errorf("protocol verification: regression action %q requires Nexus and Activity lifecycles", action.Schema.Name)
	}
	return verify.Action{
		Name: "regression." + action.Schema.Name,
		Parameters: []verify.Parameter{
			{Name: "activity", Type: string(model.ActivityType), Binding: verify.FreshBinding},
			{Name: "operation", Type: string(model.NexusOperationType), Binding: verify.InputBinding},
		},
		Guard: verify.StateIs(string(model.NexusOperationType), "operation", model.NexusScheduled),
		Effects: []verify.Effect{
			{Kind: verify.CreateEffect, Entity: string(model.ActivityType), Ref: "activity", State: model.ActivityCompleted},
			{Kind: verify.SetStateEffect, Entity: string(model.NexusOperationType), Ref: "operation", State: model.NexusSucceeded},
			{Kind: verify.AddRelationEffect, Relation: string(NexusActivityRelation), Source: "operation", Target: "activity"},
			{Kind: verify.AddRelationEffect, Relation: string(ActivityNexusRelation), Source: "activity", Target: "operation"},
		},
		Hosting:      umpire.Embedded.String(),
		Capabilities: slices.Clone(action.Requires),
		Source:       verify.Provenance{Path: "tests/umpire2/protocol/regress_domain.go", Symbol: action.Schema.Name},
	}, nil
}

func (p *Protocol) addVerificationInventory(result *verify.Model) {
	for _, entity := range result.Entities {
		result.Inventory = append(result.Inventory, verify.InventoryItem{Kind: "entity", Name: entity.Name, Included: true, Source: entity.Source})
		for _, state := range entity.States {
			result.Inventory = append(result.Inventory, verify.InventoryItem{Kind: "lifecycle-state", Name: entity.Name + "." + state.Name, Included: true, Source: entity.Source})
		}
	}
	for _, relation := range result.Relations {
		result.Inventory = append(result.Inventory, verify.InventoryItem{Kind: "relation", Name: relation.Name, Included: true, Source: relation.SourceLocation})
	}
	for _, action := range result.Actions {
		result.Inventory = append(result.Inventory, verify.InventoryItem{Kind: "verification-action", Name: action.Name, Included: true, Source: action.Source})
	}
	for _, property := range result.Properties {
		result.Inventory = append(result.Inventory, verify.InventoryItem{Kind: "property", Name: property.Name, Included: true, Source: property.Source})
	}
	for _, fact := range p.facts {
		result.Inventory = append(result.Inventory, verify.InventoryItem{
			Kind: "fact", Name: fact.Name(), Reason: "facts are observations outside the atomic verification kernel",
			Source: verify.Provenance{Path: "tests/umpire2/model"},
		})
	}
	for _, rule := range []string{"SpeculativeTaskCreation", "NexusOperationClosure", "NexusActivityLinkConsistency", "NexusOperationTimeoutSemantics", "WorkflowTaskStarvation", "EntityProgress"} {
		included := rule == "NexusActivityLinkConsistency" || rule == "EntityProgress"
		reason := ""
		if !included {
			reason = "imperative runtime rule has not been migrated to the shared property algebra"
		}
		result.Inventory = append(result.Inventory, verify.InventoryItem{
			Kind: "rule", Name: rule, Included: included, Reason: reason,
			Source: verify.Provenance{Path: "tests/umpire2/rule"},
		})
	}
	if p.regression != nil {
		catalog := p.regression.Snapshot()
		actionNameCounts := make(map[string]int, len(catalog.Actions))
		for _, action := range catalog.Actions {
			actionNameCounts[action.Schema.Name]++
		}
		for _, predicate := range catalog.Predicates {
			result.Inventory = append(result.Inventory, verify.InventoryItem{
				Kind: "regression-predicate", Name: predicate.Schema.Name,
				Reason: "predicate is inventoried; lifecycle and relation projections are selected explicitly",
				Source: verify.Provenance{Path: "tests/umpire2/protocol/regress_domain.go", Symbol: predicate.Schema.Name},
			})
		}
		for _, action := range catalog.Actions {
			included := action.Schema.Name == "nexus.start_activity"
			inventoryName := action.Schema.Name
			if actionNameCounts[action.Schema.Name] > 1 {
				inventoryName += "[" + action.Realization + "]"
			}
			reason := ""
			if !included {
				reason = "outside the initial bounded Nexus lifecycle/relation slice"
				result.Abstractions = append(result.Abstractions, verify.Abstraction{
					Name: "regression." + inventoryName, Reason: reason,
					Source: verify.Provenance{Path: "tests/umpire2/protocol/regress_domain.go", Symbol: action.Schema.Name},
				})
			}
			result.Inventory = append(result.Inventory, verify.InventoryItem{
				Kind: "regression-action", Name: inventoryName, Included: included, Reason: reason,
				Source: verify.Provenance{Path: "tests/umpire2/protocol/regress_domain.go", Symbol: action.Schema.Name},
			})
		}
		for _, resource := range catalog.Resources {
			result.Inventory = append(result.Inventory, verify.InventoryItem{
				Kind: "regression-resource", Name: resource.Name,
				Reason: "execution resource is outside the atomic verification kernel",
				Source: verify.Provenance{Path: "tests/umpire2/protocol/regress_domain.go", Symbol: resource.Realization},
			})
		}
		for _, policy := range catalog.Policies {
			result.Inventory = append(result.Inventory, verify.InventoryItem{
				Kind: "regression-policy", Name: policy.Schema.Name,
				Reason: "fault policy is outside the initial atomic verification kernel",
				Source: verify.Provenance{Path: "tests/umpire2/protocol/regress_domain.go", Symbol: policy.Realization},
			})
		}
	}
	if footprints, err := DefaultCausalFootprints(); err == nil {
		for _, footprint := range footprints {
			result.Inventory = append(result.Inventory, verify.InventoryItem{
				Kind: "causal-footprint", Name: footprint.Name,
				Reason: "retained as a live execution refinement rather than transition behavior",
				Source: verify.Provenance{Path: "tests/umpire2/protocol/causal_footprints.go", Symbol: footprint.Footprint.Action},
			})
		}
	}
}

func addLifecycleRefinements(result *verify.Model) {
	mapped := make(map[string]struct{}, len(result.Refinements))
	for _, refinement := range result.Refinements {
		mapped[refinement.Action] = struct{}{}
	}
	for _, action := range result.Actions {
		if _, exists := mapped[action.Name]; exists {
			continue
		}
		result.Refinements = append(result.Refinements, verify.Refinement{
			Name:             "lifecycle." + action.Name,
			Action:           action.Name,
			LifecycleActions: []string{action.Name},
			Source:           action.Source,
		})
	}
}

func (p *Protocol) verificationAction(
	entry ActionCatalogEntry,
	lifecycles map[umpire.EntityType]*umpire.Lifecycle,
) (verify.Action, error) {
	key := entry.Key
	lifecycle := lifecycles[key.Entity]
	to, found := lifecycleEdgeDestination(lifecycle, key.From, key.Event)
	if !found {
		return verify.Action{}, fmt.Errorf("protocol verification: unknown edge %v", key)
	}
	name := fmt.Sprintf("%s.%s.%s.%s", key.Entity, key.From, key.Event, key.Hosting)
	if entry.Action == nil {
		binding := verify.InputBinding
		effectKind := verify.SetStateEffect
		if key.From == lifecycle.Initial() {
			binding = verify.FreshBinding
			effectKind = verify.CreateEffect
		}
		return verify.Action{
			Name:       name,
			Parameters: []verify.Parameter{{Name: "entity", Type: string(key.Entity), Binding: binding}},
			Guard:      verify.StateIs(string(key.Entity), "entity", key.From),
			Effects: []verify.Effect{{
				Kind:   effectKind,
				Entity: string(key.Entity),
				Ref:    "entity",
				State:  to,
			}},
			Hosting:      key.Hosting.String(),
			Capabilities: verificationCapabilities(lifecycle.EdgeRequires(key.From, key.Event)),
			Unrealized:   true,
			Source:       verify.Provenance{Path: "tests/umpire2/protocol/default.go", Symbol: entry.GapReason},
		}, nil
	}
	parameters := map[string]verify.Parameter{}
	var guards []verify.Expr
	mainRef := ""
	for _, effect := range entry.Action.Effects {
		binding := verify.InputBinding
		if effect.Ref.Fresh {
			binding = verify.FreshBinding
		}
		if effect.Ref.LinkedFrom != "" {
			binding = verify.ObservedBinding
		}
		if effect.Ref.Type == key.Entity && effect.Event == key.Event {
			mainRef = effect.Ref.Var
			if key.From != lifecycle.Initial() {
				binding = verify.InputBinding
			}
		}
		parameters[effect.Ref.Var] = verify.Parameter{Name: effect.Ref.Var, Type: string(effect.Ref.Type), Binding: binding}
	}
	for _, requirement := range entry.Action.Requires {
		parameters[requirement.Ref.Var] = verify.Parameter{Name: requirement.Ref.Var, Type: string(requirement.Ref.Type), Binding: verify.InputBinding}
		guards = appendUniqueExpr(guards, verify.StateIs(string(requirement.Ref.Type), requirement.Ref.Var, requirement.State))
	}
	if mainRef == "" {
		return verify.Action{}, fmt.Errorf("protocol verification: action %q has no ref for %v", entry.Action.Name, key)
	}
	guards = appendUniqueExpr([]verify.Expr{verify.StateIs(string(key.Entity), mainRef, key.From)}, guards...)
	parameterList := make([]verify.Parameter, 0, len(parameters))
	for _, parameter := range parameters {
		parameterList = append(parameterList, parameter)
	}
	slices.SortFunc(parameterList, func(left, right verify.Parameter) int {
		if left.Name < right.Name {
			return -1
		}
		if left.Name > right.Name {
			return 1
		}
		return 0
	})
	effects := make([]verify.Effect, 0, len(entry.Action.Effects))
	for _, effect := range entry.Action.Effects {
		targetLifecycle := lifecycles[effect.Ref.Type]
		if targetLifecycle == nil {
			return verify.Action{}, fmt.Errorf("protocol verification: action %q effect %q targets non-lifecycled entity %q", entry.Action.Name, effect.Event, effect.Ref.Type)
		}
		destination, found := targetLifecycle.Destination(effect.Event)
		if !found {
			return verify.Action{}, fmt.Errorf("protocol verification: action %q effect references unknown %s event %q", entry.Action.Name, effect.Ref.Type, effect.Event)
		}
		kind := verify.SetStateEffect
		if parameters[effect.Ref.Var].Binding == verify.FreshBinding {
			kind = verify.CreateEffect
		}
		effects = append(effects, verify.Effect{
			Kind:   kind,
			Entity: string(effect.Ref.Type),
			Ref:    effect.Ref.Var,
			State:  destination,
		})
	}
	return verify.Action{
		Name:         name,
		Parameters:   parameterList,
		Guard:        verify.And(guards...),
		Effects:      effects,
		Hosting:      key.Hosting.String(),
		Capabilities: verificationCapabilities(lifecycle.EdgeRequires(key.From, key.Event)),
		Source:       verify.Provenance{Path: "tests/umpire2/protocol/default.go", Symbol: entry.Action.Name},
	}, nil
}

func quiescentProgressProperty(entity, state string) verify.Property {
	return verify.Property{
		Name: entity + "." + state + ".quiescent-progress",
		Kind: verify.QuiescentProperty,
		Expr: verify.Expr{
			Op:     verify.ForAllExpr,
			Entity: entity,
			Var:    "entity",
			Args: []verify.Expr{
				verify.Not(verify.StateIs(entity, "entity", state)),
			},
		},
		Source: verify.Provenance{Path: "common/testing/umpire/lifecycle.go", Symbol: "MustProgress"},
	}
}

func verificationIDs(entityType umpire.EntityType, bound int) []string {
	result := make([]string, bound)
	for index := range bound {
		result[index] = fmt.Sprintf("%s#%d", entityType, index)
	}
	return result
}

func verificationCardinality(cardinality umpire.RelationCardinality) verify.Cardinality {
	if cardinality == umpire.RelationOne {
		return verify.One
	}
	return verify.Many
}

func verificationCapabilities(capabilities []umpire.Capability) []string {
	result := make([]string, len(capabilities))
	for index, capability := range capabilities {
		result[index] = capability.String()
	}
	slices.Sort(result)
	return result
}

func hasVerificationEntry(covered map[ActionKey]struct{}, entityType umpire.EntityType, from, event string) bool {
	for key := range covered {
		if key.Entity == entityType && key.From == from && key.Event == event {
			return true
		}
	}
	return false
}

func appendUniqueExpr(expressions []verify.Expr, candidates ...verify.Expr) []verify.Expr {
	for _, candidate := range candidates {
		if slices.ContainsFunc(expressions, func(existing verify.Expr) bool {
			return reflect.DeepEqual(existing, candidate)
		}) {
			continue
		}
		expressions = append(expressions, candidate)
	}
	return expressions
}
