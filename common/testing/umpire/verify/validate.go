package verify

import (
	"errors"
	"fmt"
)

func Validate(model Model) error {
	if model.Version == "" {
		return errors.New("verification model version is empty")
	}
	entities := make(map[string]EntityType, len(model.Entities))
	for index, entity := range model.Entities {
		if entity.Name == "" {
			return fmt.Errorf("entity %d has an empty name", index)
		}
		if _, duplicate := entities[entity.Name]; duplicate {
			return fmt.Errorf("duplicate entity %q", entity.Name)
		}
		if err := validateEntity(entity); err != nil {
			return err
		}
		entities[entity.Name] = entity
	}
	relations := make(map[string]Relation, len(model.Relations))
	for index, relation := range model.Relations {
		if relation.Name == "" {
			return fmt.Errorf("relation %d has an empty name", index)
		}
		if _, duplicate := relations[relation.Name]; duplicate {
			return fmt.Errorf("duplicate relation %q", relation.Name)
		}
		if _, found := entities[relation.Source]; !found {
			return fmt.Errorf("relation %q references unknown source entity %q", relation.Name, relation.Source)
		}
		if _, found := entities[relation.Target]; !found {
			return fmt.Errorf("relation %q references unknown target entity %q", relation.Name, relation.Target)
		}
		if !validCardinality(relation.SourceCardinality) || !validCardinality(relation.TargetCardinality) {
			return fmt.Errorf("relation %q has invalid cardinality", relation.Name)
		}
		relations[relation.Name] = relation
	}
	actions := make(map[string]struct{}, len(model.Actions))
	for index, action := range model.Actions {
		if action.Name == "" {
			return fmt.Errorf("action %d has an empty name", index)
		}
		if _, duplicate := actions[action.Name]; duplicate {
			return fmt.Errorf("duplicate action %q", action.Name)
		}
		actions[action.Name] = struct{}{}
		if err := validateAction(action, entities, relations); err != nil {
			return err
		}
	}
	properties := make(map[string]struct{}, len(model.Properties))
	for index, property := range model.Properties {
		if property.Name == "" {
			return fmt.Errorf("property %d has an empty name", index)
		}
		if _, duplicate := properties[property.Name]; duplicate {
			return fmt.Errorf("duplicate property %q", property.Name)
		}
		properties[property.Name] = struct{}{}
		if property.Kind != SafetyProperty && property.Kind != QuiescentProperty && property.Kind != ProgressProperty {
			return fmt.Errorf("property %q has invalid kind %q", property.Name, property.Kind)
		}
		if property.Kind == ProgressProperty && len(property.Fairness) == 0 {
			return fmt.Errorf("progress property %q has no fairness assumptions", property.Name)
		}
		if err := validateExpr(property.Expr, entities, relations, map[string]string{}); err != nil {
			return fmt.Errorf("property %q: %w", property.Name, err)
		}
	}
	return validateInventoryAndRefinements(model, actions)
}

func validateInventoryAndRefinements(model Model, actions map[string]struct{}) error {
	inventory := map[string]struct{}{}
	for index, item := range model.Inventory {
		if item.Kind == "" || item.Name == "" {
			return fmt.Errorf("inventory item %d requires a kind and name", index)
		}
		key := item.Kind + "\x00" + item.Name
		if _, duplicate := inventory[key]; duplicate {
			return fmt.Errorf("duplicate inventory item %q %q", item.Kind, item.Name)
		}
		inventory[key] = struct{}{}
	}
	names := map[string]struct{}{}
	actionMappings := map[string]struct{}{}
	regressionMappings := map[string]struct{}{}
	for index, refinement := range model.Refinements {
		if refinement.Name == "" {
			return fmt.Errorf("refinement %d has an empty name", index)
		}
		if _, duplicate := names[refinement.Name]; duplicate {
			return fmt.Errorf("duplicate refinement %q", refinement.Name)
		}
		names[refinement.Name] = struct{}{}
		if !refinement.Stutter {
			if _, found := actions[refinement.Action]; !found {
				return fmt.Errorf("refinement %q references unknown action %q", refinement.Name, refinement.Action)
			}
			if _, duplicate := actionMappings[refinement.Action]; duplicate {
				return fmt.Errorf("action %q has more than one refinement", refinement.Action)
			}
			actionMappings[refinement.Action] = struct{}{}
		}
		if len(refinement.LifecycleActions) == 0 && len(refinement.RegressionActions) == 0 {
			return fmt.Errorf("refinement %q has no source action", refinement.Name)
		}
		for _, regressionAction := range refinement.RegressionActions {
			if _, duplicate := regressionMappings[regressionAction]; duplicate {
				return fmt.Errorf("regression action %q is refined more than once", regressionAction)
			}
			regressionMappings[regressionAction] = struct{}{}
		}
	}
	if len(model.Refinements) != 0 {
		for action := range actions {
			if _, mapped := actionMappings[action]; !mapped {
				return fmt.Errorf("action %q has no refinement mapping", action)
			}
		}
	}
	return nil
}

func validateEntity(entity EntityType) error {
	ids := make(map[string]struct{}, len(entity.IDs))
	for _, id := range entity.IDs {
		if id == "" {
			return fmt.Errorf("entity %q has an empty identity", entity.Name)
		}
		if _, duplicate := ids[id]; duplicate {
			return fmt.Errorf("entity %q has duplicate identity %q", entity.Name, id)
		}
		ids[id] = struct{}{}
	}
	states := make(map[string]struct{}, len(entity.States))
	for _, state := range entity.States {
		if state.Name == "" {
			return fmt.Errorf("entity %q has an empty state", entity.Name)
		}
		if _, duplicate := states[state.Name]; duplicate {
			return fmt.Errorf("entity %q has duplicate state %q", entity.Name, state.Name)
		}
		states[state.Name] = struct{}{}
	}
	if len(entity.States) == 0 && entity.Initial == "" {
		if len(entity.InitiallyExists) != 0 {
			return fmt.Errorf("stateless entity %q cannot initially exist", entity.Name)
		}
		return nil
	}
	if _, found := states[entity.Initial]; !found {
		return fmt.Errorf("entity %q initial state %q is not declared", entity.Name, entity.Initial)
	}
	for _, id := range entity.InitiallyExists {
		if _, found := ids[id]; !found {
			return fmt.Errorf("entity %q initially-existing identity %q is not declared", entity.Name, id)
		}
	}
	return nil
}

func validateAction(action Action, entities map[string]EntityType, relations map[string]Relation) error {
	refs := make(map[string]string, len(action.Parameters))
	bindings := make(map[string]BindingMode, len(action.Parameters))
	for index, parameter := range action.Parameters {
		if parameter.Name == "" {
			return fmt.Errorf("action %q parameter %d has an empty name", action.Name, index)
		}
		if _, duplicate := refs[parameter.Name]; duplicate {
			return fmt.Errorf("action %q has duplicate parameter %q", action.Name, parameter.Name)
		}
		if _, found := entities[parameter.Type]; !found {
			return fmt.Errorf("action %q parameter %q references unknown entity %q", action.Name, parameter.Name, parameter.Type)
		}
		if parameter.Binding != InputBinding && parameter.Binding != FreshBinding && parameter.Binding != ObservedBinding {
			return fmt.Errorf("action %q parameter %q has invalid binding %q", action.Name, parameter.Name, parameter.Binding)
		}
		refs[parameter.Name] = parameter.Type
		bindings[parameter.Name] = parameter.Binding
	}
	if action.Guard.Op != "" {
		if err := validateExpr(action.Guard, entities, relations, refs); err != nil {
			return fmt.Errorf("action %q guard: %w", action.Name, err)
		}
	}
	for index, effect := range action.Effects {
		if err := validateEffect(effect, entities, relations, refs, bindings); err != nil {
			return fmt.Errorf("action %q effect %d %w", action.Name, index, err)
		}
	}
	for branchIndex, branch := range action.Branches {
		if branch.Name == "" {
			return fmt.Errorf("action %q branch %d has an empty name", action.Name, branchIndex)
		}
		for effectIndex, effect := range branch.Effects {
			if err := validateEffect(effect, entities, relations, refs, bindings); err != nil {
				return fmt.Errorf("action %q branch %q effect %d %w", action.Name, branch.Name, effectIndex, err)
			}
		}
	}
	return nil
}

func validateEffect(
	effect Effect,
	entities map[string]EntityType,
	relations map[string]Relation,
	refs map[string]string,
	bindings map[string]BindingMode,
) error {
	switch effect.Kind {
	case CreateEffect, SetStateEffect:
		entity, found := entities[effect.Entity]
		if !found {
			return fmt.Errorf("references unknown entity %q", effect.Entity)
		}
		if refs[effect.Ref] != effect.Entity {
			return fmt.Errorf("references %q as %s, expected %s", effect.Ref, refs[effect.Ref], effect.Entity)
		}
		if effect.Kind == CreateEffect && bindings[effect.Ref] != FreshBinding {
			return fmt.Errorf("creates non-fresh parameter %q", effect.Ref)
		}
		if !entityHasState(entity, effect.State) {
			return fmt.Errorf("references unknown %s state %q", effect.Entity, effect.State)
		}
	case AddRelationEffect, RemoveRelationEffect:
		relation, found := relations[effect.Relation]
		if !found {
			return fmt.Errorf("references unknown relation %q", effect.Relation)
		}
		if refs[effect.Source] != relation.Source {
			return fmt.Errorf("relation %q source %q has type %s, expected %s", effect.Relation, effect.Source, refs[effect.Source], relation.Source)
		}
		if refs[effect.Target] != relation.Target {
			return fmt.Errorf("relation %q target %q has type %s, expected %s", effect.Relation, effect.Target, refs[effect.Target], relation.Target)
		}
	default:
		return fmt.Errorf("has unknown kind %q", effect.Kind)
	}
	return nil
}

func validateExpr(expr Expr, entities map[string]EntityType, relations map[string]Relation, refs map[string]string) error {
	switch expr.Op {
	case TrueExpr, FalseExpr:
		if len(expr.Args) != 0 {
			return fmt.Errorf("%s expression has arguments", expr.Op)
		}
	case NotExpr:
		if len(expr.Args) != 1 {
			return errors.New("not expression requires one argument")
		}
	case AndExpr, OrExpr:
		if len(expr.Args) == 0 {
			return fmt.Errorf("%s expression requires arguments", expr.Op)
		}
	case ImpliesExpr:
		if len(expr.Args) != 2 {
			return errors.New("implies expression requires two arguments")
		}
	case EntityExistsExpr:
		if refs[expr.Ref] != expr.Entity {
			return fmt.Errorf("entity existence reference %q has type %s, expected %s", expr.Ref, refs[expr.Ref], expr.Entity)
		}
	case StateIsExpr:
		entity, found := entities[expr.Entity]
		if !found {
			return fmt.Errorf("state expression references unknown entity %q", expr.Entity)
		}
		if refs[expr.Ref] != expr.Entity {
			return fmt.Errorf("state reference %q has type %s, expected %s", expr.Ref, refs[expr.Ref], expr.Entity)
		}
		if !entityHasState(entity, expr.State) {
			return fmt.Errorf("state expression references unknown %s state %q", expr.Entity, expr.State)
		}
	case RelationHasExpr:
		relation, found := relations[expr.Relation]
		if !found {
			return fmt.Errorf("relation expression references unknown relation %q", expr.Relation)
		}
		if refs[expr.Source] != relation.Source || refs[expr.Target] != relation.Target {
			return fmt.Errorf("relation expression %q has incompatible references", expr.Relation)
		}
	case ForAllExpr, ExistsExpr:
		if expr.Var == "" || expr.Entity == "" || len(expr.Args) != 1 {
			return fmt.Errorf("%s expression requires a variable, entity, and body", expr.Op)
		}
		if _, found := entities[expr.Entity]; !found {
			return fmt.Errorf("%s expression references unknown entity %q", expr.Op, expr.Entity)
		}
		if _, duplicate := refs[expr.Var]; duplicate {
			return fmt.Errorf("%s expression shadows reference %q", expr.Op, expr.Var)
		}
		refs = cloneRefs(refs)
		refs[expr.Var] = expr.Entity
	default:
		return fmt.Errorf("unknown expression operator %q", expr.Op)
	}
	for _, argument := range expr.Args {
		if err := validateExpr(argument, entities, relations, refs); err != nil {
			return err
		}
	}
	return nil
}

func cloneRefs(refs map[string]string) map[string]string {
	result := make(map[string]string, len(refs)+1)
	for name, entity := range refs {
		result[name] = entity
	}
	return result
}

func entityHasState(entity EntityType, state string) bool {
	for _, declared := range entity.States {
		if declared.Name == state {
			return true
		}
	}
	return false
}

func validCardinality(cardinality Cardinality) bool {
	return cardinality == One || cardinality == Many
}
