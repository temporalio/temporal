package verify

import (
	"encoding/json"
	"fmt"
	"slices"
)

type ClosureReport struct {
	RetainedActions    []string `json:"retainedActions,omitempty"`
	EnvironmentActions []string `json:"environmentActions,omitempty"`
	StutteringActions  []string `json:"stutteringActions,omitempty"`
	OmittedActions     []string `json:"omittedActions,omitempty"`
}

func MarshalClosureReport(report ClosureReport) ([]byte, error) {
	result := report
	result.RetainedActions = sortedClone(report.RetainedActions)
	result.EnvironmentActions = sortedClone(report.EnvironmentActions)
	result.StutteringActions = sortedClone(report.StutteringActions)
	result.OmittedActions = sortedClone(report.OmittedActions)
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func Project(family ModelFamily, targetName string) (Model, ClosureReport, error) {
	if err := ValidateModelFamily(family); err != nil {
		return Model{}, ClosureReport{}, err
	}
	target, found := targetByName(family.Targets, targetName)
	if !found {
		return Model{}, ClosureReport{}, fmt.Errorf("unknown verification target %q", targetName)
	}
	modules := make(map[string]Module, len(family.Modules))
	for _, module := range family.Modules {
		modules[module.Name] = module
	}
	moduleNames := slices.Clone(target.Modules)
	propertyNames := slices.Clone(target.Properties)
	refinementMapNames := slices.Clone(target.RefinementMaps)
	compositions := make(map[string]Composition, len(family.Compositions))
	for _, composition := range family.Compositions {
		compositions[composition.Name] = composition
	}
	for _, compositionName := range target.Compositions {
		composition, found := compositions[compositionName]
		if !found {
			return Model{}, ClosureReport{}, fmt.Errorf("verification target %q references unknown composition %q", target.Name, compositionName)
		}
		moduleNames = append(moduleNames, composition.Modules...)
		propertyNames = append(propertyNames, composition.Properties...)
		refinementMapNames = append(refinementMapNames, composition.RefinementMaps...)
	}
	entities := make(map[string]struct{})
	relations := make(map[string]struct{})
	actions := make(map[string]struct{})
	properties := make(map[string]struct{})
	environmentActions := make(map[string]struct{})
	stutteringActions := make(map[string]struct{})
	includedModules := make(map[string]struct{}, len(moduleNames))
	for _, moduleName := range moduleNames {
		module, found := modules[moduleName]
		if !found {
			return Model{}, ClosureReport{}, fmt.Errorf("verification target %q references unknown module %q", target.Name, moduleName)
		}
		includedModules[moduleName] = struct{}{}
		addNames(entities, module.Entities)
		addNames(relations, module.Relations)
		addNames(actions, module.Actions)
		addNames(properties, module.Properties)
	}
	refinementMaps := make(map[string]RefinementMap, len(family.RefinementMaps))
	for _, refinementMap := range family.RefinementMaps {
		refinementMaps[refinementMap.Name] = refinementMap
	}
	for _, refinementMapName := range refinementMapNames {
		refinementMap, found := refinementMaps[refinementMapName]
		if !found {
			return Model{}, ClosureReport{}, fmt.Errorf("verification target %q references unknown refinement map %q", target.Name, refinementMapName)
		}
		for _, refinement := range refinementMap.Actions {
			if refinement.Stutter {
				stutteringActions[refinement.Concrete] = struct{}{}
			}
		}
	}
	addNames(properties, propertyNames)
	closeModelDependencies(family.Model, entities, relations, actions, properties)
	var obligationQueue []ObligationRef
	for _, module := range family.Modules {
		if _, included := includedModules[module.Name]; included {
			obligationQueue = append(obligationQueue, module.Imports...)
		}
	}
	slices.SortFunc(obligationQueue, compareObligationRef)
	visitedObligations := make(map[string]struct{}, len(obligationQueue))
	for len(obligationQueue) != 0 {
		imported := obligationQueue[0]
		obligationQueue = obligationQueue[1:]
		key := obligationKey(imported.Interface, imported.Obligation)
		if _, visited := visitedObligations[key]; visited {
			continue
		}
		visitedObligations[key] = struct{}{}
		declared, obligation := interfaceObligation(family.Interfaces, imported)
		_, providerIncluded := includedModules[declared.Provider]
		for _, action := range obligation.Actions {
			actions[action] = struct{}{}
			if !providerIncluded {
				environmentActions[action] = struct{}{}
			}
		}
		addNames(properties, obligation.Properties)
		obligationQueue = append(obligationQueue, obligation.Assumptions...)
		slices.SortFunc(obligationQueue, compareObligationRef)
	}
	closeModelDependencies(family.Model, entities, relations, actions, properties)
	for _, action := range family.Model.Actions {
		if _, included := actions[action.Name]; included {
			continue
		}
		if actionAffects(action, entities, relations) {
			return Model{}, ClosureReport{}, fmt.Errorf("verification target %q omits action %q which can affect retained state", target.Name, action.Name)
		}
	}
	result := projectedModel(family.Model, target, entities, relations, actions, properties, environmentActions)
	if err := applyTargetBounds(target, &result); err != nil {
		return Model{}, ClosureReport{}, err
	}
	if err := validateMinimumBounds(target, result); err != nil {
		return Model{}, ClosureReport{}, err
	}
	if err := Validate(result); err != nil {
		return Model{}, ClosureReport{}, fmt.Errorf("project verification target %q: %w", target.Name, err)
	}
	report := ClosureReport{}
	for _, action := range family.Model.Actions {
		if _, retained := actions[action.Name]; retained {
			if _, environment := environmentActions[action.Name]; environment {
				report.EnvironmentActions = append(report.EnvironmentActions, action.Name)
			} else {
				report.RetainedActions = append(report.RetainedActions, action.Name)
			}
		} else if _, stuttering := stutteringActions[action.Name]; stuttering {
			report.StutteringActions = append(report.StutteringActions, action.Name)
		} else {
			report.OmittedActions = append(report.OmittedActions, action.Name)
		}
	}
	return result, report, nil
}

func applyTargetBounds(target VerificationTarget, model *Model) error {
	remaining := make(map[string]int, len(target.Bounds))
	for name, bound := range target.Bounds {
		remaining[name] = bound
	}
	for index := range model.Entities {
		entity := &model.Entities[index]
		bound, selected := remaining[entity.Name]
		if !selected {
			continue
		}
		delete(remaining, entity.Name)
		if bound > len(entity.IDs) {
			return fmt.Errorf("verification target %q requires %d identities for entity %q, source model provides %d", target.Name, bound, entity.Name, len(entity.IDs))
		}
		entity.IDs = slices.Clone(entity.IDs[:bound])
		selectedIDs := make(map[string]struct{}, len(entity.IDs))
		for _, id := range entity.IDs {
			selectedIDs[id] = struct{}{}
		}
		for _, id := range entity.InitiallyExists {
			if _, retained := selectedIDs[id]; !retained {
				return fmt.Errorf("verification target %q bound for entity %q excludes initially existing identity %q", target.Name, entity.Name, id)
			}
		}
	}
	if len(remaining) != 0 {
		names := make([]string, 0, len(remaining))
		for name := range remaining {
			names = append(names, name)
		}
		slices.Sort(names)
		return fmt.Errorf("verification target %q sets a bound for excluded entity %q", target.Name, names[0])
	}
	return nil
}

func validateMinimumBounds(target VerificationTarget, model Model) error {
	names := make([]string, 0, len(target.MinimumBounds))
	for name := range target.MinimumBounds {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		minimum := target.MinimumBounds[name]
		found := false
		for _, entity := range model.Entities {
			if entity.Name != name {
				continue
			}
			found = true
			if len(entity.IDs) < minimum {
				return fmt.Errorf("verification target %q requires at least %d identities for entity %q, got %d", target.Name, minimum, name, len(entity.IDs))
			}
			break
		}
		if !found {
			return fmt.Errorf("verification target %q sets a minimum bound for excluded entity %q", target.Name, name)
		}
	}
	return nil
}

func interfaceObligation(interfaces []Interface, reference ObligationRef) (Interface, Obligation) {
	for _, declared := range interfaces {
		if declared.Name != reference.Interface {
			continue
		}
		for _, obligation := range declared.Obligations {
			if obligation.Name == reference.Obligation {
				return declared, obligation
			}
		}
	}
	return Interface{}, Obligation{}
}

func actionAffects(action Action, entities, relations map[string]struct{}) bool {
	if effectsAffect(action.Effects, entities, relations) {
		return true
	}
	for _, branch := range action.Branches {
		if effectsAffect(branch.Effects, entities, relations) {
			return true
		}
	}
	return false
}

func effectsAffect(effects []Effect, entities, relations map[string]struct{}) bool {
	for _, effect := range effects {
		if _, retained := entities[effect.Entity]; effect.Entity != "" && retained {
			return true
		}
		if _, retained := relations[effect.Relation]; effect.Relation != "" && retained {
			return true
		}
	}
	return false
}

func targetByName(targets []VerificationTarget, name string) (VerificationTarget, bool) {
	for _, target := range targets {
		if target.Name == name {
			return target, true
		}
	}
	return VerificationTarget{}, false
}

func addNames(destination map[string]struct{}, names []string) {
	for _, name := range names {
		destination[name] = struct{}{}
	}
}

func closeModelDependencies(
	model Model,
	entities map[string]struct{},
	relations map[string]struct{},
	actions map[string]struct{},
	properties map[string]struct{},
) {
	for _, relation := range model.Relations {
		if _, included := relations[relation.Name]; included {
			entities[relation.Source] = struct{}{}
			entities[relation.Target] = struct{}{}
		}
	}
	for _, action := range model.Actions {
		if _, included := actions[action.Name]; !included {
			continue
		}
		for _, parameter := range action.Parameters {
			entities[parameter.Type] = struct{}{}
		}
		collectExprDependencies(action.Guard, entities, relations)
		collectEffectDependencies(action.Effects, entities, relations)
		for _, branch := range action.Branches {
			collectEffectDependencies(branch.Effects, entities, relations)
		}
	}
	for _, property := range model.Properties {
		if _, included := properties[property.Name]; included {
			collectExprDependencies(property.Expr, entities, relations)
		}
	}
	for _, relation := range model.Relations {
		if _, included := relations[relation.Name]; included {
			entities[relation.Source] = struct{}{}
			entities[relation.Target] = struct{}{}
		}
	}
}

func collectEffectDependencies(effects []Effect, entities, relations map[string]struct{}) {
	for _, effect := range effects {
		if effect.Entity != "" {
			entities[effect.Entity] = struct{}{}
		}
		if effect.Relation != "" {
			relations[effect.Relation] = struct{}{}
		}
	}
}

func collectExprDependencies(expression Expr, entities, relations map[string]struct{}) {
	if expression.Entity != "" {
		entities[expression.Entity] = struct{}{}
	}
	if expression.Relation != "" {
		relations[expression.Relation] = struct{}{}
	}
	for _, argument := range expression.Args {
		collectExprDependencies(argument, entities, relations)
	}
}

func projectedModel(
	model Model,
	target VerificationTarget,
	entities map[string]struct{},
	relations map[string]struct{},
	actions map[string]struct{},
	properties map[string]struct{},
	environmentActions map[string]struct{},
) Model {
	result := Model{Version: model.Version}
	if target.IncludeInventory {
		result.Inventory = slices.Clone(model.Inventory)
	}
	for _, entity := range model.Entities {
		if _, included := entities[entity.Name]; included {
			result.Entities = append(result.Entities, entity)
		}
	}
	for _, relation := range model.Relations {
		if _, included := relations[relation.Name]; included {
			result.Relations = append(result.Relations, relation)
		}
	}
	for _, action := range model.Actions {
		if _, included := actions[action.Name]; included {
			if _, environment := environmentActions[action.Name]; environment {
				action.Unrealized = true
			}
			result.Actions = append(result.Actions, action)
		}
	}
	for _, property := range model.Properties {
		if _, included := properties[property.Name]; included {
			result.Properties = append(result.Properties, property)
		}
	}
	if len(actions) == len(model.Actions) {
		result.Abstractions = slices.Clone(model.Abstractions)
	} else {
		abstractions := make(map[string]struct{}, len(actions)+len(target.Abstractions))
		for action := range actions {
			abstractions[action] = struct{}{}
		}
		addNames(abstractions, target.Abstractions)
		for _, abstraction := range model.Abstractions {
			if _, included := abstractions[abstraction.Name]; included {
				result.Abstractions = append(result.Abstractions, abstraction)
			}
		}
	}
	for _, refinement := range model.Refinements {
		if _, included := actions[refinement.Action]; included {
			result.Refinements = append(result.Refinements, refinement)
		}
	}
	return result
}
