package verify

import (
	"encoding/json"
	"fmt"
	"slices"
)

type ClosureReport struct {
	RetainedActions    []string `json:"retainedActions,omitempty"`
	EnvironmentActions []string `json:"environmentActions,omitempty"`
	RefinedActions     []string `json:"refinedActions,omitempty"`
	StutteringActions  []string `json:"stutteringActions,omitempty"`
	OmittedActions     []string `json:"omittedActions,omitempty"`
}

type ProjectedTarget struct {
	ModelFamilyVersion string
	Target             VerificationTarget
	Model              Model
	Closure            ClosureReport
	Modules            []string
	Properties         []string
	Interfaces         []ManifestInterface
}

func MarshalClosureReport(report ClosureReport) ([]byte, error) {
	result := report
	result.RetainedActions = sortedClone(report.RetainedActions)
	result.EnvironmentActions = sortedClone(report.EnvironmentActions)
	result.RefinedActions = sortedClone(report.RefinedActions)
	result.StutteringActions = sortedClone(report.StutteringActions)
	result.OmittedActions = sortedClone(report.OmittedActions)
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func Project(family ModelFamily, targetName string) (ProjectedTarget, error) {
	if err := ValidateModelFamily(family); err != nil {
		return ProjectedTarget{}, err
	}
	target, found := targetByName(family.Targets, targetName)
	if !found {
		return ProjectedTarget{}, fmt.Errorf("unknown verification target %q", targetName)
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
			return ProjectedTarget{}, fmt.Errorf("verification target %q references unknown composition %q", target.Name, compositionName)
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
	refinedActions := make(map[string]struct{})
	stutteringActions := make(map[string]struct{})
	includedModules := make(map[string]struct{}, len(moduleNames))
	addNames(actions, target.Actions)
	for _, moduleName := range moduleNames {
		module, found := modules[moduleName]
		if !found {
			return ProjectedTarget{}, fmt.Errorf("verification target %q references unknown module %q", target.Name, moduleName)
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
			return ProjectedTarget{}, fmt.Errorf("verification target %q references unknown refinement map %q", target.Name, refinementMapName)
		}
		for _, refinement := range refinementMap.Actions {
			if refinement.Stutter {
				stutteringActions[refinement.Concrete] = struct{}{}
			} else if refinement.Concrete != refinement.Abstract {
				if _, concreteSelected := actions[refinement.Concrete]; concreteSelected {
					refinedActions[refinement.Abstract] = struct{}{}
				}
			}
		}
	}
	for action := range refinedActions {
		delete(actions, action)
	}
	selectedAbstractions := make(map[string]struct{}, len(target.Abstractions))
	addNames(selectedAbstractions, target.Abstractions)
	selectedOmissions := make(map[string]struct{}, len(target.Omissions))
	for _, omission := range target.Omissions {
		selectedOmissions[omission.Name] = struct{}{}
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
			if _, refined := refinedActions[action]; refined {
				continue
			}
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
	abstractedActions := make(map[string]struct{})
	for _, action := range family.Model.Actions {
		if _, included := actions[action.Name]; included {
			continue
		}
		if _, refined := refinedActions[action.Name]; refined {
			continue
		}
		if actionAffects(action, entities, relations) {
			if actionCoveredByRetainedRefinement(action, family, actions, refinedActions, entities, relations) {
				abstractedActions[action.Name] = struct{}{}
				continue
			}
			if _, explicitlyOmitted := selectedAbstractions[action.Name]; explicitlyOmitted {
				continue
			}
			if _, explicitlyOmitted := selectedOmissions[action.Name]; explicitlyOmitted {
				continue
			}
			return ProjectedTarget{}, fmt.Errorf("verification target %q omits action %q which can affect retained state", target.Name, action.Name)
		}
	}
	result := projectedModel(family.Model, target, entities, relations, actions, properties, environmentActions)
	if err := applyTargetBounds(target, &result); err != nil {
		return ProjectedTarget{}, err
	}
	if err := validateMinimumBounds(target, result); err != nil {
		return ProjectedTarget{}, err
	}
	if err := Validate(result); err != nil {
		return ProjectedTarget{}, fmt.Errorf("project verification target %q: %w", target.Name, err)
	}
	report := ClosureReport{}
	for _, action := range family.Model.Actions {
		if _, retained := actions[action.Name]; retained {
			if _, environment := environmentActions[action.Name]; environment {
				report.EnvironmentActions = append(report.EnvironmentActions, action.Name)
			} else {
				report.RetainedActions = append(report.RetainedActions, action.Name)
			}
		} else if _, refined := refinedActions[action.Name]; refined {
			report.RefinedActions = append(report.RefinedActions, action.Name)
		} else if _, abstracted := abstractedActions[action.Name]; abstracted {
			report.RefinedActions = append(report.RefinedActions, action.Name)
		} else if _, stuttering := stutteringActions[action.Name]; stuttering {
			report.StutteringActions = append(report.StutteringActions, action.Name)
		} else {
			report.OmittedActions = append(report.OmittedActions, action.Name)
		}
	}
	selectedModules := sortedCompact(moduleNames)
	return ProjectedTarget{
		ModelFamilyVersion: family.Version,
		Target:             cloneVerificationTarget(target),
		Model:              result,
		Closure:            report,
		Modules:            selectedModules,
		Properties:         sortedCompact(propertyNames),
		Interfaces:         projectedManifestInterfaces(family, selectedModules),
	}, nil
}

func actionCoveredByRetainedRefinement(
	concrete Action,
	family ModelFamily,
	retainedActions map[string]struct{},
	refinedActions map[string]struct{},
	entities map[string]struct{},
	relations map[string]struct{},
) bool {
	actions := make(map[string]Action, len(family.Model.Actions))
	for _, action := range family.Model.Actions {
		actions[action.Name] = action
	}
	var abstractEffects []Effect
	var abstractBranchEffects []Effect
	for _, refinementMap := range family.RefinementMaps {
		for _, refinement := range refinementMap.Actions {
			if refinement.Concrete != concrete.Name || refinement.Abstract == "" {
				continue
			}
			if _, retained := retainedActions[refinement.Abstract]; !retained {
				if _, replaced := refinedActions[refinement.Abstract]; !replaced {
					continue
				}
			}
			abstract := actions[refinement.Abstract]
			parameterNames := make(map[string]string, len(refinement.Parameters))
			for _, parameter := range refinement.Parameters {
				parameterNames[parameter.Abstract] = parameter.Concrete
			}
			abstractEffects = append(abstractEffects, renameRefinementEffects(abstract.Effects, parameterNames)...)
			for _, branch := range abstract.Branches {
				abstractBranchEffects = append(abstractBranchEffects, renameRefinementEffects(branch.Effects, parameterNames)...)
			}
		}
	}
	for _, effect := range concrete.Effects {
		if effectAffects(effect, entities, relations) && !slices.Contains(abstractEffects, effect) {
			return false
		}
	}
	for _, branch := range concrete.Branches {
		for _, effect := range branch.Effects {
			if effectAffects(effect, entities, relations) && !slices.Contains(abstractBranchEffects, effect) {
				return false
			}
		}
	}
	return true
}

func effectAffects(effect Effect, entities, relations map[string]struct{}) bool {
	_, entityRetained := entities[effect.Entity]
	_, relationRetained := relations[effect.Relation]
	return effect.Entity != "" && entityRetained || effect.Relation != "" && relationRetained
}

func projectedManifestInterfaces(family ModelFamily, moduleNames []string) []ManifestInterface {
	selected := make(map[string]struct{}, len(moduleNames))
	for _, module := range moduleNames {
		selected[module] = struct{}{}
	}
	owners := make(map[string]CapabilityOwner, len(family.Modules))
	for _, module := range family.Modules {
		owners[module.Name] = module.Owner
	}
	var result []ManifestInterface
	for _, declared := range family.Interfaces {
		_, providerSelected := selected[declared.Provider]
		var consumers []ManifestModuleRef
		for _, consumer := range declared.Consumers {
			if _, consumerSelected := selected[consumer]; consumerSelected {
				consumers = append(consumers, ManifestModuleRef{Module: consumer, Owner: owners[consumer]})
			}
		}
		if !providerSelected && len(consumers) == 0 {
			continue
		}
		projected := ManifestInterface{
			Name:       declared.Name,
			Provider:   ManifestModuleRef{Module: declared.Provider, Owner: owners[declared.Provider]},
			Consumers:  consumers,
			Identities: sortedClone(declared.Identities),
		}
		for _, obligation := range declared.Obligations {
			projected.Obligations = append(projected.Obligations, obligation.Name)
		}
		slices.Sort(projected.Obligations)
		slices.SortFunc(projected.Consumers, func(left, right ManifestModuleRef) int {
			return compareString(left.Module, right.Module)
		})
		result = append(result, projected)
	}
	slices.SortFunc(result, func(left, right ManifestInterface) int {
		return compareString(left.Name, right.Name)
	})
	return result
}

func cloneVerificationTarget(target VerificationTarget) VerificationTarget {
	result := target
	result.Owners = slices.Clone(target.Owners)
	result.Modules = slices.Clone(target.Modules)
	result.Actions = slices.Clone(target.Actions)
	result.Compositions = slices.Clone(target.Compositions)
	result.Properties = slices.Clone(target.Properties)
	result.RefinementMaps = slices.Clone(target.RefinementMaps)
	result.Bounds = cloneBounds(target.Bounds)
	result.MinimumBounds = cloneBounds(target.MinimumBounds)
	result.BackendRequirements = slices.Clone(target.BackendRequirements)
	result.FailurePolicy = slices.Clone(target.FailurePolicy)
	result.Abstractions = slices.Clone(target.Abstractions)
	result.Omissions = slices.Clone(target.Omissions)
	return result
}

func sortedCompact(values []string) []string {
	result := sortedClone(values)
	return slices.Compact(result)
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
	abstractions := make(map[string]struct{}, len(target.Abstractions))
	addNames(abstractions, target.Abstractions)
	for _, abstraction := range model.Abstractions {
		if _, included := abstractions[abstraction.Name]; included {
			result.Abstractions = append(result.Abstractions, abstraction)
		}
	}
	for _, refinement := range model.Refinements {
		if _, included := actions[refinement.Action]; included {
			result.Refinements = append(result.Refinements, refinement)
		}
	}
	return result
}
