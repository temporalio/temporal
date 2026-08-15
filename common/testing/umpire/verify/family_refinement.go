package verify

import (
	"fmt"
	"reflect"
	"slices"
)

func compareObligationRef(left, right ObligationRef) int {
	if comparison := compareString(left.Interface, right.Interface); comparison != 0 {
		return comparison
	}
	return compareString(left.Obligation, right.Obligation)
}

func validateCompositionClosures(family ModelFamily, obligations map[string]Obligation) error {
	interfaces := make(map[string]Interface, len(family.Interfaces))
	for _, declared := range family.Interfaces {
		interfaces[declared.Name] = declared
	}
	for _, composition := range family.Compositions {
		modules := make(map[string]struct{}, len(composition.Modules))
		for _, module := range composition.Modules {
			modules[module] = struct{}{}
		}
		closed := make(map[string]struct{}, len(composition.Closes))
		for _, reference := range composition.Closes {
			key := obligationKey(reference.Interface, reference.Obligation)
			if _, duplicate := closed[key]; duplicate {
				return fmt.Errorf("composition %q closes obligation %q more than once", composition.Name, key)
			}
			closed[key] = struct{}{}
			if _, found := obligations[key]; !found {
				return fmt.Errorf("composition %q closes unknown obligation %q", composition.Name, key)
			}
			provider := interfaces[reference.Interface].Provider
			if _, included := modules[provider]; !included {
				return fmt.Errorf("composition %q closes obligation %q without provider module %q", composition.Name, key, provider)
			}
		}
	}
	return nil
}

func compositionClosesCycle(family ModelFamily, cycle []string) bool {
	cycleObligations := make(map[string]struct{}, len(cycle)-1)
	for _, key := range cycle[:len(cycle)-1] {
		cycleObligations[key] = struct{}{}
	}
	selectedCompositions := make(map[string]struct{})
	for _, target := range family.Targets {
		for _, composition := range target.Compositions {
			selectedCompositions[composition] = struct{}{}
		}
	}
	for _, composition := range family.Compositions {
		if _, selected := selectedCompositions[composition.Name]; !selected {
			continue
		}
		closed := make(map[string]struct{}, len(composition.Closes))
		for _, reference := range composition.Closes {
			closed[obligationKey(reference.Interface, reference.Obligation)] = struct{}{}
		}
		complete := true
		for key := range cycleObligations {
			if _, found := closed[key]; !found {
				complete = false
				break
			}
		}
		if complete {
			return true
		}
	}
	return false
}

func validateRefinementMaps(
	refinementMaps []RefinementMap,
	modules []Module,
	interfaces []Interface,
	model Model,
) error {
	actions := make(map[string]Action, len(model.Actions))
	for _, action := range model.Actions {
		actions[action.Name] = action
	}
	entities := make(map[string]struct{}, len(model.Entities))
	for _, entity := range model.Entities {
		entities[entity.Name] = struct{}{}
	}
	modulesByName := make(map[string]Module, len(modules))
	entityOwners := make(map[string]CapabilityOwner, len(entities))
	for _, module := range modules {
		modulesByName[module.Name] = module
		for _, entity := range module.Entities {
			entityOwners[entity] = module.Owner
		}
	}
	interfacesByName := make(map[string]Interface, len(interfaces))
	for _, declared := range interfaces {
		interfacesByName[declared.Name] = declared
	}
	names := make(map[string]struct{}, len(refinementMaps))
	for index, refinementMap := range refinementMaps {
		if refinementMap.Name == "" {
			return fmt.Errorf("refinement map %d has an empty name", index)
		}
		if _, duplicate := names[refinementMap.Name]; duplicate {
			return fmt.Errorf("duplicate refinement map %q", refinementMap.Name)
		}
		names[refinementMap.Name] = struct{}{}
		if refinementMap.Owner == "" {
			return fmt.Errorf("refinement map %q has no owner", refinementMap.Name)
		}
		contractRefinement := refinementMap.Module != "" || refinementMap.Interface != ""
		if contractRefinement && (refinementMap.Module == "" || refinementMap.Interface == "") {
			return fmt.Errorf("refinement map %q requires both module and interface", refinementMap.Name)
		}
		module, moduleFound := modulesByName[refinementMap.Module]
		declaredInterface, interfaceFound := interfacesByName[refinementMap.Interface]
		if contractRefinement {
			if !moduleFound {
				return fmt.Errorf("refinement map %q references unknown module %q", refinementMap.Name, refinementMap.Module)
			}
			if !interfaceFound {
				return fmt.Errorf("refinement map %q references unknown interface %q", refinementMap.Name, refinementMap.Interface)
			}
			if module.Owner != refinementMap.Owner {
				return fmt.Errorf("refinement map %q owner %q does not own module %q", refinementMap.Name, refinementMap.Owner, module.Name)
			}
			if !slices.Contains(declaredInterface.Consumers, module.Name) {
				return fmt.Errorf("refinement map %q module %q is not a consumer of interface %q", refinementMap.Name, module.Name, declaredInterface.Name)
			}
		}
		abstractActions := contractRefinementActions(module, declaredInterface)
		mappedActions := make(map[string]struct{}, len(refinementMap.Actions))
		refinedActions := make(map[string]struct{}, len(refinementMap.Actions))
		for _, refinement := range refinementMap.Actions {
			concreteAction, found := actions[refinement.Concrete]
			if !found {
				return fmt.Errorf("refinement map %q references unknown concrete action %q", refinementMap.Name, refinement.Concrete)
			}
			if _, duplicate := mappedActions[refinement.Concrete]; duplicate {
				return fmt.Errorf("refinement map %q maps action %q more than once", refinementMap.Name, refinement.Concrete)
			}
			mappedActions[refinement.Concrete] = struct{}{}
			if contractRefinement && !slices.Contains(module.Actions, refinement.Concrete) {
				return fmt.Errorf("refinement map %q action %q is not owned by module %q", refinementMap.Name, refinement.Concrete, module.Name)
			}
			if refinement.Stutter == (refinement.Abstract != "") {
				return fmt.Errorf("refinement map %q action %q must select exactly one abstract action or stuttering", refinementMap.Name, refinement.Concrete)
			}
			if refinement.Abstract != "" {
				abstractAction, found := actions[refinement.Abstract]
				if !found {
					return fmt.Errorf("refinement map %q references unknown abstract action %q", refinementMap.Name, refinement.Abstract)
				}
				if contractRefinement && !slices.Contains(abstractActions, refinement.Abstract) {
					return fmt.Errorf("refinement map %q maps to action %q outside imported interface %q", refinementMap.Name, refinement.Abstract, declaredInterface.Name)
				}
				if err := validateActionRefinement(refinementMap.Name, refinement, concreteAction, abstractAction); err != nil {
					return err
				}
				refinedActions[refinement.Abstract] = struct{}{}
			}
		}
		if contractRefinement {
			for _, action := range module.Actions {
				if _, classified := mappedActions[action]; !classified {
					return fmt.Errorf("refinement map %q does not classify concrete action %q", refinementMap.Name, action)
				}
			}
			for _, action := range abstractActions {
				if _, refined := refinedActions[action]; !refined {
					return fmt.Errorf("refinement map %q does not refine imported action %q", refinementMap.Name, action)
				}
			}
		}
		mappedIdentities := make(map[string]struct{}, len(refinementMap.Identities))
		refinedIdentities := make(map[string]struct{}, len(refinementMap.Identities))
		for _, identity := range refinementMap.Identities {
			if _, found := entities[identity.Concrete]; !found {
				return fmt.Errorf("refinement map %q references unknown concrete entity %q", refinementMap.Name, identity.Concrete)
			}
			if _, found := entities[identity.Abstract]; !found {
				return fmt.Errorf("refinement map %q references unknown abstract entity %q", refinementMap.Name, identity.Abstract)
			}
			key := identity.Concrete + "\x00" + identity.Abstract
			if _, duplicate := mappedIdentities[key]; duplicate {
				return fmt.Errorf("refinement map %q maps entity %q to %q more than once", refinementMap.Name, identity.Concrete, identity.Abstract)
			}
			if contractRefinement && entityOwners[identity.Concrete] != refinementMap.Owner {
				return fmt.Errorf("refinement map %q identity %q is not owned by capability %q", refinementMap.Name, identity.Concrete, refinementMap.Owner)
			}
			if contractRefinement && !slices.Contains(declaredInterface.Identities, identity.Abstract) {
				return fmt.Errorf("refinement map %q maps to identity %q outside imported interface %q", refinementMap.Name, identity.Abstract, declaredInterface.Name)
			}
			if _, duplicate := refinedIdentities[identity.Abstract]; duplicate {
				return fmt.Errorf("refinement map %q refines imported identity %q more than once", refinementMap.Name, identity.Abstract)
			}
			mappedIdentities[key] = struct{}{}
			refinedIdentities[identity.Abstract] = struct{}{}
		}
		if contractRefinement {
			for _, identity := range declaredInterface.Identities {
				if _, refined := refinedIdentities[identity]; !refined {
					return fmt.Errorf("refinement map %q does not refine imported identity %q", refinementMap.Name, identity)
				}
			}
		}
	}
	return nil
}

func validateActionRefinement(name string, refinement ActionRefinement, concrete, abstract Action) error {
	concreteParameters := make(map[string]Parameter, len(concrete.Parameters))
	for _, parameter := range concrete.Parameters {
		concreteParameters[parameter.Name] = parameter
	}
	abstractParameters := make(map[string]Parameter, len(abstract.Parameters))
	for _, parameter := range abstract.Parameters {
		abstractParameters[parameter.Name] = parameter
	}
	parameterNames := make(map[string]string, len(refinement.Parameters))
	concreteMappings := make(map[string]struct{}, len(refinement.Parameters))
	for _, parameter := range refinement.Parameters {
		if _, found := abstractParameters[parameter.Abstract]; !found {
			return fmt.Errorf("refinement map %q action %q maps unknown abstract parameter %q", name, concrete.Name, parameter.Abstract)
		}
		if _, found := concreteParameters[parameter.Concrete]; !found {
			return fmt.Errorf("refinement map %q action %q maps unknown concrete parameter %q", name, concrete.Name, parameter.Concrete)
		}
		if _, duplicate := parameterNames[parameter.Abstract]; duplicate {
			return fmt.Errorf("refinement map %q action %q maps abstract parameter %q more than once", name, concrete.Name, parameter.Abstract)
		}
		if _, duplicate := concreteMappings[parameter.Concrete]; duplicate {
			return fmt.Errorf("refinement map %q action %q maps concrete parameter %q more than once", name, concrete.Name, parameter.Concrete)
		}
		parameterNames[parameter.Abstract] = parameter.Concrete
		concreteMappings[parameter.Concrete] = struct{}{}
	}
	for _, parameter := range abstract.Parameters {
		concreteName := parameter.Name
		if mapped, found := parameterNames[parameter.Name]; found {
			concreteName = mapped
		}
		concreteParameter, found := concreteParameters[concreteName]
		if !found || concreteParameter.Type != parameter.Type || concreteParameter.Binding != parameter.Binding {
			return fmt.Errorf("refinement map %q concrete action %q has incompatible parameter %q", name, concrete.Name, parameter.Name)
		}
	}
	abstractGuard := renameRefinementExpr(abstract.Guard, parameterNames)
	if !refinementGuardContains(concrete.Guard, abstractGuard) {
		return fmt.Errorf("refinement map %q concrete action %q does not preserve the guard of abstract action %q", name, concrete.Name, abstract.Name)
	}
	for index, effect := range abstract.Effects {
		if !slices.Contains(concrete.Effects, renameRefinementEffect(effect, parameterNames)) {
			return fmt.Errorf("refinement map %q concrete action %q omits effect %d of abstract action %q", name, concrete.Name, index, abstract.Name)
		}
	}
	for _, abstractBranch := range abstract.Branches {
		found := false
		for _, concreteBranch := range concrete.Branches {
			if concreteBranch.Name == abstractBranch.Name && effectsContain(concreteBranch.Effects, renameRefinementEffects(abstractBranch.Effects, parameterNames)) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("refinement map %q concrete action %q omits branch %q of abstract action %q", name, concrete.Name, abstractBranch.Name, abstract.Name)
		}
	}
	return nil
}

func renameRefinementExpr(expression Expr, parameters map[string]string) Expr {
	result := expression
	result.Args = slices.Clone(expression.Args)
	for index := range result.Args {
		result.Args[index] = renameRefinementExpr(result.Args[index], parameters)
	}
	if mapped, found := parameters[result.Ref]; found {
		result.Ref = mapped
	}
	if mapped, found := parameters[result.Source]; found {
		result.Source = mapped
	}
	if mapped, found := parameters[result.Target]; found {
		result.Target = mapped
	}
	return result
}

func renameRefinementEffect(effect Effect, parameters map[string]string) Effect {
	if mapped, found := parameters[effect.Ref]; found {
		effect.Ref = mapped
	}
	if mapped, found := parameters[effect.Source]; found {
		effect.Source = mapped
	}
	if mapped, found := parameters[effect.Target]; found {
		effect.Target = mapped
	}
	return effect
}

func renameRefinementEffects(effects []Effect, parameters map[string]string) []Effect {
	result := make([]Effect, len(effects))
	for index, effect := range effects {
		result[index] = renameRefinementEffect(effect, parameters)
	}
	return result
}

func refinementGuardContains(concrete, abstract Expr) bool {
	if abstract.Op == "" || abstract.Op == TrueExpr || reflect.DeepEqual(concrete, abstract) {
		return true
	}
	if concrete.Op != AndExpr {
		return false
	}
	for _, argument := range concrete.Args {
		if refinementGuardContains(argument, abstract) {
			return true
		}
	}
	return false
}

func effectsContain(concrete, abstract []Effect) bool {
	for _, effect := range abstract {
		if !slices.Contains(concrete, effect) {
			return false
		}
	}
	return true
}

func contractRefinementActions(module Module, declared Interface) []string {
	var result []string
	for _, imported := range module.Imports {
		if imported.Interface != declared.Name {
			continue
		}
		for _, obligation := range declared.Obligations {
			if obligation.Name == imported.Obligation {
				result = append(result, obligation.Actions...)
				break
			}
		}
	}
	slices.Sort(result)
	return slices.Compact(result)
}
