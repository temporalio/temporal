package verify

import (
	"fmt"
	"slices"
)

func validateTargets(family ModelFamily, modules map[string]Module) error {
	compositions := make(map[string]Composition, len(family.Compositions))
	for _, composition := range family.Compositions {
		compositions[composition.Name] = composition
	}
	refinementMaps := make(map[string]RefinementMap, len(family.RefinementMaps))
	for _, refinementMap := range family.RefinementMaps {
		refinementMaps[refinementMap.Name] = refinementMap
	}
	properties := make(map[string]struct{}, len(family.Model.Properties))
	for _, property := range family.Model.Properties {
		properties[property.Name] = struct{}{}
	}
	abstractions := make(map[string]struct{}, len(family.Model.Abstractions))
	for _, abstraction := range family.Model.Abstractions {
		abstractions[abstraction.Name] = struct{}{}
	}
	entities := make(map[string]struct{}, len(family.Model.Entities))
	for _, entity := range family.Model.Entities {
		entities[entity.Name] = struct{}{}
	}
	actions := make(map[string]string, len(family.Model.Actions))
	for _, action := range family.Model.Actions {
		actions[action.Name] = declarationOwner(modules, "action", action.Name)
	}
	names := make(map[string]struct{}, len(family.Targets))
	for index, target := range family.Targets {
		if target.Name == "" {
			return fmt.Errorf("verification target %d has an empty name", index)
		}
		if _, duplicate := names[target.Name]; duplicate {
			return fmt.Errorf("duplicate verification target %q", target.Name)
		}
		names[target.Name] = struct{}{}
		if len(target.Owners) == 0 {
			return fmt.Errorf("verification target %q has no owners", target.Name)
		}
		owners := make(map[CapabilityOwner]struct{}, len(target.Owners))
		for _, owner := range target.Owners {
			if owner == "" {
				return fmt.Errorf("verification target %q has an empty owner", target.Name)
			}
			if _, duplicate := owners[owner]; duplicate {
				return fmt.Errorf("verification target %q has duplicate owner %q", target.Name, owner)
			}
			owners[owner] = struct{}{}
		}
		for _, moduleName := range target.Modules {
			module, found := modules[moduleName]
			if !found {
				return fmt.Errorf("verification target %q references unknown module %q", target.Name, moduleName)
			}
			if _, owned := owners[module.Owner]; !owned {
				return fmt.Errorf("verification target %q is missing owner %q for module %q", target.Name, module.Owner, moduleName)
			}
		}
		targetActions := make(map[string]struct{}, len(target.Actions))
		for _, action := range target.Actions {
			moduleName, found := actions[action]
			if !found {
				return fmt.Errorf("verification target %q references unknown action %q", target.Name, action)
			}
			if _, duplicate := targetActions[action]; duplicate {
				return fmt.Errorf("verification target %q references action %q more than once", target.Name, action)
			}
			targetActions[action] = struct{}{}
			if owner := modules[moduleName].Owner; owner != "" {
				if _, owned := owners[owner]; !owned {
					return fmt.Errorf("verification target %q is missing owner %q for action %q", target.Name, owner, action)
				}
			}
		}
		for _, compositionName := range target.Compositions {
			composition, found := compositions[compositionName]
			if !found {
				return fmt.Errorf("verification target %q references unknown composition %q", target.Name, compositionName)
			}
			for _, owner := range composition.Owners {
				if _, owned := owners[owner]; !owned {
					return fmt.Errorf("verification target %q is missing owner %q for composition %q", target.Name, owner, compositionName)
				}
			}
		}
		for _, refinementMapName := range target.RefinementMaps {
			refinementMap, found := refinementMaps[refinementMapName]
			if !found {
				return fmt.Errorf("verification target %q references unknown refinement map %q", target.Name, refinementMapName)
			}
			if _, owned := owners[refinementMap.Owner]; !owned {
				return fmt.Errorf("verification target %q is missing owner %q for refinement map %q", target.Name, refinementMap.Owner, refinementMapName)
			}
		}
		for _, property := range target.Properties {
			if _, found := properties[property]; !found {
				return fmt.Errorf("verification target %q references unknown property %q", target.Name, property)
			}
			owner := modules[declarationOwner(modules, "property", property)].Owner
			if _, owned := owners[owner]; !owned {
				return fmt.Errorf("verification target %q is missing owner %q for property %q", target.Name, owner, property)
			}
		}
		backendRequirements := make(map[string]struct{}, len(target.BackendRequirements))
		for _, requirement := range target.BackendRequirements {
			if requirement == "" {
				return fmt.Errorf("verification target %q has an empty backend requirement", target.Name)
			}
			if _, duplicate := backendRequirements[requirement]; duplicate {
				return fmt.Errorf("verification target %q has duplicate backend requirement %q", target.Name, requirement)
			}
			backendRequirements[requirement] = struct{}{}
		}
		for _, abstraction := range target.Abstractions {
			if _, found := abstractions[abstraction]; !found {
				return fmt.Errorf("verification target %q references unknown abstraction %q", target.Name, abstraction)
			}
		}
		omissions := make(map[string]struct{}, len(target.Omissions))
		for _, omission := range target.Omissions {
			if _, found := actions[omission.Name]; !found {
				return fmt.Errorf("verification target %q omits unknown action %q", target.Name, omission.Name)
			}
			if omission.Reason == "" {
				return fmt.Errorf("verification target %q action omission %q has no reason", target.Name, omission.Name)
			}
			if _, duplicate := omissions[omission.Name]; duplicate {
				return fmt.Errorf("verification target %q omits action %q more than once", target.Name, omission.Name)
			}
			omissions[omission.Name] = struct{}{}
		}
		if err := validateTargetBoundMap(target.Name, "bound", target.Bounds, entities); err != nil {
			return err
		}
		if err := validateTargetBoundMap(target.Name, "minimum bound", target.MinimumBounds, entities); err != nil {
			return err
		}
	}
	return nil
}

func validateTargetBoundMap(targetName, kind string, bounds map[string]int, entities map[string]struct{}) error {
	names := make([]string, 0, len(bounds))
	for entity := range bounds {
		names = append(names, entity)
	}
	slices.Sort(names)
	for _, entity := range names {
		if _, found := entities[entity]; !found {
			return fmt.Errorf("verification target %q sets a %s for unknown entity %q", targetName, kind, entity)
		}
		if bounds[entity] <= 0 {
			return fmt.Errorf("verification target %q %s for entity %q must be positive", targetName, kind, entity)
		}
	}
	return nil
}

func validateCompositions(family ModelFamily, modules map[string]Module) error {
	properties := make(map[string]struct{}, len(family.Model.Properties))
	for _, property := range family.Model.Properties {
		properties[property.Name] = struct{}{}
	}
	refinementMaps := make(map[string]RefinementMap, len(family.RefinementMaps))
	for _, refinementMap := range family.RefinementMaps {
		refinementMaps[refinementMap.Name] = refinementMap
	}
	names := make(map[string]struct{}, len(family.Compositions))
	for index, composition := range family.Compositions {
		if composition.Name == "" {
			return fmt.Errorf("composition %d has an empty name", index)
		}
		if _, duplicate := names[composition.Name]; duplicate {
			return fmt.Errorf("duplicate composition %q", composition.Name)
		}
		names[composition.Name] = struct{}{}
		if len(composition.Owners) == 0 {
			return fmt.Errorf("composition %q has no owners", composition.Name)
		}
		owners := make(map[CapabilityOwner]struct{}, len(composition.Owners))
		for _, owner := range composition.Owners {
			if owner == "" {
				return fmt.Errorf("composition %q has an empty owner", composition.Name)
			}
			if _, duplicate := owners[owner]; duplicate {
				return fmt.Errorf("composition %q has duplicate owner %q", composition.Name, owner)
			}
			owners[owner] = struct{}{}
		}
		composedModules := make(map[string]struct{}, len(composition.Modules))
		for _, moduleName := range composition.Modules {
			if _, duplicate := composedModules[moduleName]; duplicate {
				return fmt.Errorf("composition %q references module %q more than once", composition.Name, moduleName)
			}
			composedModules[moduleName] = struct{}{}
			declared, found := modules[moduleName]
			if !found {
				return fmt.Errorf("composition %q references unknown module %q", composition.Name, moduleName)
			}
			if _, owned := owners[declared.Owner]; !owned {
				return fmt.Errorf("composition %q is missing owner %q for module %q", composition.Name, declared.Owner, moduleName)
			}
		}
		for _, property := range composition.Properties {
			if _, found := properties[property]; !found {
				return fmt.Errorf("composition %q references unknown property %q", composition.Name, property)
			}
			owner := modules[declarationOwner(modules, "property", property)].Owner
			if _, owned := owners[owner]; !owned {
				return fmt.Errorf("composition %q is missing owner %q for property %q", composition.Name, owner, property)
			}
		}
		for _, refinementMapName := range composition.RefinementMaps {
			refinementMap, found := refinementMaps[refinementMapName]
			if !found {
				return fmt.Errorf("composition %q references unknown refinement map %q", composition.Name, refinementMapName)
			}
			if _, owned := owners[refinementMap.Owner]; !owned {
				return fmt.Errorf("composition %q is missing owner %q for refinement map %q", composition.Name, refinementMap.Owner, refinementMapName)
			}
		}
	}
	return nil
}

func isInterfaceConsumer(interfaces []Interface, interfaceName, moduleName string) bool {
	for _, declared := range interfaces {
		if declared.Name != interfaceName {
			continue
		}
		for _, consumer := range declared.Consumers {
			if consumer == moduleName {
				return true
			}
		}
		return false
	}
	return false
}

func validateInterfaces(interfaces []Interface, modules map[string]Module, model Model) error {
	entities := make(map[string]struct{}, len(model.Entities))
	for _, entity := range model.Entities {
		entities[entity.Name] = struct{}{}
	}
	names := make(map[string]struct{}, len(interfaces))
	for index, declared := range interfaces {
		if declared.Name == "" {
			return fmt.Errorf("interface %d has an empty name", index)
		}
		if _, duplicate := names[declared.Name]; duplicate {
			return fmt.Errorf("duplicate interface %q", declared.Name)
		}
		names[declared.Name] = struct{}{}
		if _, found := modules[declared.Provider]; !found {
			return fmt.Errorf("interface %q references unknown provider module %q", declared.Name, declared.Provider)
		}
		consumers := make(map[string]struct{}, len(declared.Consumers))
		for _, consumer := range declared.Consumers {
			if _, found := modules[consumer]; !found {
				return fmt.Errorf("interface %q references unknown consumer module %q", declared.Name, consumer)
			}
			if _, duplicate := consumers[consumer]; duplicate {
				return fmt.Errorf("interface %q has duplicate consumer module %q", declared.Name, consumer)
			}
			consumers[consumer] = struct{}{}
		}
		identitySorts := make(map[string]struct{}, len(declared.Identities))
		for _, identity := range declared.Identities {
			if _, found := entities[identity]; !found {
				return fmt.Errorf("interface %q references unknown identity sort %q", declared.Name, identity)
			}
			if _, duplicate := identitySorts[identity]; duplicate {
				return fmt.Errorf("interface %q references identity sort %q more than once", declared.Name, identity)
			}
			identitySorts[identity] = struct{}{}
		}
		obligations := make(map[string]struct{}, len(declared.Obligations))
		for obligationIndex, obligation := range declared.Obligations {
			if obligation.Name == "" {
				return fmt.Errorf("interface %q obligation %d has an empty name", declared.Name, obligationIndex)
			}
			if _, duplicate := obligations[obligation.Name]; duplicate {
				return fmt.Errorf("interface %q has duplicate obligation %q", declared.Name, obligation.Name)
			}
			obligations[obligation.Name] = struct{}{}
			key := obligationKey(declared.Name, obligation.Name)
			if len(obligation.Actions) == 0 && len(obligation.Properties) == 0 {
				return fmt.Errorf("obligation %q has no actions or properties", key)
			}
			if duplicate := duplicateString(obligation.Actions); duplicate != "" {
				return fmt.Errorf("obligation %q contains action %q more than once", key, duplicate)
			}
			if duplicate := duplicateString(obligation.Properties); duplicate != "" {
				return fmt.Errorf("obligation %q contains property %q more than once", key, duplicate)
			}
			assumptions := make(map[string]struct{}, len(obligation.Assumptions))
			for _, assumption := range obligation.Assumptions {
				assumptionKey := obligationKey(assumption.Interface, assumption.Obligation)
				if _, duplicate := assumptions[assumptionKey]; duplicate {
					return fmt.Errorf("obligation %q assumes obligation %q more than once", key, assumptionKey)
				}
				assumptions[assumptionKey] = struct{}{}
			}
			for _, action := range obligation.Actions {
				owner := declarationOwner(modules, "action", action)
				if owner == "" {
					return fmt.Errorf("obligation %q references unknown action %q", key, action)
				}
				if owner != declared.Provider {
					return fmt.Errorf("obligation %q action %q is owned by module %q, not provider %q", key, action, owner, declared.Provider)
				}
			}
			for _, property := range obligation.Properties {
				owner := declarationOwner(modules, "property", property)
				if owner == "" {
					return fmt.Errorf("obligation %q references unknown property %q", key, property)
				}
				if owner != declared.Provider {
					return fmt.Errorf("obligation %q property %q is owned by module %q, not provider %q", key, property, owner, declared.Provider)
				}
			}
		}
	}
	return nil
}

func interfaceByName(interfaces []Interface, name string) Interface {
	for _, declared := range interfaces {
		if declared.Name == name {
			return declared
		}
	}
	return Interface{}
}

func duplicateString(values []string) string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, duplicate := seen[value]; duplicate {
			return value
		}
		seen[value] = struct{}{}
	}
	return ""
}
