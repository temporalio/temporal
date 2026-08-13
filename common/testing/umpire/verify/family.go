package verify

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

type CapabilityOwner string

type Module struct {
	Name       string          `json:"name"`
	Owner      CapabilityOwner `json:"owner"`
	Entities   []string        `json:"entities,omitempty"`
	Relations  []string        `json:"relations,omitempty"`
	Actions    []string        `json:"actions,omitempty"`
	Properties []string        `json:"properties,omitempty"`
	Imports    []ObligationRef `json:"imports,omitempty"`
}

type ObligationRef struct {
	Interface  string `json:"interface"`
	Obligation string `json:"obligation"`
}

type Obligation struct {
	Name        string          `json:"name"`
	Actions     []string        `json:"actions,omitempty"`
	Properties  []string        `json:"properties,omitempty"`
	Assumptions []ObligationRef `json:"assumptions,omitempty"`
}

type Interface struct {
	Name        string       `json:"name"`
	Provider    string       `json:"provider"`
	Consumers   []string     `json:"consumers,omitempty"`
	Identities  []string     `json:"identities,omitempty"`
	Obligations []Obligation `json:"obligations,omitempty"`
}

type ActionRefinement struct {
	Concrete string `json:"concrete"`
	Abstract string `json:"abstract,omitempty"`
	Stutter  bool   `json:"stutter,omitempty"`
}

type IdentityRefinement struct {
	Concrete string `json:"concrete"`
	Abstract string `json:"abstract"`
}

type RefinementMap struct {
	Name       string               `json:"name"`
	Owner      CapabilityOwner      `json:"owner"`
	Actions    []ActionRefinement   `json:"actions,omitempty"`
	Identities []IdentityRefinement `json:"identities,omitempty"`
}

type Composition struct {
	Name           string            `json:"name"`
	Owners         []CapabilityOwner `json:"owners"`
	Modules        []string          `json:"modules"`
	Properties     []string          `json:"properties,omitempty"`
	RefinementMaps []string          `json:"refinementMaps,omitempty"`
	Closes         []ObligationRef   `json:"closes,omitempty"`
}

type VerificationTarget struct {
	Name                string            `json:"name"`
	Owners              []CapabilityOwner `json:"owners"`
	Modules             []string          `json:"modules,omitempty"`
	Compositions        []string          `json:"compositions,omitempty"`
	Properties          []string          `json:"properties,omitempty"`
	RefinementMaps      []string          `json:"refinementMaps,omitempty"`
	Bounds              map[string]int    `json:"bounds,omitempty"`
	MinimumBounds       map[string]int    `json:"minimumBounds,omitempty"`
	BackendRequirements []string          `json:"backendRequirements,omitempty"`
	FailurePolicy       []string          `json:"failurePolicy,omitempty"`
	Abstractions        []string          `json:"abstractions,omitempty"`
	IncludeInventory    bool              `json:"includeInventory,omitempty"`
}

type ModelFamily struct {
	Version        string               `json:"version"`
	Model          Model                `json:"model"`
	Modules        []Module             `json:"modules"`
	Interfaces     []Interface          `json:"interfaces,omitempty"`
	RefinementMaps []RefinementMap      `json:"refinementMaps,omitempty"`
	Compositions   []Composition        `json:"compositions,omitempty"`
	Targets        []VerificationTarget `json:"targets,omitempty"`
}

func ValidateModelFamily(family ModelFamily) error {
	if family.Version == "" {
		return errors.New("verification model family version is empty")
	}
	if err := Validate(family.Model); err != nil {
		return fmt.Errorf("verification model family: %w", err)
	}
	modules := make(map[string]Module, len(family.Modules))
	for index, module := range family.Modules {
		if module.Name == "" {
			return fmt.Errorf("module %d has an empty name", index)
		}
		if module.Owner == "" {
			return fmt.Errorf("module %q has no owner", module.Name)
		}
		if _, duplicate := modules[module.Name]; duplicate {
			return fmt.Errorf("duplicate module %q", module.Name)
		}
		modules[module.Name] = module
	}
	if err := validateDeclarationOwnership(family.Model, family.Modules); err != nil {
		return err
	}
	if err := validateInterfaces(family.Interfaces, modules, family.Model); err != nil {
		return err
	}
	if err := validateRefinementMaps(family.RefinementMaps, family.Model); err != nil {
		return err
	}
	if err := validateCompositions(family, modules); err != nil {
		return err
	}
	if err := validateTargets(family, modules); err != nil {
		return err
	}
	obligations := make(map[string]Obligation)
	var obligationOrder []string
	for _, declared := range family.Interfaces {
		for _, obligation := range declared.Obligations {
			key := obligationKey(declared.Name, obligation.Name)
			obligations[key] = obligation
			obligationOrder = append(obligationOrder, key)
		}
	}
	slices.Sort(obligationOrder)
	moduleOrder := slices.Clone(family.Modules)
	slices.SortFunc(moduleOrder, func(left, right Module) int { return compareString(left.Name, right.Name) })
	for _, module := range moduleOrder {
		imports := make(map[string]struct{}, len(module.Imports))
		for _, imported := range module.Imports {
			key := obligationKey(imported.Interface, imported.Obligation)
			if _, duplicate := imports[key]; duplicate {
				return fmt.Errorf("module %q imports obligation %q more than once", module.Name, key)
			}
			imports[key] = struct{}{}
			if _, found := obligations[key]; !found {
				return fmt.Errorf("module %q imports unknown obligation %q", module.Name, key)
			}
			if !isInterfaceConsumer(family.Interfaces, imported.Interface, module.Name) {
				return fmt.Errorf("module %q imports %q but is not a declared consumer of interface %q", module.Name, key, imported.Interface)
			}
		}
	}
	for _, key := range obligationOrder {
		obligation := obligations[key]
		for _, assumption := range obligation.Assumptions {
			if _, found := obligations[obligationKey(assumption.Interface, assumption.Obligation)]; !found {
				return fmt.Errorf("obligation %q assumes unknown obligation %q", key, obligationKey(assumption.Interface, assumption.Obligation))
			}
		}
	}
	for _, declared := range family.Interfaces {
		for _, obligation := range declared.Obligations {
			key := obligationKey(declared.Name, obligation.Name)
			for _, assumption := range obligation.Assumptions {
				assumedInterface := interfaceByName(family.Interfaces, assumption.Interface)
				if assumedInterface.Provider != declared.Provider && !isInterfaceConsumer(family.Interfaces, assumption.Interface, declared.Provider) {
					return fmt.Errorf("obligation %q assumes %q but provider module %q is not a declared consumer of interface %q", key, obligationKey(assumption.Interface, assumption.Obligation), declared.Provider, assumption.Interface)
				}
			}
		}
	}
	if err := validateCompositionClosures(family, obligations); err != nil {
		return err
	}
	if cycle := unclosedContractCycle(obligationOrder, obligations, family); len(cycle) != 0 {
		return fmt.Errorf("contract cycle: %s", strings.Join(cycle, " -> "))
	}
	return validateObligationChecks(family)
}

func validateObligationChecks(family ModelFamily) error {
	directModules := make(map[string]struct{})
	selectedCompositions := make(map[string]struct{})
	for _, target := range family.Targets {
		for _, module := range target.Modules {
			directModules[module] = struct{}{}
		}
		for _, composition := range target.Compositions {
			selectedCompositions[composition] = struct{}{}
		}
	}
	closed := make(map[string]struct{})
	for _, composition := range family.Compositions {
		if _, selected := selectedCompositions[composition.Name]; !selected {
			continue
		}
		for _, reference := range composition.Closes {
			closed[obligationKey(reference.Interface, reference.Obligation)] = struct{}{}
		}
	}
	for _, declared := range family.Interfaces {
		if _, checked := directModules[declared.Provider]; checked {
			continue
		}
		for _, obligation := range declared.Obligations {
			key := obligationKey(declared.Name, obligation.Name)
			if _, checked := closed[key]; !checked {
				return fmt.Errorf("obligation %q has no provider module or closing composition target", key)
			}
		}
	}
	return nil
}

func MarshalModelFamily(family ModelFamily) ([]byte, error) {
	normalized := normalizeModelFamily(family)
	encoded, err := json.MarshalIndent(normalized, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func HashModelFamily(family ModelFamily) (string, error) {
	encoded, err := MarshalModelFamily(family)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func normalizeModelFamily(family ModelFamily) ModelFamily {
	result := family
	result.Model = normalizeModel(family.Model)
	result.Modules = slices.Clone(family.Modules)
	for index := range result.Modules {
		result.Modules[index].Entities = sortedClone(result.Modules[index].Entities)
		result.Modules[index].Relations = sortedClone(result.Modules[index].Relations)
		result.Modules[index].Actions = sortedClone(result.Modules[index].Actions)
		result.Modules[index].Properties = sortedClone(result.Modules[index].Properties)
		result.Modules[index].Imports = slices.Clone(result.Modules[index].Imports)
		slices.SortFunc(result.Modules[index].Imports, compareObligationRef)
	}
	slices.SortFunc(result.Modules, func(left, right Module) int { return compareString(left.Name, right.Name) })
	result.Interfaces = slices.Clone(family.Interfaces)
	for index := range result.Interfaces {
		result.Interfaces[index].Consumers = sortedClone(result.Interfaces[index].Consumers)
		result.Interfaces[index].Identities = sortedClone(result.Interfaces[index].Identities)
		result.Interfaces[index].Obligations = slices.Clone(result.Interfaces[index].Obligations)
		for obligationIndex := range result.Interfaces[index].Obligations {
			obligation := &result.Interfaces[index].Obligations[obligationIndex]
			obligation.Actions = sortedClone(obligation.Actions)
			obligation.Properties = sortedClone(obligation.Properties)
			obligation.Assumptions = slices.Clone(obligation.Assumptions)
			slices.SortFunc(obligation.Assumptions, compareObligationRef)
		}
		slices.SortFunc(result.Interfaces[index].Obligations, func(left, right Obligation) int {
			return compareString(left.Name, right.Name)
		})
	}
	slices.SortFunc(result.Interfaces, func(left, right Interface) int { return compareString(left.Name, right.Name) })
	result.RefinementMaps = slices.Clone(family.RefinementMaps)
	for index := range result.RefinementMaps {
		result.RefinementMaps[index].Actions = slices.Clone(result.RefinementMaps[index].Actions)
		slices.SortFunc(result.RefinementMaps[index].Actions, func(left, right ActionRefinement) int {
			return compareString(left.Concrete, right.Concrete)
		})
		result.RefinementMaps[index].Identities = slices.Clone(result.RefinementMaps[index].Identities)
		slices.SortFunc(result.RefinementMaps[index].Identities, func(left, right IdentityRefinement) int {
			return compareString(left.Concrete, right.Concrete)
		})
	}
	slices.SortFunc(result.RefinementMaps, func(left, right RefinementMap) int { return compareString(left.Name, right.Name) })
	result.Compositions = slices.Clone(family.Compositions)
	for index := range result.Compositions {
		result.Compositions[index].Owners = slices.Clone(result.Compositions[index].Owners)
		slices.Sort(result.Compositions[index].Owners)
		result.Compositions[index].Modules = sortedClone(result.Compositions[index].Modules)
		result.Compositions[index].Properties = sortedClone(result.Compositions[index].Properties)
		result.Compositions[index].RefinementMaps = sortedClone(result.Compositions[index].RefinementMaps)
		result.Compositions[index].Closes = slices.Clone(result.Compositions[index].Closes)
		slices.SortFunc(result.Compositions[index].Closes, compareObligationRef)
	}
	slices.SortFunc(result.Compositions, func(left, right Composition) int { return compareString(left.Name, right.Name) })
	result.Targets = slices.Clone(family.Targets)
	for index := range result.Targets {
		result.Targets[index].Owners = slices.Clone(result.Targets[index].Owners)
		slices.Sort(result.Targets[index].Owners)
		result.Targets[index].Modules = sortedClone(result.Targets[index].Modules)
		result.Targets[index].Compositions = sortedClone(result.Targets[index].Compositions)
		result.Targets[index].Properties = sortedClone(result.Targets[index].Properties)
		result.Targets[index].RefinementMaps = sortedClone(result.Targets[index].RefinementMaps)
		result.Targets[index].BackendRequirements = sortedClone(result.Targets[index].BackendRequirements)
		result.Targets[index].FailurePolicy = sortedClone(result.Targets[index].FailurePolicy)
		result.Targets[index].Abstractions = sortedClone(result.Targets[index].Abstractions)
	}
	slices.SortFunc(result.Targets, func(left, right VerificationTarget) int { return compareString(left.Name, right.Name) })
	return result
}

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

func validateRefinementMaps(refinementMaps []RefinementMap, model Model) error {
	actions := make(map[string]struct{}, len(model.Actions))
	for _, action := range model.Actions {
		actions[action.Name] = struct{}{}
	}
	entities := make(map[string]struct{}, len(model.Entities))
	for _, entity := range model.Entities {
		entities[entity.Name] = struct{}{}
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
		mappedActions := make(map[string]struct{}, len(refinementMap.Actions))
		for _, refinement := range refinementMap.Actions {
			if _, found := actions[refinement.Concrete]; !found {
				return fmt.Errorf("refinement map %q references unknown concrete action %q", refinementMap.Name, refinement.Concrete)
			}
			if _, duplicate := mappedActions[refinement.Concrete]; duplicate {
				return fmt.Errorf("refinement map %q maps action %q more than once", refinementMap.Name, refinement.Concrete)
			}
			mappedActions[refinement.Concrete] = struct{}{}
			if refinement.Stutter == (refinement.Abstract != "") {
				return fmt.Errorf("refinement map %q action %q must select exactly one abstract action or stuttering", refinementMap.Name, refinement.Concrete)
			}
			if refinement.Abstract != "" {
				if _, found := actions[refinement.Abstract]; !found {
					return fmt.Errorf("refinement map %q references unknown abstract action %q", refinementMap.Name, refinement.Abstract)
				}
			}
		}
		mappedIdentities := make(map[string]struct{}, len(refinementMap.Identities))
		for _, identity := range refinementMap.Identities {
			if _, found := entities[identity.Concrete]; !found {
				return fmt.Errorf("refinement map %q references unknown concrete entity %q", refinementMap.Name, identity.Concrete)
			}
			if _, found := entities[identity.Abstract]; !found {
				return fmt.Errorf("refinement map %q references unknown abstract entity %q", refinementMap.Name, identity.Abstract)
			}
			if _, duplicate := mappedIdentities[identity.Concrete]; duplicate {
				return fmt.Errorf("refinement map %q maps entity %q more than once", refinementMap.Name, identity.Concrete)
			}
			mappedIdentities[identity.Concrete] = struct{}{}
		}
	}
	return nil
}

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

func declarationOwner(modules map[string]Module, kind, name string) string {
	for moduleName, module := range modules {
		var declarations []string
		switch kind {
		case "action":
			declarations = module.Actions
		case "property":
			declarations = module.Properties
		default:
			return ""
		}
		for _, declaration := range declarations {
			if declaration == name {
				return moduleName
			}
		}
	}
	return ""
}

func validateDeclarationOwnership(model Model, modules []Module) error {
	declared := make(map[string]struct{})
	for _, entity := range model.Entities {
		declared[declarationKey("entity", entity.Name)] = struct{}{}
	}
	for _, relation := range model.Relations {
		declared[declarationKey("relation", relation.Name)] = struct{}{}
	}
	for _, action := range model.Actions {
		declared[declarationKey("action", action.Name)] = struct{}{}
	}
	for _, property := range model.Properties {
		declared[declarationKey("property", property.Name)] = struct{}{}
	}
	owners := make(map[string]string, len(declared))
	for _, module := range modules {
		claims := []struct {
			kind  string
			names []string
		}{
			{kind: "entity", names: module.Entities},
			{kind: "relation", names: module.Relations},
			{kind: "action", names: module.Actions},
			{kind: "property", names: module.Properties},
		}
		for _, claim := range claims {
			for _, name := range claim.names {
				key := declarationKey(claim.kind, name)
				if _, found := declared[key]; !found {
					return fmt.Errorf("module %q owns unknown %s %q", module.Name, claim.kind, name)
				}
				if previous, duplicate := owners[key]; duplicate {
					return fmt.Errorf("%s %q is owned by both modules %q and %q", claim.kind, name, previous, module.Name)
				}
				owners[key] = module.Name
			}
		}
	}
	declarations := []struct {
		kind  string
		names []string
	}{
		{kind: "entity", names: entityNames(model.Entities)},
		{kind: "relation", names: relationNames(model.Relations)},
		{kind: "action", names: actionNames(model.Actions)},
		{kind: "property", names: propertyNames(model.Properties)},
	}
	for _, declaration := range declarations {
		for _, name := range declaration.names {
			if _, found := owners[declarationKey(declaration.kind, name)]; !found {
				return fmt.Errorf("%s %q is not owned by a module", declaration.kind, name)
			}
		}
	}
	return nil
}

func declarationKey(kind, name string) string {
	return kind + "\x00" + name
}

func entityNames(entities []EntityType) []string {
	result := make([]string, len(entities))
	for index, entity := range entities {
		result[index] = entity.Name
	}
	return result
}

func relationNames(relations []Relation) []string {
	result := make([]string, len(relations))
	for index, relation := range relations {
		result[index] = relation.Name
	}
	return result
}

func actionNames(actions []Action) []string {
	result := make([]string, len(actions))
	for index, action := range actions {
		result[index] = action.Name
	}
	return result
}

func propertyNames(properties []Property) []string {
	result := make([]string, len(properties))
	for index, property := range properties {
		result[index] = property.Name
	}
	return result
}

func obligationKey(interfaceName, obligationName string) string {
	return interfaceName + "." + obligationName
}

func unclosedContractCycle(order []string, obligations map[string]Obligation, family ModelFamily) []string {
	state := make(map[string]uint8, len(obligations))
	positions := make(map[string]int, len(obligations))
	var stack []string
	var visit func(string) []string
	visit = func(key string) []string {
		state[key] = 1
		positions[key] = len(stack)
		stack = append(stack, key)
		assumptions := slices.Clone(obligations[key].Assumptions)
		slices.SortFunc(assumptions, compareObligationRef)
		for _, assumption := range assumptions {
			dependency := obligationKey(assumption.Interface, assumption.Obligation)
			switch state[dependency] {
			case 0:
				if cycle := visit(dependency); len(cycle) != 0 {
					return cycle
				}
			case 1:
				cycle := append([]string(nil), stack[positions[dependency]:]...)
				cycle = append(cycle, dependency)
				if !compositionClosesCycle(family, cycle) {
					return cycle
				}
			default:
			}
		}
		stack = stack[:len(stack)-1]
		delete(positions, key)
		state[key] = 2
		return nil
	}
	for _, key := range order {
		if state[key] == 0 {
			if cycle := visit(key); len(cycle) != 0 {
				return cycle
			}
		}
	}
	return nil
}
