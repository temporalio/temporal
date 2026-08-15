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
	Concrete   string                `json:"concrete"`
	Abstract   string                `json:"abstract,omitempty"`
	Stutter    bool                  `json:"stutter,omitempty"`
	Parameters []ParameterRefinement `json:"parameters,omitempty"`
}

type ParameterRefinement struct {
	Concrete string `json:"concrete"`
	Abstract string `json:"abstract"`
}

type IdentityRefinement struct {
	Concrete string `json:"concrete"`
	Abstract string `json:"abstract"`
}

type RefinementMap struct {
	Name       string               `json:"name"`
	Owner      CapabilityOwner      `json:"owner"`
	Module     string               `json:"module,omitempty"`
	Interface  string               `json:"interface,omitempty"`
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
	Actions             []string          `json:"actions,omitempty"`
	Compositions        []string          `json:"compositions,omitempty"`
	Properties          []string          `json:"properties,omitempty"`
	RefinementMaps      []string          `json:"refinementMaps,omitempty"`
	Bounds              map[string]int    `json:"bounds,omitempty"`
	MinimumBounds       map[string]int    `json:"minimumBounds,omitempty"`
	BackendRequirements []string          `json:"backendRequirements,omitempty"`
	FailurePolicy       []string          `json:"failurePolicy,omitempty"`
	Abstractions        []string          `json:"abstractions,omitempty"`
	Omissions           []Abstraction     `json:"omissions,omitempty"`
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
	if err := validateIncludedRuleInventory(family.Model); err != nil {
		return err
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
	if err := validateRefinementMaps(family.RefinementMaps, family.Modules, family.Interfaces, family.Model); err != nil {
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

func validateIncludedRuleInventory(model Model) error {
	propertiesByRule := make(map[string]struct{}, len(model.Properties))
	for _, property := range model.Properties {
		if property.Source.Symbol != "" {
			propertiesByRule[property.Source.Symbol] = struct{}{}
		}
	}
	for _, item := range model.Inventory {
		if item.Kind != "rule" || !item.Included {
			continue
		}
		if _, found := propertiesByRule[item.Name]; !found {
			return fmt.Errorf("included verification rule %q has no shared property", item.Name)
		}
	}
	return nil
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
		for actionIndex := range result.RefinementMaps[index].Actions {
			result.RefinementMaps[index].Actions[actionIndex].Parameters = slices.Clone(result.RefinementMaps[index].Actions[actionIndex].Parameters)
			slices.SortFunc(result.RefinementMaps[index].Actions[actionIndex].Parameters, func(left, right ParameterRefinement) int {
				return compareString(left.Abstract, right.Abstract)
			})
		}
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
		result.Targets[index].Actions = sortedClone(result.Targets[index].Actions)
		result.Targets[index].Compositions = sortedClone(result.Targets[index].Compositions)
		result.Targets[index].Properties = sortedClone(result.Targets[index].Properties)
		result.Targets[index].RefinementMaps = sortedClone(result.Targets[index].RefinementMaps)
		result.Targets[index].BackendRequirements = sortedClone(result.Targets[index].BackendRequirements)
		result.Targets[index].FailurePolicy = sortedClone(result.Targets[index].FailurePolicy)
		result.Targets[index].Abstractions = sortedClone(result.Targets[index].Abstractions)
		result.Targets[index].Omissions = slices.Clone(result.Targets[index].Omissions)
		slices.SortFunc(result.Targets[index].Omissions, func(left, right Abstraction) int {
			return compareString(left.Name, right.Name)
		})
	}
	slices.SortFunc(result.Targets, func(left, right VerificationTarget) int { return compareString(left.Name, right.Name) })
	return result
}
