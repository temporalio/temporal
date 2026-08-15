package regress

import (
	"fmt"
	"strings"

	"go.temporal.io/server/common/testing/umpire"
)

// Profile declares the environment variant against which a sparse plan is compiled.
type Profile struct {
	Name             string                    `json:"name"`
	Capabilities     []string                  `json:"capabilities,omitempty"`
	Environment      umpire.EnvironmentProfile `json:"environment,omitempty"`
	Realizations     *RealizationCatalog       `json:"realizations,omitempty"`
	Limits           CompileLimits             `json:"limits,omitempty"`
	ObservedFacts    []CompletedAtom           `json:"observedFacts,omitempty"`
	ObservedBindings Bindings                  `json:"observedBindings,omitempty"`
}

// CompileLimits are explicit caller bounds; zero values impose no semantic truncation.
type CompileLimits struct {
	MaxPaths int `json:"maxPaths,omitempty"`
}

// CompletedAction is one fully grounded model capability in an executable path.
type CompletedAction struct {
	Name        string     `json:"name"`
	Arguments   []Argument `json:"arguments,omitempty"`
	Realization string     `json:"realization,omitempty"`
	Source      int        `json:"source,omitempty"`
}

// CompletedAtom is one grounded predicate in an executable action or milestone.
type CompletedAtom struct {
	Predicate string     `json:"predicate"`
	Arguments []Argument `json:"arguments,omitempty"`
}

// CompletedStep carries the execution semantics for one completed action.
type CompletedStep struct {
	Action        CompletedAction `json:"action"`
	Mode          ActionMode      `json:"mode"`
	Preconditions []CompletedAtom `json:"preconditions,omitempty"`
	Effects       []CompletedAtom `json:"effects,omitempty"`
}

// CompletedMilestone is one key frame and the action interval synthesized to satisfy it.
type CompletedMilestone struct {
	Node         int             `json:"node"`
	Source       int             `json:"source"`
	Kind         InstructionKind `json:"kind"`
	Name         string          `json:"name"`
	Arguments    []Argument      `json:"arguments,omitempty"`
	Binding      string          `json:"binding,omitempty"`
	BeforeAction int             `json:"beforeAction"`
	AfterAction  int             `json:"afterAction"`
}

// CompletedResource is one synthesized resource in dependency creation order.
type CompletedResource struct {
	Name        string `json:"name"`
	Realization string `json:"realization,omitempty"`
	Source      int    `json:"source,omitempty"`
}

// CompletedPolicy is one policy interval over the completed action sequence [Start, End).
type CompletedPolicy struct {
	Name        string     `json:"name"`
	Arguments   []Argument `json:"arguments,omitempty"`
	Realization string     `json:"realization,omitempty"`
	Source      int        `json:"source,omitempty"`
	Start       int        `json:"start"`
	End         int        `json:"end"`
}

// CompletedPath is one validated satisfying semantic execution.
type CompletedPath struct {
	Actions    []CompletedAction    `json:"actions"`
	Steps      []CompletedStep      `json:"steps,omitempty"`
	Created    []string             `json:"created,omitempty"`
	Resources  []CompletedResource  `json:"resources,omitempty"`
	Policies   []CompletedPolicy    `json:"policies,omitempty"`
	Milestones []CompletedMilestone `json:"milestones,omitempty"`
	Bindings   Bindings             `json:"bindings,omitempty"`
}

// Suite is a completely compiled sparse regression plan.
type Suite struct {
	Name         string          `json:"name,omitempty"`
	IR           IR              `json:"ir"`
	ModelVersion string          `json:"modelVersion"`
	Profile      Profile         `json:"profile"`
	Paths        []CompletedPath `json:"paths"`
	PathCount    int             `json:"pathCount"`
}

type groundAtom struct {
	Predicate string
	Arguments []Argument
}

type world struct {
	facts      map[string]groundAtom
	created    map[string]bool
	actions    []CompletedAction
	steps      []CompletedStep
	resources  map[string]int
	ranges     map[int]actionRange
	milestones []CompletedMilestone
}

type actionRange struct {
	start int
	end   int
}

type searchContext struct {
	domain      *Domain
	ir          IR
	profile     map[string]bool
	unavailable map[string]bool
	lastGap     *CompileError
}

// Compile expands a sparse plan into one canonical path or every satisfying semantic path.
func Compile(plan Plan, domain *Domain, profile Profile) (Suite, error) {
	if domain == nil {
		return Suite{}, invalidInstruction(0, "domain is nil")
	}
	if profile.Environment.Name != "" {
		if err := umpire.ValidateEnvironmentProfile(profile.Environment); err != nil {
			return Suite{}, &CompileError{Category: ErrorUnavailableEnvironmentCapability, Detail: err.Error()}
		}
	}
	ir, err := Normalize(plan)
	if err != nil {
		return Suite{}, err
	}
	ctx := searchContext{
		domain:      domain,
		ir:          ir,
		profile:     stringSet(profile.Capabilities),
		unavailable: map[string]bool{},
	}
	if err := ctx.validateCatalog(); err != nil {
		return Suite{}, err
	}
	for _, requirement := range ir.Requirements {
		if !ctx.profile[requirement.Name] {
			return Suite{}, &CompileError{
				Category: ErrorUnavailableEnvironmentCapability,
				Source:   requirement.Source,
				Detail:   fmt.Sprintf("unavailable environment capability: %s", requirement.Name),
			}
		}
	}
	policyResources, err := ctx.validatePolicies()
	if err != nil {
		return Suite{}, err
	}
	initial, err := ctx.observedWorld(profile)
	if err != nil {
		return Suite{}, err
	}
	for resource, source := range policyResources {
		initial.resources[resource] = source
	}
	var states []world
	for _, order := range topologicalOrders(len(ir.Nodes), ir.Edges) {
		orderedStates := []world{cloneWorld(initial)}
		for _, nodeID := range order {
			node := ir.Nodes[nodeID]
			var next []world
			for _, current := range orderedStates {
				expanded, expandErr := ctx.satisfyNode(current, node)
				if expandErr != nil {
					return Suite{}, expandErr
				}
				next = append(next, expanded...)
			}
			orderedStates = deduplicateWorlds(next)
			if len(orderedStates) == 0 {
				break
			}
		}
		states = append(states, orderedStates...)
	}
	states = deduplicateWorlds(states)
	if len(states) == 0 {
		if missing := sortedKeys(ctx.unavailable); len(missing) > 0 {
			return Suite{}, &CompileError{
				Category: ErrorUnavailableEnvironmentCapability,
				Detail:   fmt.Sprintf("unavailable environment capability: %s", strings.Join(missing, ", ")),
			}
		}
		if ctx.lastGap != nil {
			return Suite{}, ctx.lastGap
		}
		return Suite{}, &CompileError{Category: ErrorUnreachableOutcome, Detail: "cannot satisfy sparse plan"}
	}

	paths := make([]CompletedPath, 0, len(states))
	for _, state := range states {
		resources, resourceErr := ctx.completedResources(state.resources)
		if resourceErr != nil {
			return Suite{}, resourceErr
		}
		paths = append(paths, CompletedPath{
			Actions:    state.actions,
			Steps:      state.steps,
			Created:    sortedKeys(state.created),
			Resources:  resources,
			Policies:   ctx.completedPolicies(state),
			Milestones: state.milestones,
			Bindings:   cloneRuntimeBindings(profile.ObservedBindings),
		})
	}
	paths = deduplicatePaths(paths, domain)
	sortCompletedPaths(paths, domain)
	if plan.Mode == AllPathsMode && profile.Limits.MaxPaths > 0 && len(paths) > profile.Limits.MaxPaths {
		return Suite{}, &CompileError{
			Category: ErrorIncompleteAllPaths,
			Detail:   fmt.Sprintf("complete enumeration contains %d paths, exceeding explicit limit %d", len(paths), profile.Limits.MaxPaths),
		}
	}
	if plan.Mode == OnePathMode && len(paths) > 1 {
		paths = paths[:1]
	}
	return validateCompiledSuite(Suite{
		Name:         plan.Name,
		IR:           ir,
		ModelVersion: domain.version,
		Profile:      profile,
		Paths:        paths,
		PathCount:    len(paths),
	})
}

func validateCompiledSuite(suite Suite) (Suite, error) {
	if err := ValidateSuite(suite); err != nil {
		return Suite{}, &CompileError{
			Category: ErrorInvalidCompletedSuite,
			Detail:   fmt.Sprintf("completed suite is invalid: %v", err),
		}
	}
	if suite.Profile.Realizations != nil {
		if err := ValidateRealizations(suite, *suite.Profile.Realizations); err != nil {
			return Suite{}, err
		}
	}
	return suite, nil
}

func (c *searchContext) observedWorld(profile Profile) (world, error) {
	result := newWorld()
	for name, value := range profile.ObservedBindings {
		symbol, exists := c.ir.Symbols[name]
		if !exists {
			return world{}, invalidInstruction(0, fmt.Sprintf("observed binding %q has no sparse-plan symbol", name))
		}
		if symbol.Type.Class == EntityTypeClass {
			identity, ok := value.(string)
			if !ok || identity == "" {
				return world{}, invalidInstruction(0, fmt.Sprintf("observed entity binding %q requires a non-empty string identity", name))
			}
		}
		if _, err := stableLiteralKey(value); err != nil {
			return world{}, invalidInstruction(0, fmt.Sprintf("observed binding %q has no stable encoding: %v", name, err))
		}
	}
	for index, observed := range profile.ObservedFacts {
		predicate, exists := c.domain.predicates[observed.Predicate]
		if !exists {
			return world{}, &CompileError{Category: ErrorMissingModelCapability, Detail: fmt.Sprintf("observed fact %d references unknown predicate %q", index, observed.Predicate)}
		}
		if len(observed.Arguments) != predicateArity(predicate.Schema) {
			return world{}, invalidInstruction(0, fmt.Sprintf("observed predicate %q expects %d arguments, got %d", observed.Predicate, predicateArity(predicate.Schema), len(observed.Arguments)))
		}
		for argumentIndex, argument := range observed.Arguments {
			if argument.Literal {
				if _, err := stableArgumentKey(argument); err != nil {
					return world{}, invalidInstruction(0, fmt.Sprintf("observed predicate %q argument %d has no stable encoding: %v", observed.Predicate, argumentIndex, err))
				}
				continue
			}
			symbol, symbolExists := c.ir.Symbols[argument.SymbolName]
			expectedType := predicateTermType(predicate.Schema, argumentIndex)
			if !symbolExists || symbol.Type != expectedType {
				return world{}, invalidInstruction(0, fmt.Sprintf("observed predicate %q argument %d requires %s", observed.Predicate, argumentIndex, expectedType))
			}
			if _, grounded := profile.ObservedBindings[argument.SymbolName]; !grounded {
				return world{}, &CompileError{Category: ErrorAmbiguousGrounding, Symbol: argument.SymbolName, Expected: expectedType.String(), Detail: fmt.Sprintf("observed predicate %q symbol %q has no concrete binding", observed.Predicate, argument.SymbolName)}
			}
		}
		atom := groundAtom{Predicate: observed.Predicate, Arguments: append([]Argument(nil), observed.Arguments...)}
		result.facts[atomKey(atom)] = atom
	}
	return result, nil
}

func (c *searchContext) validateCatalog() error {
	for _, node := range c.ir.Nodes {
		switch node.Kind {
		case OutcomeKind, RelationKind, BindingKind:
			predicate, exists := c.domain.predicates[node.Name]
			if !exists {
				return &CompileError{Category: ErrorMissingModelCapability, Source: node.Source, Detail: fmt.Sprintf("missing predicate capability %q", node.Name)}
			}
			if err := c.validateSchemaArguments(node.Source, node.Arguments, node.Binding, predicate.Schema); err != nil {
				return err
			}
		case ActionKind:
			found := false
			var validationErr error
			for _, action := range c.domain.actions {
				if action.Schema.Name == node.Name {
					err := c.validateSchemaArguments(node.Source, node.Arguments, "", action.Schema)
					if err == nil {
						found = true
						break
					}
					validationErr = err
				}
			}
			if !found {
				if validationErr != nil {
					return validationErr
				}
				return &CompileError{Category: ErrorMissingModelCapability, Source: node.Source, Detail: fmt.Sprintf("missing action capability %q", node.Name)}
			}
		default:
			return invalidInstruction(node.Source, fmt.Sprintf("unsupported node kind %d", node.Kind))
		}
	}
	return nil
}

func (c *searchContext) validateSchemaArguments(source int, arguments []Argument, binding string, schema Schema) error {
	if len(arguments) != len(schema.Parameters) {
		return &CompileError{Category: ErrorMissingModelCapability, Source: source, Detail: fmt.Sprintf("model capability %q expects %d arguments, sparse instruction has %d", schema.Name, len(schema.Parameters), len(arguments))}
	}
	for index, parameter := range schema.Parameters {
		argument := arguments[index]
		if parameter.Mode == LiteralParameterMode {
			if !argument.Literal {
				return &CompileError{Category: ErrorMissingModelCapability, Source: source, Detail: fmt.Sprintf("model capability %q parameter %q requires a literal", schema.Name, parameter.Name)}
			}
			if _, err := stableArgumentKey(argument); err != nil {
				return &CompileError{Category: ErrorInvalidInstruction, Source: source, Detail: fmt.Sprintf("model capability %q parameter %q has no stable encoding: %v", schema.Name, parameter.Name, err)}
			}
			continue
		}
		symbol, exists := c.ir.Symbols[argument.SymbolName]
		if argument.Literal || !exists || symbol.Type != parameter.Type {
			actual := Type{}
			if exists {
				actual = symbol.Type
			}
			return &CompileError{Category: ErrorMissingModelCapability, Source: source, Symbol: argument.SymbolName, Expected: parameter.Type.String(), Actual: actual.String(), Detail: fmt.Sprintf("model capability %q parameter %q requires %s, sparse symbol has %s", schema.Name, parameter.Name, parameter.Type, actual)}
		}
	}
	if schema.Kind == BindingKind {
		symbol, exists := c.ir.Symbols[binding]
		if binding == "" || !exists || symbol.Type != schema.Output {
			return &CompileError{Category: ErrorMissingModelCapability, Source: source, Symbol: binding, Expected: schema.Output.String(), Detail: fmt.Sprintf("model capability %q binding requires %s", schema.Name, schema.Output)}
		}
	}
	return nil
}
