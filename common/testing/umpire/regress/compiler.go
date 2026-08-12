package regress

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// Profile declares the environment variant against which a sparse plan is compiled.
type Profile struct {
	Name             string          `json:"name"`
	Capabilities     []string        `json:"capabilities,omitempty"`
	Limits           CompileLimits   `json:"limits,omitempty"`
	ObservedFacts    []CompletedAtom `json:"observedFacts,omitempty"`
	ObservedBindings Bindings        `json:"observedBindings,omitempty"`
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
}

// CompletedPolicy is one policy interval over the completed action sequence [Start, End).
type CompletedPolicy struct {
	Name        string     `json:"name"`
	Arguments   []Argument `json:"arguments,omitempty"`
	Realization string     `json:"realization,omitempty"`
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
	resources  map[string]bool
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
	for resource := range policyResources {
		initial.resources[resource] = true
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
	return Suite{
		Name:         plan.Name,
		IR:           ir,
		ModelVersion: domain.version,
		Profile:      profile,
		Paths:        paths,
		PathCount:    len(paths),
	}, nil
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

func newWorld() world {
	return world{
		facts:     map[string]groundAtom{},
		created:   map[string]bool{},
		resources: map[string]bool{},
		ranges:    map[int]actionRange{},
	}
}

func (c *searchContext) satisfyNode(current world, node Node) ([]world, error) {
	start := len(current.actions)
	states, err := c.satisfyNodeRaw(current, node)
	if err != nil {
		return nil, err
	}
	if len(states) == 0 {
		c.recordGap(node, current)
	}
	for index, state := range states {
		state = cloneWorld(state)
		state.ranges[node.ID] = actionRange{start: start, end: len(state.actions)}
		state.milestones = append(state.milestones, CompletedMilestone{
			Node:         node.ID,
			Source:       node.Source,
			Kind:         node.Kind,
			Name:         node.Name,
			Arguments:    append([]Argument(nil), node.Arguments...),
			Binding:      node.Binding,
			BeforeAction: start,
			AfterAction:  len(state.actions),
		})
		states[index] = state
	}
	return states, nil
}

func (c *searchContext) recordGap(node Node, current world) {
	var candidates []string
	missingChain := []string{semanticNode(node)}
	switch node.Kind {
	case ActionKind:
		for _, action := range c.domain.actions {
			if action.Schema.Name == node.Name {
				candidates = append(candidates, action.Realization)
			}
		}
	case OutcomeKind, RelationKind, BindingKind:
		goalArguments := append([]Argument(nil), node.Arguments...)
		if node.Kind == BindingKind {
			goalArguments = append(goalArguments, Symbol(node.Binding))
		}
		goal := groundAtom{Predicate: node.Name, Arguments: goalArguments}
		for _, action := range c.domain.actions {
			for _, effect := range action.Effects {
				if _, matches := unify(effect, goal); matches {
					candidates = append(candidates, action.Schema.Name)
					break
				}
			}
		}
		missingChain = c.shortestMissingChain(current, goal, map[string]bool{})
	default:
		return
	}
	slices.Sort(candidates)
	candidates = slicesCompact(candidates)
	c.lastGap = &CompileError{
		Category:     ErrorUnreachableOutcome,
		Source:       node.Source,
		Predicate:    semanticNode(node),
		Candidates:   candidates,
		MissingChain: missingChain,
		Detail:       fmt.Sprintf("cannot satisfy %s; candidate capabilities: %s", semanticNode(node), strings.Join(candidates, ", ")),
	}
}

func (c *searchContext) shortestMissingChain(current world, goal groundAtom, visiting map[string]bool) []string {
	goalKey := atomKey(goal)
	goalName := semanticAtom(goal)
	if _, exists := current.facts[goalKey]; exists {
		return nil
	}
	if visiting[goalKey] {
		return []string{goalName}
	}
	nextVisiting := cloneSet(visiting)
	nextVisiting[goalKey] = true
	var best []string
	for actionIndex := range c.domain.actions {
		action := &c.domain.actions[actionIndex]
		for _, effect := range action.Effects {
			initial, matches := unify(effect, goal)
			if !matches {
				continue
			}
			for _, grounded := range c.completeBindings(*action, initial, current) {
				preconditions, ok := instantiateAtoms(action.Preconditions, grounded)
				if !ok {
					continue
				}
				for _, precondition := range preconditions {
					if _, exists := current.facts[atomKey(precondition)]; exists {
						continue
					}
					candidate := append([]string{goalName}, c.shortestMissingChain(current, precondition, nextVisiting)...)
					if len(best) == 0 || len(candidate) < len(best) || (len(candidate) == len(best) && strings.Join(candidate, "\x00") < strings.Join(best, "\x00")) {
						best = candidate
					}
				}
			}
		}
	}
	if len(best) == 0 {
		return []string{goalName}
	}
	return best
}

func semanticAtom(atom groundAtom) string {
	arguments := make([]string, len(atom.Arguments))
	for index, argument := range atom.Arguments {
		arguments[index] = argumentKey(argument)
	}
	return atom.Predicate + "(" + strings.Join(arguments, ",") + ")"
}

func semanticNode(node Node) string {
	arguments := make([]string, len(node.Arguments))
	for index, argument := range node.Arguments {
		arguments[index] = argumentKey(argument)
	}
	return node.Name + "(" + strings.Join(arguments, ",") + ")"
}

func slicesCompact(values []string) []string {
	if len(values) < 2 {
		return values
	}
	result := values[:1]
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}

func (c *searchContext) satisfyNodeRaw(current world, node Node) ([]world, error) {
	switch node.Kind {
	case OutcomeKind, RelationKind:
		return c.satisfyGoal(current, groundAtom{Predicate: node.Name, Arguments: node.Arguments}, map[string]bool{}), nil
	case BindingKind:
		arguments := append([]Argument(nil), node.Arguments...)
		arguments = append(arguments, Symbol(node.Binding))
		return c.satisfyGoal(current, groundAtom{Predicate: node.Name, Arguments: arguments}, map[string]bool{}), nil
	case ActionKind:
		return c.satisfyPinned(current, node), nil
	case RequirementKind:
		if !c.profile[node.Name] {
			c.unavailable[node.Name] = true
			return nil, nil
		}
		return []world{current}, nil
	default:
		return nil, invalidInstruction(node.Source, fmt.Sprintf("unsupported normalized node kind %d", node.Kind))
	}
}

func (c *searchContext) validatePolicies() (map[string]bool, error) {
	resources := map[string]bool{}
	for _, scope := range c.ir.Scopes {
		policy, exists := c.domain.policies[scope.Policy.Name]
		if !exists {
			return nil, &CompileError{
				Category: ErrorMissingModelCapability,
				Source:   scope.Policy.Source,
				Detail:   fmt.Sprintf("missing policy capability %q", scope.Policy.Name),
			}
		}
		if err := c.validateSchemaArguments(scope.Policy.Source, scope.Policy.Arguments, "", policy.Schema); err != nil {
			return nil, err
		}
		for _, required := range policy.Requires {
			if !c.profile[required] {
				return nil, &CompileError{
					Category: ErrorUnavailableEnvironmentCapability,
					Source:   scope.Policy.Source,
					Detail:   fmt.Sprintf("unavailable environment capability: %s", required),
				}
			}
		}
		for _, resource := range policy.Resources {
			resources[resource] = true
		}
	}
	return resources, nil
}

func (c *searchContext) completedPolicies(state world) []CompletedPolicy {
	result := make([]CompletedPolicy, 0, len(c.ir.Scopes))
	for _, scope := range c.ir.Scopes {
		policy := c.domain.policies[scope.Policy.Name]
		start := len(state.actions)
		end := 0
		for _, node := range scope.Body {
			span, exists := state.ranges[node]
			if !exists {
				continue
			}
			if span.start < start {
				start = span.start
			}
			if span.end > end {
				end = span.end
			}
		}
		result = append(result, CompletedPolicy{
			Name:        scope.Policy.Name,
			Arguments:   append([]Argument(nil), scope.Policy.Arguments...),
			Realization: policy.Realization,
			Start:       start,
			End:         end,
		})
	}
	return result
}

func (c *searchContext) satisfyPinned(current world, node Node) []world {
	var result []world
	for actionIndex := range c.domain.actions {
		action := &c.domain.actions[actionIndex]
		if action.Schema.Name != node.Name || len(action.Schema.Parameters) != len(node.Arguments) {
			continue
		}
		bindings := map[string]Argument{}
		for index, parameter := range action.Schema.Parameters {
			bindings[parameter.Name] = node.Arguments[index]
		}
		for _, grounded := range c.completeBindings(*action, bindings, current) {
			preconditions, ok := instantiateAtoms(action.Preconditions, grounded)
			if !ok {
				continue
			}
			states := []world{current}
			for _, precondition := range preconditions {
				var next []world
				for _, state := range states {
					next = append(next, c.satisfyGoal(state, precondition, map[string]bool{})...)
				}
				states = deduplicateWorlds(next)
			}
			for _, state := range states {
				if applied, applyOK := c.applyAction(state, *action, grounded, node.Source); applyOK {
					result = append(result, applied)
				}
			}
		}
	}
	return deduplicateWorlds(result)
}

func (c *searchContext) satisfyGoal(current world, goal groundAtom, stack map[string]bool) []world {
	if _, exists := current.facts[atomKey(goal)]; exists {
		return []world{current}
	}
	searchKey := worldKey(current) + "|goal:" + atomKey(goal)
	if stack[searchKey] {
		return nil
	}
	nextStack := cloneSet(stack)
	nextStack[searchKey] = true

	var result []world
	for actionIndex := range c.domain.actions {
		action := &c.domain.actions[actionIndex]
		for _, effect := range action.Effects {
			bindings, matches := unify(effect, goal)
			if !matches {
				continue
			}
			for _, grounded := range c.completeBindings(*action, bindings, current) {
				preconditions, ok := instantiateAtoms(action.Preconditions, grounded)
				if !ok {
					continue
				}
				states := []world{current}
				for _, precondition := range preconditions {
					var next []world
					for _, state := range states {
						next = append(next, c.satisfyGoal(state, precondition, nextStack)...)
					}
					states = deduplicateWorlds(next)
					if len(states) == 0 {
						break
					}
				}
				for _, state := range states {
					applied, applyOK := c.applyAction(state, *action, grounded, 0)
					if !applyOK || worldKey(applied) == worldKey(state) {
						continue
					}
					if _, reached := applied.facts[atomKey(goal)]; reached {
						result = append(result, applied)
					}
				}
			}
		}
	}
	return deduplicateWorlds(result)
}

func (c *searchContext) completeBindings(action ActionCapability, initial map[string]Argument, current world) []map[string]Argument {
	grounded := cloneBindings(initial)
	for name, value := range action.Fixed {
		fixed := Literal(value)
		if previous, exists := grounded[name]; exists && argumentKey(previous) != argumentKey(fixed) {
			return nil
		}
		grounded[name] = fixed
	}
	bindings := deriveBindingsFromFacts(action.Preconditions, []map[string]Argument{grounded}, current)
	for _, variable := range action.Variables {
		var candidates []Argument
		for _, symbol := range c.ir.Symbols {
			if symbol.Type == variable.Type {
				candidates = append(candidates, Symbol(symbol.Name))
			}
		}
		slices.SortFunc(candidates, func(left, right Argument) int {
			return strings.Compare(left.SymbolName, right.SymbolName)
		})
		var expanded []map[string]Argument
		for _, binding := range bindings {
			if _, exists := binding[variable.Name]; exists {
				expanded = append(expanded, binding)
				continue
			}
			for _, candidate := range candidates {
				candidateBindings := cloneBindings(binding)
				candidateBindings[variable.Name] = candidate
				expanded = append(expanded, candidateBindings)
			}
		}
		bindings = expanded
		if len(bindings) == 0 {
			break
		}
	}
	return bindings
}

func deriveBindingsFromFacts(templates []AtomTemplate, seeds []map[string]Argument, current world) []map[string]Argument {
	bindings := append([]map[string]Argument(nil), seeds...)
	for _, template := range templates {
		for _, fact := range current.facts {
			derived, matches := unify(template, fact)
			if !matches {
				continue
			}
			for _, seed := range append([]map[string]Argument(nil), bindings...) {
				merged, ok := mergeBindings(seed, derived)
				if ok {
					bindings = append(bindings, merged)
				}
			}
		}
	}
	return deduplicateBindings(bindings)
}

func mergeBindings(left, right map[string]Argument) (map[string]Argument, bool) {
	result := cloneBindings(left)
	for name, argument := range right {
		if previous, exists := result[name]; exists && argumentKey(previous) != argumentKey(argument) {
			return nil, false
		}
		result[name] = argument
	}
	return result, true
}

func deduplicateBindings(bindings []map[string]Argument) []map[string]Argument {
	seen := map[string]bool{}
	result := make([]map[string]Argument, 0, len(bindings))
	for _, binding := range bindings {
		names := sortedKeys(binding)
		parts := make([]string, len(names))
		for index, name := range names {
			parts[index] = name + ":" + argumentKey(binding[name])
		}
		key := strings.Join(parts, ",")
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, binding)
	}
	return result
}

func (c *searchContext) applyAction(current world, action ActionCapability, bindings map[string]Argument, source int) (world, bool) {
	if !c.actionAvailable(action) {
		return world{}, false
	}
	for _, variable := range action.Variables {
		if variable.Binding != FreshBinding {
			continue
		}
		argument, exists := bindings[variable.Name]
		if !exists || argument.SymbolName == "" || current.created[argument.SymbolName] {
			return world{}, false
		}
	}
	preconditions, ok := instantiateAtoms(action.Preconditions, bindings)
	if !ok {
		return world{}, false
	}
	for _, precondition := range preconditions {
		if _, exists := current.facts[atomKey(precondition)]; !exists {
			return world{}, false
		}
	}
	effects, ok := instantiateAtoms(action.Effects, bindings)
	if !ok {
		return world{}, false
	}
	next := cloneWorld(current)
	for _, variable := range action.Variables {
		if variable.Binding == FreshBinding {
			next.created[bindings[variable.Name].SymbolName] = true
		}
	}
	for _, effect := range effects {
		c.applyEffect(&next, effect)
	}
	for _, resource := range action.Resources {
		next.resources[resource] = true
	}
	arguments := make([]Argument, len(action.Schema.Parameters))
	for index, parameter := range action.Schema.Parameters {
		arguments[index] = bindings[parameter.Name]
	}
	next.actions = append(next.actions, CompletedAction{
		Name:        action.Schema.Name,
		Arguments:   arguments,
		Realization: action.Realization,
		Source:      source,
	})
	next.steps = append(next.steps, CompletedStep{
		Action:        next.actions[len(next.actions)-1],
		Mode:          action.Mode,
		Preconditions: completedAtoms(preconditions),
		Effects:       completedAtoms(effects),
	})
	return next, true
}

func completedAtoms(atoms []groundAtom) []CompletedAtom {
	result := make([]CompletedAtom, len(atoms))
	for index, atom := range atoms {
		result[index] = CompletedAtom{Predicate: atom.Predicate, Arguments: append([]Argument(nil), atom.Arguments...)}
	}
	return result
}

func (c *searchContext) applyEffect(target *world, effect groundAtom) {
	if predicate, exists := c.domain.predicates[effect.Predicate]; exists && len(predicate.ExclusiveBy) > 0 {
		for key, previous := range target.facts {
			if previous.Predicate != effect.Predicate {
				continue
			}
			matches := true
			for _, index := range predicate.ExclusiveBy {
				if argumentKey(previous.Arguments[index]) != argumentKey(effect.Arguments[index]) {
					matches = false
					break
				}
			}
			if matches {
				delete(target.facts, key)
			}
		}
	}
	target.facts[atomKey(effect)] = effect
}

func (c *searchContext) actionAvailable(action ActionCapability) bool {
	available := true
	for _, required := range action.Requires {
		if !c.profile[required] {
			c.unavailable[required] = true
			available = false
		}
	}
	for _, resourceName := range action.Resources {
		resource, exists := c.domain.resources[resourceName]
		if !exists {
			continue
		}
		for _, required := range resource.Requires {
			if !c.profile[required] {
				c.unavailable[required] = true
				available = false
			}
		}
	}
	return available
}

func (c *searchContext) completedResources(selected map[string]bool) ([]CompletedResource, error) {
	var result []CompletedResource
	visiting := map[string]bool{}
	visited := map[string]bool{}
	var visit func(string) error
	visit = func(name string) error {
		if visited[name] {
			return nil
		}
		if visiting[name] {
			return fmt.Errorf("resource dependency cycle at %s", name)
		}
		resource, exists := c.domain.resources[name]
		if !exists {
			return fmt.Errorf("missing resource capability %q", name)
		}
		visiting[name] = true
		for _, dependency := range resource.DependsOn {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		delete(visiting, name)
		visited[name] = true
		result = append(result, CompletedResource{Name: name, Realization: resource.Realization})
		return nil
	}
	for _, name := range sortedKeys(selected) {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func unify(template AtomTemplate, goal groundAtom) (map[string]Argument, bool) {
	if template.Predicate != goal.Predicate || len(template.Terms) != len(goal.Arguments) {
		return nil, false
	}
	bindings := map[string]Argument{}
	for index, term := range template.Terms {
		argument := goal.Arguments[index]
		if term.Literal {
			if argumentKey(Literal(term.Value)) != argumentKey(argument) {
				return nil, false
			}
			continue
		}
		if previous, exists := bindings[term.Variable]; exists && argumentKey(previous) != argumentKey(argument) {
			return nil, false
		}
		bindings[term.Variable] = argument
	}
	return bindings, true
}

func instantiateAtoms(templates []AtomTemplate, bindings map[string]Argument) ([]groundAtom, bool) {
	result := make([]groundAtom, 0, len(templates))
	for _, template := range templates {
		atom := groundAtom{Predicate: template.Predicate, Arguments: make([]Argument, len(template.Terms))}
		for index, term := range template.Terms {
			if term.Literal {
				atom.Arguments[index] = Literal(term.Value)
				continue
			}
			argument, exists := bindings[term.Variable]
			if !exists {
				return nil, false
			}
			atom.Arguments[index] = argument
		}
		result = append(result, atom)
	}
	return result, true
}

func cloneWorld(source world) world {
	result := world{
		facts:      make(map[string]groundAtom, len(source.facts)),
		created:    make(map[string]bool, len(source.created)),
		actions:    append([]CompletedAction(nil), source.actions...),
		steps:      append([]CompletedStep(nil), source.steps...),
		resources:  make(map[string]bool, len(source.resources)),
		ranges:     make(map[int]actionRange, len(source.ranges)),
		milestones: append([]CompletedMilestone(nil), source.milestones...),
	}
	for key, value := range source.facts {
		result.facts[key] = value
	}
	for key, value := range source.created {
		result.created[key] = value
	}
	for key, value := range source.resources {
		result.resources[key] = value
	}
	for key, value := range source.ranges {
		result.ranges[key] = value
	}
	return result
}

func cloneBindings(source map[string]Argument) map[string]Argument {
	result := make(map[string]Argument, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func cloneSet(source map[string]bool) map[string]bool {
	result := make(map[string]bool, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func stringSet(values []string) map[string]bool {
	result := make(map[string]bool, len(values))
	for _, value := range values {
		result[value] = true
	}
	return result
}

func atomKey(atom groundAtom) string {
	parts := make([]string, len(atom.Arguments))
	for index, argument := range atom.Arguments {
		parts[index] = argumentKey(argument)
	}
	return atom.Predicate + "(" + strings.Join(parts, ",") + ")"
}

func argumentKey(argument Argument) string {
	key, err := stableArgumentKey(argument)
	if err != nil {
		return fmt.Sprintf("=!unsupported:%T", argument.Value)
	}
	return key
}

func stableArgumentKey(argument Argument) (string, error) {
	if !argument.Literal {
		return "$" + argument.SymbolName, nil
	}
	encoded, err := stableLiteralKey(argument.Value)
	if err != nil {
		return "", err
	}
	return "=" + encoded, nil
}

func stableLiteralKey(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func worldKey(value world) string {
	facts := sortedKeys(value.facts)
	created := sortedKeys(value.created)
	return strings.Join(facts, ";") + "|created:" + strings.Join(created, ",")
}

func deduplicateWorlds(values []world) []world {
	seen := map[string]bool{}
	result := make([]world, 0, len(values))
	for _, value := range values {
		key := worldKey(value) + "|actions:" + completedActionsKey(value.actions)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, value)
	}
	return result
}

func deduplicatePaths(paths []CompletedPath, domain *Domain) []CompletedPath {
	seen := map[string]bool{}
	result := make([]CompletedPath, 0, len(paths))
	for _, path := range paths {
		key := reducedActionsKey(path.Actions, domain) + "|policies:" + completedPoliciesKey(path.Policies)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, path)
	}
	return result
}

func reducedActionsKey(actions []CompletedAction, domain *Domain) string {
	reduced := append([]CompletedAction(nil), actions...)
	orders := actionOrders(domain)
	for changed := true; changed; {
		changed = false
		for index := 0; index+1 < len(reduced); index++ {
			left := reduced[index]
			right := reduced[index+1]
			if !actionsIndependent(left.Name, right.Name, domain) || compareActions(left, right, orders) <= 0 {
				continue
			}
			reduced[index], reduced[index+1] = right, left
			changed = true
		}
	}
	return completedActionsKey(reduced)
}

func actionOrders(domain *Domain) map[string]int {
	orders := make(map[string]int, len(domain.actions)*2)
	for _, action := range domain.actions {
		if _, exists := orders[action.Schema.Name]; !exists {
			orders[action.Schema.Name] = action.catalogOrder
		}
		orders[action.Schema.Name+"\x00"+action.Realization] = action.catalogOrder
	}
	return orders
}

func compareActions(left, right CompletedAction, orders map[string]int) int {
	leftOrder := completedActionOrder(left, orders)
	rightOrder := completedActionOrder(right, orders)
	if leftOrder < rightOrder {
		return -1
	}
	if leftOrder > rightOrder {
		return 1
	}
	leftKey := completedActionsKey([]CompletedAction{left})
	rightKey := completedActionsKey([]CompletedAction{right})
	return strings.Compare(leftKey, rightKey)
}

func completedActionOrder(action CompletedAction, orders map[string]int) int {
	if order, exists := orders[action.Name+"\x00"+action.Realization]; exists {
		return order
	}
	return orders[action.Name]
}

func actionsIndependent(left, right string, domain *Domain) bool {
	if left == right {
		return false
	}
	leftFound := false
	rightFound := false
	for _, action := range domain.actions {
		switch action.Schema.Name {
		case left:
			leftFound = true
			if !containsString(action.IndependentOf, right) {
				return false
			}
		case right:
			rightFound = true
			if !containsString(action.IndependentOf, left) {
				return false
			}
		default:
			continue
		}
	}
	return leftFound && rightFound
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func completedActionsKey(actions []CompletedAction) string {
	parts := make([]string, len(actions))
	for index, action := range actions {
		arguments := make([]string, len(action.Arguments))
		for argumentIndex, argument := range action.Arguments {
			arguments[argumentIndex] = argumentKey(argument)
		}
		parts[index] = action.Name + "[" + action.Realization + "](" + strings.Join(arguments, ",") + ")"
	}
	return strings.Join(parts, "->")
}

func completedPoliciesKey(policies []CompletedPolicy) string {
	parts := make([]string, len(policies))
	for index, policy := range policies {
		arguments := make([]string, len(policy.Arguments))
		for argumentIndex, argument := range policy.Arguments {
			arguments[argumentIndex] = argumentKey(argument)
		}
		parts[index] = fmt.Sprintf("%s[%s](%s):%d-%d", policy.Name, policy.Realization, strings.Join(arguments, ","), policy.Start, policy.End)
	}
	return strings.Join(parts, ";")
}

func sortCompletedPaths(paths []CompletedPath, domain *Domain) {
	orders := actionOrders(domain)
	slices.SortFunc(paths, func(leftPath, rightPath CompletedPath) int {
		if len(leftPath.Actions) != len(rightPath.Actions) {
			return cmp.Compare(len(leftPath.Actions), len(rightPath.Actions))
		}
		if len(leftPath.Created) != len(rightPath.Created) {
			return cmp.Compare(len(leftPath.Created), len(rightPath.Created))
		}
		if len(leftPath.Resources) != len(rightPath.Resources) {
			return cmp.Compare(len(leftPath.Resources), len(rightPath.Resources))
		}
		for index := range leftPath.Actions {
			left := leftPath.Actions[index]
			right := rightPath.Actions[index]
			leftOrder := completedActionOrder(left, orders)
			rightOrder := completedActionOrder(right, orders)
			if leftOrder != rightOrder {
				return cmp.Compare(leftOrder, rightOrder)
			}
		}
		return strings.Compare(completedActionsKey(leftPath.Actions), completedActionsKey(rightPath.Actions))
	})
}

func topologicalOrders(nodes int, edges []Edge) [][]int {
	adjacent := make([][]int, nodes)
	indegree := make([]int, nodes)
	for _, edge := range edges {
		adjacent[edge.From] = append(adjacent[edge.From], edge.To)
		indegree[edge.To]++
	}
	var result [][]int
	var visit func([]int, []int)
	visit = func(prefix []int, degrees []int) {
		if len(prefix) == nodes {
			result = append(result, append([]int(nil), prefix...))
			return
		}
		selected := make(map[int]bool, len(prefix))
		for _, node := range prefix {
			selected[node] = true
		}
		for node := 0; node < nodes; node++ {
			if selected[node] || degrees[node] != 0 {
				continue
			}
			nextDegrees := append([]int(nil), degrees...)
			nextDegrees[node] = -1
			for _, destination := range adjacent[node] {
				nextDegrees[destination]--
			}
			visit(append(prefix, node), nextDegrees)
		}
	}
	visit(nil, indegree)
	if nodes == 0 {
		return [][]int{{}}
	}
	return result
}

func sortedKeys[V any](values map[string]V) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	slices.Sort(result)
	return result
}
