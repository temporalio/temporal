package regress

import (
	"fmt"
	"slices"
	"strings"
)

func newWorld() world {
	return world{
		facts:     map[string]groundAtom{},
		created:   map[string]bool{},
		resources: map[string]int{},
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

func (c *searchContext) validatePolicies() (map[string]int, error) {
	resources := map[string]int{}
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
			resources[resource] = scope.Policy.Source
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
			Source:      scope.Policy.Source,
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
		if previous, selected := next.resources[resource]; !selected || source < previous {
			next.resources[resource] = source
		}
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

func (c *searchContext) completedResources(selected map[string]int) ([]CompletedResource, error) {
	var result []CompletedResource
	visiting := map[string]bool{}
	visited := map[string]bool{}
	var visit func(string, int, []string) error
	visit = func(name string, source int, chain []string) error {
		if visited[name] {
			return nil
		}
		if visiting[name] {
			return &CompileError{
				Category:     ErrorResourceDependencyCycle,
				Source:       source,
				Actual:       name,
				MissingChain: append(slices.Clone(chain), name),
				Detail:       fmt.Sprintf("resource dependency cycle at %s", name),
			}
		}
		resource, exists := c.domain.resources[name]
		if !exists {
			return &CompileError{
				Category:     ErrorMissingResource,
				Source:       source,
				Expected:     "resource capability",
				Actual:       name,
				MissingChain: append(slices.Clone(chain), name),
				Detail:       fmt.Sprintf("missing resource capability %q", name),
			}
		}
		visiting[name] = true
		for _, dependency := range resource.DependsOn {
			if err := visit(dependency, source, append(slices.Clone(chain), name)); err != nil {
				return err
			}
		}
		delete(visiting, name)
		visited[name] = true
		result = append(result, CompletedResource{Name: name, Realization: resource.Realization, Source: source})
		return nil
	}
	for _, name := range sortedKeys(selected) {
		if err := visit(name, selected[name], nil); err != nil {
			return nil, err
		}
	}
	return result, nil
}
