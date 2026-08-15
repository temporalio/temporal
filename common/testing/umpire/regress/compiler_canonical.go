package regress

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

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
		resources:  make(map[string]int, len(source.resources)),
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
