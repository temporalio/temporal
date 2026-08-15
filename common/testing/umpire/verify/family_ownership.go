package verify

import (
	"fmt"
	"slices"
)

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
