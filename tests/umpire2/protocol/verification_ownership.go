package protocol

import (
	"fmt"
	"slices"
	"strings"

	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/tests/umpire2/model"
)

type verificationOwnershipModule struct {
	Name             string
	Owner            verify.CapabilityOwner
	Entities         []string
	Relations        []string
	Actions          []string
	ActionPrefixes   []string
	Properties       []string
	PropertyPrefixes []string
}

type verificationDeclarationKind string

const (
	verificationEntityKind   verificationDeclarationKind = "entity"
	verificationRelationKind verificationDeclarationKind = "relation"
	verificationActionKind   verificationDeclarationKind = "action"
	verificationPropertyKind verificationDeclarationKind = "property"
)

type verificationOwnedPrefix struct {
	prefix string
	module int
}

type verificationOwnership struct {
	declarations []verificationOwnershipModule
	exact        map[verificationDeclarationKind]map[string]int
	prefixes     map[verificationDeclarationKind][]verificationOwnedPrefix
}

func defaultVerificationOwnership() (*verificationOwnership, error) {
	return compileVerificationOwnership([]verificationOwnershipModule{
		{
			Name:             "workflow",
			Owner:            "workflow",
			Entities:         []string{string(model.WorkflowType), string(model.WorkflowRunType), string(model.WorkflowTaskType)},
			Relations:        []string{string(WorkflowRunsRelation), string(WorkflowRunSuccessorRelation)},
			ActionPrefixes:   []string{string(model.WorkflowType) + ".", string(model.WorkflowRunType) + ".", string(model.WorkflowTaskType) + "."},
			PropertyPrefixes: []string{string(model.WorkflowType) + "."},
		},
		{
			Name:             "activity",
			Owner:            "activity",
			Entities:         []string{string(model.ActivityType)},
			Relations:        []string{string(ActivityNexusRelation)},
			ActionPrefixes:   []string{string(model.ActivityType) + "."},
			Properties:       []string{"NexusActivityReverseLinkConsistency"},
			PropertyPrefixes: []string{string(model.ActivityType) + "."},
		},
		{
			Name:     "matching",
			Owner:    "matching",
			Entities: []string{string(model.TaskQueueType)},
		},
		{
			Name:             "nexus",
			Owner:            "nexus",
			Entities:         []string{string(model.NexusOperationType)},
			Relations:        []string{string(NexusActivityRelation)},
			ActionPrefixes:   []string{string(model.NexusOperationType) + ".", "regression.nexus."},
			PropertyPrefixes: []string{string(model.NexusOperationType) + ".", "NexusActivity"},
		},
		{
			Name:      "callback",
			Owner:     "callback",
			Entities:  []string{string(model.CallbackType)},
			Relations: []string{string(CallbackOperationRelation), string(CallbackHandlerRunRelation)},
		},
	})
}

func compileVerificationOwnership(declarations []verificationOwnershipModule) (*verificationOwnership, error) {
	compiled := &verificationOwnership{
		declarations: make([]verificationOwnershipModule, len(declarations)),
		exact:        map[verificationDeclarationKind]map[string]int{},
		prefixes:     map[verificationDeclarationKind][]verificationOwnedPrefix{},
	}
	moduleNames := make(map[string]struct{}, len(declarations))
	for moduleIndex, declaration := range declarations {
		if declaration.Name == "" || declaration.Owner == "" {
			return nil, fmt.Errorf("verification ownership module %d has no name or owner", moduleIndex)
		}
		if _, exists := moduleNames[declaration.Name]; exists {
			return nil, fmt.Errorf("duplicate verification ownership module %q", declaration.Name)
		}
		moduleNames[declaration.Name] = struct{}{}
		compiled.declarations[moduleIndex] = cloneVerificationOwnershipModule(declaration)
		for _, selection := range []struct {
			kind   verificationDeclarationKind
			exact  []string
			prefix []string
		}{
			{kind: verificationEntityKind, exact: declaration.Entities},
			{kind: verificationRelationKind, exact: declaration.Relations},
			{kind: verificationActionKind, exact: declaration.Actions, prefix: declaration.ActionPrefixes},
			{kind: verificationPropertyKind, exact: declaration.Properties, prefix: declaration.PropertyPrefixes},
		} {
			for _, name := range selection.exact {
				if err := compiled.addExact(selection.kind, name, moduleIndex); err != nil {
					return nil, err
				}
			}
			for _, prefix := range selection.prefix {
				if err := compiled.addPrefix(selection.kind, prefix, moduleIndex); err != nil {
					return nil, err
				}
			}
		}
	}
	return compiled, nil
}

func cloneVerificationOwnershipModule(declaration verificationOwnershipModule) verificationOwnershipModule {
	declaration.Entities = slices.Clone(declaration.Entities)
	declaration.Relations = slices.Clone(declaration.Relations)
	declaration.Actions = slices.Clone(declaration.Actions)
	declaration.ActionPrefixes = slices.Clone(declaration.ActionPrefixes)
	declaration.Properties = slices.Clone(declaration.Properties)
	declaration.PropertyPrefixes = slices.Clone(declaration.PropertyPrefixes)
	return declaration
}

func (o *verificationOwnership) addExact(kind verificationDeclarationKind, name string, module int) error {
	if name == "" {
		return fmt.Errorf("verification ownership %s selector is empty", kind)
	}
	if o.exact[kind] == nil {
		o.exact[kind] = map[string]int{}
	}
	if previous, exists := o.exact[kind][name]; exists {
		return fmt.Errorf("%s %q is owned by both %q and %q", kind, name, o.declarations[previous].Name, o.declarations[module].Name)
	}
	o.exact[kind][name] = module
	return nil
}

func (o *verificationOwnership) addPrefix(kind verificationDeclarationKind, prefix string, module int) error {
	if prefix == "" {
		return fmt.Errorf("verification ownership %s prefix is empty", kind)
	}
	for _, existing := range o.prefixes[kind] {
		if existing.prefix == prefix {
			return fmt.Errorf("%s prefix %q is owned by both %q and %q", kind, prefix, o.declarations[existing.module].Name, o.declarations[module].Name)
		}
	}
	o.prefixes[kind] = append(o.prefixes[kind], verificationOwnedPrefix{prefix: prefix, module: module})
	return nil
}

func (o *verificationOwnership) Assign(model verify.Model) ([]verify.Module, error) {
	modules := make([]verify.Module, len(o.declarations))
	for index, declaration := range o.declarations {
		modules[index] = verify.Module{Name: declaration.Name, Owner: declaration.Owner}
	}
	for _, entity := range model.Entities {
		module, err := o.owner(verificationEntityKind, entity.Name)
		if err != nil {
			return nil, err
		}
		modules[module].Entities = append(modules[module].Entities, entity.Name)
	}
	for _, relation := range model.Relations {
		module, err := o.owner(verificationRelationKind, relation.Name)
		if err != nil {
			return nil, err
		}
		modules[module].Relations = append(modules[module].Relations, relation.Name)
	}
	for _, action := range model.Actions {
		module, err := o.owner(verificationActionKind, action.Name)
		if err != nil {
			return nil, err
		}
		modules[module].Actions = append(modules[module].Actions, action.Name)
	}
	for _, property := range model.Properties {
		module, err := o.owner(verificationPropertyKind, property.Name)
		if err != nil {
			return nil, err
		}
		modules[module].Properties = append(modules[module].Properties, property.Name)
	}
	return modules, nil
}

func (o *verificationOwnership) owner(kind verificationDeclarationKind, name string) (int, error) {
	if module, exists := o.exact[kind][name]; exists {
		return module, nil
	}
	selected := -1
	selectedLength := -1
	for _, candidate := range o.prefixes[kind] {
		if strings.HasPrefix(name, candidate.prefix) && len(candidate.prefix) > selectedLength {
			selected = candidate.module
			selectedLength = len(candidate.prefix)
		}
	}
	if selected < 0 {
		return 0, fmt.Errorf("protocol verification family: %s %q has no capability owner", kind, name)
	}
	return selected, nil
}
