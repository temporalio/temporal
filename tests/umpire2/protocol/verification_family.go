package protocol

import (
	"fmt"
	"strings"

	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/tests/umpire2/model"
)

const ProtocolAtomicTarget = "protocol-atomic"

func (p *Protocol) VerificationFamily(options VerificationOptions) (verify.ModelFamily, error) {
	verificationModel, err := p.VerificationModel(options)
	if err != nil {
		return verify.ModelFamily{}, err
	}
	modules := []verify.Module{
		{Name: "workflow", Owner: "workflow"},
		{Name: "activity", Owner: "activity"},
		{Name: "matching", Owner: "matching"},
		{Name: "nexus", Owner: "nexus"},
		{Name: "callback", Owner: "callback"},
	}
	byName := make(map[string]*verify.Module, len(modules))
	for index := range modules {
		byName[modules[index].Name] = &modules[index]
	}
	for _, entity := range verificationModel.Entities {
		owner, err := verificationEntityOwner(entity.Name)
		if err != nil {
			return verify.ModelFamily{}, err
		}
		byName[owner].Entities = append(byName[owner].Entities, entity.Name)
	}
	for _, relation := range verificationModel.Relations {
		owner, err := verificationRelationOwner(relation.Name)
		if err != nil {
			return verify.ModelFamily{}, err
		}
		byName[owner].Relations = append(byName[owner].Relations, relation.Name)
	}
	for _, action := range verificationModel.Actions {
		owner, err := verificationActionOwner(action.Name)
		if err != nil {
			return verify.ModelFamily{}, err
		}
		byName[owner].Actions = append(byName[owner].Actions, action.Name)
	}
	for _, property := range verificationModel.Properties {
		owner, err := verificationPropertyOwner(property.Name)
		if err != nil {
			return verify.ModelFamily{}, err
		}
		byName[owner].Properties = append(byName[owner].Properties, property.Name)
	}
	targetBounds := make(map[string]int, len(verificationModel.Entities))
	for _, entity := range verificationModel.Entities {
		targetBounds[entity.Name] = len(entity.IDs)
	}
	protocolAbstractions := make([]string, len(verificationModel.Abstractions))
	for index, abstraction := range verificationModel.Abstractions {
		protocolAbstractions[index] = abstraction.Name
	}
	foundation := foundationDeliveryVerification()
	verificationModel.Entities = append(verificationModel.Entities, foundation.Model.Entities...)
	verificationModel.Relations = append(verificationModel.Relations, foundation.Model.Relations...)
	verificationModel.Actions = append(verificationModel.Actions, foundation.Model.Actions...)
	verificationModel.Properties = append(verificationModel.Properties, foundation.Model.Properties...)
	verificationModel.Abstractions = append(verificationModel.Abstractions, foundation.Model.Abstractions...)
	verificationModel.Refinements = append(verificationModel.Refinements, foundation.Model.Refinements...)
	modules = append(modules, foundation.Modules...)
	family := verify.ModelFamily{
		Version:      "umpire2/model-family-v1",
		Model:        verificationModel,
		Modules:      modules,
		Interfaces:   foundation.Interfaces,
		Compositions: foundation.Compositions,
		Targets: []verify.VerificationTarget{{
			Name:             ProtocolAtomicTarget,
			Owners:           []verify.CapabilityOwner{"workflow", "activity", "matching", "nexus", "callback"},
			Modules:          []string{"workflow", "activity", "matching", "nexus", "callback"},
			Bounds:           targetBounds,
			Abstractions:     protocolAbstractions,
			IncludeInventory: true,
			BackendRequirements: []string{
				"ivy",
				"p",
				"tla",
			},
		}},
	}
	family.Targets = append(family.Targets, foundation.Targets...)
	if err := verify.ValidateModelFamily(family); err != nil {
		return verify.ModelFamily{}, fmt.Errorf("protocol verification family: %w", err)
	}
	return family, nil
}

func verificationEntityOwner(name string) (string, error) {
	switch name {
	case string(model.WorkflowType), string(model.WorkflowRunType), string(model.WorkflowTaskType):
		return "workflow", nil
	case string(model.ActivityType):
		return "activity", nil
	case string(model.TaskQueueType):
		return "matching", nil
	case string(model.NexusOperationType):
		return "nexus", nil
	case string(model.CallbackType):
		return "callback", nil
	default:
		return "", fmt.Errorf("protocol verification family: entity %q has no capability owner", name)
	}
}

func verificationRelationOwner(name string) (string, error) {
	switch name {
	case string(WorkflowRunsRelation), string(WorkflowRunSuccessorRelation):
		return "workflow", nil
	case string(NexusActivityRelation):
		return "nexus", nil
	case string(ActivityNexusRelation):
		return "activity", nil
	case string(CallbackOperationRelation), string(CallbackHandlerRunRelation):
		return "callback", nil
	default:
		return "", fmt.Errorf("protocol verification family: relation %q has no capability owner", name)
	}
}

func verificationActionOwner(name string) (string, error) {
	switch {
	case strings.HasPrefix(name, string(model.WorkflowType)+"."),
		strings.HasPrefix(name, string(model.WorkflowRunType)+"."),
		strings.HasPrefix(name, string(model.WorkflowTaskType)+"."):
		return "workflow", nil
	case strings.HasPrefix(name, string(model.ActivityType)+"."):
		return "activity", nil
	case strings.HasPrefix(name, string(model.NexusOperationType)+"."),
		strings.HasPrefix(name, "regression.nexus."):
		return "nexus", nil
	default:
		return "", fmt.Errorf("protocol verification family: action %q has no capability owner", name)
	}
}

func verificationPropertyOwner(name string) (string, error) {
	switch {
	case strings.HasPrefix(name, string(model.WorkflowType)+"."):
		return "workflow", nil
	case strings.HasPrefix(name, string(model.ActivityType)+"."), name == "NexusActivityReverseLinkConsistency":
		return "activity", nil
	case strings.HasPrefix(name, string(model.NexusOperationType)+"."),
		strings.HasPrefix(name, "NexusActivity"):
		return "nexus", nil
	default:
		return "", fmt.Errorf("protocol verification family: property %q has no capability owner", name)
	}
}
