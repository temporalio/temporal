package protocol

import (
	"fmt"
	"maps"
	"strings"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/tests/umpire2/internal/model"
)

const ProtocolAtomicTarget = "protocol-atomic"

func (p *Protocol) VerificationFamily(options VerificationOptions) (verify.ModelFamily, error) {
	protocolModel, err := p.VerificationModel(options)
	if err != nil {
		return verify.ModelFamily{}, err
	}
	familyOptions := options
	familyOptions.Bounds = maps.Clone(options.Bounds)
	if familyOptions.Bounds == nil {
		familyOptions.Bounds = make(map[umpire.EntityType]int)
	}
	if familyOptions.Bounds[model.WorkflowTaskType] < 2 {
		familyOptions.Bounds[model.WorkflowTaskType] = 2
	}
	if familyOptions.Bounds[model.NexusOperationType] < 2 {
		familyOptions.Bounds[model.NexusOperationType] = 2
	}
	if familyOptions.Bounds[model.ActivityType] < 2 {
		familyOptions.Bounds[model.ActivityType] = 2
	}
	if familyOptions.Bounds[model.WorkflowRunType] < 2 {
		familyOptions.Bounds[model.WorkflowRunType] = 2
	}
	if familyOptions.Bounds[model.CallbackType] < 2 {
		familyOptions.Bounds[model.CallbackType] = 2
	}
	verificationModel, err := p.VerificationModel(familyOptions)
	if err != nil {
		return verify.ModelFamily{}, err
	}
	ownership, err := defaultVerificationOwnership()
	if err != nil {
		return verify.ModelFamily{}, fmt.Errorf("protocol verification family: %w", err)
	}
	modules, err := ownership.Assign(verificationModel)
	if err != nil {
		return verify.ModelFamily{}, err
	}
	targetBounds := make(map[string]int, len(protocolModel.Entities))
	for _, entity := range protocolModel.Entities {
		targetBounds[entity.Name] = len(entity.IDs)
	}
	protocolAbstractions := make([]string, len(verificationModel.Abstractions))
	for index, abstraction := range verificationModel.Abstractions {
		protocolAbstractions[index] = abstraction.Name
	}
	foundation := foundationDeliveryVerification()
	activityDelivery := activityDeliveryVerification()
	activityDelivery.Targets[0].Actions = verificationModuleActions(modules, "activity")
	workflowDelivery := workflowDeliveryVerification()
	workflowTaskSpeculative := workflowTaskSpeculativeVerification()
	foundation.Targets[0].Abstractions = append(foundation.Targets[0].Abstractions, "workflow.task.create-speculative-direct")
	activityDelivery.Targets[0].Abstractions = append(activityDelivery.Targets[0].Abstractions, "workflow.task.create-speculative-direct")
	workflowDelivery.Targets[0].Abstractions = append(workflowDelivery.Targets[0].Abstractions, "workflow.task.create-speculative-direct")
	workflowTaskSpeculative.Targets[0].Actions = verificationModuleActionsWithPrefixes(
		modules, "workflow", "WorkflowRun.", "WorkflowTask.",
	)
	nexus := nexusVerification(verificationModel)
	nexus.Targets[0].Actions = verificationModuleActionsWithPrefixes(modules, "nexus", "NexusOperation.")
	nexus.Targets[0].Properties = verificationModulePropertiesWithPrefixes(modules, "nexus", "NexusOperation.")
	callback := callbackVerification(verificationModel)
	callback.Targets[0].Actions = verificationModuleActionsWithPrefixes(modules, "nexus", "NexusOperation.")
	routing := deliveryRoutingVerification()
	backlog := deliveryBacklogVerification()
	verificationModel.Entities = append(verificationModel.Entities, foundation.Model.Entities...)
	verificationModel.Relations = append(verificationModel.Relations, foundation.Model.Relations...)
	verificationModel.Relations = append(verificationModel.Relations, activityDelivery.Model.Relations...)
	verificationModel.Relations = append(verificationModel.Relations, workflowDelivery.Model.Relations...)
	verificationModel.Relations = append(verificationModel.Relations, workflowTaskSpeculative.Model.Relations...)
	verificationModel.Entities = append(verificationModel.Entities, nexus.Model.Entities...)
	verificationModel.Relations = append(verificationModel.Relations, nexus.Model.Relations...)
	verificationModel.Entities = append(verificationModel.Entities, callback.Model.Entities...)
	verificationModel.Relations = append(verificationModel.Relations, callback.Model.Relations...)
	verificationModel.Entities = append(verificationModel.Entities, routing.Model.Entities...)
	verificationModel.Relations = append(verificationModel.Relations, routing.Model.Relations...)
	verificationModel.Entities = append(verificationModel.Entities, backlog.Model.Entities...)
	verificationModel.Relations = append(verificationModel.Relations, backlog.Model.Relations...)
	verificationModel.Actions = append(verificationModel.Actions, foundation.Model.Actions...)
	verificationModel.Actions = append(verificationModel.Actions, activityDelivery.Model.Actions...)
	verificationModel.Actions = append(verificationModel.Actions, workflowDelivery.Model.Actions...)
	verificationModel.Actions = append(verificationModel.Actions, workflowTaskSpeculative.Model.Actions...)
	verificationModel.Actions = append(verificationModel.Actions, nexus.Model.Actions...)
	verificationModel.Actions = append(verificationModel.Actions, callback.Model.Actions...)
	verificationModel.Actions = append(verificationModel.Actions, routing.Model.Actions...)
	verificationModel.Actions = append(verificationModel.Actions, backlog.Model.Actions...)
	verificationModel.Properties = append(verificationModel.Properties, foundation.Model.Properties...)
	verificationModel.Properties = append(verificationModel.Properties, activityDelivery.Model.Properties...)
	verificationModel.Properties = append(verificationModel.Properties, workflowDelivery.Model.Properties...)
	verificationModel.Properties = append(verificationModel.Properties, workflowTaskSpeculative.Model.Properties...)
	verificationModel.Properties = append(verificationModel.Properties, nexus.Model.Properties...)
	verificationModel.Properties = append(verificationModel.Properties, callback.Model.Properties...)
	verificationModel.Properties = append(verificationModel.Properties, routing.Model.Properties...)
	verificationModel.Properties = append(verificationModel.Properties, backlog.Model.Properties...)
	verificationModel.Abstractions = append(verificationModel.Abstractions, foundation.Model.Abstractions...)
	verificationModel.Abstractions = append(verificationModel.Abstractions, activityDelivery.Model.Abstractions...)
	verificationModel.Abstractions = append(verificationModel.Abstractions, workflowDelivery.Model.Abstractions...)
	verificationModel.Abstractions = append(verificationModel.Abstractions, workflowTaskSpeculative.Model.Abstractions...)
	verificationModel.Abstractions = append(verificationModel.Abstractions, nexus.Model.Abstractions...)
	verificationModel.Abstractions = append(verificationModel.Abstractions, callback.Model.Abstractions...)
	verificationModel.Abstractions = append(verificationModel.Abstractions, routing.Model.Abstractions...)
	verificationModel.Abstractions = append(verificationModel.Abstractions, backlog.Model.Abstractions...)
	verificationModel.Refinements = append(verificationModel.Refinements, foundation.Model.Refinements...)
	verificationModel.Refinements = append(verificationModel.Refinements, activityDelivery.Model.Refinements...)
	verificationModel.Refinements = append(verificationModel.Refinements, workflowDelivery.Model.Refinements...)
	verificationModel.Refinements = append(verificationModel.Refinements, workflowTaskSpeculative.Model.Refinements...)
	verificationModel.Refinements = append(verificationModel.Refinements, nexus.Model.Refinements...)
	verificationModel.Refinements = append(verificationModel.Refinements, callback.Model.Refinements...)
	verificationModel.Refinements = append(verificationModel.Refinements, routing.Model.Refinements...)
	verificationModel.Refinements = append(verificationModel.Refinements, backlog.Model.Refinements...)
	modules = append(modules, foundation.Modules...)
	modules = append(modules, activityDelivery.Modules...)
	modules = append(modules, workflowDelivery.Modules...)
	modules = append(modules, workflowTaskSpeculative.Modules...)
	modules = append(modules, nexus.Modules...)
	modules = append(modules, callback.Modules...)
	modules = append(modules, routing.Modules...)
	modules = append(modules, backlog.Modules...)
	family := verify.ModelFamily{
		Version:    "umpire2/model-family-v1",
		Model:      verificationModel,
		Modules:    modules,
		Interfaces: foundation.Interfaces,
		RefinementMaps: append(
			append(foundation.Refinements, activityDelivery.Refinements...),
			workflowDelivery.Refinements...,
		),
		Compositions: append(
			append(foundation.Compositions, activityDelivery.Compositions...),
			workflowDelivery.Compositions...,
		),
		Targets: []verify.VerificationTarget{{
			Name:             ProtocolAtomicTarget,
			Owners:           []verify.CapabilityOwner{"workflow", "activity", "matching", "nexus", "callback"},
			Modules:          []string{"workflow", "activity", "matching", "nexus", "callback"},
			Bounds:           targetBounds,
			Abstractions:     protocolAbstractions,
			Omissions:        verificationActionOmissions(callback.Model.Actions, "checked by the Callback integration targets"),
			IncludeInventory: true,
			BackendRequirements: []string{
				"fizz",
				"ivy",
				"p",
				"tla",
			},
		}},
	}
	family.Targets = append(family.Targets, foundation.Targets...)
	family.Targets = append(family.Targets, activityDelivery.Targets...)
	family.Targets = append(family.Targets, workflowDelivery.Targets...)
	family.RefinementMaps = append(family.RefinementMaps, workflowTaskSpeculative.Refinements...)
	family.Compositions = append(family.Compositions, workflowTaskSpeculative.Compositions...)
	family.Targets = append(family.Targets, workflowTaskSpeculative.Targets...)
	family.RefinementMaps = append(family.RefinementMaps, nexus.Refinements...)
	family.Compositions = append(family.Compositions, nexus.Compositions...)
	family.Targets = append(family.Targets, nexus.Targets...)
	family.RefinementMaps = append(family.RefinementMaps, callback.Refinements...)
	family.Compositions = append(family.Compositions, callback.Compositions...)
	family.Targets = append(family.Targets, callback.Targets...)
	family.RefinementMaps = append(family.RefinementMaps, routing.Refinements...)
	family.Compositions = append(family.Compositions, routing.Compositions...)
	family.Targets = append(family.Targets, routing.Targets...)
	family.RefinementMaps = append(family.RefinementMaps, backlog.Refinements...)
	family.Compositions = append(family.Compositions, backlog.Compositions...)
	family.Targets = append(family.Targets, backlog.Targets...)
	if err := verify.ValidateModelFamily(family); err != nil {
		return verify.ModelFamily{}, fmt.Errorf("protocol verification family: %w", err)
	}
	return family, nil
}

func verificationActionOmissions(actions []verify.Action, reason string) []verify.Abstraction {
	result := make([]verify.Abstraction, len(actions))
	for index, action := range actions {
		result[index] = verify.Abstraction{Name: action.Name, Reason: reason, Source: action.Source}
	}
	return result
}

func verificationModuleActions(modules []verify.Module, name string) []string {
	for _, module := range modules {
		if module.Name == name {
			return append([]string(nil), module.Actions...)
		}
	}
	return nil
}

func verificationModuleActionsWithPrefixes(modules []verify.Module, name string, prefixes ...string) []string {
	var result []string
	for _, action := range verificationModuleActions(modules, name) {
		for _, prefix := range prefixes {
			if strings.HasPrefix(action, prefix) {
				result = append(result, action)
				break
			}
		}
	}
	return result
}

func verificationModulePropertiesWithPrefixes(modules []verify.Module, name string, prefixes ...string) []string {
	for _, module := range modules {
		if module.Name != name {
			continue
		}
		var result []string
		for _, property := range module.Properties {
			for _, prefix := range prefixes {
				if strings.HasPrefix(property, prefix) {
					result = append(result, property)
					break
				}
			}
		}
		return result
	}
	return nil
}
