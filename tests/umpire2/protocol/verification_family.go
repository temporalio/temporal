package protocol

import (
	"fmt"

	"go.temporal.io/server/common/testing/umpire/verify"
)

const ProtocolAtomicTarget = "protocol-atomic"

func (p *Protocol) VerificationFamily(options VerificationOptions) (verify.ModelFamily, error) {
	verificationModel, err := p.VerificationModel(options)
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
