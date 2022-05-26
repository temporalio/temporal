package matching

import (
	"fmt"
	"reflect"

	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/persistence/v1"
)

// TODO: Add validator of requests that can return proper error codes @ frontend

// TODO: Should propagate as invalid argument/payload
type invalidVersionUpdateError struct {
	msg string
}

func (e *invalidVersionUpdateError) Error() string {
	return e.msg
}

// TODO: Should propagate as 404
type versionNotFoundError struct {
	msg string
	ver *workflow.VersionId
}

func (e *versionNotFoundError) Error() string {
	return fmt.Sprintf("version %v not found: %s", e.ver, e.msg)
}

func ToBuildIdOrderingResponse(g *persistence.VersioningData) *workflowservice.GetWorkerBuildIdOrderingResponse {
	return &workflowservice.GetWorkerBuildIdOrderingResponse{}
}

func UpdateVersionsGraph(
	existingData *persistence.VersioningData,
	req *workflowservice.UpdateWorkerBuildIdOrderingRequest,
) error {
	if req.GetVersionId().GetWorkerBuildId() != "" {
		// If the version is to become the new default, add it to the list of current defaults, possibly replacing
		// the currently set one.
		if req.GetBecomeDefault() {
			curDefaults := existingData.GetCurrentDefaults()
			isExistingDefault := false
			for i, def := range curDefaults {
				asWorkerId := def.GetVersion().GetWorkerBuildId()
				// There is already a default worker-build-id based version, replace it.
				if asWorkerId != "" {
					isExistingDefault = true
					// If the current default is going to be the previous compat version with the one we're adding,
					// then we need to skip over it when setting the previous *incompatible* version.
					if req.GetPreviousCompatible().GetWorkerBuildId() == asWorkerId {
						curDefaults[i] = &workflow.VersionIdNode{
							Version:              req.VersionId,
							PreviousCompatible:   def,
							PreviousIncompatible: def.PreviousIncompatible,
						}
					} else {
						// Otherwise, set the previous incompatible version to the current default.
						curDefaults[i] = &workflow.VersionIdNode{
							Version:              req.VersionId,
							PreviousCompatible:   nil,
							PreviousIncompatible: def,
						}
					}
				}
			}
			if !isExistingDefault {
				if req.GetPreviousCompatible() != nil {
					return &invalidVersionUpdateError{msg: "adding a new default version which is compatible " +
						" with some previous version, when there is no existing default version, is not allowed." +
						" There must be a default version before this operation makes sense"}
				}
				newNode := &workflow.VersionIdNode{
					Version:              req.VersionId,
					PreviousCompatible:   nil,
					PreviousIncompatible: nil,
				}
				existingData.CurrentDefaults = append(existingData.CurrentDefaults, newNode)
			}
		} else {
			if req.GetPreviousCompatible() != nil {
				prevCompat, indexInCompatLeaves := findCompatibleNode(existingData, req.GetPreviousCompatible())
				if prevCompat != nil {
					newNode := &workflow.VersionIdNode{
						Version:              req.VersionId,
						PreviousCompatible:   prevCompat,
						PreviousIncompatible: nil,
					}
					if indexInCompatLeaves >= 0 {
						existingData.CompatibleLeaves[indexInCompatLeaves] = newNode
					} else {
						existingData.CompatibleLeaves = append(existingData.CompatibleLeaves, newNode)
					}
				} else {
					return &versionNotFoundError{
						msg: "previous compatible version not found", ver: req.GetPreviousCompatible()}
				}
			} else {
				// Check if the version is already a default, and remove it from being one if it is.
				for i, def := range existingData.GetCurrentDefaults() {
					if def.GetVersion().Equal(req.GetVersionId()) {
						existingData.CurrentDefaults =
							append(existingData.CurrentDefaults[:i], existingData.CurrentDefaults[i+1:]...)
						if def.GetPreviousCompatible() != nil {
							existingData.CurrentDefaults =
								append(existingData.CurrentDefaults, def.GetPreviousCompatible())
						} else if def.GetPreviousIncompatible() != nil {
							existingData.CurrentDefaults =
								append(existingData.CurrentDefaults, def.GetPreviousIncompatible())
						}
						return nil
					}
				}
				// Check if it's a compatible leaf, and remove it from being one if it is.
				for i, def := range existingData.GetCompatibleLeaves() {
					if def.GetVersion().Equal(req.GetVersionId()) {
						existingData.CompatibleLeaves =
							append(existingData.CompatibleLeaves[:i], existingData.CompatibleLeaves[i+1:]...)
						if def.GetPreviousCompatible() != nil {
							existingData.CompatibleLeaves =
								append(existingData.CompatibleLeaves, def.GetPreviousCompatible())
						}
						return nil
					}
				}
				return &invalidVersionUpdateError{
					msg: "Requests to update build id ordering cannot create a new non-default version with no links"}
			}
		}
	} else {
		return &invalidVersionUpdateError{
			msg: "request to update worker build id ordering is missing a valid version identifier"}
	}
	return nil
}

// Finds the node that the provided version should point at, given that it says it's compatible with the provided
// version. Note that this does not necessary mean *that* node. If the version being targeted as compatible has nodes
// which already point at it as their previous compatible version, that chain will be followed out to the leaf, which
// will be returned.
func findCompatibleNode(
	existingData *persistence.VersioningData,
	versionId *workflow.VersionId,
) (*workflow.VersionIdNode, int) {
	// First search down from all existing compatible leaves, as if any of those chains point at the desired version,
	// we will need to return that leaf.
	for ix, node := range existingData.GetCompatibleLeaves() {
		if node.GetVersion().Equal(versionId) {
			return node, ix
		}
		if findInNode(node, versionId) != nil {
			return node, ix
		}
	}
	// Otherwise, this must be targeting some version in the default/incompatible chain, and it will become a new leaf
	for _, node := range existingData.GetCurrentDefaults() {
		// There can only be one version of each type in the default list, so ignore any others.
		if reflect.TypeOf(versionId) == reflect.TypeOf(node.GetVersion()) {
			if node.GetVersion().Equal(versionId) {
				return node, -1
			}
			if nn := findInNode(node, versionId); nn != nil {
				return nn, -1
			}
		}
	}

	return nil, -1
}

func findInNode(
	node *workflow.VersionIdNode,
	versionId *workflow.VersionId,
) *workflow.VersionIdNode {
	if node.GetVersion().Equal(versionId) {
		return node
	}
	if node.GetPreviousCompatible() != nil {
		return findInNode(node.GetPreviousCompatible(), versionId)
	}
	if node.GetPreviousIncompatible() != nil {
		return findInNode(node.GetPreviousIncompatible(), versionId)
	}
	return nil
}
