package matching

import (
	"fmt"
	"reflect"

	"github.com/gogo/protobuf/proto"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/persistence/v1"
)

// TODO: Add validator of requests that can return proper error codes @ frontend

func ToBuildIdOrderingResponse(g *persistence.VersioningData, maxDepth int) *workflowservice.GetWorkerBuildIdOrderingResponse {
	curDefault := g.GetCurrentDefault()
	curDepth := 1
	if maxDepth > 0 {
		// Mutating the passed in graph is a no-no.
		curDefault = proto.Clone(g.GetCurrentDefault()).(*taskqueuepb.VersionIdNode)
		curNode := curDefault
		for curDepth < maxDepth {
			if curNode.GetPreviousIncompatible() == nil {
				break
			}
			curNode = curNode.GetPreviousIncompatible()
			curDepth++
		}
		if curNode != nil {
			curNode.PreviousIncompatible = nil
		}
	}
	return &workflowservice.GetWorkerBuildIdOrderingResponse{
		CurrentDefault:   curDefault,
		CompatibleLeaves: g.GetCompatibleLeaves(),
	}
}

func UpdateVersionsGraph(
	existingData *persistence.VersioningData,
	req *workflowservice.UpdateWorkerBuildIdOrderingRequest,
) error {
	if req.GetVersionId().GetWorkerBuildId() != "" {
		// If the version is to become the new default, add it to the list of current defaults, possibly replacing
		// the currently set one.
		if req.GetBecomeDefault() {
			curDefault := existingData.GetCurrentDefault()
			if curDefault != nil {
				// If the current default is going to be the previous compat version with the one we're adding,
				// then we need to skip over it when setting the previous *incompatible* version.
				if req.GetPreviousCompatible().GetWorkerBuildId() == curDefault.GetVersion().GetWorkerBuildId() {
					existingData.CurrentDefault = &taskqueuepb.VersionIdNode{
						Version:              req.VersionId,
						PreviousCompatible:   curDefault,
						PreviousIncompatible: curDefault.PreviousIncompatible,
					}
				} else {
					// Otherwise, set the previous incompatible version to the current default.
					existingData.CurrentDefault = &taskqueuepb.VersionIdNode{
						Version:              req.VersionId,
						PreviousCompatible:   nil,
						PreviousIncompatible: curDefault,
					}
				}
			} else {
				if req.GetPreviousCompatible() != nil {
					return serviceerror.NewInvalidArgument("adding a new default version which is compatible " +
						" with some previous version, when there is no existing default version, is not allowed." +
						" There must be a default version before this operation makes sense")
				}
				newNode := &taskqueuepb.VersionIdNode{
					Version:              req.VersionId,
					PreviousCompatible:   nil,
					PreviousIncompatible: nil,
				}
				existingData.CurrentDefault = newNode
			}
		} else {
			if req.GetPreviousCompatible() != nil {
				prevCompat, indexInCompatLeaves := findCompatibleNode(existingData, req.GetPreviousCompatible())
				if prevCompat != nil {
					newNode := &taskqueuepb.VersionIdNode{
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
					return serviceerror.NewNotFound(
						fmt.Sprintf("previous compatible version %v not found", req.GetPreviousCompatible()))
				}
			} else {
				// Check if the version is already a default, and remove it from being one if it is.
				curDefault := existingData.GetCurrentDefault()
				if curDefault.GetVersion().Equal(req.GetVersionId()) {
					existingData.CurrentDefault = nil
					if curDefault.GetPreviousCompatible() != nil {
						existingData.CurrentDefault = curDefault.GetPreviousCompatible()
					} else if curDefault.GetPreviousIncompatible() != nil {
						existingData.CurrentDefault = curDefault.GetPreviousIncompatible()
					}
					return nil
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
				return serviceerror.NewInvalidArgument(
					"requests to update build id ordering cannot create a new non-default version with no links")
			}
		}
	} else {
		return serviceerror.NewInvalidArgument(
			"request to update worker build id ordering is missing a valid version identifier")
	}
	return nil
}

// Finds the node that the provided version should point at, given that it says it's compatible with the provided
// version. Note that this does not necessary mean *that* node. If the version being targeted as compatible has nodes
// which already point at it as their previous compatible version, that chain will be followed out to the leaf, which
// will be returned.
func findCompatibleNode(
	existingData *persistence.VersioningData,
	versionId *taskqueuepb.VersionId,
) (*taskqueuepb.VersionIdNode, int) {
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
	curDefault := existingData.GetCurrentDefault()
	// There can only be one version of each type in the default list, so ignore any others.
	if reflect.TypeOf(versionId) == reflect.TypeOf(curDefault.GetVersion()) {
		if curDefault.GetVersion().Equal(versionId) {
			return curDefault, -1
		}
		if nn := findInNode(curDefault, versionId); nn != nil {
			return nn, -1
		}
	}

	return nil, -1
}

func findInNode(
	node *taskqueuepb.VersionIdNode,
	versionId *taskqueuepb.VersionId,
) *taskqueuepb.VersionIdNode {
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
