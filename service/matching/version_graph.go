// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package matching

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/persistence/v1"
)

func ToBuildIdOrderingResponse(g *persistence.VersioningData, maxDepth int) *workflowservice.GetWorkerBuildIdOrderingResponse {
	return depthLimiter(g, maxDepth, true)
}

func depthLimiter(g *persistence.VersioningData, maxDepth int, noMutate bool) *workflowservice.GetWorkerBuildIdOrderingResponse {
	curDefault := g.GetCurrentDefault()
	compatLeaves := g.GetCompatibleLeaves()
	if maxDepth > 0 {
		if noMutate {
			curDefault = proto.Clone(g.GetCurrentDefault()).(*taskqueuepb.VersionIdNode)
		}
		curNode := curDefault
		curDepth := 1
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
		// Apply to compatible leaves as well
		newCompatLeaves := make([]*taskqueuepb.VersionIdNode, len(g.GetCompatibleLeaves()))
		for ix := range compatLeaves {
			compatLeaf := compatLeaves[ix]
			if noMutate {
				compatLeaf = proto.Clone(compatLeaves[ix]).(*taskqueuepb.VersionIdNode)
			}
			curNode = compatLeaf
			curDepth = 1
			for curDepth < maxDepth {
				if curNode.GetPreviousCompatible() == nil {
					break
				}
				curNode = curNode.GetPreviousCompatible()
				curDepth++
			}
			if curNode != nil {
				curNode.PreviousCompatible = nil
			}
			newCompatLeaves[ix] = compatLeaf
		}
		compatLeaves = newCompatLeaves
	}
	return &workflowservice.GetWorkerBuildIdOrderingResponse{
		CurrentDefault:   curDefault,
		CompatibleLeaves: compatLeaves,
	}
}

// Given an existing graph and an update request, update the graph appropriately.
//
// See the API docs for more detail. In short, the graph looks like one long line of default versions, each of which
// is incompatible with the previous, optionally with branching compatibility branches. Like so:
//
// ─┬─1.0───2.0─┬─3.0───4.0
//  │           ├─3.1
//  │           └─3.2
//  ├─1.1
//  ├─1.2
//  └─1.3
//
// In the above graph, 4.0 is the current default, and [1.3, 3.2] is the set of current compatible leaves. Links
// going left are incompatible relationships, and links going up are compatible relationships.
//
// A request may:
// 1. Add a new version to the graph, as a default version
// 2. Add a new version to the graph, compatible with some existing version.
// 3. Add a new version to the graph, compatible with some existing version and as the new default.
// 4. Unset a version as a default. It will be dropped and its previous incompatible version becomes default.
// 5. Unset a version as a compatible. It will be dropped and its previous compatible version will become the new
//    compatible leaf for that branch.
func UpdateVersionsGraph(existingData *persistence.VersioningData, req *workflowservice.UpdateWorkerBuildIdOrderingRequest, maxSize int) error {
	if req.GetVersionId().GetWorkerBuildId() == "" {
		return serviceerror.NewInvalidArgument(
			"request to update worker build id ordering is missing a valid version identifier")
	}
	err := updateImpl(existingData, req)
	if err != nil {
		return err
	}
	// Limit graph size if it's grown too large
	depthLimiter(existingData, maxSize, false)
	return nil
}

func updateImpl(existingData *persistence.VersioningData, req *workflowservice.UpdateWorkerBuildIdOrderingRequest) error {
	// If the version is to become the new default, add it to the list of current defaults, possibly replacing
	// the currently set one.
	if req.GetBecomeDefault() {
		curDefault := existingData.GetCurrentDefault()
		isCompatWithCurDefault :=
			req.GetPreviousCompatible().GetWorkerBuildId() == curDefault.GetVersion().GetWorkerBuildId()
		if req.GetPreviousCompatible() != nil && !isCompatWithCurDefault {
			// It does not make sense to introduce a version which is the new overall default, but is somehow also
			// supposed to be compatible with some existing version, as that would necessarily imply that the newly
			// added version is somehow both compatible and incompatible with the same target version.
			return serviceerror.NewInvalidArgument("adding a new default version which is compatible " +
				" with any version other than the existing default is not allowed.")
		}
		if curDefault != nil {
			// If the current default is going to be the previous compat version with the one we're adding,
			// then we need to skip over it when setting the previous *incompatible* version.
			if isCompatWithCurDefault {
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
			existingData.CurrentDefault = &taskqueuepb.VersionIdNode{
				Version:              req.VersionId,
				PreviousCompatible:   nil,
				PreviousIncompatible: nil,
			}
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
	if curDefault.GetVersion().Equal(versionId) {
		return curDefault, -1
	}
	if nn := findInNode(curDefault, versionId); nn != nil {
		return nn, -1
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
