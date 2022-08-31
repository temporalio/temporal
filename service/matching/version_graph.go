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
	"encoding/binary"
	"fmt"

	"github.com/dgryski/go-farm"

	"github.com/gogo/protobuf/proto"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/persistence/v1"
)

func ToBuildIdOrderingResponse(g *persistence.VersioningData, maxDepth int) *workflowservice.GetWorkerBuildIdOrderingResponse {
	return depthLimiter(g, maxDepth, true)
}

// HashVersioningData returns a farm.Fingerprint64 hash of the versioning data as bytes. If the data is nonexistent or
// invalid, returns nil.
func HashVersioningData(data *persistence.VersioningData) []byte {
	if data == nil || data.GetCurrentDefault() == nil {
		return nil
	}
	asBytes, err := data.Marshal()
	if err != nil {
		return nil
	}
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, farm.Fingerprint64(asBytes))
	return b
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
//	─┬─1.0───2.0─┬─3.0───4.0
//	 │           ├─3.1
//	 │           └─3.2
//	 ├─1.1
//	 ├─1.2
//	 └─1.3
//
// In the above graph, 4.0 is the current default, and [1.3, 3.2] is the set of current compatible leaves. Links
// going left are incompatible relationships, and links going up are compatible relationships.
//
// A request may:
//  1. Add a new version to the graph, as a default version
//  2. Add a new version to the graph, compatible with some existing version.
//  3. Add a new version to the graph, compatible with some existing version and as the new default.
//  4. Reorder an existing version (and it's whole compatible branch). We allow moving a node in the incompatible line
//     to the front (along with its entire compatible branch), as long as the user has targeted the most recent
//     version in that compatible branch.
//
// Deletions are not allowed, as it leads to confusion about what to do with open workflows who were operating on that
// version. It's better to simply add a new version (possibly with no associated workers) instead.
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
			if req.GetVersionId() == curDefault.GetVersion() {
				// User is setting current default as... current default. Do nothing.
				return nil
			}

			didReorder, err := reorderExistingNodeIfNeeded(existingData, req.VersionId)
			if err != nil {
				return err
			}
			if didReorder {
				return nil
			}

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
			return serviceerror.NewInvalidArgument(
				"requests to update build id ordering cannot create a new non-default version with no links")
		}
	}
	return nil
}

// Finds the node that some new version should point at, given that it says it's compatible with the provided
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
		if found, _, _ := findInNode(node, versionId, searchModeBoth); found != nil {
			return node, ix
		}
	}
	// Otherwise, this must be targeting some version in the default/incompatible chain, and it will become a new leaf
	curDefault := existingData.GetCurrentDefault()
	if nn, _, _ := findInNode(curDefault, versionId, searchModeBoth); nn != nil {
		return nn, -1
	}

	return nil, -1
}

// The provided node wants to become the default, and thus move its compatible branch (which may be just itself) to the
// front of the incompatible list. If the node is the head of its compatible branch, this is acceptable, otherwise it is
// an error.
func reorderExistingNodeIfNeeded(existingData *persistence.VersioningData, versionId *taskqueuepb.VersionId) (bool, error) {
	// First find if this node is inside a compatible branch. It could never be in more than one, since we
	// do not allow forks.
	for _, node := range existingData.GetCompatibleLeaves() {
		if node.GetVersion().Equal(versionId) {
			// We've got to find the node in the compatible branch which is pointed to by some node in the incompatible
			// branch
			entireCompatBranch := make(map[string]struct{})
			curCompatNode := node
			for curCompatNode != nil {
				entireCompatBranch[curCompatNode.GetVersion().GetWorkerBuildId()] = struct{}{}
				curCompatNode = curCompatNode.GetPreviousCompatible()
			}
			found, nextIncompat, _ := findAnyInNode(existingData.GetCurrentDefault(), entireCompatBranch, searchModeIncompat)
			if found != nil && nextIncompat != nil {
				nextIncompat.PreviousIncompatible = found.PreviousIncompatible
				found.PreviousIncompatible = nil
				node.PreviousIncompatible = nextIncompat
				existingData.CurrentDefault = node
				return true, nil
			}
			return false, nil
		}
		if found, _, _ := findInNode(node, versionId, searchModeCompat); found != nil {
			return false, serviceerror.NewInvalidArgument("A node which is already inside a compatible branch, but " +
				"is not the leaf node of that branch, cannot be made default")
		}
	}

	// Now check in the incompatible branch, and if this node is found in it, shuffle its branch to the front.
	found, nextIncompat, _ := findInNode(existingData.GetCurrentDefault(), versionId, searchModeIncompat)
	if found != nil && nextIncompat != nil {
		nextIncompat.PreviousIncompatible = found.PreviousIncompatible
		found.PreviousIncompatible = existingData.GetCurrentDefault()
		existingData.CurrentDefault = found
		return true, nil
	}

	return false, nil
}

type searchMode int

const (
	searchModeBoth searchMode = iota
	searchModeCompat
	searchModeIncompat
)

func findInNode(
	node *taskqueuepb.VersionIdNode,
	versionId *taskqueuepb.VersionId,
	mode searchMode,
) (*taskqueuepb.VersionIdNode, *taskqueuepb.VersionIdNode, *taskqueuepb.VersionIdNode) {
	s := make(map[string]struct{})
	s[versionId.GetWorkerBuildId()] = struct{}{}
	return _findInNode(node, s, mode, nil, nil)
}

func findAnyInNode(
	node *taskqueuepb.VersionIdNode,
	versionIds map[string]struct{},
	mode searchMode,
) (*taskqueuepb.VersionIdNode, *taskqueuepb.VersionIdNode, *taskqueuepb.VersionIdNode) {
	return _findInNode(node, versionIds, mode, nil, nil)
}

func _findInNode(
	node *taskqueuepb.VersionIdNode,
	versionIds map[string]struct{},
	mode searchMode,
	nextIncompat *taskqueuepb.VersionIdNode,
	nextCompat *taskqueuepb.VersionIdNode,
) (*taskqueuepb.VersionIdNode, *taskqueuepb.VersionIdNode, *taskqueuepb.VersionIdNode) {
	if _, ok := versionIds[node.GetVersion().GetWorkerBuildId()]; ok {
		return node, nextIncompat, nextCompat
	}
	if (mode == searchModeBoth || mode == searchModeCompat) && node.GetPreviousCompatible() != nil {
		return _findInNode(node.GetPreviousCompatible(), versionIds, mode, nextIncompat, node)
	}
	if (mode == searchModeBoth || mode == searchModeIncompat) && node.GetPreviousIncompatible() != nil {
		return _findInNode(node.GetPreviousIncompatible(), versionIds, mode, node, nextCompat)
	}
	return nil, nextIncompat, nextCompat
}
