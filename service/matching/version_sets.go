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
	"github.com/pborman/uuid"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/util"
)

func ToBuildIdOrderingResponse(g *persistence.VersioningData, maxDepth int) *workflowservice.GetWorkerBuildIdCompatabilityResponse {
	return depthLimiter(g, maxDepth)
}

// HashVersioningData returns a farm.Fingerprint64 hash of the versioning data as bytes. If the data is nonexistent or
// invalid, returns nil.
func HashVersioningData(data *persistence.VersioningData) []byte {
	if data == nil || data.GetVersionSets() == nil {
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

func depthLimiter(g *persistence.VersioningData, maxDepth int) *workflowservice.GetWorkerBuildIdCompatabilityResponse {
	if maxDepth <= 0 || maxDepth >= len(g.GetVersionSets()) {
		return &workflowservice.GetWorkerBuildIdCompatabilityResponse{MajorVersionSets: g.GetVersionSets()}
	}
	shortened := util.SliceTail(g.GetVersionSets(), maxDepth)
	return &workflowservice.GetWorkerBuildIdCompatabilityResponse{MajorVersionSets: shortened}
}

// Given existing versioning data and an update request, update the version sets appropriately. The request is expected
// to have already been validated.
//
// See the API docs for more detail. In short, the versioning data representation consists of a sequence of sequences of
// compatible versions. Like so:
//
//	                     *
//	┬─1.0───2.0─┬─3.0───4.0
//	│           ├─3.1
//	│           └─3.2
//	├─1.1
//	├─1.2
//	└─1.3
//
// In the above example, 4.0 is the current default version and no other versions are compatible with it. The previous
// compatible set is the 3.x set, with 3.2 being the current default for that set, and so on. The * represents the
// current default set pointer, which can be shifted around by the user.
//
// A request may:
//  1. Add a new version possibly as the new overall default version, creating a new set.
//  2. Add a new version, compatible with some existing version, adding it to that existing set and making it the new
//     default for that set.
//  3. Target some existing version, marking it (and thus its set) as the default set.
//
// Deletions are not permitted, as inserting new versions can accomplish the same goals with less complexity. However,
// sets may be dropped when the number of sets limit is reached. They are dropped oldest first - the current default set
// is never dropped, instead dropping the next oldest set.
func UpdateVersionSets(existingData *persistence.VersioningData, req *workflowservice.UpdateWorkerBuildIdCompatabilityRequest, maxSize int) error {
	err := updateImpl(existingData, req)
	if err != nil {
		return err
	}
	// Limit graph size if it's grown too large
	newResp := depthLimiter(existingData, maxSize)
	existingData.VersionSets = newResp.GetMajorVersionSets()
	return nil
}

//nolint:revive // cyclomatic complexity
func updateImpl(existingData *persistence.VersioningData, req *workflowservice.UpdateWorkerBuildIdCompatabilityRequest) error {
	// First find if the targeted version is already in the sets
	targetedVersion := extractTargetedVersion(req)
	findRes := findVersion(existingData, targetedVersion)
	targetSetIx, versionInSetIx := findRes.setIx, findRes.indexInSet

	if req.GetAddNewBuildIdInNewDefaultSet() != "" {
		// If it's not already in the sets, add it as the new default set
		if targetSetIx != -1 {
			return serviceerror.NewInvalidArgument(fmt.Sprintf("version %s already exists", targetedVersion))
		}

		existingData.VersionSets = append(existingData.GetVersionSets(), &taskqueuepb.CompatibleVersionSet{
			VersionSetId: uuid.New(),
			BuildIds:     []string{targetedVersion},
		})
	} else if addNew := req.GetAddNewCompatibleBuildId(); addNew != nil {
		compatVer := addNew.GetExistingCompatibleBuildId()
		compatSetIx := findVersion(existingData, compatVer).setIx
		if compatSetIx == -1 {
			return serviceerror.NewNotFound(
				fmt.Sprintf("targeted compatible_version %v not found", compatVer))
		}
		if targetSetIx != -1 {
			// If the version does exist, this operation can't do anything meaningful, but we can fail if the user
			// says the version is now compatible with some different set.
			return serviceerror.NewInvalidArgument(fmt.Sprintf("version %s already exists", targetedVersion))
		}

		// If the version doesn't exist, add it to the compatible set
		existingData.VersionSets[compatSetIx].BuildIds =
			append(existingData.VersionSets[compatSetIx].BuildIds, targetedVersion)
		if addNew.GetMakeSetDefault() {
			makeDefaultSet(existingData, compatSetIx)
		}
	} else if req.GetPromoteSetByBuildId() != "" {
		if targetSetIx == -1 {
			return serviceerror.NewNotFound(fmt.Sprintf("targeted version %v not found", targetedVersion))
		}
		makeDefaultSet(existingData, targetSetIx)
	} else if req.GetPromoteBuildIdWithinSet() != "" {
		if targetSetIx == -1 {
			return serviceerror.NewNotFound(fmt.Sprintf("targeted version %v not found", targetedVersion))
		}
		makeVersionInSetDefault(existingData, targetSetIx, versionInSetIx)
	}

	return nil
}

func extractTargetedVersion(req *workflowservice.UpdateWorkerBuildIdCompatabilityRequest) string {
	if req.GetAddNewCompatibleBuildId() != nil {
		return req.GetAddNewCompatibleBuildId().GetNewBuildId()
	} else if req.GetPromoteSetByBuildId() != "" {
		return req.GetPromoteSetByBuildId()
	} else if req.GetPromoteBuildIdWithinSet() != "" {
		return req.GetPromoteBuildIdWithinSet()
	}
	return req.GetAddNewBuildIdInNewDefaultSet()
}

type findVersionRes struct {
	setIx      int
	indexInSet int
}

// Finds the version in the version sets, returning (set index, index within that set)
// Returns -1, -1 if not found.
func findVersion(data *persistence.VersioningData, buildID string) findVersionRes {
	for setIx, set := range data.GetVersionSets() {
		for versionIx, version := range set.GetBuildIds() {
			if version == buildID {
				return findVersionRes{
					setIx:      setIx,
					indexInSet: versionIx,
				}
			}
		}
	}
	return findVersionRes{
		setIx:      -1,
		indexInSet: -1,
	}
}

func makeDefaultSet(data *persistence.VersioningData, setIx int) {
	if len(data.VersionSets) <= 1 {
		return
	}
	// Move the set to the end and shift all the others down
	moveMe := data.VersionSets[setIx]
	copy(data.VersionSets[setIx:], data.VersionSets[setIx+1:])
	data.VersionSets[len(data.VersionSets)-1] = moveMe
}

func makeVersionInSetDefault(data *persistence.VersioningData, setIx, versionIx int) {
	setVersions := data.VersionSets[setIx].BuildIds
	if len(setVersions) <= 1 {
		return
	}
	moveMe := setVersions[versionIx]
	copy(setVersions[versionIx:], setVersions[versionIx+1:])
	setVersions[len(setVersions)-1] = moveMe
}
