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

	"crypto/sha256"
	"encoding/base64"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/persistence/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
)

var (
	// TODO: go over error types, maybe not all should be invalid argument
	errBuildNotFound                     = serviceerror.NewInvalidArgument("build id not found")
	errNewerBuildFound                   = serviceerror.NewInvalidArgument("newer compatible build exists")
	errEmptyVersioningData               = serviceerror.NewInvalidArgument("versioning data is empty")
	errPollWithVersionOnUnversionedQueue = serviceerror.NewInvalidArgument("poll with version capabilities on unversioned queue")
	errPollOnVersionedQueueWithNoVersion = serviceerror.NewInvalidArgument("poll on versioned queue with no version capabilities")
	errVersionedTaskForUnversionedQueue  = serviceerror.NewInvalidArgument("got task with version stamp for unversioned queue")
)

// ToBuildIdOrderingResponse transforms the internal VersioningData representation to public representation.
// If maxSets is given, the last sets up to maxSets will be returned.
func ToBuildIdOrderingResponse(data *persistencespb.VersioningData, maxSets int) *workflowservice.GetWorkerBuildIdCompatibilityResponse {
	lenSets := len(data.GetVersionSets())
	numSets := lenSets
	if maxSets > 0 && numSets > maxSets {
		numSets = maxSets
	}
	versionSets := make([]*taskqueuepb.CompatibleVersionSet, numSets)
	for i := range versionSets {
		set := data.GetVersionSets()[i+lenSets-numSets]
		buildIds := make([]string, len(set.GetBuildIds()))
		for j, version := range set.GetBuildIds() {
			buildIds[j] = version.Id
		}
		versionSets[i] = &taskqueuepb.CompatibleVersionSet{BuildIds: buildIds}
	}
	return &workflowservice.GetWorkerBuildIdCompatibilityResponse{MajorVersionSets: versionSets}
}

func checkLimits(g *persistencespb.VersioningData, maxSets, maxBuildIDs int) error {
	sets := g.GetVersionSets()
	if maxSets > 0 && len(sets) > maxSets {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("update would exceed number of compatible version sets permitted in namespace dynamic config (%v/%v)", len(sets), maxSets))
	}
	if maxBuildIDs == 0 {
		return nil
	}
	numBuildIDs := 0
	for _, set := range sets {
		numBuildIDs += len(set.GetBuildIds())
	}
	if numBuildIDs > maxBuildIDs {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("update would exceed number of build IDs permitted in namespace dynamic config (%v/%v)", numBuildIDs, maxBuildIDs))
	}
	return nil
}

// UpdateVersionSets updates version sets given existing versioning data and an update request. The request is expected
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
// Deletions are performed by a background process which verifies build IDs are no longer in use and safe to delete (not yet implemented).
//
// Update may fail with FailedPrecondition if it would cause exceeding the supplied limits.
func UpdateVersionSets(clock hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerBuildIdCompatibilityRequest, maxSets, maxBuildIDs int) (*persistencespb.VersioningData, error) {
	data, err := updateImpl(clock, data, req)
	if err != nil {
		return nil, err
	}
	if err := checkLimits(data, maxSets, maxBuildIDs); err != nil {
		return nil, err
	}
	return data, nil
}

func hashBuildID(buildID string) string {
	bytes := []byte(buildID)
	summed := sha256.Sum256(bytes)
	// 20 base64 chars of entropy is enough for this case
	return base64.URLEncoding.EncodeToString(summed[:])[:20]
}

//nolint:revive // cyclomatic complexity
func updateImpl(timestamp hlc.Clock, existingData *persistencespb.VersioningData, req *workflowservice.UpdateWorkerBuildIdCompatibilityRequest) (*persistencespb.VersioningData, error) {
	// First find if the targeted version is already in the sets
	targetedVersion := extractTargetedVersion(req)
	targetSetIdx, versionInSetIdx := findVersion(existingData, targetedVersion)
	numExistingSets := len(existingData.GetVersionSets())
	modifiedData := persistence.VersioningData{
		VersionSets:            make([]*persistencespb.CompatibleVersionSet, len(existingData.GetVersionSets())),
		DefaultUpdateTimestamp: existingData.GetDefaultUpdateTimestamp(),
	}
	copy(modifiedData.VersionSets, existingData.GetVersionSets())

	if req.GetAddNewBuildIdInNewDefaultSet() != "" {
		targetIsInDefaultSet := targetSetIdx == numExistingSets-1
		targetIsOnlyBuildIdInSet := versionInSetIdx == 0 && len(existingData.VersionSets[numExistingSets-1].BuildIds) == 1
		// Make the request idempotent
		if numExistingSets > 0 && targetIsInDefaultSet && targetIsOnlyBuildIdInSet {
			return existingData, nil
		}
		// If it's not already in the sets, add it as the new default set
		if targetSetIdx != -1 {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("version %s already exists", targetedVersion))
		}

		modifiedData.VersionSets = append(modifiedData.VersionSets, &persistencespb.CompatibleVersionSet{
			SetIds:   []string{hashBuildID(targetedVersion)},
			BuildIds: []*persistencespb.BuildID{{Id: targetedVersion, State: persistencespb.STATE_ACTIVE, StateUpdateTimestamp: &timestamp}},
		})
		makeVersionInSetDefault(&modifiedData, len(modifiedData.VersionSets)-1, 0, &timestamp)
		makeDefaultSet(&modifiedData, len(modifiedData.VersionSets)-1, &timestamp)
	} else if addNew := req.GetAddNewCompatibleBuildId(); addNew != nil {
		compatVer := addNew.GetExistingCompatibleBuildId()
		compatSetIdx, _ := findVersion(&modifiedData, compatVer)
		if compatSetIdx == -1 {
			return nil, serviceerror.NewNotFound(
				fmt.Sprintf("targeted compatible_version %v not found", compatVer))
		}
		if targetSetIdx != -1 {
			// If the version does exist, this operation can't do anything meaningful, but we can fail if the user
			// says the version is now compatible with some different set.
			if compatSetIdx == targetSetIdx {
				if addNew.GetMakeSetDefault() && targetSetIdx != numExistingSets-1 {
					return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("version %s already exists and is not default for queue", targetedVersion))
				}
				if versionInSetIdx != len(existingData.GetVersionSets()[targetSetIdx].BuildIds)-1 {
					return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("version %s already exists and is not default in set", targetedVersion))
				}
				// Make the operation idempotent
				return existingData, nil
			}
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("%s requested to be made compatible with %s but both versions exist and are incompatible", targetedVersion, compatVer))
		}

		// First duplicate the build IDs to avoid mutation
		lastIdx := len(existingData.VersionSets[compatSetIdx].BuildIds)
		modifiedData.VersionSets[compatSetIdx] = &persistencespb.CompatibleVersionSet{
			SetIds:   existingData.VersionSets[compatSetIdx].SetIds,
			BuildIds: make([]*persistencespb.BuildID, lastIdx+1),
		}
		copy(modifiedData.VersionSets[compatSetIdx].BuildIds, existingData.VersionSets[compatSetIdx].BuildIds)

		// If the version doesn't exist, add it to the compatible set
		modifiedData.VersionSets[compatSetIdx].BuildIds[lastIdx] =
			&persistencespb.BuildID{Id: targetedVersion, State: persistencespb.STATE_ACTIVE, StateUpdateTimestamp: &timestamp}
		makeVersionInSetDefault(&modifiedData, compatSetIdx, lastIdx, &timestamp)
		if addNew.GetMakeSetDefault() {
			makeDefaultSet(&modifiedData, compatSetIdx, &timestamp)
		}
	} else if req.GetPromoteSetByBuildId() != "" {
		if targetSetIdx == -1 {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("targeted version %v not found", targetedVersion))
		}
		if targetSetIdx == numExistingSets-1 {
			// Make the request idempotent
			return existingData, nil
		}
		makeDefaultSet(&modifiedData, targetSetIdx, &timestamp)
	} else if req.GetPromoteBuildIdWithinSet() != "" {
		if targetSetIdx == -1 {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("targeted version %v not found", targetedVersion))
		}
		if versionInSetIdx == len(existingData.GetVersionSets()[targetSetIdx].BuildIds)-1 {
			// Make the request idempotent
			return existingData, nil
		}
		// We're gonna have to copy here to to avoid mutating the original
		numBuildIDs := len(existingData.GetVersionSets()[targetSetIdx].BuildIds)
		buildIDsCopy := make([]*persistencespb.BuildID, numBuildIDs)
		copy(buildIDsCopy, existingData.VersionSets[targetSetIdx].BuildIds)
		modifiedData.VersionSets[targetSetIdx] = &persistencespb.CompatibleVersionSet{
			SetIds:   existingData.VersionSets[targetSetIdx].SetIds,
			BuildIds: buildIDsCopy,
		}
		makeVersionInSetDefault(&modifiedData, targetSetIdx, versionInSetIdx, &timestamp)
	}

	return &modifiedData, nil
}

func extractTargetedVersion(req *workflowservice.UpdateWorkerBuildIdCompatibilityRequest) string {
	if req.GetAddNewCompatibleBuildId() != nil {
		return req.GetAddNewCompatibleBuildId().GetNewBuildId()
	} else if req.GetPromoteSetByBuildId() != "" {
		return req.GetPromoteSetByBuildId()
	} else if req.GetPromoteBuildIdWithinSet() != "" {
		return req.GetPromoteBuildIdWithinSet()
	}
	return req.GetAddNewBuildIdInNewDefaultSet()
}

// Finds the version in the version sets, returning (set index, index within that set)
// Returns -1, -1 if not found.
func findVersion(data *persistencespb.VersioningData, buildID string) (setIndex, indexInSet int) {
	for setIndex, set := range data.GetVersionSets() {
		for indexInSet, version := range set.GetBuildIds() {
			if version.Id == buildID {
				return setIndex, indexInSet
			}
		}
	}
	return -1, -1
}

func makeDefaultSet(data *persistencespb.VersioningData, setIx int, timestamp *hlc.Clock) {
	data.DefaultUpdateTimestamp = timestamp
	if len(data.VersionSets) <= 1 {
		return
	}
	if setIx < len(data.VersionSets)-1 {
		// Move the set to the end and shift all the others down
		moveMe := data.VersionSets[setIx]
		copy(data.VersionSets[setIx:], data.VersionSets[setIx+1:])
		data.VersionSets[len(data.VersionSets)-1] = moveMe
	}
}

func makeVersionInSetDefault(data *persistencespb.VersioningData, setIx, versionIx int, timestamp *hlc.Clock) {
	data.VersionSets[setIx].DefaultUpdateTimestamp = timestamp
	setVersions := data.VersionSets[setIx].BuildIds
	if len(setVersions) <= 1 {
		return
	}
	if versionIx < len(setVersions)-1 {
		// Move the build ID to the end and shift all the others down
		moveMe := setVersions[versionIx]
		copy(setVersions[versionIx:], setVersions[versionIx+1:])
		setVersions[len(setVersions)-1] = moveMe
	}
}

func lookupVersionSetForPoll(data *persistencespb.VersioningData, caps *commonpb.WorkerVersionCapabilities) (string, error) {
	// for poll, only the latest version in the compatible set can get tasks
	// find the version set that this worker is in
	setIdx, _ := findVersion(data, caps.BuildId)
	if setIdx < 0 {
		return "", errBuildNotFound
	}
	set := data.VersionSets[setIdx]
	if caps.BuildId != set.BuildIds[len(set.BuildIds)-1].Id {
		return "", errNewerBuildFound
	}
	return minSetID(set), nil
}

func lookupVersionSetForAdd(data *persistencespb.VersioningData, stamp *commonpb.WorkerVersionStamp) (string, error) {
	var set *persistencespb.CompatibleVersionSet
	if stamp == nil {
		// if this is a new workflow, assign it to the latest version.
		// (if it's an unversioned workflow that has already completed one or more tasks, then
		// leave it on the unversioned one. that case is handled already before we get here.)
		setLen := len(data.VersionSets)
		if setLen == 0 || data.VersionSets[setLen-1] == nil {
			return "", errEmptyVersioningData
		}
		set = data.VersionSets[setLen-1]
	} else {
		// for add, any version in the compatible set maps to the set
		setIdx, _ := findVersion(data, stamp.BuildId)
		if setIdx < 0 {
			return "", errBuildNotFound
		}
		set = data.VersionSets[setIdx]
	}
	return minSetID(set), nil
}

// FIXME: is it correct to use this?
func minSetID(set *persistencespb.CompatibleVersionSet) string {
	minID := set.SetIds[0]
	for _, id := range set.SetIds[1:] {
		if id < minID {
			minID = id
		}
	}
	return minID
}
