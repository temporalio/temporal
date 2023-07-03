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
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/common/worker_versioning"
)

var (
	// Error used to signal that a queue has no versioning data. This shouldn't escape matching.
	errEmptyVersioningData = serviceerror.NewInternal("versioning data is empty")

	// Temporary until we persist guessed set ids
	errUnknownBuildId = serviceerror.NewFailedPrecondition("unknown build id")
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
		buildIds := make([]string, 0, len(set.GetBuildIds()))
		for _, version := range set.GetBuildIds() {
			if version.State == persistencespb.STATE_ACTIVE {
				buildIds = append(buildIds, version.Id)
			}
		}
		versionSets[i] = &taskqueuepb.CompatibleVersionSet{BuildIds: buildIds}
	}
	return &workflowservice.GetWorkerBuildIdCompatibilityResponse{MajorVersionSets: versionSets}
}

func checkLimits(g *persistencespb.VersioningData, maxSets, maxBuildIds int) error {
	sets := g.GetVersionSets()
	if maxSets > 0 && len(sets) > maxSets {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("update would exceed number of compatible version sets permitted in namespace dynamic config (%v/%v)", len(sets), maxSets))
	}
	if maxBuildIds == 0 {
		return nil
	}
	numBuildIds := 0
	for _, set := range sets {
		numBuildIds += len(set.GetBuildIds())
	}
	if numBuildIds > maxBuildIds {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("update would exceed number of build IDs permitted in namespace dynamic config (%v/%v)", numBuildIds, maxBuildIds))
	}
	return nil
}

func gatherBuildIds(data *persistencespb.VersioningData) map[string]struct{} {
	buildIds := make(map[string]struct{}, 0)
	for _, set := range data.GetVersionSets() {
		for _, buildId := range set.BuildIds {
			if buildId.State == persistencespb.STATE_ACTIVE {
				buildIds[buildId.Id] = struct{}{}
			}
		}
	}
	return buildIds
}

// RemoveBuildIds removes given buildIds from versioning data.
// Assumes that build ids are safe to remove, ex: a set default is never removed unless it is a single set member and
// that set is not default for the queue.
func RemoveBuildIds(clock hlc.Clock, versioningData *persistencespb.VersioningData, buildIds []string) *persistencespb.VersioningData {
	buildIdsMap := make(map[string]struct{}, len(buildIds))
	for _, buildId := range buildIds {
		buildIdsMap[buildId] = struct{}{}
	}
	modifiedData := shallowCloneVersioningData(versioningData)
	for setIdx, original := range modifiedData.GetVersionSets() {
		set := shallowCloneVersionSet(original)
		modifiedData.VersionSets[setIdx] = set
		for buildIdIdx, buildId := range set.BuildIds {
			if _, found := buildIdsMap[buildId.Id]; found {
				set.BuildIds[buildIdIdx] = &persistencespb.BuildId{
					Id:                     buildId.Id,
					State:                  persistencespb.STATE_DELETED,
					StateUpdateTimestamp:   &clock,
					BecameDefaultTimestamp: buildId.BecameDefaultTimestamp,
				}
			}
		}
	}
	return modifiedData
}

// GetBuildIdDeltas compares all active build ids in prev and curr sets and returns sets of added and removed build ids.
func GetBuildIdDeltas(prev *persistencespb.VersioningData, curr *persistencespb.VersioningData) (added []string, removed []string) {
	prevBuildIds := gatherBuildIds(prev)
	currBuildIds := gatherBuildIds(curr)

	for buildId := range prevBuildIds {
		if _, found := currBuildIds[buildId]; !found {
			removed = append(removed, buildId)
		}
	}
	for buildId := range currBuildIds {
		if _, found := prevBuildIds[buildId]; !found {
			added = append(added, buildId)
		}
	}
	return added, removed
}

func hashBuildId(buildID string) string {
	bytes := []byte(buildID)
	summed := sha256.Sum256(bytes)
	// 20 base64 chars of entropy is enough for this case
	return base64.URLEncoding.EncodeToString(summed[:])[:20]
}

func shallowCloneVersioningData(data *persistencespb.VersioningData) *persistencespb.VersioningData {
	clone := persistencespb.VersioningData{
		VersionSets: make([]*persistencespb.CompatibleVersionSet, len(data.GetVersionSets())),
	}
	copy(clone.VersionSets, data.GetVersionSets())
	return &clone
}

func shallowCloneVersionSet(set *persistencespb.CompatibleVersionSet) *persistencespb.CompatibleVersionSet {
	clone := &persistencespb.CompatibleVersionSet{
		SetIds:                 set.SetIds,
		BuildIds:               make([]*persistencespb.BuildId, len(set.BuildIds)),
		BecameDefaultTimestamp: set.BecameDefaultTimestamp,
	}
	copy(clone.BuildIds, set.BuildIds)
	return clone
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
func UpdateVersionSets(clock hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerBuildIdCompatibilityRequest, maxSets, maxBuildIds int) (*persistencespb.VersioningData, error) {
	if data == nil {
		data = &persistencespb.VersioningData{VersionSets: make([]*persistencespb.CompatibleVersionSet, 0)}
	} else {
		data = common.CloneProto(data)
	}
	data, err := updateImpl(clock, data, req)
	if err != nil {
		return nil, err
	}
	return data, checkLimits(data, maxSets, maxBuildIds)
}

//nolint:revive // cyclomatic complexity
func updateImpl(timestamp hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerBuildIdCompatibilityRequest) (*persistencespb.VersioningData, error) {
	// First find if the targeted version is already in the sets
	targetedVersion := extractTargetedVersion(req)
	targetSetIdx, versionInSetIdx := worker_versioning.FindBuildId(data, targetedVersion)
	numExistingSets := len(data.GetVersionSets())

	if req.GetAddNewBuildIdInNewDefaultSet() != "" {
		targetIsInDefaultSet := targetSetIdx == numExistingSets-1
		targetIsOnlyBuildIdInSet := versionInSetIdx == 0 && len(data.VersionSets[numExistingSets-1].BuildIds) == 1
		// Make the request idempotent
		if numExistingSets > 0 && targetIsInDefaultSet && targetIsOnlyBuildIdInSet {
			return data, nil
		}
		// If it's not already in the sets, add it as the new default set
		if targetSetIdx != -1 {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("version %s already exists", targetedVersion))
		}

		data.VersionSets = append(data.GetVersionSets(), &persistencespb.CompatibleVersionSet{
			SetIds:   []string{hashBuildId(targetedVersion)},
			BuildIds: []*persistencespb.BuildId{{Id: targetedVersion, State: persistencespb.STATE_ACTIVE, StateUpdateTimestamp: &timestamp}},
		})
		makeVersionInSetDefault(data, len(data.VersionSets)-1, 0, &timestamp)
		makeDefaultSet(data, len(data.VersionSets)-1, &timestamp)
	} else if addNew := req.GetAddNewCompatibleBuildId(); addNew != nil {
		compatVer := addNew.GetExistingCompatibleBuildId()
		compatSetIdx, _ := worker_versioning.FindBuildId(data, compatVer)
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
				if versionInSetIdx != len(data.GetVersionSets()[targetSetIdx].BuildIds)-1 {
					return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("version %s already exists and is not default in set", targetedVersion))
				}
				// Make the operation idempotent
				return data, nil
			}
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("%s requested to be made compatible with %s but both versions exist and are incompatible", targetedVersion, compatVer))
		}

		lastIdx := len(data.VersionSets[compatSetIdx].BuildIds)

		// If the version doesn't exist, add it to the compatible set
		data.VersionSets[compatSetIdx].BuildIds = append(data.VersionSets[compatSetIdx].BuildIds,
			&persistencespb.BuildId{Id: targetedVersion, State: persistencespb.STATE_ACTIVE, StateUpdateTimestamp: &timestamp})
		makeVersionInSetDefault(data, compatSetIdx, lastIdx, &timestamp)
		if addNew.GetMakeSetDefault() {
			makeDefaultSet(data, compatSetIdx, &timestamp)
		}
	} else if req.GetPromoteSetByBuildId() != "" {
		if targetSetIdx == -1 {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("targeted version %v not found", targetedVersion))
		}
		if targetSetIdx == numExistingSets-1 {
			// Make the request idempotent
			return data, nil
		}
		makeDefaultSet(data, targetSetIdx, &timestamp)
	} else if req.GetPromoteBuildIdWithinSet() != "" {
		if targetSetIdx == -1 {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("targeted version %v not found", targetedVersion))
		}
		if versionInSetIdx == len(data.GetVersionSets()[targetSetIdx].BuildIds)-1 {
			// Make the request idempotent
			return data, nil
		}
		makeVersionInSetDefault(data, targetSetIdx, versionInSetIdx, &timestamp)
	} else if mergeSets := req.GetMergeSets(); mergeSets != nil {
		if targetSetIdx == -1 {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("targeted primary version %v not found", targetedVersion))
		}
		secondaryBuildID := mergeSets.GetSecondarySetBuildId()
		secondarySetIdx, _ := worker_versioning.FindBuildId(data, secondaryBuildID)
		if secondarySetIdx == -1 {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("targeted secondary version %v not found", secondaryBuildID))
		}
		if targetSetIdx == secondarySetIdx {
			// Nothing to be done
			return data, nil
		}
		// Merge the sets together, preserving the primary set's default by making it have the most recent timestamp.
		primarySet := data.VersionSets[targetSetIdx]
		primaryBuildId := primarySet.BuildIds[len(primarySet.BuildIds)-1]
		primaryBuildId.BecameDefaultTimestamp = &timestamp
		justPrimaryData := &persistencespb.VersioningData{
			VersionSets: []*persistencespb.CompatibleVersionSet{primarySet},
		}
		secondarySet := data.VersionSets[secondarySetIdx]
		secondarySet.SetIds = mergeSetIDs(primarySet.SetIds, secondarySet.SetIds)
		data = MergeVersioningData(justPrimaryData, data)
	}

	return data, nil
}

func extractTargetedVersion(req *workflowservice.UpdateWorkerBuildIdCompatibilityRequest) string {
	if req.GetAddNewCompatibleBuildId() != nil {
		return req.GetAddNewCompatibleBuildId().GetNewBuildId()
	} else if req.GetPromoteSetByBuildId() != "" {
		return req.GetPromoteSetByBuildId()
	} else if req.GetPromoteBuildIdWithinSet() != "" {
		return req.GetPromoteBuildIdWithinSet()
	} else if req.GetAddNewBuildIdInNewDefaultSet() != "" {
		return req.GetAddNewBuildIdInNewDefaultSet()
	}
	return req.GetMergeSets().GetPrimarySetBuildId()
}

func makeDefaultSet(data *persistencespb.VersioningData, setIx int, timestamp *hlc.Clock) {
	set := data.VersionSets[setIx]
	set.BecameDefaultTimestamp = timestamp

	if setIx < len(data.VersionSets)-1 {
		// Move the set to the end and shift all the others down
		copy(data.VersionSets[setIx:], data.VersionSets[setIx+1:])
		data.VersionSets[len(data.VersionSets)-1] = set
	}
}

func makeVersionInSetDefault(data *persistencespb.VersioningData, setIx, versionIx int, timestamp *hlc.Clock) {
	setVersions := data.VersionSets[setIx].BuildIds
	buildId := setVersions[versionIx]
	buildId.BecameDefaultTimestamp = timestamp
	if len(setVersions) <= 1 {
		return
	}
	if versionIx < len(setVersions)-1 {
		// Move the build ID to the end and shift all the others down
		copy(setVersions[versionIx:], setVersions[versionIx+1:])
		setVersions[len(setVersions)-1] = buildId
	}
}

// Requires: caps is not nil
func lookupVersionSetForPoll(data *persistencespb.VersioningData, caps *commonpb.WorkerVersionCapabilities) (string, error) {
	// For poll, only the latest version in the compatible set can get tasks.
	// Find the version set that this worker is in.
	// Note data may be nil here, findVersion will return -1 then.
	setIdx, indexInSet := worker_versioning.FindBuildId(data, caps.BuildId)
	if setIdx < 0 {
		// A poller is using a build ID but we don't know about that build ID. This can happen
		// in a replication scenario if pollers are running on the passive side before the data
		// has been replicated. Instead of rejecting, we can guess a set id based on the build
		// ID. If the build ID was the first in its set on the other side, then our guess is
		// right and things will work out. If not, then we'll guess wrong, but when the
		// versioning data replicates, we'll redirect the poll to the correct set id.
		// In the meantime (e.g. during an ungraceful failover) we can at least match tasks
		// using the exact same build ID.
		// TODO: add metric and log to make this situation visible
		guessedSetId := hashBuildId(caps.BuildId)
		return guessedSetId, nil
	}
	set := data.VersionSets[setIdx]
	lastIndex := len(set.BuildIds) - 1
	if indexInSet != lastIndex {
		return "", serviceerror.NewNewerBuildExists(set.BuildIds[lastIndex].Id)
	}
	return getSetID(set), nil
}

// Requires: caps is not nil
func checkVersionForStickyPoll(data *persistencespb.VersioningData, caps *commonpb.WorkerVersionCapabilities) error {
	// For poll, only the latest version in the compatible set can get tasks.
	// Find the version set that this worker is in.
	// Note data may be nil here, findVersion will return -1 then.
	setIdx, indexInSet := worker_versioning.FindBuildId(data, caps.BuildId)
	if setIdx < 0 {
		// A poller is using a build ID but we don't know about that build ID. See comments in
		// lookupVersionSetForPoll. If we consider it the default for its set, then we should
		// leave it on the sticky queue here.
		return nil
	}
	set := data.VersionSets[setIdx]
	lastIndex := len(set.BuildIds) - 1
	if indexInSet != lastIndex {
		return serviceerror.NewNewerBuildExists(set.BuildIds[lastIndex].Id)
	}
	return nil
}

// For this function, buildId == "" means "use default"
func lookupVersionSetForAdd(data *persistencespb.VersioningData, buildId string) (string, error) {
	var set *persistencespb.CompatibleVersionSet
	if buildId == "" {
		// If this is a new workflow, assign it to the latest version.
		// (If it's an unversioned workflow that has already completed one or more tasks, then
		// leave it on the unversioned one. That case is handled already before we get here.)
		setLen := len(data.GetVersionSets())
		if setLen == 0 || data.VersionSets[setLen-1] == nil {
			return "", errEmptyVersioningData
		}
		set = data.VersionSets[setLen-1]
	} else {
		// For add, any version in the compatible set maps to the set.
		// Note data may be nil here, findVersion will return -1 then.
		setIdx, _ := worker_versioning.FindBuildId(data, buildId)
		if setIdx < 0 {
			// TODO: persist guessed set it and then remove this
			return "", errUnknownBuildId
			// A workflow has a build ID set, but we don't know about that build ID. This can
			// happen in replication scenario: the workflow itself was migrated and we failed
			// over, but the versioning data hasn't been migrated yet. Instead of rejecting it,
			// we can guess a set ID based on the build ID. If the build ID was the first in
			// its set on the other side, then our guess is right and things will work out. If
			// not, then we'll guess wrong, but when we get the replication event, we'll merge
			// the sets and use both ids.
			// TODO: add metric and log to make this situation visible
			// guessedSetId := hashBuildId(buildId)
			// return guessedSetId, nil
		}
		set = data.VersionSets[setIdx]
	}
	return getSetID(set), nil
}

// For this function, buildId == "" means "use default"
func checkVersionForStickyAdd(data *persistencespb.VersioningData, buildId string) error {
	if buildId == "" {
		// This shouldn't happen.
		return serviceerror.NewInternal("should have a build id directive on versioned sticky queue")
	}
	// For add, any version in the compatible set maps to the set.
	// Note data may be nil here, findVersion will return -1 then.
	setIdx, indexInSet := worker_versioning.FindBuildId(data, buildId)
	if setIdx < 0 {
		// A poller is using a build ID but we don't know about that build ID. See comments in
		// lookupVersionSetForAdd. If we consider it the default for its set, then we should
		// leave it on the sticky queue here.
		return nil
	}
	// If this is not the set's default anymore, we need to kick it back to the regular queue.
	if indexInSet != len(data.VersionSets[setIdx].BuildIds)-1 {
		return serviceerrors.NewStickyWorkerUnavailable()
	}
	return nil
}

// getSetID returns an arbitrary but consistent member of the set.
// We want Add and Poll requests for the same set to converge on a single id so we can match
// them, but we don't have a single id for a set in the general case: in rare cases we may have
// multiple ids (due to failovers). We can do this by picking an arbitrary id in the set, e.g.
// the first. If the versioning data changes in any way, we'll re-resolve the set id, so this
// choice only has to be consistent within one version of the versioning data. (For correct
// handling of spooled tasks in Add, this does need to be an actual set id, not an arbitrary
// string.)
func getSetID(set *persistencespb.CompatibleVersionSet) string {
	return set.SetIds[0]
}

// ClearTombstones clears all tombstone build ids (with STATE_DELETED) from versioning data.
// Clones data to avoid mutating in place.
func ClearTombstones(versioningData *persistencespb.VersioningData) *persistencespb.VersioningData {
	modifiedData := shallowCloneVersioningData(versioningData)
	for setIdx, set := range modifiedData.GetVersionSets() {
		modifiedData.VersionSets[setIdx] = shallowCloneVersionSet(set)
	}
	for _, set := range modifiedData.GetVersionSets() {
		set.BuildIds = util.FilterSlice(set.BuildIds, func(buildId *persistencespb.BuildId) bool {
			return buildId.State != persistencespb.STATE_DELETED
		})
	}
	modifiedData.VersionSets = util.FilterSlice(modifiedData.VersionSets, func(set *persistencespb.CompatibleVersionSet) bool {
		return len(set.BuildIds) > 0
	})
	return modifiedData
}
