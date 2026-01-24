package matching

import (
	"sort"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
)

// Merge and sort two sets of set IDs
func mergeSetIDs(a []string, b []string) []string {
	var mergedSetIDs []string
	seenSetIDs := make(map[string]struct{}, len(a))
	mergedSetIDs = append(mergedSetIDs, a...)
	for _, setID := range a {
		seenSetIDs[setID] = struct{}{}
	}
	for _, setID := range b {
		if _, found := seenSetIDs[setID]; !found {
			mergedSetIDs = append(mergedSetIDs, setID)
		}
	}
	sort.Strings(mergedSetIDs)
	return mergedSetIDs
}

// Check if a set contains any of the given set IDs.
func setContainsSetIDs(set *persistencespb.CompatibleVersionSet, ids []string) bool {
	for _, needle := range ids {
		for _, id := range set.GetSetIds() {
			if needle == id {
				return true
			}
		}
	}
	return false
}

func findSetWithSetIDs(sets []*persistencespb.CompatibleVersionSet, ids []string) *persistencespb.CompatibleVersionSet {
	for _, set := range sets {
		if setContainsSetIDs(set, ids) {
			return set
		}
	}
	return nil
}

type buildIDInfo struct {
	state                persistencespb.BuildId_State
	stateUpdateTimestamp *hlc.Clock
	setIDs               []string
	madeDefaultAt        *hlc.Clock
	setMadeDefaultAt     *hlc.Clock
}

func collectBuildIdInfo(sets []*persistencespb.CompatibleVersionSet) map[string]buildIDInfo {
	buildIDToInfo := make(map[string]buildIDInfo, 0)
	for _, set := range sets {
		for _, buildID := range set.GetBuildIds() {
			if info, found := buildIDToInfo[buildID.GetId()]; found {
				// A build ID appears in more than one source, merge its information, and track it
				state := info.state
				stateUpdateTimestamp := hlc.Max(buildID.GetStateUpdateTimestamp(), info.stateUpdateTimestamp)
				if hlc.Equal(stateUpdateTimestamp, buildID.GetStateUpdateTimestamp()) {
					state = buildID.GetState()
				}
				buildIDToInfo[buildID.GetId()] = buildIDInfo{
					state:                state,
					stateUpdateTimestamp: stateUpdateTimestamp,
					setIDs:               mergeSetIDs(info.setIDs, set.GetSetIds()),
					madeDefaultAt:        hlc.Max(buildID.GetBecameDefaultTimestamp(), info.madeDefaultAt),
					setMadeDefaultAt:     hlc.Max(set.GetBecameDefaultTimestamp(), info.setMadeDefaultAt),
				}
			} else {
				// A build ID was seen for the first time, track it
				buildIDToInfo[buildID.GetId()] = buildIDInfo{
					state:                buildID.GetState(),
					stateUpdateTimestamp: buildID.GetStateUpdateTimestamp(),
					setIDs:               set.GetSetIds(),
					madeDefaultAt:        buildID.GetBecameDefaultTimestamp(),
					setMadeDefaultAt:     set.GetBecameDefaultTimestamp(),
				}
			}
		}
	}
	return buildIDToInfo
}

func intoVersionSets(buildIDToInfo map[string]buildIDInfo) []*persistencespb.CompatibleVersionSet {
	sets := make([]*persistencespb.CompatibleVersionSet, 0)
	for id, info := range buildIDToInfo {
		info := info
		set := findSetWithSetIDs(sets, info.setIDs)
		if set == nil {
			set = persistencespb.CompatibleVersionSet_builder{
				SetIds:                 info.setIDs,
				BuildIds:               make([]*persistencespb.BuildId, 0),
				BecameDefaultTimestamp: info.setMadeDefaultAt,
			}.Build()
			sets = append(sets, set)
		} else {
			set.SetSetIds(mergeSetIDs(set.GetSetIds(), info.setIDs))
			set.SetBecameDefaultTimestamp(hlc.Max(info.setMadeDefaultAt, set.GetBecameDefaultTimestamp()))
		}
		buildID := persistencespb.BuildId_builder{
			Id:                     id,
			State:                  info.state,
			StateUpdateTimestamp:   info.stateUpdateTimestamp,
			BecameDefaultTimestamp: info.madeDefaultAt,
		}.Build()
		set.SetBuildIds(append(set.GetBuildIds(), buildID))
	}
	// Sort the sets based on their default update timestamp, ensuring the default set comes last
	sortSets(sets)
	for _, set := range sets {
		sortBuildIds(set.GetBuildIds())
	}
	return sets
}

func sortSets(sets []*persistencespb.CompatibleVersionSet) {
	sort.Slice(sets, func(i, j int) bool {
		return hlc.Less(sets[i].GetBecameDefaultTimestamp(), sets[j].GetBecameDefaultTimestamp())
	})
}

func sortBuildIds(buildIds []*persistencespb.BuildId) {
	sort.Slice(buildIds, func(i, j int) bool {
		return hlc.Less(buildIds[i].GetBecameDefaultTimestamp(), buildIds[j].GetBecameDefaultTimestamp())
	})
}

// MergeVersioningData merges two VersioningData structs.
// If a build ID appears in both data structures, the merged structure will include that latest status and timestamp.
// If a build ID appears in different sets in the different structures, those sets will be merged.
// The merged data's per set default and global default will be set according to the latest timestamps in the sources.
// if (a) is nil, (b) is returned as is, otherwise, if (b) is nil (a) is returned as is.
func MergeVersioningData(a *persistencespb.VersioningData, b *persistencespb.VersioningData) *persistencespb.VersioningData {
	if a == nil {
		return b
	} else if b == nil {
		return a
	}

	// Collect information about each build ID from both sources
	buildIDToInfo := collectBuildIdInfo(append(a.GetVersionSets(), b.GetVersionSets()...))
	// Build the merged compatible sets using collected build ID information
	sets := intoVersionSets(buildIDToInfo)

	return persistencespb.VersioningData_builder{
		VersionSets: sets,
	}.Build()
}
