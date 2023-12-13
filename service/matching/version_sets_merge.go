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
		for _, id := range set.SetIds {
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
		for _, buildID := range set.BuildIds {
			if info, found := buildIDToInfo[buildID.Id]; found {
				// A build ID appears in more than one source, merge its information, and track it
				state := info.state
				stateUpdateTimestamp := hlc.Max(buildID.StateUpdateTimestamp, info.stateUpdateTimestamp)
				if hlc.Equal(stateUpdateTimestamp, buildID.StateUpdateTimestamp) {
					state = buildID.State
				}
				buildIDToInfo[buildID.Id] = buildIDInfo{
					state:                state,
					stateUpdateTimestamp: stateUpdateTimestamp,
					setIDs:               mergeSetIDs(info.setIDs, set.SetIds),
					madeDefaultAt:        hlc.Max(buildID.BecameDefaultTimestamp, info.madeDefaultAt),
					setMadeDefaultAt:     hlc.Max(set.BecameDefaultTimestamp, info.setMadeDefaultAt),
				}
			} else {
				// A build ID was seen for the first time, track it
				buildIDToInfo[buildID.Id] = buildIDInfo{
					state:                buildID.State,
					stateUpdateTimestamp: buildID.StateUpdateTimestamp,
					setIDs:               set.SetIds,
					madeDefaultAt:        buildID.BecameDefaultTimestamp,
					setMadeDefaultAt:     set.BecameDefaultTimestamp,
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
			set = &persistencespb.CompatibleVersionSet{
				SetIds:                 info.setIDs,
				BuildIds:               make([]*persistencespb.BuildId, 0),
				BecameDefaultTimestamp: info.setMadeDefaultAt,
			}
			sets = append(sets, set)
		} else {
			set.SetIds = mergeSetIDs(set.SetIds, info.setIDs)
			set.BecameDefaultTimestamp = hlc.Max(info.setMadeDefaultAt, set.BecameDefaultTimestamp)
		}
		buildID := &persistencespb.BuildId{
			Id:                     id,
			State:                  info.state,
			StateUpdateTimestamp:   info.stateUpdateTimestamp,
			BecameDefaultTimestamp: info.madeDefaultAt,
		}
		set.BuildIds = append(set.BuildIds, buildID)
	}
	// Sort the sets based on their default update timestamp, ensuring the default set comes last
	sortSets(sets)
	for _, set := range sets {
		sortBuildIds(set.BuildIds)
	}
	return sets
}

func sortSets(sets []*persistencespb.CompatibleVersionSet) {
	sort.Slice(sets, func(i, j int) bool {
		return hlc.Less(sets[i].BecameDefaultTimestamp, sets[j].BecameDefaultTimestamp)
	})
}

func sortBuildIds(buildIds []*persistencespb.BuildId) {
	sort.Slice(buildIds, func(i, j int) bool {
		return hlc.Less(buildIds[i].BecameDefaultTimestamp, buildIds[j].BecameDefaultTimestamp)
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
	buildIDToInfo := collectBuildIdInfo(append(a.VersionSets, b.VersionSets...))
	// Build the merged compatible sets using collected build ID information
	sets := intoVersionSets(buildIDToInfo)

	return &persistencespb.VersioningData{
		VersionSets: sets,
	}
}
