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
	"testing"

	"github.com/stretchr/testify/assert"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	commonclock "go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
)

func fromWallClock(wallclock int64) *hlc.Clock {
	return &hlc.Clock{WallClock: wallclock, Version: 0, ClusterId: 1}
}

func buildID(wallclock int64, id string, optionalState ...persistencespb.BuildId_State) *persistencespb.BuildId {
	state := persistencespb.STATE_ACTIVE
	if len(optionalState) == 1 {
		state = optionalState[0]
	}

	return &persistencespb.BuildId{
		Id:                     id,
		State:                  state,
		StateUpdateTimestamp:   fromWallClock(wallclock),
		BecameDefaultTimestamp: fromWallClock(wallclock),
	}
}

func mkSet(setID string, buildIDs ...*persistencespb.BuildId) *persistencespb.CompatibleVersionSet {
	return &persistencespb.CompatibleVersionSet{
		SetIds:                 []string{setID},
		BuildIds:               buildIDs,
		BecameDefaultTimestamp: buildIDs[len(buildIDs)-1].BecameDefaultTimestamp,
	}
}

func mkSingleSetData(setID string, buildIDs ...*persistencespb.BuildId) *persistencespb.VersioningData {
	return &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{mkSet(setID, buildIDs...)},
	}
}

func TestSetMerge_IdenticalBuildIdsAndGreaterUpdateTimestamp_SetsMaxUpdateTimestamp(t *testing.T) {
	//                                           look here 👇
	a := mkSingleSetData("0.1", buildID(1, "0.1"), buildID(6, "0.2"))
	b := mkSingleSetData("0.1", buildID(1, "0.1"), buildID(3, "0.2"))
	assert.Equal(t, a, MergeVersioningData(a, b))
	assert.Equal(t, a, MergeVersioningData(b, a))
}

func TestSetMerge_DataIsNil(t *testing.T) {
	data := mkSingleSetData("0.1", buildID(1, "0.1"), buildID(3, "0.2"))
	assert.Equal(t, data, MergeVersioningData(nil, data))
	assert.Equal(t, data, MergeVersioningData(data, nil))
	var nilData *persistencespb.VersioningData
	assert.Equal(t, nilData, MergeVersioningData(nil, nil))
}

func TestSetMerge_AdditionalBuildIdAndGreaterUpdateTimestamp_MergesBuildIdsAndSetsMaxUpdateTimestamp(t *testing.T) {
	a := mkSingleSetData("0.1", buildID(6, "0.1"))
	b := mkSingleSetData("0.1", buildID(1, "0.1"), buildID(3, "0.2"))
	expected := mkSingleSetData("0.1", buildID(3, "0.2"), buildID(6, "0.1"))
	assert.Equal(t, expected, MergeVersioningData(a, b))
	assert.Equal(t, expected, MergeVersioningData(b, a))
}

func TestSetMerge_NewerDefault_PrefersDefaultAndSetsMaxUpdatedAt(t *testing.T) {
	a := mkSingleSetData("0.1", buildID(1, "0.1"), buildID(6, "0.3"))
	b := mkSingleSetData("0.1", buildID(1, "0.1"), buildID(3, "0.2"))
	expected := mkSingleSetData("0.1", buildID(1, "0.1"), buildID(3, "0.2"), buildID(6, "0.3"))
	assert.Equal(t, expected, MergeVersioningData(a, b))
	assert.Equal(t, expected, MergeVersioningData(b, a))
}

func TestDataMerge_PrefersNewerDefaultAndMergesDefault(t *testing.T) {
	a := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSet("0.1", buildID(2, "0.1")),
			mkSet("1.0", buildID(3, "1.0")),
		},
	}
	b := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSet("1.0", buildID(3, "1.0")),
			mkSet("0.1", buildID(2, "0.1"), buildID(4, "0.2")),
		},
	}
	assert.Equal(t, b, MergeVersioningData(a, b))
	assert.Equal(t, b, MergeVersioningData(b, a))
}

func TestSetMerge_DifferentSetIDs_MergesSetIDs(t *testing.T) {
	a := mkSingleSetData("0.1", buildID(1, "0.1"), buildID(6, "0.2"))
	b := mkSingleSetData("0.2", buildID(3, "0.2"))
	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{{
			SetIds:                 []string{"0.1", "0.2"},
			BuildIds:               []*persistencespb.BuildId{buildID(1, "0.1"), buildID(6, "0.2")},
			BecameDefaultTimestamp: fromWallClock(6),
		}},
	}
	assert.Equal(t, expected, MergeVersioningData(a, b))
	assert.Equal(t, expected, MergeVersioningData(b, a))
}

func TestSetMerge_DifferentStates_UpdatesTimestampsAndState(t *testing.T) {
	a := mkSingleSetData("0.1", buildID(1, "0.1"), buildID(6, "0.2", persistencespb.STATE_DELETED), buildID(7, "0.3", persistencespb.STATE_DELETED))
	b := mkSingleSetData("0.1", buildID(3, "0.1", persistencespb.STATE_DELETED), buildID(7, "0.2"), buildID(8, "0.3"))
	assert.Equal(t, b, MergeVersioningData(a, b))
	assert.Equal(t, b, MergeVersioningData(b, a))
}

func TestSetMerge_MultipleMatches_MergesSets(t *testing.T) {
	a := mkSingleSetData("0.1", buildID(1, "0.1"), buildID(3, "0.2"))
	b := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSet("0.1", buildID(1, "0.1")),
			mkSet("0.2", buildID(2, "0.2")),
		},
	}
	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{{
			SetIds:                 []string{"0.1", "0.2"},
			BuildIds:               []*persistencespb.BuildId{buildID(1, "0.1"), buildID(3, "0.2")},
			BecameDefaultTimestamp: fromWallClock(3),
		}},
	}
	assert.Equal(t, expected, MergeVersioningData(a, b))
	assert.Equal(t, expected, MergeVersioningData(b, a))
}

func TestSetMerge_BuildIdPromoted_PreservesSetDefault(t *testing.T) {
	a := mkSingleSetData("0.1", buildID(2, "0.1"), buildID(1, "0.2"))
	a.VersionSets[0].BuildIds[len(a.VersionSets[0].BuildIds)-1].BecameDefaultTimestamp = fromWallClock(3)
	b := mkSingleSetData("0.1", buildID(2, "0.1"), buildID(1, "0.2"))
	b.VersionSets[0].BuildIds[len(b.VersionSets[0].BuildIds)-1].BecameDefaultTimestamp = fromWallClock(3)
	assert.Equal(t, b, MergeVersioningData(a, b))
	assert.Equal(t, b, MergeVersioningData(b, a))
}

func TestSetMerge_SetPromoted_PreservesGlobalDefault(t *testing.T) {
	set01 := mkSet("0.1", buildID(1, "0.1"))
	set01.BecameDefaultTimestamp = fromWallClock(3)
	a := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSet("0.2", buildID(2, "0.2")),
			set01,
		},
	}
	b := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSet("0.2", buildID(2, "0.2")),
			set01,
		},
	}
	assert.Equal(t, b, MergeVersioningData(a, b))
	assert.Equal(t, b, MergeVersioningData(b, a))
}

func TestPersistUnknownBuildId_Merge(t *testing.T) {
	t.Parallel()
	clock := hlc.Next(hlc.Zero(1), commonclock.NewRealTimeSource())
	initialData := mkInitialData(2, clock) // ids: "0", "1"

	// on a's side, 1.1 was added as unknown
	a := PersistUnknownBuildId(clock, initialData, "1.1")

	// on b's side, 1.1 was added compatible with 1
	req := mkNewCompatReq("1.1", "1", true)
	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	b, err := UpdateVersionSets(nextClock, initialData, req, 0, 0)
	assert.NoError(t, err)

	// now merge them. we should see 1.1 in a set with 1, but it should have two set ids
	ab := MergeVersioningData(a, b)
	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSingleBuildIdSet("0", clock),
			{
				SetIds: []string{hashBuildId("1"), hashBuildId("1.1")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("1", clock),
					mkBuildId("1.1", nextClock),
				},
				BecameDefaultTimestamp: nextClock,
			},
		},
	}
	assert.Equal(t, expected, ab)

	// the other way too
	ba := MergeVersioningData(b, a)
	assert.Equal(t, expected, ba)
}
