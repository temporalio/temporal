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
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
)

func fromWallClock(wallclock int64) *hlc.Clock {
	return &hlc.Clock{WallClock: wallclock, Version: 0, ClusterId: 1}
}

func buildID(wallclock int64, id string, optionalState ...persistencespb.BuildID_State) *persistencespb.BuildID {
	state := persistencespb.STATE_ACTIVE
	if len(optionalState) == 1 {
		state = optionalState[0]
	}

	return &persistencespb.BuildID{
		Id:                   id,
		State:                state,
		StateUpdateTimestamp: fromWallClock(wallclock),
	}
}

func mkBuildIDs(buildIDs ...*persistencespb.BuildID) []*persistencespb.BuildID {
	buildIDStructs := make([]*persistencespb.BuildID, len(buildIDs))
	for i, buildID := range buildIDs {
		buildIDStructs[i] = &persistencespb.BuildID{
			Id:                   buildID.Id,
			State:                persistencespb.STATE_ACTIVE,
			StateUpdateTimestamp: buildID.StateUpdateTimestamp,
		}
	}
	return buildIDStructs
}

func mkSet(setID string, buildIDs ...*persistencespb.BuildID) *persistencespb.CompatibleVersionSet {
	return &persistencespb.CompatibleVersionSet{
		SetIds:                 []string{setID},
		BuildIds:               mkBuildIDs(buildIDs...),
		DefaultUpdateTimestamp: buildIDs[len(buildIDs)-1].StateUpdateTimestamp,
	}
}

func mkSingleSetData(setID string, buildIDs ...*persistencespb.BuildID) *persistencespb.VersioningData {
	return &persistencespb.VersioningData{
		VersionSets:            []*persistencespb.CompatibleVersionSet{mkSet(setID, buildIDs...)},
		DefaultUpdateTimestamp: buildIDs[len(buildIDs)-1].StateUpdateTimestamp,
	}
}

func TestSetMerge_IdenticalBuildIDsAndGreaterUpdateTimestamp_SetsMaxUpdateTimestamp(t *testing.T) {
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

func TestSetMerge_AdditionalBuildIDAndGreaterUpdateTimestamp_MergesBuildIDsAndSetsMaxUpdateTimestamp(t *testing.T) {
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
		DefaultUpdateTimestamp: fromWallClock(3),
	}
	b := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSet("1.0", buildID(3, "1.0")),
			mkSet("0.1", buildID(2, "0.1"), buildID(4, "0.2")),
		},
		DefaultUpdateTimestamp: fromWallClock(4),
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
			BuildIds:               mkBuildIDs(buildID(1, "0.1"), buildID(6, "0.2")),
			DefaultUpdateTimestamp: fromWallClock(6),
		}},
		DefaultUpdateTimestamp: fromWallClock(6),
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
		DefaultUpdateTimestamp: fromWallClock(2),
	}
	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{{
			SetIds:                 []string{"0.1", "0.2"},
			BuildIds:               mkBuildIDs(buildID(1, "0.1"), buildID(3, "0.2")),
			DefaultUpdateTimestamp: fromWallClock(3),
		}},
		DefaultUpdateTimestamp: fromWallClock(3),
	}
	assert.Equal(t, expected, MergeVersioningData(a, b))
	assert.Equal(t, expected, MergeVersioningData(b, a))
}

func TestSetMerge_BuildIDPromoted_PreservesSetDefault(t *testing.T) {
	a := mkSingleSetData("0.1", buildID(2, "0.1"), buildID(1, "0.2"))
	a.VersionSets[0].DefaultUpdateTimestamp = fromWallClock(3)
	b := mkSingleSetData("0.1", buildID(2, "0.1"), buildID(1, "0.2"))
	b.VersionSets[0].DefaultUpdateTimestamp = fromWallClock(3)
	assert.Equal(t, b, MergeVersioningData(a, b))
	assert.Equal(t, b, MergeVersioningData(b, a))
}

func TestSetMerge_SetPromoted_PreservesGlobalDefault(t *testing.T) {
	a := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSet("0.2", buildID(2, "0.2")),
			mkSet("0.1", buildID(1, "0.1")),
		},
		DefaultUpdateTimestamp: fromWallClock(3),
	}
	b := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSet("0.2", buildID(2, "0.2")),
			mkSet("0.1", buildID(1, "0.1")),
		},
		DefaultUpdateTimestamp: fromWallClock(3),
	}
	assert.Equal(t, b, MergeVersioningData(a, b))
	assert.Equal(t, b, MergeVersioningData(b, a))
}
