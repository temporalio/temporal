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
	"testing"

	"github.com/stretchr/testify/assert"

	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockpb "go.temporal.io/server/api/clock/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
)

func mkNewSet(id string, clock clockpb.HybridLogicalClock) *persistencepb.CompatibleVersionSet {
	return &persistencepb.CompatibleVersionSet{
		SetIds:                 []string{id},
		BuildIds:               []*persistencepb.BuildID{{Id: id, State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock}},
		DefaultUpdateTimestamp: &clock,
	}
}

func mkInitialData(numSets int, clock clockpb.HybridLogicalClock) *persistencepb.VersioningData {
	sets := make([]*persistencepb.CompatibleVersionSet, numSets)
	for i := 0; i < numSets; i++ {
		sets[i] = mkNewSet(fmt.Sprintf("%v", i), clock)
	}
	return &persistencepb.VersioningData{
		VersionSets:            sets,
		DefaultUpdateTimestamp: &clock,
	}
}

func mkUserData(numSets int) *persistencepb.TaskQueueUserData {
	clock := hlc.Zero(1)
	return &persistencepb.TaskQueueUserData{
		Clock:          &clock,
		VersioningData: mkInitialData(numSets, clock),
	}
}

func mkNewDefReq(id string) *workflowservice.UpdateWorkerBuildIdCompatibilityRequest {
	return &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: id,
		},
	}
}
func mkNewCompatReq(id, compat string, becomeDefault bool) *workflowservice.UpdateWorkerBuildIdCompatibilityRequest {
	return &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId{
			AddNewCompatibleBuildId: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
				NewBuildId:                id,
				ExistingCompatibleBuildId: compat,
				MakeSetDefault:            becomeDefault,
			},
		},
	}
}
func mkExistingDefault(id string) *workflowservice.UpdateWorkerBuildIdCompatibilityRequest {
	return &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_PromoteSetByBuildId{
			PromoteSetByBuildId: id,
		},
	}
}
func mkPromoteInSet(id string) *workflowservice.UpdateWorkerBuildIdCompatibilityRequest {
	return &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_PromoteBuildIdWithinSet{
			PromoteBuildIdWithinSet: id,
		},
	}
}

// Asserts that clock1 is greater than clock2
func assertClockGreater(t *testing.T, clock1 clockpb.HybridLogicalClock, clock2 clockpb.HybridLogicalClock) {
	assert.True(t, hlc.Greater(clock1, clock2))
}

func TestNewDefaultUpdate(t *testing.T) {
	clock := hlc.Zero(1)
	initialData := mkInitialData(2, clock)

	req := mkNewDefReq("2")
	nextClock, updatedData, err := UpdateVersionSets(clock, initialData, req, 0, 0)
	assert.NoError(t, err)
	assertClockGreater(t, nextClock, clock)
	assert.Equal(t, mkInitialData(2, clock), initialData)

	expected := &persistencepb.VersioningData{
		DefaultUpdateTimestamp: &nextClock,
		VersionSets: []*persistencepb.CompatibleVersionSet{
			{
				SetIds:                 []string{"0"},
				BuildIds:               []*persistencepb.BuildID{{Id: "0", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock}},
				DefaultUpdateTimestamp: &clock,
			},
			{
				SetIds:                 []string{"1"},
				BuildIds:               []*persistencepb.BuildID{{Id: "1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock}},
				DefaultUpdateTimestamp: &clock,
			},
			{
				SetIds:                 []string{"2"},
				BuildIds:               []*persistencepb.BuildID{{Id: "2", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &nextClock}},
				DefaultUpdateTimestamp: &nextClock,
			},
		},
	}
	assert.Equal(t, expected, updatedData)

	asResp := ToBuildIdOrderingResponse(updatedData, 0)
	assert.Equal(t, "2", asResp.MajorVersionSets[2].BuildIds[0])
}

func TestNewDefaultSetUpdateOfEmptyData(t *testing.T) {
	clock := hlc.Zero(1)
	initialData := mkInitialData(0, clock)

	req := mkNewDefReq("1")
	nextClock, updatedData, err := UpdateVersionSets(clock, initialData, req, 0, 0)
	assert.NoError(t, err)
	assertClockGreater(t, nextClock, clock)
	assert.Equal(t, mkInitialData(0, clock), initialData)

	expected := &persistencepb.VersioningData{
		DefaultUpdateTimestamp: &nextClock,
		VersionSets: []*persistencepb.CompatibleVersionSet{
			{
				SetIds:                 []string{"1"},
				BuildIds:               []*persistencepb.BuildID{{Id: "1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &nextClock}},
				DefaultUpdateTimestamp: &nextClock,
			},
		},
	}
	assert.Equal(t, expected, updatedData)
}

func TestNewDefaultSetUpdateCompatWithCurDefault(t *testing.T) {
	clock := hlc.Zero(1)
	initialData := mkInitialData(2, clock)

	req := mkNewCompatReq("1.1", "1", true)
	nextClock, updatedData, err := UpdateVersionSets(clock, initialData, req, 0, 0)
	assert.NoError(t, err)
	assertClockGreater(t, nextClock, clock)
	assert.Equal(t, mkInitialData(2, clock), initialData)

	expected := &persistencepb.VersioningData{
		DefaultUpdateTimestamp: &nextClock,
		VersionSets: []*persistencepb.CompatibleVersionSet{
			{
				SetIds:                 []string{"0"},
				BuildIds:               []*persistencepb.BuildID{{Id: "0", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock}},
				DefaultUpdateTimestamp: &clock,
			},
			{
				SetIds: []string{"1"},
				BuildIds: []*persistencepb.BuildID{
					{Id: "1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock},
					{Id: "1.1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &nextClock},
				},
				DefaultUpdateTimestamp: &nextClock,
			},
		},
	}
	assert.Equal(t, expected, updatedData)
}

func TestNewDefaultSetUpdateCompatWithNonDefaultSet(t *testing.T) {
	clock := hlc.Zero(1)
	initialData := mkInitialData(2, clock)

	req := mkNewCompatReq("0.1", "0", true)
	nextClock, updatedData, err := UpdateVersionSets(clock, initialData, req, 0, 0)
	assert.NoError(t, err)
	assertClockGreater(t, nextClock, clock)
	assert.Equal(t, mkInitialData(2, clock), initialData)

	expected := &persistencepb.VersioningData{
		DefaultUpdateTimestamp: &nextClock,
		VersionSets: []*persistencepb.CompatibleVersionSet{
			{
				SetIds:                 []string{"1"},
				BuildIds:               []*persistencepb.BuildID{{Id: "1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock}},
				DefaultUpdateTimestamp: &clock,
			},
			{
				SetIds: []string{"0"},
				BuildIds: []*persistencepb.BuildID{
					{Id: "0", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock},
					{Id: "0.1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &nextClock},
				},
				DefaultUpdateTimestamp: &nextClock,
			},
		},
	}
	assert.Equal(t, expected, updatedData)
}

func TestNewCompatibleWithVerInOlderSet(t *testing.T) {
	clock := hlc.Zero(1)
	initialData := mkInitialData(2, clock)

	req := mkNewCompatReq("0.1", "0", false)
	nextClock, updatedData, err := UpdateVersionSets(clock, initialData, req, 0, 0)
	assert.NoError(t, err)
	assertClockGreater(t, nextClock, clock)
	assert.Equal(t, mkInitialData(2, clock), initialData)

	expected := &persistencepb.VersioningData{
		DefaultUpdateTimestamp: &clock,
		VersionSets: []*persistencepb.CompatibleVersionSet{
			{
				SetIds: []string{"0"},
				BuildIds: []*persistencepb.BuildID{
					{Id: "0", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock},
					{Id: "0.1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &nextClock},
				},
				DefaultUpdateTimestamp: &nextClock,
			},
			{
				SetIds:                 []string{"1"},
				BuildIds:               []*persistencepb.BuildID{{Id: "1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock}},
				DefaultUpdateTimestamp: &clock,
			},
		},
	}

	assert.Equal(t, expected, updatedData)
	asResp := ToBuildIdOrderingResponse(updatedData, 0)
	assert.Equal(t, "0.1", asResp.MajorVersionSets[0].BuildIds[1])
}

func TestNewCompatibleWithNonDefaultSetUpdate(t *testing.T) {
	clock0 := hlc.Zero(1)
	data := mkInitialData(2, clock0)

	req := mkNewCompatReq("0.1", "0", false)
	clock1, data, err := UpdateVersionSets(clock0, data, req, 0, 0)
	assert.NoError(t, err)

	req = mkNewCompatReq("0.2", "0.1", false)
	clock2, data, err := UpdateVersionSets(clock1, data, req, 0, 0)
	assert.NoError(t, err)

	expected := &persistencepb.VersioningData{
		DefaultUpdateTimestamp: &clock0,
		VersionSets: []*persistencepb.CompatibleVersionSet{
			{
				SetIds: []string{"0"},
				BuildIds: []*persistencepb.BuildID{
					{Id: "0", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock0},
					{Id: "0.1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock1},
					{Id: "0.2", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock2},
				},
				DefaultUpdateTimestamp: &clock2,
			},
			{
				SetIds:                 []string{"1"},
				BuildIds:               []*persistencepb.BuildID{{Id: "1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock0}},
				DefaultUpdateTimestamp: &clock0,
			},
		},
	}

	assert.Equal(t, expected, data)
	// Ensure setting a compatible version which targets a non-leaf compat version ends up without a branch
	req = mkNewCompatReq("0.3", "0.1", false)
	clock3, data, err := UpdateVersionSets(clock2, data, req, 0, 0)
	assert.NoError(t, err)

	expected = &persistencepb.VersioningData{
		DefaultUpdateTimestamp: &clock0,
		VersionSets: []*persistencepb.CompatibleVersionSet{
			{
				SetIds: []string{"0"},
				BuildIds: []*persistencepb.BuildID{
					{Id: "0", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock0},
					{Id: "0.1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock1},
					{Id: "0.2", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock2},
					{Id: "0.3", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock3},
				},
				DefaultUpdateTimestamp: &clock3,
			},
			{
				SetIds:                 []string{"1"},
				BuildIds:               []*persistencepb.BuildID{{Id: "1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock0}},
				DefaultUpdateTimestamp: &clock0,
			},
		},
	}

	assert.Equal(t, expected, data)
}

func TestCompatibleTargetsNotFound(t *testing.T) {
	clock := hlc.Zero(1)
	data := mkInitialData(1, clock)

	req := mkNewCompatReq("1.1", "1", false)
	_, _, err := UpdateVersionSets(clock, data, req, 0, 0)
	assert.Error(t, err)
	assert.IsType(t, &serviceerror.NotFound{}, err)
}

func TestMakeExistingSetDefault(t *testing.T) {
	clock0 := hlc.Zero(1)
	data := mkInitialData(3, clock0)

	req := mkExistingDefault("1")
	clock1, data, err := UpdateVersionSets(clock0, data, req, 0, 0)
	assert.NoError(t, err)

	expected := &persistencepb.VersioningData{
		DefaultUpdateTimestamp: &clock1,
		VersionSets: []*persistencepb.CompatibleVersionSet{
			{
				SetIds: []string{"0"},
				BuildIds: []*persistencepb.BuildID{
					{Id: "0", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock0},
				},
				DefaultUpdateTimestamp: &clock0,
			},
			{
				SetIds:                 []string{"2"},
				BuildIds:               []*persistencepb.BuildID{{Id: "2", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock0}},
				DefaultUpdateTimestamp: &clock0,
			},
			{
				SetIds:                 []string{"1"},
				BuildIds:               []*persistencepb.BuildID{{Id: "1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock0}},
				DefaultUpdateTimestamp: &clock0,
			},
		},
	}

	assert.Equal(t, expected, data)
	// Add a compatible version to a set and then make that set the default via the compatible version
	req = mkNewCompatReq("0.1", "0", true)

	clock2, data, err := UpdateVersionSets(clock1, data, req, 0, 0)
	assert.NoError(t, err)

	expected = &persistencepb.VersioningData{
		DefaultUpdateTimestamp: &clock2,
		VersionSets: []*persistencepb.CompatibleVersionSet{
			{
				SetIds:                 []string{"2"},
				BuildIds:               []*persistencepb.BuildID{{Id: "2", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock0}},
				DefaultUpdateTimestamp: &clock0,
			},
			{
				SetIds:                 []string{"1"},
				BuildIds:               []*persistencepb.BuildID{{Id: "1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock0}},
				DefaultUpdateTimestamp: &clock0,
			},
			{
				SetIds: []string{"0"},
				BuildIds: []*persistencepb.BuildID{
					{Id: "0", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock0},
					{Id: "0.1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock2},
				},
				DefaultUpdateTimestamp: &clock2,
			},
		},
	}
	assert.Equal(t, expected, data)
}

func TestSayVersionIsCompatWithDifferentSetThanItsAlreadyCompatWithNotAllowed(t *testing.T) {
	clock := hlc.Zero(1)
	data := mkInitialData(3, clock)

	req := mkNewCompatReq("0.1", "0", false)
	_, data, err := UpdateVersionSets(clock, data, req, 0, 0)
	assert.NoError(t, err)

	req = mkNewCompatReq("0.1", "1", false)
	_, _, err = UpdateVersionSets(clock, data, req, 0, 0)
	assert.Error(t, err)
	assert.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestLimitsMaxSets(t *testing.T) {
	clock := hlc.Zero(1)
	maxSets := 10
	data := mkInitialData(maxSets, clock)

	req := mkNewDefReq("10")
	_, _, err := UpdateVersionSets(clock, data, req, maxSets, 0)
	assert.Error(t, err)
	assert.IsType(t, &serviceerror.FailedPrecondition{}, err)
}

func TestLimitsMaxBuildIDs(t *testing.T) {
	clock := hlc.Zero(1)
	maxBuildIDs := 10
	data := mkInitialData(maxBuildIDs, clock)

	req := mkNewDefReq("10")
	_, _, err := UpdateVersionSets(clock, data, req, 0, maxBuildIDs)
	assert.Error(t, err)
	assert.IsType(t, &serviceerror.FailedPrecondition{}, err)
}

func TestPromoteWithinVersion(t *testing.T) {
	clock0 := hlc.Zero(1)
	data := mkInitialData(2, clock0)

	req := mkNewCompatReq("0.1", "0", false)
	clock1, data, err := UpdateVersionSets(clock0, data, req, 0, 0)
	assert.NoError(t, err)
	req = mkNewCompatReq("0.2", "0", false)
	clock2, data, err := UpdateVersionSets(clock1, data, req, 0, 0)
	assert.NoError(t, err)
	req = mkPromoteInSet("0.1")
	clock3, data, err := UpdateVersionSets(clock2, data, req, 0, 0)
	assert.NoError(t, err)

	expected := &persistencepb.VersioningData{
		DefaultUpdateTimestamp: &clock0,
		VersionSets: []*persistencepb.CompatibleVersionSet{
			{
				SetIds: []string{"0"},
				BuildIds: []*persistencepb.BuildID{
					{Id: "0", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock0},
					{Id: "0.2", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock2},
					{Id: "0.1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock1},
				},
				DefaultUpdateTimestamp: &clock3,
			},
			{
				SetIds:                 []string{"1"},
				BuildIds:               []*persistencepb.BuildID{{Id: "1", State: persistencepb.STATE_ACTIVE, StateUpdateTimestamp: &clock0}},
				DefaultUpdateTimestamp: &clock0,
			},
		},
	}
	assert.Equal(t, expected, data)
}

func TestAddAlreadyExtantVersionAsDefaultErrors(t *testing.T) {
	clock := hlc.Zero(1)
	data := mkInitialData(3, clock)

	req := mkNewDefReq("0")
	_, _, err := UpdateVersionSets(clock, data, req, 0, 0)
	assert.Error(t, err)
	assert.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestAddAlreadyExtantVersionToAnotherSetErrors(t *testing.T) {
	clock := hlc.Zero(1)
	data := mkInitialData(3, clock)

	req := mkNewCompatReq("0", "1", false)
	_, _, err := UpdateVersionSets(clock, data, req, 0, 0)
	assert.Error(t, err)
	assert.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestMakeSetDefaultTargetingNonexistentVersionErrors(t *testing.T) {
	clock := hlc.Zero(1)
	data := mkInitialData(3, clock)

	req := mkExistingDefault("crab boi")
	_, _, err := UpdateVersionSets(clock, data, req, 0, 0)
	assert.Error(t, err)
	assert.IsType(t, &serviceerror.NotFound{}, err)
}

func TestPromoteWithinSetTargetingNonexistentVersionErrors(t *testing.T) {
	clock := hlc.Zero(1)
	data := mkInitialData(3, clock)

	req := mkPromoteInSet("i'd rather be writing rust ;)")
	_, _, err := UpdateVersionSets(clock, data, req, 0, 0)
	assert.Error(t, err)
	assert.IsType(t, &serviceerror.NotFound{}, err)
}

func TestToBuildIdOrderingResponseTrimsResponse(t *testing.T) {
	clock := hlc.Zero(1)
	data := mkInitialData(3, clock)
	actual := ToBuildIdOrderingResponse(data, 2)
	expected := []*taskqueuepb.CompatibleVersionSet{{BuildIds: []string{"1"}}, {BuildIds: []string{"2"}}}
	assert.Equal(t, expected, actual.MajorVersionSets)
}
