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
	clockspb "go.temporal.io/server/api/clock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	commonclock "go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/testing/protoassert"
)

func mkNewSet(id string, clock *clockspb.HybridLogicalClock) *persistencespb.CompatibleVersionSet {
	return &persistencespb.CompatibleVersionSet{
		SetIds:                 []string{hashBuildId(id)},
		BuildIds:               []*persistencespb.BuildId{{Id: id, State: persistencespb.STATE_ACTIVE, StateUpdateTimestamp: clock, BecameDefaultTimestamp: clock}},
		BecameDefaultTimestamp: clock,
	}
}

func mkInitialData(numSets int, clock *clockspb.HybridLogicalClock) *persistencespb.VersioningData {
	sets := make([]*persistencespb.CompatibleVersionSet, numSets)
	for i := 0; i < numSets; i++ {
		sets[i] = mkNewSet(fmt.Sprintf("%v", i), clock)
	}
	return &persistencespb.VersioningData{
		VersionSets: sets,
	}
}

func mkUserData(numSets int) *persistencespb.TaskQueueUserData {
	clock := hlc.Zero(1)
	return &persistencespb.TaskQueueUserData{
		Clock:          clock,
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
func mkMergeSet(primaryId string, secondaryId string) *workflowservice.UpdateWorkerBuildIdCompatibilityRequest {
	return &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_MergeSets_{
			MergeSets: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_MergeSets{
				PrimarySetBuildId:   primaryId,
				SecondarySetBuildId: secondaryId,
			},
		},
	}
}

func mkBuildId(id string, clock *hlc.Clock) *persistencespb.BuildId {
	return &persistencespb.BuildId{
		Id:                     id,
		State:                  persistencespb.STATE_ACTIVE,
		StateUpdateTimestamp:   clock,
		BecameDefaultTimestamp: clock,
	}
}

func mkSingleBuildIdSet(id string, clock *hlc.Clock) *persistencespb.CompatibleVersionSet {
	return &persistencespb.CompatibleVersionSet{
		SetIds:                 []string{hashBuildId(id)},
		BuildIds:               []*persistencespb.BuildId{mkBuildId(id, clock)},
		BecameDefaultTimestamp: clock,
	}
}

func TestNewDefaultUpdate(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	initialData := mkInitialData(2, clock)

	req := mkNewDefReq("2")
	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	updatedData, err := UpdateVersionSets(nextClock, initialData, req, 0, 0)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, mkInitialData(2, clock), initialData)

	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSingleBuildIdSet("0", clock),
			mkSingleBuildIdSet("1", clock),
			mkSingleBuildIdSet("2", nextClock),
		},
	}
	protoassert.ProtoEqual(t, expected, updatedData)

	asResp := ToBuildIdOrderingResponse(updatedData, 0)
	assert.Equal(t, "2", asResp.MajorVersionSets[2].BuildIds[0])
}

func TestNewDefaultSetUpdateOfEmptyData(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	initialData := mkInitialData(0, clock)

	req := mkNewDefReq("1")
	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	updatedData, err := UpdateVersionSets(nextClock, initialData, req, 0, 0)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, mkInitialData(0, clock), initialData)

	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSingleBuildIdSet("1", nextClock),
		},
	}
	protoassert.ProtoEqual(t, expected, updatedData)
}

func TestNewDefaultSetUpdateCompatWithCurDefault(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	initialData := mkInitialData(2, clock)

	req := mkNewCompatReq("1.1", "1", true)
	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	updatedData, err := UpdateVersionSets(nextClock, initialData, req, 0, 0)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, mkInitialData(2, clock), initialData)

	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSingleBuildIdSet("0", clock),
			{
				SetIds: []string{hashBuildId("1")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("1", clock),
					mkBuildId("1.1", nextClock),
				},
				BecameDefaultTimestamp: nextClock,
			},
		},
	}
	protoassert.ProtoEqual(t, expected, updatedData)
}

func TestNewDefaultSetUpdateCompatWithNonDefaultSet(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	initialData := mkInitialData(2, clock)

	req := mkNewCompatReq("0.1", "0", true)
	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	updatedData, err := UpdateVersionSets(nextClock, initialData, req, 0, 0)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, mkInitialData(2, clock), initialData)

	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSingleBuildIdSet("1", clock),
			{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("0", clock),
					mkBuildId("0.1", nextClock),
				},
				BecameDefaultTimestamp: nextClock,
			},
		},
	}
	protoassert.ProtoEqual(t, expected, updatedData)
}

func TestNewCompatibleWithVerInOlderSet(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	initialData := mkInitialData(2, clock)

	req := mkNewCompatReq("0.1", "0", false)
	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	updatedData, err := UpdateVersionSets(nextClock, initialData, req, 0, 0)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, mkInitialData(2, clock), initialData)

	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("0", clock),
					mkBuildId("0.1", nextClock),
				},
				BecameDefaultTimestamp: clock,
			},
			mkSingleBuildIdSet("1", clock),
		},
	}

	protoassert.ProtoEqual(t, expected, updatedData)
	asResp := ToBuildIdOrderingResponse(updatedData, 0)
	assert.Equal(t, "0.1", asResp.MajorVersionSets[0].BuildIds[1])
}

func TestNewCompatibleWithNonDefaultSetUpdate(t *testing.T) {
	t.Parallel()
	clock0 := hlc.Zero(1)
	data := mkInitialData(2, clock0)

	req := mkNewCompatReq("0.1", "0", false)
	clock1 := hlc.Next(clock0, commonclock.NewRealTimeSource())
	data, err := UpdateVersionSets(clock1, data, req, 0, 0)
	assert.NoError(t, err)

	req = mkNewCompatReq("0.2", "0.1", false)
	clock2 := hlc.Next(clock1, commonclock.NewRealTimeSource())
	data, err = UpdateVersionSets(clock2, data, req, 0, 0)
	assert.NoError(t, err)

	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("0", clock0),
					mkBuildId("0.1", clock1),
					mkBuildId("0.2", clock2),
				},
				BecameDefaultTimestamp: clock0,
			},
			mkSingleBuildIdSet("1", clock0),
		},
	}

	protoassert.ProtoEqual(t, expected, data)
	// Ensure setting a compatible version which targets a non-leaf compat version ends up without a branch
	req = mkNewCompatReq("0.3", "0.1", false)
	clock3 := hlc.Next(clock1, commonclock.NewRealTimeSource())
	data, err = UpdateVersionSets(clock3, data, req, 0, 0)
	assert.NoError(t, err)

	expected = &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("0", clock0),
					mkBuildId("0.1", clock1),
					mkBuildId("0.2", clock2),
					mkBuildId("0.3", clock3),
				},
				BecameDefaultTimestamp: clock0,
			},
			mkSingleBuildIdSet("1", clock0),
		},
	}

	protoassert.ProtoEqual(t, expected, data)
}

func TestCompatibleTargetsNotFound(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(1, clock)

	req := mkNewCompatReq("1.1", "1", false)
	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	_, err := UpdateVersionSets(nextClock, data, req, 0, 0)
	var notFound *serviceerror.NotFound
	assert.ErrorAs(t, err, &notFound)
}

func TestMakeExistingSetDefault(t *testing.T) {
	t.Parallel()
	clock0 := hlc.Zero(1)
	data := mkInitialData(3, clock0)

	req := mkExistingDefault("1")
	clock1 := hlc.Next(clock0, commonclock.NewRealTimeSource())
	data, err := UpdateVersionSets(clock1, data, req, 0, 0)
	assert.NoError(t, err)

	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSingleBuildIdSet("0", clock0),
			mkSingleBuildIdSet("2", clock0),
			{
				SetIds:                 []string{hashBuildId("1")},
				BuildIds:               []*persistencespb.BuildId{mkBuildId("1", clock0)},
				BecameDefaultTimestamp: clock1,
			},
		},
	}

	protoassert.ProtoEqual(t, expected, data)
	// Add a compatible version to a set and then make that set the default via the compatible version
	req = mkNewCompatReq("0.1", "0", true)

	clock2 := hlc.Next(clock1, commonclock.NewRealTimeSource())
	data, err = UpdateVersionSets(clock2, data, req, 0, 0)
	assert.NoError(t, err)

	expected = &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSingleBuildIdSet("2", clock0),
			{
				SetIds:                 []string{hashBuildId("1")},
				BuildIds:               []*persistencespb.BuildId{mkBuildId("1", clock0)},
				BecameDefaultTimestamp: clock1,
			},
			{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("0", clock0),
					mkBuildId("0.1", clock2),
				},
				BecameDefaultTimestamp: clock2,
			},
		},
	}
	protoassert.ProtoEqual(t, expected, data)
}

func TestSayVersionIsCompatWithDifferentSetThanItsAlreadyCompatWithNotAllowed(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(3, clock)

	req := mkNewCompatReq("0.1", "0", false)
	data, err := UpdateVersionSets(clock, data, req, 0, 0)
	assert.NoError(t, err)

	req = mkNewCompatReq("0.1", "1", false)
	_, err = UpdateVersionSets(clock, data, req, 0, 0)
	var invalidArgument *serviceerror.InvalidArgument
	assert.ErrorAs(t, err, &invalidArgument)
}

func TestLimitsMaxSets(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	maxSets := 10
	data := mkInitialData(maxSets, clock)

	req := mkNewDefReq("10")
	_, err := UpdateVersionSets(clock, data, req, maxSets, 0)
	var failedPrecondition *serviceerror.FailedPrecondition
	assert.ErrorAs(t, err, &failedPrecondition)
}

func TestLimitsMaxBuildIds(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	maxBuildIds := 10
	data := mkInitialData(maxBuildIds, clock)

	req := mkNewDefReq("10")
	_, err := UpdateVersionSets(clock, data, req, 0, maxBuildIds)
	var failedPrecondition *serviceerror.FailedPrecondition
	assert.ErrorAs(t, err, &failedPrecondition)
}

func TestPromoteWithinVersion(t *testing.T) {
	t.Parallel()
	clock0 := hlc.Zero(1)
	data := mkInitialData(2, clock0)

	req := mkNewCompatReq("0.1", "0", false)
	clock1 := hlc.Next(clock0, commonclock.NewRealTimeSource())
	data, err := UpdateVersionSets(clock1, data, req, 0, 0)
	assert.NoError(t, err)
	req = mkNewCompatReq("0.2", "0", false)
	clock2 := hlc.Next(clock1, commonclock.NewRealTimeSource())
	data, err = UpdateVersionSets(clock2, data, req, 0, 0)
	assert.NoError(t, err)
	req = mkPromoteInSet("0.1")
	clock3 := hlc.Next(clock2, commonclock.NewRealTimeSource())
	data, err = UpdateVersionSets(clock3, data, req, 0, 0)
	assert.NoError(t, err)

	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("0", clock0),
					mkBuildId("0.2", clock2),
					{Id: "0.1", State: persistencespb.STATE_ACTIVE, StateUpdateTimestamp: clock1, BecameDefaultTimestamp: clock3},
				},
				BecameDefaultTimestamp: clock0,
			},
			mkSingleBuildIdSet("1", clock0),
		},
	}
	protoassert.ProtoEqual(t, expected, data)
}

func TestAddNewDefaultAlreadyExtantVersionWithNoConflictSucceeds(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	original := mkInitialData(3, clock)

	req := mkNewDefReq("2")
	updated, err := UpdateVersionSets(clock, original, req, 0, 0)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, original, updated)
}

func TestAddToExistingSetAlreadyExtantVersionWithNoConflictSucceeds(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	req := mkNewCompatReq("1.1", "1", false)
	original, err := UpdateVersionSets(clock, mkInitialData(3, clock), req, 0, 0)
	assert.NoError(t, err)
	updated, err := UpdateVersionSets(clock, original, req, 0, 0)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, original, updated)
}

func TestAddToExistingSetAlreadyExtantVersionErrorsIfNotDefault(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	req := mkNewCompatReq("1.1", "1", true)
	original, err := UpdateVersionSets(clock, mkInitialData(3, clock), req, 0, 0)
	assert.NoError(t, err)
	req = mkNewCompatReq("1", "1.1", true)
	_, err = UpdateVersionSets(clock, original, req, 0, 0)
	var invalidArgument *serviceerror.InvalidArgument
	assert.ErrorAs(t, err, &invalidArgument)
}

func TestAddToExistingSetAlreadyExtantVersionErrorsIfNotDefaultSet(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	req := mkNewCompatReq("1.1", "1", false)
	original, err := UpdateVersionSets(clock, mkInitialData(3, clock), req, 0, 0)
	assert.NoError(t, err)
	req = mkNewCompatReq("1.1", "1", true)
	_, err = UpdateVersionSets(clock, original, req, 0, 0)
	var invalidArgument *serviceerror.InvalidArgument
	assert.ErrorAs(t, err, &invalidArgument)
}

func TestPromoteWithinSetAlreadyPromotedIsANoop(t *testing.T) {
	t.Parallel()
	clock0 := hlc.Zero(1)
	original := mkInitialData(3, clock0)
	req := mkPromoteInSet("1")
	clock1 := hlc.Zero(2)
	updated, err := UpdateVersionSets(clock1, original, req, 0, 0)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, original, updated)
}

func TestPromoteSetAlreadyPromotedIsANoop(t *testing.T) {
	t.Parallel()
	clock0 := hlc.Zero(1)
	original := mkInitialData(3, clock0)
	req := mkExistingDefault("2")
	clock1 := hlc.Zero(2)
	updated, err := UpdateVersionSets(clock1, original, req, 0, 0)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, original, updated)
}

func TestAddAlreadyExtantVersionAsDefaultErrors(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(3, clock)

	req := mkNewDefReq("0")
	_, err := UpdateVersionSets(clock, data, req, 0, 0)
	var invalidArgument *serviceerror.InvalidArgument
	assert.ErrorAs(t, err, &invalidArgument)
}

func TestAddAlreadyExtantVersionToAnotherSetErrors(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(3, clock)

	req := mkNewCompatReq("0", "1", false)
	_, err := UpdateVersionSets(clock, data, req, 0, 0)
	var invalidArgument *serviceerror.InvalidArgument
	assert.ErrorAs(t, err, &invalidArgument)
}

func TestMakeSetDefaultTargetingNonexistentVersionErrors(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(3, clock)

	req := mkExistingDefault("crab boi")
	_, err := UpdateVersionSets(clock, data, req, 0, 0)
	var notFound *serviceerror.NotFound
	assert.ErrorAs(t, err, &notFound)
}

func TestPromoteWithinSetTargetingNonexistentVersionErrors(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(3, clock)

	req := mkPromoteInSet("i'd rather be writing rust ;)")
	_, err := UpdateVersionSets(clock, data, req, 0, 0)
	var notFound *serviceerror.NotFound
	assert.ErrorAs(t, err, &notFound)
}

func TestToBuildIdOrderingResponseTrimsResponse(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(3, clock)
	actual := ToBuildIdOrderingResponse(data, 2)
	expected := []*taskqueuepb.CompatibleVersionSet{{BuildIds: []string{"1"}}, {BuildIds: []string{"2"}}}
	protoassert.ProtoSliceEqual(t, expected, actual.MajorVersionSets)
}

func TestToBuildIdOrderingResponseOmitsDeleted(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					{Id: "0", State: persistencespb.STATE_DELETED, StateUpdateTimestamp: clock, BecameDefaultTimestamp: clock},
					{Id: "0.1", State: persistencespb.STATE_ACTIVE, StateUpdateTimestamp: clock, BecameDefaultTimestamp: clock},
				},
				BecameDefaultTimestamp: clock,
			},
		},
	}
	actual := ToBuildIdOrderingResponse(data, 0)
	expected := []*taskqueuepb.CompatibleVersionSet{{BuildIds: []string{"0.1"}}}
	protoassert.ProtoSliceEqual(t, expected, actual.MajorVersionSets)
}

func TestHashBuildId(t *testing.T) {
	t.Parallel()
	// This function should never change.
	assert.Equal(t, "ftrPuUeORv2JD4Wp2wTU", hashBuildId("my-build-id"))
}

func TestGetBuildIdDeltas(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(0)
	prev := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			{
				SetIds:                 []string{hashBuildId("0")},
				BuildIds:               []*persistencespb.BuildId{{Id: "0", State: persistencespb.STATE_DELETED}, {Id: "0.1", State: persistencespb.STATE_ACTIVE}},
				BecameDefaultTimestamp: clock,
			},
			{
				SetIds:                 []string{hashBuildId("1")},
				BuildIds:               []*persistencespb.BuildId{{Id: "1", State: persistencespb.STATE_ACTIVE}},
				BecameDefaultTimestamp: clock,
			},
		},
	}
	curr := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			{
				SetIds:                 []string{hashBuildId("0")},
				BuildIds:               []*persistencespb.BuildId{{Id: "0.1", State: persistencespb.STATE_ACTIVE}},
				BecameDefaultTimestamp: clock,
			},
			{
				SetIds:                 []string{hashBuildId("1")},
				BuildIds:               []*persistencespb.BuildId{{Id: "1", State: persistencespb.STATE_DELETED}, {Id: "1.1", State: persistencespb.STATE_ACTIVE}},
				BecameDefaultTimestamp: clock,
			},
		},
	}
	added, removed := GetBuildIdDeltas(prev, curr)
	assert.Equal(t, []string{"1"}, removed)
	assert.Equal(t, []string{"1.1"}, added)
}

func TestGetBuildIdDeltas_AcceptsNils(t *testing.T) {
	t.Parallel()
	added, removed := GetBuildIdDeltas(nil, nil)
	assert.Equal(t, []string(nil), removed)
	assert.Equal(t, []string(nil), added)
}

func Test_RemoveBuildIds_PutsTombstonesOnSuppliedBuildIds(t *testing.T) {
	t.Parallel()
	c0 := hlc.Zero(0)
	data := mkInitialData(3, c0)
	c1 := c0
	c1.Version++

	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					{
						Id:                     "0",
						State:                  persistencespb.STATE_DELETED,
						StateUpdateTimestamp:   c1,
						BecameDefaultTimestamp: c0,
					},
				},
				BecameDefaultTimestamp: c0,
			},
			{
				SetIds: []string{hashBuildId("1")},
				BuildIds: []*persistencespb.BuildId{
					{
						Id:                     "1",
						State:                  persistencespb.STATE_DELETED,
						StateUpdateTimestamp:   c1,
						BecameDefaultTimestamp: c0,
					},
				},
				BecameDefaultTimestamp: c0,
			},
			{
				SetIds: []string{hashBuildId("2")},
				BuildIds: []*persistencespb.BuildId{
					{
						Id:                     "2",
						State:                  persistencespb.STATE_ACTIVE,
						StateUpdateTimestamp:   c0,
						BecameDefaultTimestamp: c0,
					},
				},
				BecameDefaultTimestamp: c0,
			},
		},
	}

	actual := RemoveBuildIds(c1, data, []string{"0", "1"})
	protoassert.ProtoEqual(t, expected, actual)
	// Method does not mutate original data
	protoassert.ProtoEqual(t, mkInitialData(3, c0), data)
}

func Test_ClearTombstones(t *testing.T) {
	t.Parallel()
	c0 := hlc.Zero(0)

	makeData := func() *persistencespb.VersioningData {
		return &persistencespb.VersioningData{
			VersionSets: []*persistencespb.CompatibleVersionSet{
				{
					SetIds: []string{hashBuildId("0")},
					BuildIds: []*persistencespb.BuildId{
						{
							Id:                     "0",
							State:                  persistencespb.STATE_DELETED,
							StateUpdateTimestamp:   c0,
							BecameDefaultTimestamp: c0,
						},
					},
					BecameDefaultTimestamp: c0,
				},
				{
					SetIds: []string{hashBuildId("1")},
					BuildIds: []*persistencespb.BuildId{
						{
							Id:                     "1",
							State:                  persistencespb.STATE_DELETED,
							StateUpdateTimestamp:   c0,
							BecameDefaultTimestamp: c0,
						},
						{
							Id:                     "1.1",
							State:                  persistencespb.STATE_ACTIVE,
							StateUpdateTimestamp:   c0,
							BecameDefaultTimestamp: c0,
						},
					},
					BecameDefaultTimestamp: c0,
				},
				{
					SetIds: []string{hashBuildId("2")},
					BuildIds: []*persistencespb.BuildId{
						{
							Id:                     "2",
							State:                  persistencespb.STATE_ACTIVE,
							StateUpdateTimestamp:   c0,
							BecameDefaultTimestamp: c0,
						},
					},
					BecameDefaultTimestamp: c0,
				},
			},
		}
	}
	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			{
				SetIds: []string{hashBuildId("1")},
				BuildIds: []*persistencespb.BuildId{
					{
						Id:                     "1.1",
						State:                  persistencespb.STATE_ACTIVE,
						StateUpdateTimestamp:   c0,
						BecameDefaultTimestamp: c0,
					},
				},
				BecameDefaultTimestamp: c0,
			},
			{
				SetIds: []string{hashBuildId("2")},
				BuildIds: []*persistencespb.BuildId{
					{
						Id:                     "2",
						State:                  persistencespb.STATE_ACTIVE,
						StateUpdateTimestamp:   c0,
						BecameDefaultTimestamp: c0,
					},
				},
				BecameDefaultTimestamp: c0,
			},
		},
	}
	original := makeData()
	actual := ClearTombstones(original)
	protoassert.ProtoEqual(t, expected, actual)
	// Method does not mutate original data
	protoassert.ProtoEqual(t, makeData(), original)
}

func TestMergeSets(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	clockQueueDefault := hlc.Next(clock, commonclock.NewRealTimeSource())
	initialData := mkInitialData(4, clock)

	// Make sure the clocks are incrementing per version set for the merge to be predictable
	initialData.VersionSets[1].BecameDefaultTimestamp = hlc.Next(clock, commonclock.NewRealTimeSource())
	initialData.VersionSets[2].BecameDefaultTimestamp = hlc.Next(initialData.VersionSets[1].BecameDefaultTimestamp, commonclock.NewRealTimeSource())
	initialData.VersionSets[3].BecameDefaultTimestamp = hlc.Next(initialData.VersionSets[2].BecameDefaultTimestamp, commonclock.NewRealTimeSource())

	req := mkMergeSet("1", "2")
	nextClock := hlc.Next(clockQueueDefault, commonclock.NewRealTimeSource())
	updatedData, err := UpdateVersionSets(nextClock, initialData, req, 0, 0)
	assert.NoError(t, err)
	// Should only be three sets now
	assert.Len(t, updatedData.VersionSets, 3)
	// The overall default set should not have changed
	assert.Equal(t, "3", updatedData.GetVersionSets()[2].GetBuildIds()[0].Id)
	// But set 1 should now have 2, maintaining 1 as the default ID
	assert.Equal(t, "1", updatedData.GetVersionSets()[1].GetBuildIds()[1].Id)
	assert.Equal(t, "2", updatedData.GetVersionSets()[1].GetBuildIds()[0].Id)
	// Ensure it has the set ids of both sets
	bothSetIds := mergeSetIDs([]string{hashBuildId("1")}, []string{hashBuildId("2")})
	assert.Equal(t, bothSetIds, updatedData.GetVersionSets()[1].GetSetIds())
	assert.Equal(t, initialData.GetVersionSets()[2].BecameDefaultTimestamp, updatedData.GetVersionSets()[1].BecameDefaultTimestamp)
	buildIds := updatedData.VersionSets[1].BuildIds
	protoassert.ProtoEqual(t, nextClock, buildIds[len(buildIds)-1].BecameDefaultTimestamp)
	// Initial data should not have changed
	assert.Len(t, initialData.VersionSets, 4)
	for _, set := range initialData.VersionSets {
		assert.Len(t, set.GetSetIds(), 1)
	}

	// Same merge request must be idempotent
	nextClock2 := hlc.Next(nextClock, commonclock.NewRealTimeSource())
	updatedData2, err := UpdateVersionSets(nextClock2, updatedData, req, 0, 0)
	assert.NoError(t, err)
	assert.Len(t, updatedData2.VersionSets, 3)
	assert.Equal(t, "3", updatedData2.GetVersionSets()[2].GetBuildIds()[0].Id)
	assert.Equal(t, "1", updatedData2.GetVersionSets()[1].GetBuildIds()[1].Id)
	assert.Equal(t, "2", updatedData2.GetVersionSets()[1].GetBuildIds()[0].Id)
	assert.Equal(t, initialData.GetVersionSets()[2].BecameDefaultTimestamp, updatedData2.GetVersionSets()[1].BecameDefaultTimestamp)
	// Clock shouldn't have changed
	buildIds = updatedData2.VersionSets[1].BuildIds
	protoassert.ProtoEqual(t, nextClock, buildIds[len(buildIds)-1].BecameDefaultTimestamp)

	// Verify merging into the current default maintains that set as the default
	req = mkMergeSet("3", "0")
	nextClock3 := hlc.Next(nextClock2, commonclock.NewRealTimeSource())
	updatedData3, err := UpdateVersionSets(nextClock3, updatedData2, req, 0, 0)
	assert.NoError(t, err)
	assert.Len(t, updatedData3.VersionSets, 2)
	assert.Equal(t, "3", updatedData3.GetVersionSets()[1].GetBuildIds()[1].Id)
	assert.Equal(t, "0", updatedData3.GetVersionSets()[1].GetBuildIds()[0].Id)
	assert.Equal(t, "1", updatedData3.GetVersionSets()[0].GetBuildIds()[1].Id)
	assert.Equal(t, "2", updatedData3.GetVersionSets()[0].GetBuildIds()[0].Id)
	assert.Equal(t, initialData.GetVersionSets()[3].BecameDefaultTimestamp, updatedData3.GetVersionSets()[1].BecameDefaultTimestamp)
	buildIds = updatedData3.VersionSets[1].BuildIds
	protoassert.ProtoEqual(t, nextClock3, buildIds[len(buildIds)-1].BecameDefaultTimestamp)
}

func TestMergeInvalidTargets(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	initialData := mkInitialData(4, clock)

	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	req := mkMergeSet("lol", "2")
	_, err := UpdateVersionSets(nextClock, initialData, req, 0, 0)
	assert.Error(t, err)

	req2 := mkMergeSet("2", "nope")
	_, err2 := UpdateVersionSets(nextClock, initialData, req2, 0, 0)
	assert.Error(t, err2)
}

func TestPersistUnknownBuildId(t *testing.T) {
	t.Parallel()
	clock := hlc.Next(hlc.Zero(1), commonclock.NewRealTimeSource())
	initialData := mkInitialData(2, clock)

	actual := PersistUnknownBuildId(clock, initialData, "new-build-id")
	assert.Len(t, actual.VersionSets, 3)
	newSet := actual.VersionSets[0]
	assert.Len(t, newSet.BuildIds, 1)
	assert.Equal(t, "new-build-id", newSet.BuildIds[0].Id)
}

func TestPersistUnknownBuildIdAlreadyThere(t *testing.T) {
	t.Parallel()
	clock := hlc.Next(hlc.Zero(1), commonclock.NewRealTimeSource())

	initial := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			{
				SetIds:                 []string{hashBuildId("1")},
				BuildIds:               []*persistencespb.BuildId{mkBuildId("1", clock), mkBuildId("2", clock)},
				BecameDefaultTimestamp: clock,
			},
		},
	}

	actual := PersistUnknownBuildId(clock, initial, "1")
	protoassert.ProtoEqual(t, initial, actual)

	// build ID is already there but adds set id
	actual = PersistUnknownBuildId(clock, initial, "2")
	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			{
				SetIds:                 []string{hashBuildId("1"), hashBuildId("2")},
				BuildIds:               []*persistencespb.BuildId{mkBuildId("1", clock), mkBuildId("2", clock)},
				BecameDefaultTimestamp: clock,
			},
		},
	}
	protoassert.ProtoEqual(t, expected, actual)
}
