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
	"google.golang.org/protobuf/proto"
)

func mkNewSet(id string, clock *clockspb.HybridLogicalClock) *persistencespb.CompatibleVersionSet {
	return persistencespb.CompatibleVersionSet_builder{
		SetIds:                 []string{hashBuildId(id)},
		BuildIds:               []*persistencespb.BuildId{persistencespb.BuildId_builder{Id: id, State: persistencespb.STATE_ACTIVE, StateUpdateTimestamp: clock, BecameDefaultTimestamp: clock}.Build()},
		BecameDefaultTimestamp: clock,
	}.Build()
}

func mkInitialData(numSets int, clock *clockspb.HybridLogicalClock) *persistencespb.VersioningData {
	sets := make([]*persistencespb.CompatibleVersionSet, numSets)
	for i := 0; i < numSets; i++ {
		sets[i] = mkNewSet(fmt.Sprintf("%v", i), clock)
	}
	return persistencespb.VersioningData_builder{
		VersionSets: sets,
	}.Build()
}

func mkUserData(numSets int) *persistencespb.TaskQueueUserData {
	clock := hlc.Zero(1)
	return persistencespb.TaskQueueUserData_builder{
		Clock:          clock,
		VersioningData: mkInitialData(numSets, clock),
	}.Build()
}

func mkNewDefReq(id string) *workflowservice.UpdateWorkerBuildIdCompatibilityRequest {
	return workflowservice.UpdateWorkerBuildIdCompatibilityRequest_builder{
		AddNewBuildIdInNewDefaultSet: proto.String(id),
	}.Build()
}
func mkNewCompatReq(id, compat string, becomeDefault bool) *workflowservice.UpdateWorkerBuildIdCompatibilityRequest {
	return workflowservice.UpdateWorkerBuildIdCompatibilityRequest_builder{
		AddNewCompatibleBuildId: workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion_builder{
			NewBuildId:                id,
			ExistingCompatibleBuildId: compat,
			MakeSetDefault:            becomeDefault,
		}.Build(),
	}.Build()
}
func mkExistingDefault(id string) *workflowservice.UpdateWorkerBuildIdCompatibilityRequest {
	return workflowservice.UpdateWorkerBuildIdCompatibilityRequest_builder{
		PromoteSetByBuildId: proto.String(id),
	}.Build()
}
func mkPromoteInSet(id string) *workflowservice.UpdateWorkerBuildIdCompatibilityRequest {
	return workflowservice.UpdateWorkerBuildIdCompatibilityRequest_builder{
		PromoteBuildIdWithinSet: proto.String(id),
	}.Build()
}
func mkMergeSet(primaryId string, secondaryId string) *workflowservice.UpdateWorkerBuildIdCompatibilityRequest {
	return workflowservice.UpdateWorkerBuildIdCompatibilityRequest_builder{
		MergeSets: workflowservice.UpdateWorkerBuildIdCompatibilityRequest_MergeSets_builder{
			PrimarySetBuildId:   primaryId,
			SecondarySetBuildId: secondaryId,
		}.Build(),
	}.Build()
}

func mkBuildId(id string, clock *hlc.Clock) *persistencespb.BuildId {
	return persistencespb.BuildId_builder{
		Id:                     id,
		State:                  persistencespb.STATE_ACTIVE,
		StateUpdateTimestamp:   clock,
		BecameDefaultTimestamp: clock,
	}.Build()
}

func mkSingleBuildIdSet(id string, clock *hlc.Clock) *persistencespb.CompatibleVersionSet {
	return persistencespb.CompatibleVersionSet_builder{
		SetIds:                 []string{hashBuildId(id)},
		BuildIds:               []*persistencespb.BuildId{mkBuildId(id, clock)},
		BecameDefaultTimestamp: clock,
	}.Build()
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

	expected := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSingleBuildIdSet("0", clock),
			mkSingleBuildIdSet("1", clock),
			mkSingleBuildIdSet("2", nextClock),
		},
	}.Build()
	protoassert.ProtoEqual(t, expected, updatedData)

	asResp := ToBuildIdOrderingResponse(updatedData, 0)
	assert.Equal(t, "2", asResp.GetMajorVersionSets()[2].GetBuildIds()[0])
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

	expected := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSingleBuildIdSet("1", nextClock),
		},
	}.Build()
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

	expected := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSingleBuildIdSet("0", clock),
			persistencespb.CompatibleVersionSet_builder{
				SetIds: []string{hashBuildId("1")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("1", clock),
					mkBuildId("1.1", nextClock),
				},
				BecameDefaultTimestamp: nextClock,
			}.Build(),
		},
	}.Build()
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

	expected := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSingleBuildIdSet("1", clock),
			persistencespb.CompatibleVersionSet_builder{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("0", clock),
					mkBuildId("0.1", nextClock),
				},
				BecameDefaultTimestamp: nextClock,
			}.Build(),
		},
	}.Build()
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

	expected := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			persistencespb.CompatibleVersionSet_builder{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("0", clock),
					mkBuildId("0.1", nextClock),
				},
				BecameDefaultTimestamp: clock,
			}.Build(),
			mkSingleBuildIdSet("1", clock),
		},
	}.Build()

	protoassert.ProtoEqual(t, expected, updatedData)
	asResp := ToBuildIdOrderingResponse(updatedData, 0)
	assert.Equal(t, "0.1", asResp.GetMajorVersionSets()[0].GetBuildIds()[1])
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

	expected := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			persistencespb.CompatibleVersionSet_builder{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("0", clock0),
					mkBuildId("0.1", clock1),
					mkBuildId("0.2", clock2),
				},
				BecameDefaultTimestamp: clock0,
			}.Build(),
			mkSingleBuildIdSet("1", clock0),
		},
	}.Build()

	protoassert.ProtoEqual(t, expected, data)
	// Ensure setting a compatible version which targets a non-leaf compat version ends up without a branch
	req = mkNewCompatReq("0.3", "0.1", false)
	clock3 := hlc.Next(clock1, commonclock.NewRealTimeSource())
	data, err = UpdateVersionSets(clock3, data, req, 0, 0)
	assert.NoError(t, err)

	expected = persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			persistencespb.CompatibleVersionSet_builder{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("0", clock0),
					mkBuildId("0.1", clock1),
					mkBuildId("0.2", clock2),
					mkBuildId("0.3", clock3),
				},
				BecameDefaultTimestamp: clock0,
			}.Build(),
			mkSingleBuildIdSet("1", clock0),
		},
	}.Build()

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

	expected := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSingleBuildIdSet("0", clock0),
			mkSingleBuildIdSet("2", clock0),
			persistencespb.CompatibleVersionSet_builder{
				SetIds:                 []string{hashBuildId("1")},
				BuildIds:               []*persistencespb.BuildId{mkBuildId("1", clock0)},
				BecameDefaultTimestamp: clock1,
			}.Build(),
		},
	}.Build()

	protoassert.ProtoEqual(t, expected, data)
	// Add a compatible version to a set and then make that set the default via the compatible version
	req = mkNewCompatReq("0.1", "0", true)

	clock2 := hlc.Next(clock1, commonclock.NewRealTimeSource())
	data, err = UpdateVersionSets(clock2, data, req, 0, 0)
	assert.NoError(t, err)

	expected = persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			mkSingleBuildIdSet("2", clock0),
			persistencespb.CompatibleVersionSet_builder{
				SetIds:                 []string{hashBuildId("1")},
				BuildIds:               []*persistencespb.BuildId{mkBuildId("1", clock0)},
				BecameDefaultTimestamp: clock1,
			}.Build(),
			persistencespb.CompatibleVersionSet_builder{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("0", clock0),
					mkBuildId("0.1", clock2),
				},
				BecameDefaultTimestamp: clock2,
			}.Build(),
		},
	}.Build()
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

	expected := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			persistencespb.CompatibleVersionSet_builder{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					mkBuildId("0", clock0),
					mkBuildId("0.2", clock2),
					persistencespb.BuildId_builder{Id: "0.1", State: persistencespb.STATE_ACTIVE, StateUpdateTimestamp: clock1, BecameDefaultTimestamp: clock3}.Build(),
				},
				BecameDefaultTimestamp: clock0,
			}.Build(),
			mkSingleBuildIdSet("1", clock0),
		},
	}.Build()
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
	expected := []*taskqueuepb.CompatibleVersionSet{taskqueuepb.CompatibleVersionSet_builder{BuildIds: []string{"1"}}.Build(), taskqueuepb.CompatibleVersionSet_builder{BuildIds: []string{"2"}}.Build()}
	protoassert.ProtoSliceEqual(t, expected, actual.GetMajorVersionSets())
}

func TestToBuildIdOrderingResponseOmitsDeleted(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			persistencespb.CompatibleVersionSet_builder{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					persistencespb.BuildId_builder{Id: "0", State: persistencespb.STATE_DELETED, StateUpdateTimestamp: clock, BecameDefaultTimestamp: clock}.Build(),
					persistencespb.BuildId_builder{Id: "0.1", State: persistencespb.STATE_ACTIVE, StateUpdateTimestamp: clock, BecameDefaultTimestamp: clock}.Build(),
				},
				BecameDefaultTimestamp: clock,
			}.Build(),
		},
	}.Build()
	actual := ToBuildIdOrderingResponse(data, 0)
	expected := []*taskqueuepb.CompatibleVersionSet{taskqueuepb.CompatibleVersionSet_builder{BuildIds: []string{"0.1"}}.Build()}
	protoassert.ProtoSliceEqual(t, expected, actual.GetMajorVersionSets())
}

func TestHashBuildId(t *testing.T) {
	t.Parallel()
	// This function should never change.
	assert.Equal(t, "ftrPuUeORv2JD4Wp2wTU", hashBuildId("my-build-id"))
}

func TestGetBuildIdDeltas(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(0)
	prev := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			persistencespb.CompatibleVersionSet_builder{
				SetIds:                 []string{hashBuildId("0")},
				BuildIds:               []*persistencespb.BuildId{persistencespb.BuildId_builder{Id: "0", State: persistencespb.STATE_DELETED}.Build(), persistencespb.BuildId_builder{Id: "0.1", State: persistencespb.STATE_ACTIVE}.Build()},
				BecameDefaultTimestamp: clock,
			}.Build(),
			persistencespb.CompatibleVersionSet_builder{
				SetIds:                 []string{hashBuildId("1")},
				BuildIds:               []*persistencespb.BuildId{persistencespb.BuildId_builder{Id: "1", State: persistencespb.STATE_ACTIVE}.Build()},
				BecameDefaultTimestamp: clock,
			}.Build(),
		},
	}.Build()
	curr := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			persistencespb.CompatibleVersionSet_builder{
				SetIds:                 []string{hashBuildId("0")},
				BuildIds:               []*persistencespb.BuildId{persistencespb.BuildId_builder{Id: "0.1", State: persistencespb.STATE_ACTIVE}.Build()},
				BecameDefaultTimestamp: clock,
			}.Build(),
			persistencespb.CompatibleVersionSet_builder{
				SetIds:                 []string{hashBuildId("1")},
				BuildIds:               []*persistencespb.BuildId{persistencespb.BuildId_builder{Id: "1", State: persistencespb.STATE_DELETED}.Build(), persistencespb.BuildId_builder{Id: "1.1", State: persistencespb.STATE_ACTIVE}.Build()},
				BecameDefaultTimestamp: clock,
			}.Build(),
		},
	}.Build()
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
	c1.SetVersion(c1.GetVersion() + 1)

	expected := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			persistencespb.CompatibleVersionSet_builder{
				SetIds: []string{hashBuildId("0")},
				BuildIds: []*persistencespb.BuildId{
					persistencespb.BuildId_builder{
						Id:                     "0",
						State:                  persistencespb.STATE_DELETED,
						StateUpdateTimestamp:   c1,
						BecameDefaultTimestamp: c0,
					}.Build(),
				},
				BecameDefaultTimestamp: c0,
			}.Build(),
			persistencespb.CompatibleVersionSet_builder{
				SetIds: []string{hashBuildId("1")},
				BuildIds: []*persistencespb.BuildId{
					persistencespb.BuildId_builder{
						Id:                     "1",
						State:                  persistencespb.STATE_DELETED,
						StateUpdateTimestamp:   c1,
						BecameDefaultTimestamp: c0,
					}.Build(),
				},
				BecameDefaultTimestamp: c0,
			}.Build(),
			persistencespb.CompatibleVersionSet_builder{
				SetIds: []string{hashBuildId("2")},
				BuildIds: []*persistencespb.BuildId{
					persistencespb.BuildId_builder{
						Id:                     "2",
						State:                  persistencespb.STATE_ACTIVE,
						StateUpdateTimestamp:   c0,
						BecameDefaultTimestamp: c0,
					}.Build(),
				},
				BecameDefaultTimestamp: c0,
			}.Build(),
		},
	}.Build()

	actual := RemoveBuildIds(c1, data, []string{"0", "1"})
	protoassert.ProtoEqual(t, expected, actual)
	// Method does not mutate original data
	protoassert.ProtoEqual(t, mkInitialData(3, c0), data)
}

func Test_ClearTombstones(t *testing.T) {
	t.Parallel()
	c0 := hlc.Zero(0)

	makeData := func() *persistencespb.VersioningData {
		return persistencespb.VersioningData_builder{
			VersionSets: []*persistencespb.CompatibleVersionSet{
				persistencespb.CompatibleVersionSet_builder{
					SetIds: []string{hashBuildId("0")},
					BuildIds: []*persistencespb.BuildId{
						persistencespb.BuildId_builder{
							Id:                     "0",
							State:                  persistencespb.STATE_DELETED,
							StateUpdateTimestamp:   c0,
							BecameDefaultTimestamp: c0,
						}.Build(),
					},
					BecameDefaultTimestamp: c0,
				}.Build(),
				persistencespb.CompatibleVersionSet_builder{
					SetIds: []string{hashBuildId("1")},
					BuildIds: []*persistencespb.BuildId{
						persistencespb.BuildId_builder{
							Id:                     "1",
							State:                  persistencespb.STATE_DELETED,
							StateUpdateTimestamp:   c0,
							BecameDefaultTimestamp: c0,
						}.Build(),
						persistencespb.BuildId_builder{
							Id:                     "1.1",
							State:                  persistencespb.STATE_ACTIVE,
							StateUpdateTimestamp:   c0,
							BecameDefaultTimestamp: c0,
						}.Build(),
					},
					BecameDefaultTimestamp: c0,
				}.Build(),
				persistencespb.CompatibleVersionSet_builder{
					SetIds: []string{hashBuildId("2")},
					BuildIds: []*persistencespb.BuildId{
						persistencespb.BuildId_builder{
							Id:                     "2",
							State:                  persistencespb.STATE_ACTIVE,
							StateUpdateTimestamp:   c0,
							BecameDefaultTimestamp: c0,
						}.Build(),
					},
					BecameDefaultTimestamp: c0,
				}.Build(),
			},
		}.Build()
	}
	expected := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			persistencespb.CompatibleVersionSet_builder{
				SetIds: []string{hashBuildId("1")},
				BuildIds: []*persistencespb.BuildId{
					persistencespb.BuildId_builder{
						Id:                     "1.1",
						State:                  persistencespb.STATE_ACTIVE,
						StateUpdateTimestamp:   c0,
						BecameDefaultTimestamp: c0,
					}.Build(),
				},
				BecameDefaultTimestamp: c0,
			}.Build(),
			persistencespb.CompatibleVersionSet_builder{
				SetIds: []string{hashBuildId("2")},
				BuildIds: []*persistencespb.BuildId{
					persistencespb.BuildId_builder{
						Id:                     "2",
						State:                  persistencespb.STATE_ACTIVE,
						StateUpdateTimestamp:   c0,
						BecameDefaultTimestamp: c0,
					}.Build(),
				},
				BecameDefaultTimestamp: c0,
			}.Build(),
		},
	}.Build()
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
	initialData.GetVersionSets()[1].SetBecameDefaultTimestamp(hlc.Next(clock, commonclock.NewRealTimeSource()))
	initialData.GetVersionSets()[2].SetBecameDefaultTimestamp(hlc.Next(initialData.GetVersionSets()[1].GetBecameDefaultTimestamp(), commonclock.NewRealTimeSource()))
	initialData.GetVersionSets()[3].SetBecameDefaultTimestamp(hlc.Next(initialData.GetVersionSets()[2].GetBecameDefaultTimestamp(), commonclock.NewRealTimeSource()))

	req := mkMergeSet("1", "2")
	nextClock := hlc.Next(clockQueueDefault, commonclock.NewRealTimeSource())
	updatedData, err := UpdateVersionSets(nextClock, initialData, req, 0, 0)
	assert.NoError(t, err)
	// Should only be three sets now
	assert.Len(t, updatedData.GetVersionSets(), 3)
	// The overall default set should not have changed
	assert.Equal(t, "3", updatedData.GetVersionSets()[2].GetBuildIds()[0].GetId())
	// But set 1 should now have 2, maintaining 1 as the default ID
	assert.Equal(t, "1", updatedData.GetVersionSets()[1].GetBuildIds()[1].GetId())
	assert.Equal(t, "2", updatedData.GetVersionSets()[1].GetBuildIds()[0].GetId())
	// Ensure it has the set ids of both sets
	bothSetIds := mergeSetIDs([]string{hashBuildId("1")}, []string{hashBuildId("2")})
	assert.Equal(t, bothSetIds, updatedData.GetVersionSets()[1].GetSetIds())
	assert.Equal(t, initialData.GetVersionSets()[2].GetBecameDefaultTimestamp(), updatedData.GetVersionSets()[1].GetBecameDefaultTimestamp())
	buildIds := updatedData.GetVersionSets()[1].GetBuildIds()
	protoassert.ProtoEqual(t, nextClock, buildIds[len(buildIds)-1].GetBecameDefaultTimestamp())
	// Initial data should not have changed
	assert.Len(t, initialData.GetVersionSets(), 4)
	for _, set := range initialData.GetVersionSets() {
		assert.Len(t, set.GetSetIds(), 1)
	}

	// Same merge request must be idempotent
	nextClock2 := hlc.Next(nextClock, commonclock.NewRealTimeSource())
	updatedData2, err := UpdateVersionSets(nextClock2, updatedData, req, 0, 0)
	assert.NoError(t, err)
	assert.Len(t, updatedData2.GetVersionSets(), 3)
	assert.Equal(t, "3", updatedData2.GetVersionSets()[2].GetBuildIds()[0].GetId())
	assert.Equal(t, "1", updatedData2.GetVersionSets()[1].GetBuildIds()[1].GetId())
	assert.Equal(t, "2", updatedData2.GetVersionSets()[1].GetBuildIds()[0].GetId())
	assert.Equal(t, initialData.GetVersionSets()[2].GetBecameDefaultTimestamp(), updatedData2.GetVersionSets()[1].GetBecameDefaultTimestamp())
	// Clock shouldn't have changed
	buildIds = updatedData2.GetVersionSets()[1].GetBuildIds()
	protoassert.ProtoEqual(t, nextClock, buildIds[len(buildIds)-1].GetBecameDefaultTimestamp())

	// Verify merging into the current default maintains that set as the default
	req = mkMergeSet("3", "0")
	nextClock3 := hlc.Next(nextClock2, commonclock.NewRealTimeSource())
	updatedData3, err := UpdateVersionSets(nextClock3, updatedData2, req, 0, 0)
	assert.NoError(t, err)
	assert.Len(t, updatedData3.GetVersionSets(), 2)
	assert.Equal(t, "3", updatedData3.GetVersionSets()[1].GetBuildIds()[1].GetId())
	assert.Equal(t, "0", updatedData3.GetVersionSets()[1].GetBuildIds()[0].GetId())
	assert.Equal(t, "1", updatedData3.GetVersionSets()[0].GetBuildIds()[1].GetId())
	assert.Equal(t, "2", updatedData3.GetVersionSets()[0].GetBuildIds()[0].GetId())
	assert.Equal(t, initialData.GetVersionSets()[3].GetBecameDefaultTimestamp(), updatedData3.GetVersionSets()[1].GetBecameDefaultTimestamp())
	buildIds = updatedData3.GetVersionSets()[1].GetBuildIds()
	protoassert.ProtoEqual(t, nextClock3, buildIds[len(buildIds)-1].GetBecameDefaultTimestamp())
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
	assert.Len(t, actual.GetVersionSets(), 3)
	newSet := actual.GetVersionSets()[0]
	assert.Len(t, newSet.GetBuildIds(), 1)
	assert.Equal(t, "new-build-id", newSet.GetBuildIds()[0].GetId())
}

func TestPersistUnknownBuildIdAlreadyThere(t *testing.T) {
	t.Parallel()
	clock := hlc.Next(hlc.Zero(1), commonclock.NewRealTimeSource())

	initial := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			persistencespb.CompatibleVersionSet_builder{
				SetIds:                 []string{hashBuildId("1")},
				BuildIds:               []*persistencespb.BuildId{mkBuildId("1", clock), mkBuildId("2", clock)},
				BecameDefaultTimestamp: clock,
			}.Build(),
		},
	}.Build()

	actual := PersistUnknownBuildId(clock, initial, "1")
	protoassert.ProtoEqual(t, initial, actual)

	// build ID is already there but adds set id
	actual = PersistUnknownBuildId(clock, initial, "2")
	expected := persistencespb.VersioningData_builder{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			persistencespb.CompatibleVersionSet_builder{
				SetIds:                 []string{hashBuildId("1"), hashBuildId("2")},
				BuildIds:               []*persistencespb.BuildId{mkBuildId("1", clock), mkBuildId("2", clock)},
				BecameDefaultTimestamp: clock,
			}.Build(),
		},
	}.Build()
	protoassert.ProtoEqual(t, expected, actual)
}
