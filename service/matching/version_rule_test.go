package matching

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	commonclock "go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
)

const (
	ignoreMaxRules            = 1000
	ignoreMaxUpstreamBuildIDs = 1000
)

func mkNewInsertAssignmentReq(rule *taskqueuepb.BuildIdAssignmentRule, ruleIdx int32) *workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule {
	return &workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule{
		RuleIndex: ruleIdx,
		Rule:      rule,
	}
}

func mkNewReplaceAssignmentReq(rule *taskqueuepb.BuildIdAssignmentRule, ruleIdx int32, force bool) *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceBuildIdAssignmentRule {
	return &workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceBuildIdAssignmentRule{
		RuleIndex: ruleIdx,
		Rule:      rule,
		Force:     force,
	}
}

func mkNewDeleteAssignmentReq(ruleIdx int32, force bool) *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteBuildIdAssignmentRule {
	return &workflowservice.UpdateWorkerVersioningRulesRequest_DeleteBuildIdAssignmentRule{
		RuleIndex: ruleIdx,
		Force:     force,
	}
}

func mkAssignmentRulePersistence(rule *taskqueuepb.BuildIdAssignmentRule, createTS, deleteTS *hlc.Clock) *persistencespb.AssignmentRule {
	return &persistencespb.AssignmentRule{
		Rule:            rule,
		CreateTimestamp: createTS,
		DeleteTimestamp: deleteTS,
	}
}

func mkAssignmentRuleWithoutRamp(target string) *taskqueuepb.BuildIdAssignmentRule {
	ret := &taskqueuepb.BuildIdAssignmentRule{
		TargetBuildId: target,
	}
	return ret
}

func mkAssignmentRuleWithRamp(target string, rampPercentage float32) *taskqueuepb.BuildIdAssignmentRule {
	ret := &taskqueuepb.BuildIdAssignmentRule{
		TargetBuildId: target,
		Ramp:          mkNewAssignmentPercentageRamp(rampPercentage),
	}
	return ret
}

func mkNewInsertRedirectReq(rule *taskqueuepb.CompatibleBuildIdRedirectRule) *workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule {
	return &workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule{
		Rule: rule,
	}
}

func mkNewReplaceRedirectReq(rule *taskqueuepb.CompatibleBuildIdRedirectRule) *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleBuildIdRedirectRule {
	return &workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleBuildIdRedirectRule{
		Rule: rule,
	}
}

func mkNewDeleteRedirectReq(source string) *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleBuildIdRedirectRule {
	return &workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleBuildIdRedirectRule{
		SourceBuildId: source,
	}
}

func mkNewCommitBuildIDReq(target string, force bool) *workflowservice.UpdateWorkerVersioningRulesRequest_CommitBuildId {
	return &workflowservice.UpdateWorkerVersioningRulesRequest_CommitBuildId{
		TargetBuildId: target,
		Force:         force,
	}
}

func mkRedirectRulePersistence(rule *taskqueuepb.CompatibleBuildIdRedirectRule, createTS, deleteTS *hlc.Clock) *persistencespb.RedirectRule {
	return &persistencespb.RedirectRule{
		Rule:            rule,
		CreateTimestamp: createTS,
		DeleteTimestamp: deleteTS,
	}
}

func mkRedirectRule(source, target string) *taskqueuepb.CompatibleBuildIdRedirectRule {
	ret := &taskqueuepb.CompatibleBuildIdRedirectRule{
		TargetBuildId: target,
		SourceBuildId: source,
	}
	return ret
}

func mkNewAssignmentPercentageRamp(percent float32) *taskqueuepb.BuildIdAssignmentRule_PercentageRamp {
	return &taskqueuepb.BuildIdAssignmentRule_PercentageRamp{
		PercentageRamp: &taskqueuepb.RampByPercentage{
			RampPercentage: percent,
		},
	}
}

func insertAssignmentRule(rule *taskqueuepb.BuildIdAssignmentRule,
	data *persistencespb.VersioningData,
	clock *hlc.Clock,
	idx int32,
	maxAssignmentRules int,
) (*persistencespb.VersioningData, error) {
	return InsertAssignmentRule(clock, data, mkNewInsertAssignmentReq(rule, idx), maxAssignmentRules)
}

func insertRedirectRule(rule *taskqueuepb.CompatibleBuildIdRedirectRule,
	data *persistencespb.VersioningData,
	clock *hlc.Clock,
	maxRedirectRules,
	maxUpstreamBuildIDs int,
) (*persistencespb.VersioningData, error) {
	return AddCompatibleRedirectRule(clock, data, mkNewInsertRedirectReq(rule), maxRedirectRules, maxUpstreamBuildIDs)
}

func replaceAssignmentRule(rule *taskqueuepb.BuildIdAssignmentRule,
	data *persistencespb.VersioningData,
	clock *hlc.Clock,
	idx int32,
	force bool,
) (*persistencespb.VersioningData, error) {
	return ReplaceAssignmentRule(clock, data, mkNewReplaceAssignmentReq(rule, idx, force))
}

func replaceRedirectRule(rule *taskqueuepb.CompatibleBuildIdRedirectRule,
	data *persistencespb.VersioningData,
	clock *hlc.Clock,
	maxUpstreamBuildIDs int,
) (*persistencespb.VersioningData, error) {
	return ReplaceCompatibleRedirectRule(clock, data, mkNewReplaceRedirectReq(rule), maxUpstreamBuildIDs)
}

func deleteAssignmentRule(data *persistencespb.VersioningData,
	clock *hlc.Clock,
	idx int32,
	force bool,
) (*persistencespb.VersioningData, error) {
	return DeleteAssignmentRule(clock, data, mkNewDeleteAssignmentReq(idx, force))
}

func deleteRedirectRule(source string,
	data *persistencespb.VersioningData,
	clock *hlc.Clock,
) (*persistencespb.VersioningData, error) {
	return DeleteCompatibleRedirectRule(clock, data, mkNewDeleteRedirectReq(source))
}

func getActiveRedirectRuleBySrc(src string, data *persistencespb.VersioningData) *persistencespb.RedirectRule {
	for _, r := range data.GetRedirectRules() {
		if r.GetDeleteTimestamp() == nil && r.GetRule().GetSourceBuildId() == src {
			return r
		}
	}
	return nil
}

func getDeletedRedirectRuleBySrc(src string, data *persistencespb.VersioningData) []*persistencespb.RedirectRule {
	ret := make([]*persistencespb.RedirectRule, 0)
	for _, r := range data.GetRedirectRules() {
		if r.GetDeleteTimestamp() != nil && r.GetRule().GetSourceBuildId() == src {
			ret = append(ret, r)
		}
	}
	return ret
}

func TestInsertAssignmentRuleBasic(t *testing.T) {
	t.Parallel()
	maxRules := 10
	clock := hlc.Zero(1)
	initialData := mkInitialData(0, clock)
	require.False(t, containsFullyRamped(initialData.GetAssignmentRules()))
	expected := &persistencespb.VersioningData{AssignmentRules: []*persistencespb.AssignmentRule{}}

	// insert at index 0
	rule1 := mkAssignmentRuleWithoutRamp("1")
	data, err := insertAssignmentRule(rule1, initialData, clock, 0, maxRules)
	require.NoError(t, err)
	expected.AssignmentRules = slices.Insert(expected.AssignmentRules, 0, mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("1", 100), clock, nil))
	protorequire.ProtoEqual(t, expected, data)

	rule2 := mkAssignmentRuleWithoutRamp("2")
	data, err = insertAssignmentRule(rule2, data, clock, 0, maxRules)
	require.NoError(t, err)
	expected.AssignmentRules = slices.Insert(expected.AssignmentRules, 0, mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("2", 100), clock, nil))
	protorequire.ProtoEqual(t, expected, data)

	rule3 := mkAssignmentRuleWithoutRamp("3")
	data, err = insertAssignmentRule(rule3, data, clock, 0, maxRules)
	require.NoError(t, err)
	expected.AssignmentRules = slices.Insert(expected.AssignmentRules, 0, mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("3", 100), clock, nil))
	protorequire.ProtoEqual(t, expected, data)

	// insert into the middle
	rule4 := mkAssignmentRuleWithoutRamp("4")
	data, err = insertAssignmentRule(rule4, data, clock, 2, maxRules)
	require.NoError(t, err)
	expected.AssignmentRules = slices.Insert(expected.AssignmentRules, 2, mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("4", 100), clock, nil))
	protorequire.ProtoEqual(t, expected, data)

	// insert with a too-big index, it should be at the back
	rule5 := mkAssignmentRuleWithoutRamp("5")
	data, err = insertAssignmentRule(rule5, data, clock, 100, maxRules)
	require.NoError(t, err)
	expected.AssignmentRules = append(expected.AssignmentRules, mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("5", 100), clock, nil))
	protorequire.ProtoEqual(t, expected, data)

	// initial data should be unmodified
	protorequire.ProtoEqual(t, mkInitialData(0, clock), initialData)
}

func TestInsertAssignmentRuleMaxRules(t *testing.T) {
	t.Parallel()
	maxRules := 3
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)
	var err error

	// insert 3x --> success
	for range 3 {
		data, err = insertAssignmentRule(mkAssignmentRuleWithoutRamp("1"), data, clock, 0, maxRules)
		require.NoError(t, err)
	}

	// insert fourth --> error
	_, err = insertAssignmentRule(mkAssignmentRuleWithoutRamp("1"), data, clock, 0, maxRules)
	require.Equal(t, errExceedsMaxAssignmentRules(4, maxRules), err)
}

// Test requirement that target id isn't in a version set
func TestInsertAssignmentRuleInVersionSet(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(1, clock)

	// target 0 --> failure
	_, err := insertAssignmentRule(mkAssignmentRuleWithoutRamp("0"), data, clock, 0, ignoreMaxRules)
	require.Equal(t, errTargetIsVersionSetMember, err)
}

func TestInsertAssignmentRulePartiallyRampedRuleIsRedirectSource(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data, err := insertRedirectRule(mkRedirectRule("0", "1"), mkInitialData(0, clock), clock, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.NoError(t, err)

	// insert 1 --> failure
	_, err = insertAssignmentRule(mkAssignmentRuleWithRamp("0", 10), data, clock, 0, ignoreMaxRules)
	require.Equal(t, errPartiallyRampedAssignmentRuleIsRedirectRuleSource, err)
}

func TestInsertAssignmentRuleInvalidNegativeIndex(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)

	// insert @ -1 --> failure
	_, err := insertAssignmentRule(mkAssignmentRuleWithoutRamp("0"), data, clock, -1, ignoreMaxRules)
	require.Equal(t, errInvalidNegativeIndex, err)
}

func TestInsertAssignmentRuleInvalidRampPercentage(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)

	// insert with ramp percent < 0 --> failure
	_, err := insertAssignmentRule(mkAssignmentRuleWithRamp("0", -1), data, clock, 0, ignoreMaxRules)
	require.Equal(t, errInvalidRampPercentage, err)

	// insert with ramp percent > 100 --> failure
	_, err = insertAssignmentRule(mkAssignmentRuleWithRamp("0", 101), data, clock, 0, ignoreMaxRules)
	require.Equal(t, errInvalidRampPercentage, err)
}

func TestReplaceAssignmentRuleBasic(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	data := mkInitialData(0, clock)
	expected := &persistencespb.VersioningData{AssignmentRules: []*persistencespb.AssignmentRule{}}
	var err error

	// start with three rules to replace
	rule1 := mkAssignmentRuleWithoutRamp("1")
	data.AssignmentRules = []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(rule1, clock, nil),
		mkAssignmentRulePersistence(rule1, clock, nil),
		mkAssignmentRulePersistence(rule1, clock, nil),
	}
	expected.AssignmentRules = []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(rule1, clock, nil),
		mkAssignmentRulePersistence(rule1, clock, nil),
		mkAssignmentRulePersistence(rule1, clock, nil),
	}

	// [1, 1, 1] --> [1, 1, 2]
	// [1A, 1A, 1A] --> [1A, 1A, 2A, 1D]
	rule2 := mkAssignmentRuleWithoutRamp("2")
	clock = hlc.Next(clock, timesource)
	data, err = replaceAssignmentRule(rule2, data, clock, 2, false)
	require.NoError(t, err)
	expected.AssignmentRules[2].DeleteTimestamp = clock
	expected.AssignmentRules = slices.Insert(expected.AssignmentRules, 2, mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("2", 100), clock, nil))
	protorequire.ProtoEqual(t, expected, data)

	// [1, 1, 2] --> [0, 1, 2]
	// [1A, 1A, 2A, 1D] --> [0A, 1D, 1A, 2A, 1D]
	rule0 := mkAssignmentRuleWithoutRamp("0")
	clock = hlc.Next(clock, timesource)
	data, err = replaceAssignmentRule(rule0, data, clock, 0, false)
	require.NoError(t, err)
	expected.AssignmentRules[0].DeleteTimestamp = clock
	expected.AssignmentRules = slices.Insert(expected.AssignmentRules, 0, mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("0", 100), clock, nil))
	protorequire.ProtoEqual(t, expected, data)

	// [0, 1, 2] --> [0, 11, 2]
	// [0A, 1D, 1A, 2A, 1D] --> [0A, 1D, 11A, 1D, 2A, 1D]
	rule11 := mkAssignmentRuleWithoutRamp("11")
	clock = hlc.Next(clock, timesource)
	data, err = replaceAssignmentRule(rule11, data, clock, 1, false)
	require.NoError(t, err)
	expected.AssignmentRules[2].DeleteTimestamp = clock
	expected.AssignmentRules = slices.Insert(expected.AssignmentRules, 2, mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("11", 100), clock, nil))
	protorequire.ProtoEqual(t, expected, data)

	// out-of-bounds indices --> failure
	_, err = replaceAssignmentRule(rule11, data, clock, 99, false)
	require.Error(t, err)

	_, err = replaceAssignmentRule(rule11, data, clock, -1, false)
	require.Error(t, err)
}

func TestReplaceAssignmentRuleInVersionSet(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(1, clock)
	var err error
	data.AssignmentRules = []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("1"), clock, nil),
	}

	// replace 0 --> failure
	_, err = replaceAssignmentRule(mkAssignmentRuleWithoutRamp("0"), data, clock, 0, false)
	require.Equal(t, errTargetIsVersionSetMember, err)
}

func TestReplaceAssignmentRulePartiallyRampedRuleIsRedirectSource(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)
	data.AssignmentRules = []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("9"), clock, nil),
		mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("10"), clock, nil), // to avoid triggering "fully-ramped" error
	}
	data.RedirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("0", "1"), clock, nil),
	}

	// replace with target isSource and ramp < 100 --> failure
	_, err := replaceAssignmentRule(mkAssignmentRuleWithRamp("0", 10), data, clock, 0, false)
	require.Equal(t, errPartiallyRampedAssignmentRuleIsRedirectRuleSource, err)
}

func TestReplaceAssignmentRuleTestRequireFullyRamped(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)
	var err error

	// replace fully-ramped rule with partially-ramped rule --> failure
	data.AssignmentRules = []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("1"), clock, nil),
	}
	_, err = replaceAssignmentRule(mkAssignmentRuleWithRamp("2", 20), data, clock, 0, false)
	require.Equal(t, errRequireFullyRampedAssignmentRule, err)

	// same as above but with force --> success
	_, err = replaceAssignmentRule(mkAssignmentRuleWithRamp("4", 20), data, clock, 0, true)
	require.NoError(t, err)
}

func TestReplaceAssignmentRuleIndexOutOfBounds(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)
	data.AssignmentRules = []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("1"), clock, nil),
	}

	// replace @ -1 --> failure
	_, err := replaceAssignmentRule(mkAssignmentRuleWithoutRamp("0"), data, clock, -1, false)
	require.Equal(t, errAssignmentRuleIndexOutOfBounds(-1, len(data.AssignmentRules)), err)

	// replace @ 1 --> failure
	_, err = replaceAssignmentRule(mkAssignmentRuleWithoutRamp("0"), data, clock, 1, false)
	require.Equal(t, errAssignmentRuleIndexOutOfBounds(1, len(data.AssignmentRules)), err)
}

func TestReplaceAssignmentRuleInvalidRampPercentage(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)
	data.AssignmentRules = []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("1"), clock, nil),
	}

	// replace with ramp percent < 0 --> failure
	_, err := replaceAssignmentRule(mkAssignmentRuleWithRamp("0", -1), data, clock, 0, false)
	require.Equal(t, errInvalidRampPercentage, err)

	// replace with ramp percent > 100 --> failure
	_, err = replaceAssignmentRule(mkAssignmentRuleWithRamp("0", 101), data, clock, 0, false)
	require.Equal(t, errInvalidRampPercentage, err)
}

func TestDeleteAssignmentRuleBasic(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	timesource := commonclock.NewEventTimeSource().Update(time.Now())
	data := mkInitialData(0, clock)
	data.AssignmentRules = []*persistencespb.AssignmentRule{}
	expected := &persistencespb.VersioningData{AssignmentRules: []*persistencespb.AssignmentRule{}}
	var err error

	nextClock := func() *hlc.Clock {
		timesource.Advance(time.Minute)
		clock = hlc.Next(clock, timesource)
		return clock
	}

	// start with three rules inserted at different times
	rule1 := mkAssignmentRuleWithoutRamp("1")
	data.AssignmentRules = slices.Insert(data.AssignmentRules, 0, mkAssignmentRulePersistence(rule1, clock, nil))
	expected.AssignmentRules = slices.Insert(expected.AssignmentRules, 0, mkAssignmentRulePersistence(rule1, clock, nil))
	data.AssignmentRules = slices.Insert(data.AssignmentRules, 0, mkAssignmentRulePersistence(rule1, nextClock(), nil))
	expected.AssignmentRules = slices.Insert(expected.AssignmentRules, 0, mkAssignmentRulePersistence(rule1, clock, nil))
	data.AssignmentRules = slices.Insert(data.AssignmentRules, 0, mkAssignmentRulePersistence(rule1, nextClock(), nil))
	expected.AssignmentRules = slices.Insert(expected.AssignmentRules, 0, mkAssignmentRulePersistence(rule1, clock, nil))

	// in-bounds index --> success
	data, err = deleteAssignmentRule(data, nextClock(), 2, false)
	require.NoError(t, err)
	expected.AssignmentRules[2].DeleteTimestamp = clock
	protorequire.ProtoEqual(t, expected, data)

	data, err = deleteAssignmentRule(data, nextClock(), 0, false)
	require.NoError(t, err)
	expected.AssignmentRules[0].DeleteTimestamp = clock
	protorequire.ProtoEqual(t, expected, data)

	// out-of-bounds index --> failure
	_, err = deleteAssignmentRule(data, nextClock(), 99, false)
	require.Error(t, err)

	_, err = deleteAssignmentRule(data, nextClock(), -1, false)
	require.Error(t, err)
}

func TestDeleteAssignmentRuleTestRequireFullyRamped(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)
	var err error

	// delete only fully-ramped rule --> failure
	data.AssignmentRules = []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("1"), clock, nil),
	}
	_, err = deleteAssignmentRule(data, clock, 0, false)
	require.Equal(t, errRequireFullyRampedAssignmentRule, err)

	// same as above but with force --> success
	_, err = deleteAssignmentRule(data, clock, 0, true)
	require.NoError(t, err)

	// delete one of two fully-ramped rules --> success
	data.AssignmentRules = []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("1"), clock, nil),
		mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("1"), clock, nil),
	}
	_, err = deleteAssignmentRule(data, clock, 0, false)
	require.NoError(t, err)
}

func TestDeleteAssignmentRuleIndexOutOfBounds(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)
	data.AssignmentRules = []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("1"), clock, nil),
	}

	// delete @ -1 --> failure
	_, err := deleteAssignmentRule(data, clock, -1, false)
	require.Equal(t, errAssignmentRuleIndexOutOfBounds(-1, len(data.AssignmentRules)), err)

	// delete @ 1 --> failure
	_, err = deleteAssignmentRule(data, clock, 1, false)
	require.Equal(t, errAssignmentRuleIndexOutOfBounds(1, len(data.AssignmentRules)), err)
}

func TestAddRedirectRuleBasic(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	initialData := mkInitialData(0, clock)
	expectedSet := make([]*persistencespb.RedirectRule, 0)

	rule1 := mkRedirectRule("1", "0")
	data, err := insertRedirectRule(rule1, initialData, clock, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.NoError(t, err)
	expectedSet = append(expectedSet, mkRedirectRulePersistence(rule1, clock, nil))
	protorequire.ProtoElementsMatch(t, expectedSet, data.RedirectRules)

	rule2 := mkRedirectRule("2", "0")
	data, err = insertRedirectRule(rule2, data, clock, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.NoError(t, err)
	expectedSet = append(expectedSet, mkRedirectRulePersistence(rule2, clock, nil))
	protorequire.ProtoElementsMatch(t, expectedSet, data.RedirectRules)

	rule3 := mkRedirectRule("3", "0")
	data, err = insertRedirectRule(rule3, data, clock, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.NoError(t, err)
	expectedSet = append(expectedSet, mkRedirectRulePersistence(rule3, clock, nil))
	protorequire.ProtoElementsMatch(t, expectedSet, data.RedirectRules)

	protorequire.ProtoEqual(t, mkInitialData(0, clock), initialData)
}

func TestAddRedirectRuleMaxRules(t *testing.T) {
	t.Parallel()
	maxRules := 3
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)
	var err error

	// insert 3x --> success
	for i := range 3 {
		src := fmt.Sprintf("%d", i)
		dst := fmt.Sprintf("%d", i+1)
		data, err = insertRedirectRule(mkRedirectRule(src, dst), data, clock, maxRules, ignoreMaxUpstreamBuildIDs)
		require.NoError(t, err)
	}

	// insert fourth --> error
	_, err = insertRedirectRule(mkRedirectRule("10", "20"), data, clock, maxRules, ignoreMaxUpstreamBuildIDs)
	require.Equal(t, errExceedsMaxRedirectRules(4, maxRules), err)
}

func TestAddRedirectRuleInVersionSet(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	// make version set with build ID "0" in it
	initialData := mkInitialData(1, clock)

	// insert with source build id "0" --> failure
	_, err := insertRedirectRule(mkRedirectRule("0", "1"), initialData, clock, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.Equal(t, errSourceIsVersionSetMember, err)

	// insert with target build id "0" --> failure
	_, err = insertRedirectRule(mkRedirectRule("1", "0"), initialData, clock, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.Equal(t, errTargetIsVersionSetMember, err)
}

func TestAddRedirectRuleSourceIsPartiallyRampedAssignmentRuleTarget(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)
	data.AssignmentRules = []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("1", 10), clock, nil),
		mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("2"), clock, nil),
	}

	// insert redirect rule with target 1 --> failure
	_, err := insertRedirectRule(mkRedirectRule("1", "0"), data, clock, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.Equal(t, errSourceIsPartiallyRampedAssignmentRuleTarget, err)
}

func TestAddRedirectRuleAlreadyExists(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	initialData := mkInitialData(0, clock)

	// insert with source build id "0"
	data, err := insertRedirectRule(mkRedirectRule("0", "1"), initialData, clock, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.NoError(t, err)

	// insert with source build id "0" --> failure
	_, err = insertRedirectRule(mkRedirectRule("0", "6"), data, clock, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.Equal(t, errSourceAlreadyExists("0", "1"), err)
}

func TestAddRedirectRuleCreateCycle(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	initialData := mkInitialData(0, clock)

	// insert with source -> target == "0" -> "0" --> failure
	_, err := insertRedirectRule(mkRedirectRule("0", "0"), initialData, clock, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.Equal(t, errIsCyclic, err)

	// insert with source -> target == "0" -> "1" --> success
	data, err := insertRedirectRule(mkRedirectRule("0", "1"), initialData, clock, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.NoError(t, err)

	// insert with source build id "1" -> "0" --> failure
	_, err = insertRedirectRule(mkRedirectRule("1", "0"), data, clock, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.Equal(t, errIsCyclic, err)
}

func TestAddRedirectRuleMaxUpstreamBuildIDs(t *testing.T) {
	t.Parallel()
	maxUpstreamBuildIDs := 2
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)

	// insert (4->5)
	// 4 ---> 5
	data, err := insertRedirectRule(mkRedirectRule("4", "5"), data, clock, ignoreMaxRules, maxUpstreamBuildIDs)
	require.NoError(t, err)

	// insert (5->6)
	// 4 ---> 5 ---> 6
	data, err = insertRedirectRule(mkRedirectRule("5", "6"), data, clock, ignoreMaxRules, maxUpstreamBuildIDs)
	require.NoError(t, err)

	// insert (6->7)
	// 4 ---> 5 ---> 6 ---> 7
	_, err = insertRedirectRule(mkRedirectRule("6", "7"), data, clock, ignoreMaxRules, maxUpstreamBuildIDs)
	require.Equal(t, errExceedsMaxUpstreamBuildIDs(3, maxUpstreamBuildIDs), err)
}

func TestAddRedirectRuleUnversionedTarget(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)

	// insert (1->"") errors
	_, err := insertRedirectRule(mkRedirectRule("1", ""), data, clock, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.ErrorIs(t, err, errUnversionedRedirectRuleTarget)
}

func TestReplaceRedirectRuleBasic(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	data := mkInitialData(0, clock)
	data.RedirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("1", "0"), clock, nil),
		mkRedirectRulePersistence(mkRedirectRule("2", "0"), clock, nil),
		mkRedirectRulePersistence(mkRedirectRule("3", "0"), clock, nil),
	}
	var err error

	replaceTest := func(source, target string) {
		prevRule := getActiveRedirectRuleBySrc(source, data)
		rule := mkRedirectRule(source, target)
		data, err = replaceRedirectRule(rule, data, clock, ignoreMaxUpstreamBuildIDs)
		require.NoError(t, err)
		newActive := getActiveRedirectRuleBySrc(source, data)
		protorequire.ProtoEqual(t, rule, newActive.GetRule())
		deleted := getDeletedRedirectRuleBySrc(source, data)
		require.Len(t, deleted, 1)
		require.Equal(t, prevRule.GetRule().GetSourceBuildId(), deleted[0].GetRule().GetSourceBuildId())
		require.Equal(t, prevRule.GetRule().GetTargetBuildId(), deleted[0].GetRule().GetTargetBuildId())
		for _, dr := range deleted {
			require.GreaterOrEqual(t, newActive.GetCreateTimestamp().GetWallClock(), dr.GetDeleteTimestamp().GetWallClock())
		}
	}

	clock = hlc.Next(clock, timesource)
	replaceTest("1", "100")
	replaceTest("2", "100")
	replaceTest("3", "100")
}

// Test requirement that target id and source id are not in a version set (success and failure)
func TestReplaceRedirectRuleInVersionSet(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	// make a version set with build ID 0
	data := mkInitialData(1, clock)
	data.RedirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("1", "2"), clock, nil),
	}
	var err error

	// replace with target 0 --> failure
	_, err = replaceRedirectRule(mkRedirectRule("1", "0"), data, clock, ignoreMaxUpstreamBuildIDs)
	require.Equal(t, errTargetIsVersionSetMember, err)
}

func TestReplaceRedirectRuleCreateCycle(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)
	data.RedirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("0", "1"), clock, nil),
		mkRedirectRulePersistence(mkRedirectRule("1", "2"), clock, nil),
		mkRedirectRulePersistence(mkRedirectRule("2", "3"), clock, nil),
	}
	var err error

	_, err = replaceRedirectRule(mkRedirectRule("0", "0"), data, clock, ignoreMaxUpstreamBuildIDs)
	require.Equal(t, errIsCyclic, err)

	_, err = replaceRedirectRule(mkRedirectRule("2", "0"), data, clock, ignoreMaxUpstreamBuildIDs)
	require.Equal(t, errIsCyclic, err)

	_, err = replaceRedirectRule(mkRedirectRule("1", "0"), data, clock, ignoreMaxUpstreamBuildIDs)
	require.Equal(t, errIsCyclic, err)

	_, err = replaceRedirectRule(mkRedirectRule("2", "1"), data, clock, ignoreMaxUpstreamBuildIDs)
	require.Equal(t, errIsCyclic, err)
}

func TestReplaceRedirectRuleMaxUpstreamBuildIDs(t *testing.T) {
	t.Parallel()
	maxUpstreamBuildIDs := 2
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)

	// 2 ---> 3, 4 ---> 5 ---> 6
	data.RedirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("2", "3"), clock, nil),
		mkRedirectRulePersistence(mkRedirectRule("4", "5"), clock, nil),
		mkRedirectRulePersistence(mkRedirectRule("5", "6"), clock, nil),
	}

	// replace(2, new_target=1)
	// 2 ---> 1, 4 ---> 5 ---> 6
	data, err := replaceRedirectRule(mkRedirectRule("2", "1"), data, clock, maxUpstreamBuildIDs)
	require.NoError(t, err)

	// replace(2, new_target=4)
	// 2 ---> 4 ---> 5 ---> 6
	_, err = replaceRedirectRule(mkRedirectRule("2", "4"), data, clock, maxUpstreamBuildIDs)
	require.Equal(t, errExceedsMaxUpstreamBuildIDs(3, maxUpstreamBuildIDs), err)
}

func TestReplaceRedirectRuleUnversionedTarget(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)

	// insert (1->2) so that we can replace
	data, err := insertRedirectRule(mkRedirectRule("1", "2"), data, clock, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.NoError(t, err)

	// replace (1->"") errors
	_, err = replaceRedirectRule(mkRedirectRule("1", ""), data, clock, ignoreMaxUpstreamBuildIDs)
	require.ErrorIs(t, err, errUnversionedRedirectRuleTarget)
}

func TestReplaceRedirectRuleNotFound(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)
	var err error

	// fails because no rules to replace
	_, err = replaceRedirectRule(mkRedirectRule("1", "100"), data, clock, ignoreMaxUpstreamBuildIDs)
	require.Equal(t, errSourceNotFound("1"), err)

	data.RedirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("0", "1"), clock, nil),
	}

	// fails because source doesnt exist
	_, err = replaceRedirectRule(mkRedirectRule("1", "100"), data, clock, ignoreMaxUpstreamBuildIDs)
	require.Equal(t, errSourceNotFound("1"), err)
}

func TestDeleteRedirectRuleBasic(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	data := mkInitialData(0, clock)
	data.RedirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("0", "1"), clock, nil),
		mkRedirectRulePersistence(mkRedirectRule("1", "2"), clock, nil),
		mkRedirectRulePersistence(mkRedirectRule("2", "3"), clock, nil),
	}
	var err error

	clock1 := hlc.Next(clock, timesource)
	data, err = deleteRedirectRule("1", data, clock1)
	require.NoError(t, err)
	deleted := getDeletedRedirectRuleBySrc("1", data)
	require.Len(t, deleted, 1)
	require.Equal(t, clock1.GetWallClock(), deleted[0].GetDeleteTimestamp().GetWallClock())
	require.Equal(t, "1", deleted[0].GetRule().GetSourceBuildId())
	require.Equal(t, "2", deleted[0].GetRule().GetTargetBuildId())

	clock2 := hlc.Next(clock1, timesource)
	data, err = deleteRedirectRule("2", data, clock2)
	require.NoError(t, err)
	deleted = getDeletedRedirectRuleBySrc("2", data)
	require.Len(t, deleted, 1)
	require.Equal(t, clock2.GetWallClock(), deleted[0].GetDeleteTimestamp().GetWallClock())
	require.Equal(t, "2", deleted[0].GetRule().GetSourceBuildId())
	require.Equal(t, "3", deleted[0].GetRule().GetTargetBuildId())
}

func TestDeleteRedirectRuleNotFound(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)

	// fails because no rules to delete
	_, err := deleteRedirectRule("1", data, clock)
	require.Equal(t, errSourceNotFound("1"), err)

	// insert a rule to replace
	data.RedirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("0", "1"), clock, nil),
	}

	// fails because no rule with that source
	_, err = deleteRedirectRule("1", data, clock)
	require.Equal(t, errSourceNotFound("1"), err)
}

func TestGetWorkerVersioningRules(t *testing.T) {
	t.Parallel()
	clock1 := hlc.Zero(1)
	clock2 := hlc.Next(clock1, commonclock.NewRealTimeSource())
	data := &persistencespb.VersioningData{
		AssignmentRules: []*persistencespb.AssignmentRule{
			mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("1"), clock1, nil),
			mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("10"), clock2, nil),
			mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("10"), clock1, clock2),
			mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("100"), clock2, nil),
		},
		RedirectRules: []*persistencespb.RedirectRule{
			mkRedirectRulePersistence(mkRedirectRule("1", "2"), clock1, nil),
			mkRedirectRulePersistence(mkRedirectRule("3", "4"), clock2, nil),
			mkRedirectRulePersistence(mkRedirectRule("4", "5"), clock2, nil),
			mkRedirectRulePersistence(mkRedirectRule("4", "6"), clock1, clock2),
		},
	}

	// Call list successfully
	dummyClock := hlc.Zero(99) // used to generate conflict token, but not in this test
	resp, err := GetTimestampedWorkerVersioningRules(data, dummyClock)
	require.NoError(t, err)

	// check assignment rules
	assignmentRules := resp.GetResponse().GetAssignmentRules()
	require.Len(t, assignmentRules, 3)
	protorequire.ProtoEqual(t, &taskqueuepb.TimestampedBuildIdAssignmentRule{
		Rule:       mkAssignmentRuleWithoutRamp("1"),
		CreateTime: hlc.ProtoTimestamp(clock1),
	}, assignmentRules[0])
	protorequire.ProtoEqual(t, &taskqueuepb.TimestampedBuildIdAssignmentRule{
		Rule:       mkAssignmentRuleWithoutRamp("10"),
		CreateTime: hlc.ProtoTimestamp(clock2),
	}, assignmentRules[1])
	protorequire.ProtoEqual(t, &taskqueuepb.TimestampedBuildIdAssignmentRule{
		Rule:       mkAssignmentRuleWithoutRamp("100"),
		CreateTime: hlc.ProtoTimestamp(clock2),
	}, assignmentRules[2])

	// check redirect rules, no ordering guarantee
	redirectRules := resp.GetResponse().GetCompatibleRedirectRules()
	require.Len(t, redirectRules, 3)
	contains := func(expected *taskqueuepb.TimestampedCompatibleBuildIdRedirectRule) bool {
		for _, r := range redirectRules {
			if proto.Equal(expected, r) {
				return true
			}
		}
		return false
	}
	require.True(t, contains(&taskqueuepb.TimestampedCompatibleBuildIdRedirectRule{
		Rule:       mkRedirectRule("1", "2"),
		CreateTime: hlc.ProtoTimestamp(clock1),
	}))
	require.True(t, contains(&taskqueuepb.TimestampedCompatibleBuildIdRedirectRule{
		Rule:       mkRedirectRule("3", "4"),
		CreateTime: hlc.ProtoTimestamp(clock2),
	}))
	require.True(t, contains(&taskqueuepb.TimestampedCompatibleBuildIdRedirectRule{
		Rule:       mkRedirectRule("4", "5"),
		CreateTime: hlc.ProtoTimestamp(clock2),
	}))
	require.False(t, contains(&taskqueuepb.TimestampedCompatibleBuildIdRedirectRule{
		Rule:       mkRedirectRule("4", "6"),
		CreateTime: hlc.ProtoTimestamp(clock1),
	}))
}

func TestCleanupRedirectRuleTombstones(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	initialData := mkInitialData(0, clock)

	// start time one hour ago
	timesource := commonclock.NewEventTimeSource().Update(time.Now().Add(-1 * time.Hour))

	// insert 3x to get three rules in there
	rule1 := mkRedirectRule("1", "10")
	clock1 := hlc.Next(clock, timesource)
	data, err := insertRedirectRule(rule1, initialData, clock1, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.NoError(t, err)
	rule2 := mkRedirectRule("2", "10")
	data, err = insertRedirectRule(rule2, data, clock1, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.NoError(t, err)
	rule3 := mkRedirectRule("3", "10")
	data, err = insertRedirectRule(rule3, data, clock1, ignoreMaxRules, ignoreMaxUpstreamBuildIDs)
	require.NoError(t, err)

	// delete "now," ~1 hour ago
	clock4 := hlc.Next(clock, timesource)
	data, err = deleteRedirectRule("1", data, clock4)
	require.NoError(t, err)
	// delete 35 min later, ~25 min ago
	timesource.Advance(35 * time.Minute)
	clock5 := hlc.Next(clock, timesource)
	data, err = deleteRedirectRule("2", data, clock5)
	require.NoError(t, err)
	// delete 25 min later, ~now (real time.Now())
	timesource.Advance(25 * time.Minute)
	clock6 := hlc.Next(clock, timesource)
	data, err = deleteRedirectRule("3", data, clock6)
	require.NoError(t, err)

	// Remove data that was deleted > 30 min ago --> remove first rule
	data = CleanupRuleTombstones(data, 30*time.Minute)
	sources := make([]string, 0)
	for _, r := range data.GetRedirectRules() {
		sources = append(sources, r.GetRule().GetSourceBuildId())
	}
	require.NotContains(t, sources, "1")
	require.Contains(t, sources, "2")
	require.Contains(t, sources, "3")

	// Remove data that was deleted > 5 min ago --> remove second rule
	data = CleanupRuleTombstones(data, 5*time.Minute)
	sources = make([]string, 0)
	for _, r := range data.GetRedirectRules() {
		sources = append(sources, r.GetRule().GetSourceBuildId())
	}
	require.NotContains(t, sources, "1")
	require.NotContains(t, sources, "2")
	require.Contains(t, sources, "3")
}

func TestCommitBuildIDBasic(t *testing.T) {
	t.Parallel()
	timesource := commonclock.NewRealTimeSource()
	clock1 := hlc.Zero(1)
	clock2 := hlc.Next(clock1, timesource)
	data := &persistencespb.VersioningData{
		AssignmentRules: []*persistencespb.AssignmentRule{
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("1", 1), clock1, nil),
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("10", 1), clock1, nil),
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("100", 100), clock1, nil),
		},
	}
	expected := &persistencespb.VersioningData{
		AssignmentRules: []*persistencespb.AssignmentRule{
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("1", 1), clock1, nil),
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("10", 1), clock1, clock2),
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("100", 100), clock1, clock2),
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("10", 100), clock2, nil),
		},
	}
	var err error

	data, err = CommitBuildID(clock2, data, mkNewCommitBuildIDReq("10", false), true, ignoreMaxRules)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, expected, data)

	// make sure multiple commits are idempotent except for timestamps
	clock3 := hlc.Next(clock2, timesource)
	expected = &persistencespb.VersioningData{
		AssignmentRules: []*persistencespb.AssignmentRule{
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("1", 1), clock1, nil),
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("10", 1), clock1, clock2),
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("100", 100), clock1, clock2),
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("10", 100), clock2, clock3),
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("10", 100), clock3, nil),
		},
	}
	data, err = CommitBuildID(clock3, data, mkNewCommitBuildIDReq("10", false), true, ignoreMaxRules)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, expected, data)
}

func TestCommitBuildIDNoRecentPoller(t *testing.T) {
	// note: correctly generating hasRecentPoller needs to be tested in the end-to-end tests
	t.Parallel()
	timesource := commonclock.NewRealTimeSource()
	clock1 := hlc.Zero(1)
	clock2 := hlc.Next(clock1, timesource)
	data := &persistencespb.VersioningData{
		AssignmentRules: []*persistencespb.AssignmentRule{
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("1", 1), clock1, nil),
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("10", 1), clock1, nil),
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("100", 100), clock1, nil),
		},
	}
	var err error

	// without force --> fail
	_, err = CommitBuildID(clock2, data, mkNewCommitBuildIDReq("10", false), false, ignoreMaxRules)
	require.Equal(t, errNoRecentPollerOnCommitVersion("10"), err)

	// with force --> success
	_, err = CommitBuildID(clock2, data, mkNewCommitBuildIDReq("10", true), false, ignoreMaxRules)
	require.NoError(t, err)
}

func TestCommitBuildIDInVersionSet(t *testing.T) {
	t.Parallel()
	timesource := commonclock.NewRealTimeSource()
	clock1 := hlc.Zero(1)
	clock2 := hlc.Next(clock1, timesource)
	data := mkInitialData(1, clock1)
	data.AssignmentRules = []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("0", 1), clock1, nil),
		mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("10", 1), clock1, nil),
		mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("100"), clock1, nil),
	}
	var err error

	// with target 0 --> fail
	_, err = CommitBuildID(clock2, data, mkNewCommitBuildIDReq("0", false), true, ignoreMaxRules)
	require.Equal(t, errTargetIsVersionSetMember, err)
}

func TestCommitBuildIDMaxAssignmentRules(t *testing.T) {
	t.Parallel()
	maxRules := 3
	timesource := commonclock.NewRealTimeSource()
	clock1 := hlc.Zero(1)
	clock2 := hlc.Next(clock1, timesource)
	data := &persistencespb.VersioningData{
		AssignmentRules: []*persistencespb.AssignmentRule{
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("1", 1), clock1, nil),
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("10", 1), clock1, nil),
			mkAssignmentRulePersistence(mkAssignmentRuleWithRamp("100", 1), clock1, nil),
		},
	}
	var err error

	// commit a new target, no rules to be deleted --> fail
	_, err = CommitBuildID(clock2, data, mkNewCommitBuildIDReq("1000", false), true, maxRules)
	require.Equal(t, errExceedsMaxAssignmentRules(4, maxRules), err)
}

/*
e.g.
Redirect Rules:
1 ------> 2
^         |
|         v
5 <------ 3 ------> 4
*/
func TestIsCyclic(t *testing.T) {
	rules := []*persistencespb.RedirectRule{
		{Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{SourceBuildId: "1", TargetBuildId: "2"}},
		{Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{SourceBuildId: "5", TargetBuildId: "1"}},
		{Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{SourceBuildId: "3", TargetBuildId: "4"}},
		{Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{SourceBuildId: "3", TargetBuildId: "5"}},
		{Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{SourceBuildId: "2", TargetBuildId: "3"}},
	}
	if !isCyclic(rules) {
		t.Fail()
	}

	rules = slices.Delete(rules, 3, 4)
	if isCyclic(rules) {
		t.Fail()
	}

	rules = append(rules, &persistencespb.RedirectRule{
		Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{SourceBuildId: "4", TargetBuildId: "2"},
	})
	if !isCyclic(rules) {
		t.Fail()
	}
}

func TestFindTerminalBuildId(t *testing.T) {
	t.Parallel()
	/*
		e.g.
		Redirect Rules:
		10
		^
		|
		1 <------ 2
		^
		|
		5 <------ 3 <------ 4
	*/
	createTS := hlc.Zero(1)

	redirectRules := []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("1", "10"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("2", "1"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("3", "5"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("4", "3"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("5", "1"), createTS, nil),
	}

	require.Equal(t, "10", findTerminalBuildId("1", redirectRules))
	require.Equal(t, "10", findTerminalBuildId("2", redirectRules))
	require.Equal(t, "10", findTerminalBuildId("3", redirectRules))
	require.Equal(t, "10", findTerminalBuildId("4", redirectRules))
	require.Equal(t, "10", findTerminalBuildId("5", redirectRules))
	require.Equal(t, "10", findTerminalBuildId("10", redirectRules))

	// empty rule set
	require.Equal(t, "11", findTerminalBuildId("11", []*persistencespb.RedirectRule{}))

	// single rule
	redirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("1", "2"), createTS, nil),
	}
	require.Equal(t, "2", findTerminalBuildId("1", redirectRules))
	require.Equal(t, "2", findTerminalBuildId("2", redirectRules))

	// cyclic rule set
	redirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("1", "2"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("2", "1"), createTS, nil),
	}
	require.Empty(t, findTerminalBuildId("1", redirectRules))
	require.Empty(t, findTerminalBuildId("2", redirectRules))
}

func TestGetUpstreamBuildIds_NoCycle(t *testing.T) {
	t.Parallel()
	/*
		e.g.
		Redirect Rules:
		10
		^
		|
		1 <------ 2
		^
		|
		5 <------ 3 <------ 4
	*/
	createTS := hlc.Zero(1)

	redirectRules := []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("1", "10"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("2", "1"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("3", "5"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("4", "3"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("5", "1"), createTS, nil),
	}

	expectedUpstreamBuildIds := []string{"2", "5", "3", "4"}
	upstreamBuildIds := getUpstreamBuildIds("1", redirectRules)
	slices.Sort(expectedUpstreamBuildIds)
	slices.Sort(upstreamBuildIds)
	require.Equal(t, expectedUpstreamBuildIds, upstreamBuildIds)
}

func TestGetUpstreamBuildIds_WithCycle(t *testing.T) {
	t.Parallel()
	/*
		e.g.
		Redirect Rules:
		1 ------> 2
		^         |
		|         v
		5 <------ 3 ------> 4
	*/
	createTS := hlc.Zero(1)
	redirectRules := []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("1", "2"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("2", "3"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("3", "4"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("3", "5"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("5", "1"), createTS, nil),
	}
	expectedUpstreamBuildIds := []string{"5", "3", "2"}
	upstreamBuildIds := getUpstreamBuildIds("1", redirectRules)
	slices.Sort(expectedUpstreamBuildIds)
	slices.Sort(upstreamBuildIds)
	require.Equal(t, expectedUpstreamBuildIds, upstreamBuildIds)

	/*
		e.g.
		Redirect Rules:
		1         2 <---
		^         |     \
		|         v      \
		5 <------ 3 ------> 4
	*/
	redirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("2", "3"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("3", "4"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("3", "5"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("4", "2"), createTS, nil),
		mkRedirectRulePersistence(mkRedirectRule("5", "1"), createTS, nil),
	}
	expectedUpstreamBuildIds = []string{"5", "3", "2", "4"}
	upstreamBuildIds = getUpstreamBuildIds("1", redirectRules)
	slices.Sort(expectedUpstreamBuildIds)
	slices.Sort(upstreamBuildIds)
	require.Equal(t, expectedUpstreamBuildIds, upstreamBuildIds)
}
