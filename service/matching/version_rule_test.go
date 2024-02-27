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
	"github.com/stretchr/testify/assert"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	commonclock "go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/testing/protoassert"
	"slices"
	"testing"
	"time"
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

func mkAssignmentRulePersistence(rule *taskqueuepb.BuildIdAssignmentRule, createTs, deleteTs *hlc.Clock) *persistencepb.AssignmentRule {
	return &persistencepb.AssignmentRule{
		Rule:            rule,
		CreateTimestamp: createTs,
		DeleteTimestamp: deleteTs,
	}
}

func mkAssignmentRule(target string, ramp *taskqueuepb.BuildIdAssignmentRule_PercentageRamp) *taskqueuepb.BuildIdAssignmentRule {
	ret := &taskqueuepb.BuildIdAssignmentRule{
		TargetBuildId: target,
	}
	// if ramp == nil and is set above, there is a nil-pointer error in GetPercentageRamp()
	// because casting Rule to (*BuildIdAssignmentRule_WorkerRatioRamp) succeeds
	if ramp != nil {
		ret.Ramp = ramp
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

func mkRedirectRulePersistence(rule *taskqueuepb.CompatibleBuildIdRedirectRule, createTs, deleteTs *hlc.Clock) *persistencepb.RedirectRule {
	return &persistencepb.RedirectRule{
		Rule:            rule,
		CreateTimestamp: createTs,
		DeleteTimestamp: deleteTs,
	}
}

func mkRedirectRule(source, target string, ramp *taskqueuepb.CompatibleBuildIdRedirectRule_WorkerRatioRamp) *taskqueuepb.CompatibleBuildIdRedirectRule {
	ret := &taskqueuepb.CompatibleBuildIdRedirectRule{
		TargetBuildId: target,
		SourceBuildId: source,
	}
	// if ramp == nil and is set above, there is a nil-pointer error in GetPercentageRamp()
	// because casting Rule to (*BuildIdAssignmentRule_WorkerRatioRamp) succeeds
	if ramp != nil {
		ret.Ramp = ramp
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
	data *persistencepb.VersioningData,
	clock *hlc.Clock,
	idx int32,
	maxAssignmentRules int,
) (*persistencepb.VersioningData, error) {
	return InsertAssignmentRule(clock, data, mkNewInsertAssignmentReq(rule, idx), maxAssignmentRules)
}

func insertRedirectRule(rule *taskqueuepb.CompatibleBuildIdRedirectRule,
	data *persistencepb.VersioningData,
	clock *hlc.Clock,
	maxAssignmentRules int,
) (*persistencepb.VersioningData, error) {
	return InsertCompatibleRedirectRule(clock, data, mkNewInsertRedirectReq(rule), maxAssignmentRules)
}

func replaceAssignmentRule(rule *taskqueuepb.BuildIdAssignmentRule,
	data *persistencepb.VersioningData,
	clock *hlc.Clock,
	idx int32,
	force bool,
) (*persistencepb.VersioningData, error) {
	return ReplaceAssignmentRule(clock, data, mkNewReplaceAssignmentReq(rule, idx, force))
}

func replaceRedirectRule(rule *taskqueuepb.CompatibleBuildIdRedirectRule,
	data *persistencepb.VersioningData,
	clock *hlc.Clock,
) (*persistencepb.VersioningData, error) {
	return ReplaceCompatibleRedirectRule(clock, data, mkNewReplaceRedirectReq(rule))
}

func deleteAssignmentRule(data *persistencepb.VersioningData,
	clock *hlc.Clock,
	idx int32,
	force bool,
) (*persistencepb.VersioningData, error) {
	return DeleteAssignmentRule(clock, data, mkNewDeleteAssignmentReq(idx, force))
}

func deleteRedirectRule(source string,
	data *persistencepb.VersioningData,
	clock *hlc.Clock,
) (*persistencepb.VersioningData, error) {
	return DeleteCompatibleRedirectRule(clock, data, mkNewDeleteRedirectReq(source))
}

func testList(t *testing.T, expected, actual *persistencepb.VersioningData) {
	resp := getListResp(t, actual)
	activeAssignmentRules := getActiveAssignmentRules(expected.GetAssignmentRules())
	activeRedirectRules := getActiveRedirectRules(expected.GetRedirectRules())
	for i, r := range resp.GetAssignmentRules() {
		protoassert.ProtoEqual(t, activeAssignmentRules[i].GetRule(), r.GetRule())
	}
	for i, r := range resp.GetCompatibleRedirectRules() {
		protoassert.ProtoEqual(t, activeRedirectRules[i].GetRule(), r.GetRule())
	}
}

func getListResp(t *testing.T, data *persistencepb.VersioningData) *workflowservice.ListWorkerVersioningRulesResponse {
	dummyClock := hlc.Zero(99)
	resp, err := ListWorkerVersioningRules(data, dummyClock)
	assert.NoError(t, err)
	return resp.GetResponse()
}

// Test inserting before and after hitting the max rules limit.
func TestInsertAssignmentRuleMaxRules(t *testing.T) {
	t.Parallel()
	maxRules := 3
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	initialData := mkInitialData(0, clock)
	assert.False(t, containsUnfiltered(initialData.GetAssignmentRules()))

	// insert to empty versioning data --> success
	rule1 := mkAssignmentRule("1", nil)
	clock1 := hlc.Next(clock, timesource)
	updatedData, err := insertAssignmentRule(rule1, initialData, clock1, 0, maxRules)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, mkInitialData(0, clock), initialData)
	expected := &persistencepb.VersioningData{
		AssignmentRules: []*persistencepb.AssignmentRule{
			mkAssignmentRulePersistence(rule1, clock1, nil),
		},
	}
	protoassert.ProtoEqual(t, expected, updatedData)

	// insert again --> success
	assert.True(t, containsUnfiltered(updatedData.GetAssignmentRules()))
	rule2 := mkAssignmentRule("2", nil)
	clock2 := hlc.Next(clock1, timesource)
	updatedData, err = insertAssignmentRule(rule2, updatedData, clock2, 0, maxRules)
	assert.NoError(t, err)
	expected = &persistencepb.VersioningData{
		AssignmentRules: slices.Insert(expected.GetAssignmentRules(), 0, mkAssignmentRulePersistence(rule2, clock2, nil)),
	}
	protoassert.ProtoEqual(t, expected, updatedData)

	// insert twice more --> failure due to max rules
	rule3 := mkAssignmentRule("3", nil)
	clock3 := hlc.Next(clock2, timesource)
	updatedData, err = insertAssignmentRule(rule3, updatedData, clock3, 0, maxRules)
	assert.NoError(t, err)
	expected = &persistencepb.VersioningData{
		AssignmentRules: slices.Insert(expected.GetAssignmentRules(), 0, mkAssignmentRulePersistence(rule3, clock3, nil)),
	}
	testList(t, expected, updatedData)
	rule4 := mkAssignmentRule("4", nil)
	clock4 := hlc.Next(clock2, timesource)
	updatedData, err = insertAssignmentRule(rule4, updatedData, clock4, 0, maxRules)
	assert.Error(t, err)
}

// Test requirement that target id isn't in a version set (success and failure)
func TestInsertAssignmentRuleInVersionSet(t *testing.T) {
	t.Parallel()
	maxRules := 3
	clock := hlc.Zero(1)
	initialData := mkInitialData(1, clock)
	assert.False(t, containsUnfiltered(initialData.GetAssignmentRules()))

	// insert "0" to versioning data with build id "0" --> failure
	rule0 := mkAssignmentRule("0", nil)
	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	_, err := insertAssignmentRule(rule0, initialData, nextClock, 0, maxRules)
	assert.Error(t, err)
	protoassert.ProtoEqual(t, mkInitialData(1, clock), initialData)

	// insert "1" --> success
	rule1 := mkAssignmentRule("1", nil)
	updatedData, err := insertAssignmentRule(rule1, initialData, nextClock, 0, maxRules)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, mkInitialData(1, clock), initialData)
	expected := &persistencepb.VersioningData{
		VersionSets: []*persistencepb.CompatibleVersionSet{
			mkSingleBuildIdSet("0", clock),
		},
		AssignmentRules: []*persistencepb.AssignmentRule{
			mkAssignmentRulePersistence(rule1, nextClock, nil),
		},
	}
	protoassert.ProtoEqual(t, expected, updatedData)
}

func TestInsertAssignmentRuleTerminalBuildID(t *testing.T) {
	// setup
	t.Parallel()
	maxRules := 3
	clock := hlc.Zero(1)
	data, err := insertRedirectRule(mkRedirectRule("0", "1", nil), mkInitialData(0, clock), clock, maxRules)
	assert.NoError(t, err)

	// insert assignment rule with target 1
	_, err = insertAssignmentRule(mkAssignmentRule("1", mkNewAssignmentPercentageRamp(10)), data, clock, 0, maxRules)
	assert.Error(t, err)
}

// Test inserting assignment rules with non-zero indexes. List to confirm.
func TestInsertAssignmentRuleNonzeroIdx(t *testing.T) {
	t.Parallel()
	maxRules := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	initialData := mkInitialData(0, clock)
	assert.False(t, containsUnfiltered(initialData.GetAssignmentRules()))

	// insert 3x to get three rules in there
	rule1 := mkAssignmentRule("1", nil)
	clock1 := hlc.Next(clock, timesource)
	data, err := insertAssignmentRule(rule1, initialData, clock1, 0, maxRules)
	assert.NoError(t, err)
	data, err = insertAssignmentRule(rule1, data, clock1, 0, maxRules)
	assert.NoError(t, err)
	data, err = insertAssignmentRule(rule1, data, clock1, 0, maxRules)
	assert.NoError(t, err)

	// test inserting with a given index, then listing, it should be at the desired index
	rule2 := mkAssignmentRule("2", nil)
	clock2 := hlc.Next(clock1, timesource)
	data, err = insertAssignmentRule(rule2, data, clock2, 2, maxRules)
	assert.NoError(t, err)
	assert.Equal(t, "2", getListResp(t, data).GetAssignmentRules()[2].GetRule().GetTargetBuildId())

	// test inserting with a too-big index, then listing, it should be at the back
	rule3 := mkAssignmentRule("3", nil)
	clock3 := hlc.Next(clock2, timesource)
	data, err = insertAssignmentRule(rule3, data, clock3, 100, maxRules)
	assert.NoError(t, err)
	listedRules := getListResp(t, data).GetAssignmentRules()
	assert.Equal(t, "3", listedRules[len(listedRules)-1].GetRule().GetTargetBuildId())
}

// Test replacing assignment rules at various indices. List to confirm.
func TestReplaceAssignmentRuleVariousIdx(t *testing.T) {
	t.Parallel()
	maxRules := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	initialData := mkInitialData(0, clock)
	assert.False(t, containsUnfiltered(initialData.GetAssignmentRules()))

	// insert 3x to get three rules in there
	rule1 := mkAssignmentRule("1", nil)
	clock1 := hlc.Next(clock, timesource)
	data, err := insertAssignmentRule(rule1, initialData, clock1, 0, maxRules)
	assert.NoError(t, err)
	data, err = insertAssignmentRule(rule1, data, clock1, 0, maxRules)
	assert.NoError(t, err)
	data, err = insertAssignmentRule(rule1, data, clock1, 0, maxRules)
	assert.NoError(t, err)

	// test replacing with a given index, then listing, it should be at the desired index
	replace := func(idx int32, expectSuccess bool) {
		target := fmt.Sprintf("%d", idx)
		rule := mkAssignmentRule(target, nil)
		data, err = replaceAssignmentRule(rule, data, hlc.Next(clock, timesource), idx, false)
		if !expectSuccess {
			assert.Error(t, err)
			return
		}
		assert.NoError(t, err)
		assert.Equal(t, target, getListResp(t, data).GetAssignmentRules()[idx].GetRule().GetTargetBuildId())
	}
	replace(2, true)
	replace(0, true)
	replace(1, true)

	// test replacing with an out-of-bounds index --> should fail
	replace(99, false)
	replace(-1, false)
}

func TestReplaceAssignmentRuleInVersionSet(t *testing.T) {
	t.Parallel()
	maxRules := 3
	clock := hlc.Zero(1)
	data := mkInitialData(1, clock)
	assert.False(t, containsUnfiltered(data.GetAssignmentRules()))
	var err error

	// insert a rule so there's something to replace
	rule1 := mkAssignmentRule("1", nil)
	data, err = insertAssignmentRule(rule1, data, clock, 0, maxRules)
	expected := &persistencepb.VersioningData{
		AssignmentRules: []*persistencepb.AssignmentRule{
			mkAssignmentRulePersistence(rule1, clock, nil),
		},
		VersionSets: data.GetVersionSets(),
	}
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, expected, data)

	// replace "0" to versioning data with build id "0" --> failure
	rule0 := mkAssignmentRule("0", nil)
	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	_, err = replaceAssignmentRule(rule0, data, nextClock, 0, false)
	assert.Error(t, err)

	// replace "3" to versioning data with build id "0" --> success
	rule3 := mkAssignmentRule("3", nil)
	updatedData, err := replaceAssignmentRule(rule3, data, nextClock, 0, false)
	assert.NoError(t, err)
	expReplacedRule := expected.GetAssignmentRules()[0]
	expReplacedRule.DeleteTimestamp = nextClock
	expected = &persistencepb.VersioningData{
		AssignmentRules: []*persistencepb.AssignmentRule{
			mkAssignmentRulePersistence(rule3, nextClock, nil),
			expReplacedRule,
		},
		VersionSets: data.GetVersionSets(),
	}
	protoassert.ProtoEqual(t, expected, updatedData)
	assert.Equal(t, getListResp(t, updatedData).GetAssignmentRules()[0].GetRule().GetTargetBuildId(), updatedData.GetAssignmentRules()[0].GetRule().GetTargetBuildId())
}

func TestReplaceAssignmentRuleTerminalBuildID(t *testing.T) {
	// setup
	t.Parallel()
	maxRules := 3
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)
	data, err := insertRedirectRule(mkRedirectRule("0", "1", nil), data, clock, maxRules)
	assert.NoError(t, err)
	data, err = insertAssignmentRule(mkAssignmentRule("1", nil), data, clock, 0, maxRules)
	assert.NoError(t, err)

	// insert assignment rule with target 1
	_, err = replaceAssignmentRule(mkAssignmentRule("1", mkNewAssignmentPercentageRamp(10)), data, clock, 0, false)
	assert.Error(t, err)
}

// Test replacing assignment rule and hitting / not hitting the unfiltered error, and forcing past it
func TestReplaceAssignmentRuleTestRequireUnfiltered(t *testing.T) {
	t.Parallel()
	maxRules := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	data := mkInitialData(0, clock)
	assert.False(t, containsUnfiltered(data.GetAssignmentRules()))
	var err error

	// test adding a filtered rule and then replacing it (should work)
	clock1 := hlc.Next(clock, timesource)
	data, err = insertAssignmentRule(mkAssignmentRule("1", mkNewAssignmentPercentageRamp(10)), data, clock1, 0, maxRules)
	assert.NoError(t, err)
	data, err = replaceAssignmentRule(mkAssignmentRule("2", mkNewAssignmentPercentageRamp(20)), data, clock1, 0, false)
	assert.NoError(t, err)
	assert.Equal(t, "2", getListResp(t, data).GetAssignmentRules()[0].GetRule().GetTargetBuildId())
	assert.Equal(t, float32(20), getListResp(t, data).GetAssignmentRules()[0].GetRule().GetPercentageRamp().GetRampPercentage())

	// test adding an unfiltered rule and then replacing it (should fail)
	data, err = insertAssignmentRule(mkAssignmentRule("3", nil), data, clock1, 0, maxRules)
	assert.NoError(t, err)
	data, err = replaceAssignmentRule(mkAssignmentRule("4", mkNewAssignmentPercentageRamp(20)), data, clock1, 0, false)
	assert.Error(t, err)

	// test the same as above but with force (should work)
	data, err = replaceAssignmentRule(mkAssignmentRule("4", mkNewAssignmentPercentageRamp(20)), data, clock1, 0, true)
	assert.NoError(t, err)
	assert.Equal(t, "4", getListResp(t, data).GetAssignmentRules()[0].GetRule().GetTargetBuildId())
	assert.Equal(t, float32(20), getListResp(t, data).GetAssignmentRules()[0].GetRule().GetPercentageRamp().GetRampPercentage())
}

func TestDeleteAssignmentRuleVariousIdx(t *testing.T) {
	t.Parallel()
	maxRules := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewEventTimeSource().Update(time.Now())
	initialData := mkInitialData(0, clock)
	assert.False(t, containsUnfiltered(initialData.GetAssignmentRules()))

	// insert 3x to get three rules in there
	timesource.Advance(time.Minute)
	clock = hlc.Next(clock, timesource)
	data, err := insertAssignmentRule(mkAssignmentRule("1", nil), initialData, clock, 0, maxRules)
	assert.NoError(t, err)
	timesource.Advance(time.Minute)
	clock = hlc.Next(clock, timesource)
	data, err = insertAssignmentRule(mkAssignmentRule("1", nil), data, clock, 0, maxRules)
	assert.NoError(t, err)
	timesource.Advance(time.Minute)
	clock = hlc.Next(clock, timesource)
	data, err = insertAssignmentRule(mkAssignmentRule("1", nil), data, clock, 0, maxRules)
	assert.NoError(t, err)

	// test deleting at a given index, then listing, we should see the difference
	deleter := func(idx int32, expectSuccess bool) {
		clock = hlc.Next(clock, timesource)
		if !expectSuccess {
			data, err = deleteAssignmentRule(data, clock, idx, false)
			assert.Error(t, err)
			return
		}
		rule := getListResp(t, data).GetAssignmentRules()[idx]
		actualIdx := given2ActualIdx(idx, data.GetAssignmentRules())
		data, err = deleteAssignmentRule(data, clock, idx, false)
		assert.NoError(t, err)
		assert.NotContains(t, getListResp(t, data).GetAssignmentRules(), rule)
		assert.Equal(t, clock, data.GetAssignmentRules()[actualIdx].GetDeleteTimestamp())
	}
	deleter(2, true)
	deleter(0, true)

	// test deleting with an out-of-bounds index --> should fail
	deleter(99, false)
	deleter(-1, false)
}

func TestDeleteAssignmentRuleTestRequireUnfiltered(t *testing.T) {
	t.Parallel()
	maxRules := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	data := mkInitialData(0, clock)
	assert.False(t, containsUnfiltered(data.GetAssignmentRules()))
	var err error

	// test adding a filtered rule and then deleting it (should work)
	clock1 := hlc.Next(clock, timesource)
	data, err = insertAssignmentRule(mkAssignmentRule("1", mkNewAssignmentPercentageRamp(10)), data, clock1, 0, maxRules)
	assert.NoError(t, err)
	data, err = deleteAssignmentRule(data, clock1, 0, false)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(getListResp(t, data).GetAssignmentRules()))

	// test adding an unfiltered rule and then deleting it (should fail)
	data, err = insertAssignmentRule(mkAssignmentRule("3", nil), data, clock1, 0, maxRules)
	assert.NoError(t, err)
	_, err = deleteAssignmentRule(data, clock1, 0, false)
	assert.Error(t, err)

	// test the same as above but with force (should work)
	data, err = deleteAssignmentRule(data, clock1, 0, true)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(getListResp(t, data).GetAssignmentRules()))
}

// Test inserting, deleting, and replacing rules and listing in between.
func TestListAssignmentRules(t *testing.T) {
	t.Parallel()
	maxRules := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	// make a version set with build id 0
	initialData := mkInitialData(1, clock)

	// insert 3x
	rule1 := mkAssignmentRule("1", nil)
	clock1 := hlc.Next(clock, timesource)
	data, err := insertAssignmentRule(rule1, initialData, clock1, 0, maxRules)
	assert.NoError(t, err)
	rule2 := mkAssignmentRule("2", nil)
	data, err = insertAssignmentRule(rule2, data, clock1, 1, maxRules)
	assert.NoError(t, err)
	rule3 := mkAssignmentRule("3", nil)
	data, err = insertAssignmentRule(rule3, data, clock1, 2, maxRules)
	assert.NoError(t, err)

	// expect they're all in there, no ordering guarantee
	rules := getListResp(t, data).GetAssignmentRules()
	protoassert.ProtoEqual(t, rule1, rules[0].GetRule())
	protoassert.ProtoEqual(t, rule2, rules[1].GetRule())
	protoassert.ProtoEqual(t, rule3, rules[2].GetRule())

	// replace a rule and expect that list shows the replacement
	clock2 := hlc.Next(clock1, timesource)
	rule10 := mkAssignmentRule("10", nil)
	data, err = replaceAssignmentRule(rule10, data, clock2, 0, false)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, rule10, getListResp(t, data).GetAssignmentRules()[0].GetRule())

	// delete a rule and expect that list shows the deletion
	clock3 := hlc.Next(clock2, timesource)
	data, err = deleteAssignmentRule(data, clock3, 0, false)
	assert.NoError(t, err)
	rules2 := make([]*taskqueuepb.BuildIdAssignmentRule, 0)
	for _, r := range getListResp(t, data).GetAssignmentRules() {
		rules2 = append(rules2, r.GetRule())
	}
	assert.NotContains(t, rules2, rule10)
}

// Test requirement that number of rules does not exceed max rules.
func TestInsertRedirectRuleMaxRules(t *testing.T) {
	t.Parallel()
	maxRules := 3
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	initialData := mkInitialData(0, clock)

	// insert to empty versioning data --> success
	rule1 := mkRedirectRule("0", "1", nil)
	clock1 := hlc.Next(clock, timesource)
	updatedData, err := insertRedirectRule(rule1, initialData, clock1, maxRules)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, mkInitialData(0, clock), initialData)
	expected := &persistencepb.VersioningData{
		RedirectRules: []*persistencepb.RedirectRule{
			mkRedirectRulePersistence(rule1, clock1, nil),
		},
	}
	protoassert.ProtoEqual(t, expected, updatedData)

	// insert again --> success
	rule2 := mkRedirectRule("2", "3", nil)
	clock2 := hlc.Next(clock1, timesource)
	updatedData, err = insertRedirectRule(rule2, updatedData, clock2, maxRules)
	assert.NoError(t, err)
	expected = &persistencepb.VersioningData{
		RedirectRules: slices.Insert(expected.GetRedirectRules(), 0, mkRedirectRulePersistence(rule2, clock2, nil)),
	}
	protoassert.ProtoEqual(t, expected, updatedData)

	// insert twice more --> failure due to max rules
	rule3 := mkRedirectRule("4", "5", nil)
	clock3 := hlc.Next(clock2, timesource)
	updatedData, err = insertRedirectRule(rule3, updatedData, clock3, maxRules)
	assert.NoError(t, err)
	expected = &persistencepb.VersioningData{
		RedirectRules: slices.Insert(expected.GetRedirectRules(), 0, mkRedirectRulePersistence(rule3, clock3, nil)),
	}
	testList(t, expected, updatedData)
	rule4 := mkRedirectRule("6", "7", nil)
	clock4 := hlc.Next(clock2, timesource)
	updatedData, err = insertRedirectRule(rule4, updatedData, clock4, maxRules)
	assert.Error(t, err)
}

// Test requirement that target id and source id are not in a version set (success and failure)
func TestInsertRedirectRuleInVersionSet(t *testing.T) {
	t.Parallel()
	maxRules := 3
	clock := hlc.Zero(1)
	// make version set with build id "0" in it
	initialData := mkInitialData(1, clock)

	// insert with source build id "0" --> failure
	_, err := insertRedirectRule(mkRedirectRule("0", "1", nil), initialData, clock, maxRules)
	assert.Error(t, err)
	protoassert.ProtoEqual(t, mkInitialData(1, clock), initialData)
	// insert with target build id "0" --> failure
	_, err = insertRedirectRule(mkRedirectRule("1", "0", nil), initialData, clock, maxRules)
	assert.Error(t, err)
	protoassert.ProtoEqual(t, mkInitialData(1, clock), initialData)

	// insert "1" --> success
	rule1 := mkRedirectRule("1", "2", nil)
	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	updatedData, err := insertRedirectRule(rule1, initialData, nextClock, maxRules)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, mkInitialData(1, clock), initialData)
	expected := &persistencepb.VersioningData{
		VersionSets: []*persistencepb.CompatibleVersionSet{
			mkSingleBuildIdSet("0", clock),
		},
		RedirectRules: []*persistencepb.RedirectRule{
			mkRedirectRulePersistence(rule1, nextClock, nil),
		},
	}
	protoassert.ProtoEqual(t, expected, updatedData)
}

func TestInsertRedirectRuleTerminalBuildID(t *testing.T) {
	// setup
	t.Parallel()
	maxRules := 3
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)
	data, err := insertAssignmentRule(mkAssignmentRule("1", mkNewAssignmentPercentageRamp(10)), data, clock, 0, maxRules)
	assert.NoError(t, err)

	// insert redirect rule with target 1
	_, err = insertRedirectRule(mkRedirectRule("0", "1", nil), data, clock, maxRules)
	assert.Error(t, err)
}

// Test inserting a rule with a source that already exists.
func TestInsertRedirectRuleAlreadyExists(t *testing.T) {
	t.Parallel()
	maxRules := 3
	clock := hlc.Zero(1)
	initialData := mkInitialData(0, clock)

	// insert with source build id "0"
	data, err := insertRedirectRule(mkRedirectRule("0", "1", nil), initialData, clock, maxRules)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, mkInitialData(0, clock), initialData)
	// insert with source build id "0" --> failure
	_, err = insertRedirectRule(mkRedirectRule("0", "6", nil), data, clock, maxRules)
	assert.Error(t, err)
}

// Test inserting redirect rules and creating a cycle
func TestInsertRedirectRuleCreateCycle(t *testing.T) {
	t.Parallel()
	maxRules := 3
	clock := hlc.Zero(1)
	initialData := mkInitialData(0, clock)

	// insert with source -> target == "0" -> "0" --> failure
	_, err := insertRedirectRule(mkRedirectRule("0", "0", nil), initialData, clock, maxRules)
	assert.Error(t, err)

	// insert with source -> target == "0" -> "1" --> success
	data, err := insertRedirectRule(mkRedirectRule("0", "1", nil), initialData, clock, maxRules)
	assert.NoError(t, err)

	// insert with source build id "1" -> "0" --> failure
	_, err = insertRedirectRule(mkRedirectRule("1", "0", nil), data, clock, maxRules)
	assert.Error(t, err)
}

// test replacing with a given index, then listing, it should be at the desired index
func replaceTest(t *testing.T,
	source, target string,
	data *persistencepb.VersioningData,
	clk *hlc.Clock,
	expectSuccess bool) *persistencepb.VersioningData {
	prevRule := getActiveRedirectRuleBySrc(source, data)
	rule := mkRedirectRule(source, target, nil)
	tmp, err := replaceRedirectRule(rule, data, clk)
	if !expectSuccess {
		assert.Error(t, err)
		return nil
	}
	data = tmp
	assert.NoError(t, err)
	newActive := getActiveRedirectRuleBySrc(source, data)
	protoassert.ProtoEqual(t, newActive.GetRule(), rule)
	deleted := getDeletedRedirectRuleBySrc(source, data)
	assert.Equal(t, deleted[0].GetRule().GetSourceBuildId(), prevRule.GetRule().GetSourceBuildId())
	assert.Equal(t, deleted[0].GetRule().GetTargetBuildId(), prevRule.GetRule().GetTargetBuildId())
	for _, dr := range deleted {
		assert.GreaterOrEqual(t, newActive.GetCreateTimestamp().GetWallClock(), dr.GetDeleteTimestamp().GetWallClock())
	}
	return data
}

func getActiveRedirectRuleBySrc(src string, data *persistencepb.VersioningData) *persistencepb.RedirectRule {
	for _, r := range data.GetRedirectRules() {
		if r.GetDeleteTimestamp() == nil && r.GetRule().GetSourceBuildId() == src {
			return r
		}
	}
	return nil
}

func getDeletedRedirectRuleBySrc(src string, data *persistencepb.VersioningData) []*persistencepb.RedirectRule {
	ret := make([]*persistencepb.RedirectRule, 0)
	for _, r := range data.GetRedirectRules() {
		if r.GetDeleteTimestamp() != nil && r.GetRule().GetSourceBuildId() == src {
			ret = append(ret, r)
		}
	}
	return ret
}

// Test replacing redirect rules. Confirm existence of new rule and deletion of old rule.
func TestReplaceRedirectRuleVariousIdx(t *testing.T) {
	t.Parallel()
	maxRules := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	initialData := mkInitialData(0, clock)

	// insert 3x to get three rules in there
	rule1 := mkRedirectRule("1", "0", nil)
	clock1 := hlc.Next(clock, timesource)
	data, err := insertRedirectRule(rule1, initialData, clock1, maxRules)
	assert.NoError(t, err)
	rule2 := mkRedirectRule("2", "0", nil)
	data, err = insertRedirectRule(rule2, data, clock1, maxRules)
	assert.NoError(t, err)
	rule3 := mkRedirectRule("3", "0", nil)
	data, err = insertRedirectRule(rule3, data, clock1, maxRules)
	assert.NoError(t, err)

	clock2 := hlc.Next(clock1, timesource)
	data = replaceTest(t, "1", "100", data, clock2, true)
	data = replaceTest(t, "2", "100", data, clock2, true)
	data = replaceTest(t, "3", "100", data, clock2, true)
}

// Test requirement that target id and source id are not in a version set (success and failure)
func TestReplaceRedirectRuleInVersionSet(t *testing.T) {
	t.Parallel()
	maxRules := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	// make a version set with build id 0
	initialData := mkInitialData(1, clock)

	// insert 3x to get three rules in there
	rule1 := mkRedirectRule("1", "10", nil)
	clock1 := hlc.Next(clock, timesource)
	data, err := insertRedirectRule(rule1, initialData, clock1, maxRules)
	assert.NoError(t, err)
	rule2 := mkRedirectRule("2", "10", nil)
	data, err = insertRedirectRule(rule2, data, clock1, maxRules)
	assert.NoError(t, err)
	rule3 := mkRedirectRule("3", "10", nil)
	data, err = insertRedirectRule(rule3, data, clock1, maxRules)
	assert.NoError(t, err)

	clock2 := hlc.Next(clock1, timesource)
	_ = replaceTest(t, "0", "100", data, clock2, false)
	data = replaceTest(t, "1", "100", data, clock2, true)
	_ = replaceTest(t, "1", "0", data, clock2, false)
	data = replaceTest(t, "1", "8", data, clock2, true)
}

func TestReplaceRedirectRuleTerminalBuildID(t *testing.T) {
	// setup
	t.Parallel()
	maxRules := 3
	clock := hlc.Zero(1)
	data := mkInitialData(0, clock)
	data, err := insertAssignmentRule(mkAssignmentRule("1", mkNewAssignmentPercentageRamp(10)), data, clock, 0, maxRules)
	assert.NoError(t, err)
	data, err = insertRedirectRule(mkRedirectRule("0", "2", nil), data, clock, maxRules)
	assert.NoError(t, err)

	// insert redirect rule with target 1
	_, err = replaceRedirectRule(mkRedirectRule("0", "1", nil), data, clock)
	assert.Error(t, err)
}

// Test replacing redirect rules and creating a cycle
func TestReplaceRedirectRuleCreateCycle(t *testing.T) {
	t.Parallel()
	maxRules := 3
	clock := hlc.Zero(1)
	initialData := mkInitialData(0, clock)

	// insert with source -> target == "0" -> "0" --> failure
	_, err := insertRedirectRule(mkRedirectRule("0", "0", nil), initialData, clock, maxRules)
	assert.Error(t, err)

	// insert with source -> target == "0" -> "1" --> success
	data, err := insertRedirectRule(mkRedirectRule("0", "1", nil), initialData, clock, maxRules)
	assert.NoError(t, err)

	// insert with source build id "1" -> "0" --> failure
	_, err = insertRedirectRule(mkRedirectRule("1", "0", nil), data, clock, maxRules)
	assert.Error(t, err)
}

// Test replacing a redirect rule that doesn't exist
func TestReplaceRedirectRuleNotFound(t *testing.T) {
	t.Parallel()
	maxRules := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	initialData := mkInitialData(0, clock)

	// fails because no rules to replace
	replaceTest(t, "1", "100", initialData, clock, false)

	// insert a rule to replace
	rule1 := mkRedirectRule("1", "0", nil)
	clock1 := hlc.Next(clock, timesource)
	data, err := insertRedirectRule(rule1, initialData, clock1, maxRules)
	assert.NoError(t, err)

	// try again --> fail
	replaceTest(t, "1", "100", data, clock, true)
}

// Test deleting a redirect rule. List and check timestamp to confirm.
func TestDeleteRedirectRuleSuccess(t *testing.T) {
	t.Parallel()
	maxRules := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	// make a version set with build id 0
	initialData := mkInitialData(1, clock)

	// insert 3x to get three rules in there
	rule1 := mkRedirectRule("1", "11", nil)
	clock1 := hlc.Next(clock, timesource)
	data, err := insertRedirectRule(rule1, initialData, clock1, maxRules)
	assert.NoError(t, err)
	rule2 := mkRedirectRule("2", "12", nil)
	data, err = insertRedirectRule(rule2, data, clock1, maxRules)
	assert.NoError(t, err)
	rule3 := mkRedirectRule("3", "13", nil)
	data, err = insertRedirectRule(rule3, data, clock1, maxRules)
	assert.NoError(t, err)

	clock2 := hlc.Next(clock1, timesource)
	data, err = deleteRedirectRule("1", data, clock2)
	assert.NoError(t, err)
	deleted := getDeletedRedirectRuleBySrc("1", data)
	assert.Equal(t, 1, len(deleted))
	assert.Equal(t, clock2.GetWallClock(), deleted[0].GetDeleteTimestamp().GetWallClock())
	assert.Equal(t, "1", deleted[0].GetRule().GetSourceBuildId())
	assert.Equal(t, "11", deleted[0].GetRule().GetTargetBuildId())

	clock3 := hlc.Next(clock2, timesource)
	data, err = deleteRedirectRule("3", data, clock3)
	assert.NoError(t, err)
	deleted = getDeletedRedirectRuleBySrc("3", data)
	assert.Equal(t, 1, len(deleted))
	assert.Equal(t, clock2.GetWallClock(), deleted[0].GetDeleteTimestamp().GetWallClock())
	assert.Equal(t, "3", deleted[0].GetRule().GetSourceBuildId())
	assert.Equal(t, "13", deleted[0].GetRule().GetTargetBuildId())

}

// Test deleting a redirect rule that doesn't exist
func TestDeleteRedirectRuleNotFound(t *testing.T) {
	t.Parallel()
	maxRules := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	initialData := mkInitialData(0, clock)

	// fails because no rules to delete
	_, err := deleteRedirectRule("1", initialData, clock)
	assert.Error(t, err)

	// insert a rule to replace
	rule1 := mkRedirectRule("1", "0", nil)
	clock1 := hlc.Next(clock, timesource)
	data, err := insertRedirectRule(rule1, initialData, clock1, maxRules)
	assert.NoError(t, err)

	// try again --> success
	_, err = deleteRedirectRule("1", data, clock1)
	assert.NoError(t, err)
}

// Test inserting, deleting, and replacing rules and listing in between.
func TestListRedirectRules(t *testing.T) {
	t.Parallel()
	maxRules := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	// make a version set with build id 0
	initialData := mkInitialData(1, clock)

	// insert 3x to get three rules in there
	rule1 := mkRedirectRule("1", "10", nil)
	clock1 := hlc.Next(clock, timesource)
	data, err := insertRedirectRule(rule1, initialData, clock1, maxRules)
	assert.NoError(t, err)
	rule2 := mkRedirectRule("2", "10", nil)
	data, err = insertRedirectRule(rule2, data, clock1, maxRules)
	assert.NoError(t, err)
	rule3 := mkRedirectRule("3", "10", nil)
	data, err = insertRedirectRule(rule3, data, clock1, maxRules)
	assert.NoError(t, err)

	// expect they're all in there, no ordering guarantee
	rules := make([]*taskqueuepb.CompatibleBuildIdRedirectRule, 0)
	for _, r := range getListResp(t, data).GetCompatibleRedirectRules() {
		rules = append(rules, r.GetRule())
	}
	assert.Contains(t, rules, rule1)
	assert.Contains(t, rules, rule2)
	assert.Contains(t, rules, rule3)

	// replace a rule
	clock2 := hlc.Next(clock1, timesource)
	rule1100 := mkRedirectRule("1", "100", nil)
	data, err = replaceRedirectRule(rule1100, data, clock2)
	assert.NoError(t, err)

	// expect that list shows the replacement
	rules = make([]*taskqueuepb.CompatibleBuildIdRedirectRule, 0)
	for _, r := range getListResp(t, data).GetCompatibleRedirectRules() {
		rules = append(rules, r.GetRule())
	}
	assert.NotContains(t, rules, rule1)
	assert.Contains(t, rules, rule1100)

	// delete a rule
	clock3 := hlc.Next(clock2, timesource)
	data, err = deleteRedirectRule("2", data, clock3)
	assert.NoError(t, err)

	// expect that list shows the deletion
	rules = make([]*taskqueuepb.CompatibleBuildIdRedirectRule, 0)
	for _, r := range getListResp(t, data).GetCompatibleRedirectRules() {
		rules = append(rules, r.GetRule())
	}
	assert.NotContains(t, rules, rule2)
}

func TestCleanupRedirectRuleTombstones(t *testing.T) {
	t.Parallel()
	maxRules := 10
	clock := hlc.Zero(1)
	initialData := mkInitialData(0, clock)

	// start time one hour ago
	timesource := commonclock.NewEventTimeSource().Update(time.Now().Add(-1 * time.Hour))

	// insert 3x to get three rules in there
	rule1 := mkRedirectRule("1", "10", nil)
	clock1 := hlc.Next(clock, timesource)
	data, err := insertRedirectRule(rule1, initialData, clock1, maxRules)
	assert.NoError(t, err)
	rule2 := mkRedirectRule("2", "10", nil)
	data, err = insertRedirectRule(rule2, data, clock1, maxRules)
	assert.NoError(t, err)
	rule3 := mkRedirectRule("3", "10", nil)
	data, err = insertRedirectRule(rule3, data, clock1, maxRules)
	assert.NoError(t, err)

	// delete "now," ~1 hour ago
	clock4 := hlc.Next(clock, timesource)
	data, err = deleteRedirectRule("1", data, clock4)
	assert.NoError(t, err)
	// delete 35 min later, ~25 min ago
	timesource.Advance(35 * time.Minute)
	clock5 := hlc.Next(clock, timesource)
	data, err = deleteRedirectRule("2", data, clock5)
	assert.NoError(t, err)
	// delete 25 min later, ~now (real time.Now())
	timesource.Advance(25 * time.Minute)
	clock6 := hlc.Next(clock, timesource)
	data, err = deleteRedirectRule("3", data, clock6)
	assert.NoError(t, err)

	// Remove data that was deleted > 30 min ago --> remove first rule
	data = CleanupRuleTombstones(data, 30*time.Minute)
	sources := make([]string, 0)
	for _, r := range data.GetRedirectRules() {
		sources = append(sources, r.GetRule().GetSourceBuildId())
	}
	assert.NotContains(t, sources, "1")
	assert.Contains(t, sources, "2")
	assert.Contains(t, sources, "3")

	// Remove data that was deleted > 5 min ago --> remove second rule
	data = CleanupRuleTombstones(data, 5*time.Minute)
	sources = make([]string, 0)
	for _, r := range data.GetRedirectRules() {
		sources = append(sources, r.GetRule().GetSourceBuildId())
	}
	assert.NotContains(t, sources, "1")
	assert.NotContains(t, sources, "2")
	assert.Contains(t, sources, "3")
}

// eg.
// 1 ------> 2
// ^        |
// |        v
// 5 <------ 3 ------> 4
func TestIsCycle(t *testing.T) {
	rules := []*persistencepb.RedirectRule{
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

	rules = append(rules, &persistencepb.RedirectRule{
		Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{SourceBuildId: "4", TargetBuildId: "2"},
	})
	if !isCyclic(rules) {
		t.Fail()
	}
}
