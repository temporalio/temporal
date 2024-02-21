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

func replaceAssignmentRule(rule *taskqueuepb.BuildIdAssignmentRule,
	data *persistencepb.VersioningData,
	clock *hlc.Clock,
	idx int32,
	force bool,
) (*persistencepb.VersioningData, error) {
	return ReplaceAssignmentRule(clock, data, mkNewReplaceAssignmentReq(rule, idx, force))
}

func deleteAssignmentRule(data *persistencepb.VersioningData,
	clock *hlc.Clock,
	idx int32,
	force bool,
) (*persistencepb.VersioningData, error) {
	return DeleteAssignmentRule(clock, data, mkNewDeleteAssignmentReq(idx, force))
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
	maxARs := 3
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	initialData := mkInitialData(0, clock)
	assert.False(t, containsUnfiltered(initialData.GetAssignmentRules()))

	// insert to empty versioning data --> success
	rule1 := mkAssignmentRule("1", nil)
	clock1 := hlc.Next(clock, timesource)
	updatedData, err := insertAssignmentRule(rule1, initialData, clock1, 0, maxARs)
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
	updatedData, err = insertAssignmentRule(rule2, updatedData, clock2, 0, maxARs)
	assert.NoError(t, err)
	expected = &persistencepb.VersioningData{
		AssignmentRules: slices.Insert(expected.GetAssignmentRules(), 0, mkAssignmentRulePersistence(rule2, clock2, nil)),
	}
	protoassert.ProtoEqual(t, expected, updatedData)

	// insert twice more --> failure due to max rules
	rule3 := mkAssignmentRule("3", nil)
	clock3 := hlc.Next(clock2, timesource)
	updatedData, err = insertAssignmentRule(rule3, updatedData, clock3, 0, maxARs)
	assert.NoError(t, err)
	expected = &persistencepb.VersioningData{
		AssignmentRules: slices.Insert(expected.GetAssignmentRules(), 0, mkAssignmentRulePersistence(rule3, clock3, nil)),
	}
	testList(t, expected, updatedData)
	rule4 := mkAssignmentRule("4", nil)
	clock4 := hlc.Next(clock2, timesource)
	updatedData, err = insertAssignmentRule(rule4, updatedData, clock4, 0, maxARs)
	assert.Error(t, err)
}

// Test requirement that target id isn't in a version set (success and failure)
func TestInsertAssignmentRuleInVersionSet(t *testing.T) {
	t.Parallel()
	maxARs := 3
	clock := hlc.Zero(1)
	initialData := mkInitialData(1, clock)
	assert.False(t, containsUnfiltered(initialData.GetAssignmentRules()))

	// insert "0" to versioning data with build id "0" --> failure
	rule0 := mkAssignmentRule("0", nil)
	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	_, err := insertAssignmentRule(rule0, initialData, nextClock, 0, maxARs)
	assert.Error(t, err)
	protoassert.ProtoEqual(t, mkInitialData(1, clock), initialData)

	// insert "1" --> success
	rule1 := mkAssignmentRule("1", nil)
	updatedData, err := insertAssignmentRule(rule1, initialData, nextClock, 0, maxARs)
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

// Test inserting assignment rules with non-zero indexes. List to confirm.
func TestInsertAssignmentRuleNonzeroIdx(t *testing.T) {
	t.Parallel()
	maxARs := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	initialData := mkInitialData(0, clock)
	assert.False(t, containsUnfiltered(initialData.GetAssignmentRules()))

	// insert 3x to get three rules in there
	rule1 := mkAssignmentRule("1", nil)
	clock1 := hlc.Next(clock, timesource)
	data, err := insertAssignmentRule(rule1, initialData, clock1, 0, maxARs)
	assert.NoError(t, err)
	data, err = insertAssignmentRule(rule1, data, clock1, 0, maxARs)
	assert.NoError(t, err)
	data, err = insertAssignmentRule(rule1, data, clock1, 0, maxARs)
	assert.NoError(t, err)

	// test inserting with a given index, then listing, it should be at the desired index
	rule2 := mkAssignmentRule("2", nil)
	clock2 := hlc.Next(clock1, timesource)
	data, err = insertAssignmentRule(rule2, data, clock2, 2, maxARs)
	assert.NoError(t, err)
	assert.Equal(t, "2", getListResp(t, data).GetAssignmentRules()[2].GetRule().GetTargetBuildId())

	// test inserting with a too-big index, then listing, it should be at the back
	rule3 := mkAssignmentRule("3", nil)
	clock3 := hlc.Next(clock2, timesource)
	data, err = insertAssignmentRule(rule3, data, clock3, 100, maxARs)
	assert.NoError(t, err)
	listedRules := getListResp(t, data).GetAssignmentRules()
	assert.Equal(t, "3", listedRules[len(listedRules)-1].GetRule().GetTargetBuildId())
}

// Test replacing assignment rules at various indices. List to confirm.
func TestReplaceAssignmentRuleVariousIdx(t *testing.T) {
	t.Parallel()
	maxARs := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	initialData := mkInitialData(0, clock)
	assert.False(t, containsUnfiltered(initialData.GetAssignmentRules()))

	// insert 3x to get three rules in there
	rule1 := mkAssignmentRule("1", nil)
	clock1 := hlc.Next(clock, timesource)
	data, err := insertAssignmentRule(rule1, initialData, clock1, 0, maxARs)
	assert.NoError(t, err)
	data, err = insertAssignmentRule(rule1, data, clock1, 0, maxARs)
	assert.NoError(t, err)
	data, err = insertAssignmentRule(rule1, data, clock1, 0, maxARs)
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

// Test replacing assignment rule and hitting / not hitting the unfiltered error, and forcing past it
func TestReplaceAssignmentRuleTestRequireUnfiltered(t *testing.T) {
	t.Parallel()
	maxARs := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	data := mkInitialData(0, clock)
	assert.False(t, containsUnfiltered(data.GetAssignmentRules()))
	var err error

	// test adding a filtered rule and then replacing it (should work)
	clock1 := hlc.Next(clock, timesource)
	data, err = insertAssignmentRule(mkAssignmentRule("1", mkNewAssignmentPercentageRamp(10)), data, clock1, 0, maxARs)
	assert.NoError(t, err)
	data, err = replaceAssignmentRule(mkAssignmentRule("2", mkNewAssignmentPercentageRamp(20)), data, clock1, 0, false)
	assert.NoError(t, err)
	assert.Equal(t, "2", getListResp(t, data).GetAssignmentRules()[0].GetRule().GetTargetBuildId())
	assert.Equal(t, float32(20), getListResp(t, data).GetAssignmentRules()[0].GetRule().GetPercentageRamp().GetRampPercentage())

	// test adding an unfiltered rule and then replacing it (should fail)
	data, err = insertAssignmentRule(mkAssignmentRule("3", nil), data, clock1, 0, maxARs)
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
	maxARs := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	initialData := mkInitialData(0, clock)
	assert.False(t, containsUnfiltered(initialData.GetAssignmentRules()))

	// insert 3x to get three rules in there
	rule1 := mkAssignmentRule("1", nil)
	clock1 := hlc.Next(clock, timesource)
	data, err := insertAssignmentRule(rule1, initialData, clock1, 0, maxARs)
	assert.NoError(t, err)
	data, err = insertAssignmentRule(rule1, data, clock1, 0, maxARs)
	assert.NoError(t, err)
	data, err = insertAssignmentRule(rule1, data, clock1, 0, maxARs)
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
		fmt.Printf("%+v\n", rule) // todo: removing this print causes the "NotContains" check to fail, which makes no sense
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
	maxARs := 10
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	data := mkInitialData(0, clock)
	assert.False(t, containsUnfiltered(data.GetAssignmentRules()))
	var err error

	// test adding a filtered rule and then deleting it (should work)
	clock1 := hlc.Next(clock, timesource)
	data, err = insertAssignmentRule(mkAssignmentRule("1", mkNewAssignmentPercentageRamp(10)), data, clock1, 0, maxARs)
	assert.NoError(t, err)
	data, err = deleteAssignmentRule(data, clock1, 0, false)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(getListResp(t, data).GetAssignmentRules()))

	// test adding an unfiltered rule and then deleting it (should fail)
	data, err = insertAssignmentRule(mkAssignmentRule("3", nil), data, clock1, 0, maxARs)
	assert.NoError(t, err)
	_, err = deleteAssignmentRule(data, clock1, 0, false)
	assert.Error(t, err)

	// test the same as above but with force (should work)
	data, err = deleteAssignmentRule(data, clock1, 0, true)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(getListResp(t, data).GetAssignmentRules()))
}

// Test requirement that number of rules does not exceed max rules, including that deleting a rule fixes the error.
func TestInsertRedirectRuleMaxRules(t *testing.T) {
}

// Test requirement that target id and source id are not in a version set (success and failure)
func TestInsertRedirectRuleInVersionSet(t *testing.T) {
}

// Test inserting a rule with a source that already exists.
func TestInsertRedirectRuleAlreadyExists(t *testing.T) {
}

// Test inserting redirect rules and creating a cycle
func TestInsertRedirectRuleCreateCycle(t *testing.T) {
	// should error
}

// Test replacing redirect rules and creating a cycle
func TestReplaceRedirectRuleSuccess(t *testing.T) {
}

// Test requirement that target id and source id are not in a version set (success and failure)
func TestReplaceRedirectRuleInVersionSet(t *testing.T) {
	// should error
}

// Test replacing redirect rules and creating a cycle
func TestReplaceRedirectRuleCreateCycle(t *testing.T) {
	// should error
}

// Test replacing a redirect rule that doesn't exist
func TestReplaceRedirectRuleNotFound(t *testing.T) {
	// should error
}

// Test deleting a redirect rule that doesn't exist
func TestDeleteRedirectRuleNotFound(t *testing.T) {
	// should error
}

// Test inserting, deleting, and replacing rules and listing in between. Also check raw data.
func TestList(t *testing.T) {
	// should error
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
