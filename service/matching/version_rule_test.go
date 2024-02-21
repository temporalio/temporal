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
	"slices"
	"testing"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	commonclock "go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/testing/protoassert"
)

const (
	maxARs = 3
	maxRRs = 3
)

func mkNewInsertAssignmentReq(rule *taskqueuepb.BuildIdAssignmentRule, ruleIdx int32) *workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule {
	return &workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule{
		RuleIndex: ruleIdx,
		Rule:      rule,
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

func printrules(rules []*persistencepb.AssignmentRule) {
	for i, r := range rules {
		fmt.Printf("[%d] (%v, %v, %v)\n", i, r.Rule.TargetBuildId, r.CreateTimestamp.String(), r.DeleteTimestamp.String())
	}
	fmt.Print("\n")
}

func insertRule(rule *taskqueuepb.BuildIdAssignmentRule,
	data *persistencepb.VersioningData,
	clock *hlc.Clock,
	idx int32,
) (*persistencepb.VersioningData, error) {
	return InsertAssignmentRule(clock, data, mkNewInsertAssignmentReq(rule, idx), maxARs)
}

func testList(t *testing.T, expected, actual *persistencepb.VersioningData) {
	resp, err := ListWorkerVersioningRules(actual, hlc.Zero(1))
	assert.NoError(t, err)
	activeAssignmentRules := getActiveAssignmentRules(expected.GetAssignmentRules())
	activeRedirectRules := getActiveRedirectRules(expected.GetRedirectRules())
	for i, r := range resp.GetResponse().GetAssignmentRules() {
		protoassert.ProtoEqual(t, activeAssignmentRules[i].GetRule(), r.GetRule())
	}
	for i, r := range resp.GetResponse().GetCompatibleRedirectRules() {
		protoassert.ProtoEqual(t, activeRedirectRules[i].GetRule(), r.GetRule())
	}
}

// Test inserting before and after hitting the max rules limit.
func TestInsertAssignmentRuleMaxRules(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	timesource := commonclock.NewRealTimeSource()
	initialData := mkInitialData(0, clock)
	assert.False(t, containsUnfiltered(initialData.GetAssignmentRules()))

	// insert to empty versioning data --> success
	rule1 := mkAssignmentRule("1", nil)
	clock1 := hlc.Next(clock, timesource)
	updatedData, err := insertRule(rule1, initialData, clock1, 0)
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
	updatedData, err = insertRule(rule2, updatedData, clock2, 0)
	assert.NoError(t, err)
	expected = &persistencepb.VersioningData{
		AssignmentRules: slices.Insert(expected.GetAssignmentRules(), 0, mkAssignmentRulePersistence(rule2, clock2, nil)),
	}
	protoassert.ProtoEqual(t, expected, updatedData)

	// insert twice more --> failure due to max rules
	rule3 := mkAssignmentRule("3", nil)
	clock3 := hlc.Next(clock2, timesource)
	updatedData, err = insertRule(rule3, updatedData, clock3, 0)
	assert.NoError(t, err)
	expected = &persistencepb.VersioningData{
		AssignmentRules: slices.Insert(expected.GetAssignmentRules(), 0, mkAssignmentRulePersistence(rule3, clock3, nil)),
	}
	testList(t, expected, updatedData)
	rule4 := mkAssignmentRule("4", nil)
	clock4 := hlc.Next(clock2, timesource)
	updatedData, err = insertRule(rule4, updatedData, clock4, 0)
	assert.Error(t, err)
}

// Test requirement that target id isn't in a version set (success and failure)
func TestInsertAssignmentRuleInVersionSet(t *testing.T) {
	t.Parallel()
	clock := hlc.Zero(1)
	initialData := mkInitialData(1, clock)
	assert.False(t, containsUnfiltered(initialData.GetAssignmentRules()))

	// insert "0" to versioning data with build id "0" --> failure
	rule0 := mkAssignmentRule("0", nil)
	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	_, err := insertRule(rule0, initialData, nextClock, 0)
	assert.Error(t, err)
	protoassert.ProtoEqual(t, mkInitialData(1, clock), initialData)

	// insert "1" --> success
	rule1 := mkAssignmentRule("1", nil)
	updatedData, err := insertRule(rule1, initialData, nextClock, 0)
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
	// test inserting with a given index, then listing, it should be at the desired index
	// test inserting with a too-big index, then listing, it should be at the back
}

// Test replacing assignment rules at various indices. List to confirm.
func TestReplaceAssignmentRuleVariousIdx(t *testing.T) {
	// test adding a filtered rule and then deleting it (should work)
	// test adding an unfiltered rule and then deleting it (should fail)
}

// Test replacing assignment rule and hitting / not hitting the unfiltered error, and forcing past it
func TestReplaceAssignmentRuleTestRequireUnfiltered(t *testing.T) {
	// test adding a filtered rule and then replacing it (should work)
	// test adding an unfiltered rule and then replacing it (should fail)
	// test the same as above but with force (should work)
}

// Test deleting assignment rule at various indices. List to confirm.
func TestDeleteAssignmentRuleVariousIdx(t *testing.T) {
	// test adding a filtered rule and then deleting it (should work)
	// test adding an unfiltered rule and then deleting it (should fail)
}

// Test deleting assignment rule and hitting / not hitting the unfiltered error, and forcing past it
func TestDeleteAssignmentRuleTestRequireUnfiltered(t *testing.T) {
	// test adding a filtered rule and then deleting it (should work)
	// test adding an unfiltered rule and then deleting it (should fail)
	// test same as above but with force (should work)
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
