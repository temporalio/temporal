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
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	commonclock "go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/testing/protoassert"
)

func mkNewInsertAssignmentReq(target string, ruleIdx int32, ramp *taskqueuepb.BuildIdAssignmentRule_PercentageRamp) *workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule {
	ret := &workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule{
		RuleIndex: ruleIdx,
		Rule: &taskqueuepb.BuildIdAssignmentRule{
			TargetBuildId: target,
		},
	}
	// if ramp == nil and is set above, there is a nil-pointer error in GetPercentageRamp()
	// because casting Rule to (*BuildIdAssignmentRule_WorkerRatioRamp) succeeds
	if ramp != nil {
		ret.Rule.Ramp = ramp
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

func mkAssignmentRule(target string, ramp *taskqueuepb.BuildIdAssignmentRule_PercentageRamp, createTs, deleteTs *hlc.Clock) *persistencepb.AssignmentRule {
	return &persistencepb.AssignmentRule{
		Rule: &taskqueuepb.BuildIdAssignmentRule{
			TargetBuildId: target,
			Ramp:          ramp,
		},
		CreateTimestamp: createTs,
		DeleteTimestamp: deleteTs,
	}
}

func printrules(rules []*persistencepb.AssignmentRule) {
	for i, r := range rules {
		fmt.Printf("[%d] (%v, %v, %v)\n", i, r.Rule.TargetBuildId, r.CreateTimestamp.String(), r.DeleteTimestamp.String())
	}
	fmt.Print("\n")
}

func TestInsertAssignmentRuleBasic(t *testing.T) {
	t.Parallel()
	maxARs := 3
	clock := hlc.Zero(1)
	initialData := mkInitialData(0, clock)
	hadUnfiltered := containsUnfiltered(initialData.GetAssignmentRules())
	assert.False(t, hadUnfiltered)

	// insert to empty versioning data --> success
	req := mkNewInsertAssignmentReq("1", 0, nil)
	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	updatedData, err := InsertAssignmentRule(nextClock, initialData, req, maxARs, hadUnfiltered)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, mkInitialData(0, clock), initialData)
	expected := &persistencepb.VersioningData{
		AssignmentRules: []*persistencepb.AssignmentRule{
			mkAssignmentRule("1", nil, nextClock, nil),
		},
	}
	protoassert.ProtoEqual(t, expected, updatedData)

	// insert again --> success
	hadUnfiltered = containsUnfiltered(updatedData.GetAssignmentRules())
	assert.True(t, hadUnfiltered)
	req = mkNewInsertAssignmentReq("2", 0, nil)
	nextClock = hlc.Next(clock, commonclock.NewRealTimeSource())
	updatedData, err = InsertAssignmentRule(nextClock, updatedData, req, maxARs, hadUnfiltered)
	assert.NoError(t, err)
	expected = &persistencepb.VersioningData{
		AssignmentRules: slices.Insert(expected.GetAssignmentRules(), 0, mkAssignmentRule("2", nil, nextClock, nil)),
	}
	protoassert.ProtoEqual(t, expected, updatedData)

	// insert twice more --> failure due to max rules
	req = mkNewInsertAssignmentReq("3", 0, nil)
	nextClock = hlc.Next(clock, commonclock.NewRealTimeSource())
	updatedData, err = InsertAssignmentRule(nextClock, updatedData, req, maxARs, hadUnfiltered)
	req = mkNewInsertAssignmentReq("4", 0, nil)
	nextClock = hlc.Next(clock, commonclock.NewRealTimeSource())
	updatedData, err = InsertAssignmentRule(nextClock, updatedData, req, maxARs, hadUnfiltered)
	assert.Error(t, err)
}

func TestInsertAssignmentRuleExistingVersionSet(t *testing.T) {
	t.Parallel()
	maxARs := 3
	clock := hlc.Zero(1)
	initialData := mkInitialData(1, clock)
	hadUnfiltered := containsUnfiltered(initialData.GetAssignmentRules())
	assert.False(t, hadUnfiltered)

	// insert "0" to versioning data with build id "0" --> failure
	req := mkNewInsertAssignmentReq("0", 0, nil)
	nextClock := hlc.Next(clock, commonclock.NewRealTimeSource())
	_, err := InsertAssignmentRule(nextClock, initialData, req, maxARs, hadUnfiltered)
	assert.Error(t, err)
	protoassert.ProtoEqual(t, mkInitialData(1, clock), initialData)

	// insert "1" --> success
	req = mkNewInsertAssignmentReq("1", 0, nil)
	updatedData, err := InsertAssignmentRule(nextClock, initialData, req, maxARs, hadUnfiltered)
	assert.NoError(t, err)
	protoassert.ProtoEqual(t, mkInitialData(1, clock), initialData)
	expected := &persistencepb.VersioningData{
		VersionSets: []*persistencepb.CompatibleVersionSet{
			mkSingleBuildIdSet("0", clock),
		},
		AssignmentRules: []*persistencepb.AssignmentRule{
			mkAssignmentRule("1", nil, nextClock, nil),
		},
	}
	protoassert.ProtoEqual(t, expected, updatedData)
}

// eg.
// 1 ------> 2
// ^        |
// |        |
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
