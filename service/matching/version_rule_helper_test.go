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

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func TestFindAssignmentBuildId_NoRules(t *testing.T) {
	assert.Equal(t, "", FindAssignmentBuildId(nil))
}

func TestFindAssignmentBuildId_OneFullRule(t *testing.T) {
	buildId := "bld"
	assert.Equal(t, buildId, FindAssignmentBuildId([]*persistencespb.AssignmentRule{createFullAssignmentRule(buildId)}))
}

func TestFindAssignmentBuildId_TwoFullRules(t *testing.T) {
	buildId := "bld"
	buildId2 := "bld2"
	assert.Equal(t, buildId, FindAssignmentBuildId([]*persistencespb.AssignmentRule{createFullAssignmentRule(buildId), createFullAssignmentRule(buildId2)}))
}

func createFullAssignmentRule(buildId string) *persistencespb.AssignmentRule {
	return &persistencespb.AssignmentRule{Rule: &taskqueuepb.BuildIdAssignmentRule{TargetBuildId: buildId}}
}

func TestIsTimestampedRedirectRuleSource(t *testing.T) {
	t.Parallel()
}

/*
e.g.
Redirect Rules:
1 ------> 2
^         |
|         v
5 <------ 3 ------> 4
*/
func TestIsCycle(t *testing.T) {
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

/*
Redirect Rules:
10
^
|
1 <------ 2
^
|
5 <------ 3 <------ 4
*/
func TestGetUpstreamBuildIds(t *testing.T) {
	t.Parallel()
	redirectRules := []*taskqueuepb.TimestampedCompatibleBuildIdRedirectRule{
		{Rule: mkRedirectRule("1", "10")},
		{Rule: mkRedirectRule("2", "1")},
		{Rule: mkRedirectRule("3", "5")},
		{Rule: mkRedirectRule("4", "3")},
		{Rule: mkRedirectRule("5", "1")},
	}

	expectedUpstreamBuildIds := []string{"2", "5", "3", "4"}
	upstreamBuildIds := getUpstreamBuildIds("1", redirectRules)
	fmt.Printf("%+v\n", upstreamBuildIds)

	for _, bid := range expectedUpstreamBuildIds {
		assert.Contains(t, upstreamBuildIds, bid)
	}
	assert.Equal(t, len(expectedUpstreamBuildIds), len(upstreamBuildIds))
}

func TestExistsBackloggedActivityOrWFAssignedTo(t *testing.T) {
	t.Parallel()
	// todo after we have backlog info
}

/*
Assignment Rules:
[ (3, 50%), (2, nil) (1, nil) ]
*/
func TestIsReachableAssignmentRuleTarget(t *testing.T) {
	t.Parallel()
	assignmentRules := []*taskqueuepb.TimestampedBuildIdAssignmentRule{
		{Rule: mkAssignmentRule("3", mkNewAssignmentPercentageRamp(50))},
		{Rule: mkAssignmentRule("2", nil)},
		{Rule: mkAssignmentRule("1", nil)},
	}

	assert.True(t, isReachableAssignmentRuleTarget("3", assignmentRules))
	assert.True(t, isReachableAssignmentRuleTarget("2", assignmentRules))
	assert.False(t, isReachableAssignmentRuleTarget("1", assignmentRules))
	assert.False(t, isReachableAssignmentRuleTarget("0", assignmentRules))
}

func TestExistsWFAssignedToAny(t *testing.T) {
	t.Parallel()
}

func TestMakeBuildIdQuery(t *testing.T) {
	t.Parallel()
	tq := "test-query-tq"

	buildIdsOfInterest := []string{"0", "1", "2", ""}
	queryOpen := makeBuildIdQuery(buildIdsOfInterest, tq, true)
	expectedQueryOpen := "TaskQueue = 'test-query-tq' AND (BuildIds IS NULL OR BuildIds IN ('assigned:0','assigned:1','assigned:2',unversioned)) AND ExecutionStatus = \"Running\""
	assert.Equal(t, expectedQueryOpen, queryOpen)
	queryClosed := makeBuildIdQuery(buildIdsOfInterest, tq, false)
	expectedQueryClosed := "TaskQueue = 'test-query-tq' AND (BuildIds IS NULL OR BuildIds IN ('versioned:0','versioned:1','versioned:2',unversioned)) AND ExecutionStatus != \"Running\""
	assert.Equal(t, expectedQueryClosed, queryClosed)

	buildIdsOfInterest = []string{"0", "1", "2"}
	queryOpen = makeBuildIdQuery(buildIdsOfInterest, tq, true)
	expectedQueryOpen = "TaskQueue = 'test-query-tq' AND BuildIds IN ('assigned:0','assigned:1','assigned:2') AND ExecutionStatus = \"Running\""
	assert.Equal(t, expectedQueryOpen, queryOpen)
	queryClosed = makeBuildIdQuery(buildIdsOfInterest, tq, false)
	expectedQueryClosed = "TaskQueue = 'test-query-tq' AND BuildIds IN ('versioned:0','versioned:1','versioned:2') AND ExecutionStatus != \"Running\""
	assert.Equal(t, expectedQueryClosed, queryClosed)
}
