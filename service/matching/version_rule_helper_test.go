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
	"context"
	"fmt"
	"github.com/golang/mock/gomock"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"slices"
	"strings"
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
	ctx := context.Background()

	tq := "test-exists-tq"
	nsID := "test-namespace-id"
	nsName := "test-namespace"
	buildIdsOfInterest := []string{"0", "1", "2", ""}

	visibilityMgr, mockStore := newMockVisibilityForReachability(t)

	// populate mockStore with an unversioned, non-running workflow
	mockStore["run1"] = mockVisibilityEntry{
		taskQueue:              tq,
		buildIdsList:           nil,
		executionStatusRunning: false,
	}

	exists, err := existsWFAssignedToAny(ctx, visibilityMgr, nsID, nsName, tq, buildIdsOfInterest, true)
	assert.Nil(t, err)
	assert.False(t, exists)

	exists, err = existsWFAssignedToAny(ctx, visibilityMgr, nsID, nsName, tq, buildIdsOfInterest, false)
	assert.Nil(t, err)
	assert.True(t, exists)
}

func TestMakeBuildIdQuery(t *testing.T) {
	t.Parallel()
	tq := "test-query-tq"

	buildIdsOfInterest := []string{"0", "1", "2", ""}
	query := makeBuildIdQuery(buildIdsOfInterest, tq, true)
	expectedQuery := "TaskQueue = 'test-query-tq' AND (BuildIds IS NULL OR BuildIds IN ('assigned:0','assigned:1','assigned:2',unversioned)) AND ExecutionStatus = \"Running\""
	assert.Equal(t, expectedQuery, query)

	query = makeBuildIdQuery(buildIdsOfInterest, tq, false)
	expectedQuery = "TaskQueue = 'test-query-tq' AND (BuildIds IS NULL OR BuildIds IN ('versioned:0','versioned:1','versioned:2',unversioned)) AND ExecutionStatus != \"Running\""
	assert.Equal(t, expectedQuery, query)

	buildIdsOfInterest = []string{"0", "1", "2"}
	query = makeBuildIdQuery(buildIdsOfInterest, tq, true)
	expectedQuery = "TaskQueue = 'test-query-tq' AND BuildIds IN ('assigned:0','assigned:1','assigned:2') AND ExecutionStatus = \"Running\""
	assert.Equal(t, expectedQuery, query)

	query = makeBuildIdQuery(buildIdsOfInterest, tq, false)
	expectedQuery = "TaskQueue = 'test-query-tq' AND BuildIds IN ('versioned:0','versioned:1','versioned:2') AND ExecutionStatus != \"Running\""
	assert.Equal(t, expectedQuery, query)
}

// Helpers for mock Visibility Manager
type mockRunId string

type mockVisibilityStore map[mockRunId]mockVisibilityEntry

type mockVisibilityEntry struct {
	taskQueue              string
	buildIdsList           []string
	executionStatusRunning bool
}

func newMockVisibilityForReachability(t *testing.T) (*manager.MockVisibilityManager, mockVisibilityStore) {
	ctrl := gomock.NewController(t)
	vm := manager.NewMockVisibilityManager(ctrl)
	mockStore := mockVisibilityStore(make(map[mockRunId]mockVisibilityEntry))
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).DoAndReturn(func(
		ctx context.Context,
		request *manager.CountWorkflowExecutionsRequest,
	) (*manager.CountWorkflowExecutionsResponse, error) {
		mockEntry, checkBuildIdNull := parseReachabilityQuery(request.Query)
		count := 0
		for _, e := range mockStore {
			if e.taskQueue == mockEntry.taskQueue {
				if e.executionStatusRunning != mockEntry.executionStatusRunning {
					continue
				}
				if e.buildIdsList == nil {
					if checkBuildIdNull {
						count++
						continue
					}
					continue
				}
				for _, bidSA := range mockEntry.buildIdsList {
					if slices.Contains(e.buildIdsList, bidSA) {
						count++
						break
					}
				}
			}
		}
		return &manager.CountWorkflowExecutionsResponse{Count: int64(count)}, nil
	}).AnyTimes()

	return vm, mockStore
}

func parseReachabilityQuery(query string) (mockVisibilityEntry, bool) {
	ret := mockVisibilityEntry{}
	checkBuildIdNull := false
	for _, c := range strings.Split(query, " AND ") {
		if strings.Contains(c, "TaskQueue") {
			ret.taskQueue = strings.ReplaceAll(strings.Split(c, " = ")[1], "'", "")
		} else if strings.Contains(c, "ExecutionStatus") {
			ret.executionStatusRunning = strings.Contains(c, "ExecutionStatus = \"Running\"")
		} else {
			checkBuildIdNull = strings.Contains(c, "BuildIds IS NULL")
			splits := strings.Split(c, "BuildIds")
			buildIdsIdx := 1
			if len(splits) == 3 {
				buildIdsIdx = 2
			}
			trimFront := strings.ReplaceAll(splits[buildIdsIdx], " IN (", "")
			trimBack := strings.ReplaceAll(trimFront, ")", "")
			ret.buildIdsList = strings.Split(trimBack, ",")
		}
	}
	return ret, checkBuildIdNull
}

func TestParseBuildIdQuery(t *testing.T) {
	t.Parallel()
	tq := "test-parse-tq"

	buildIdsOfInterest := []string{"0", "1", "2", ""}
	mockEntry, checkNull := parseReachabilityQuery(makeBuildIdQuery(buildIdsOfInterest, tq, true))
	expectedMockEntry := mockVisibilityEntry{
		taskQueue:              tq,
		buildIdsList:           []string{"'assigned:0'", "'assigned:1'", "'assigned:2'", "unversioned"},
		executionStatusRunning: true,
	}
	assert.Equal(t, expectedMockEntry, mockEntry)
	assert.True(t, checkNull)

	mockEntry, checkNull = parseReachabilityQuery(makeBuildIdQuery(buildIdsOfInterest, tq, false))
	expectedMockEntry = mockVisibilityEntry{
		taskQueue:              tq,
		buildIdsList:           []string{"'versioned:0'", "'versioned:1'", "'versioned:2'", "unversioned"},
		executionStatusRunning: false,
	}
	assert.Equal(t, expectedMockEntry, mockEntry)
	assert.True(t, checkNull)

	buildIdsOfInterest = []string{"0", "1", "2"}
	mockEntry, checkNull = parseReachabilityQuery(makeBuildIdQuery(buildIdsOfInterest, tq, true))
	expectedMockEntry = mockVisibilityEntry{
		taskQueue:              tq,
		buildIdsList:           []string{"'assigned:0'", "'assigned:1'", "'assigned:2'"},
		executionStatusRunning: true,
	}
	assert.Equal(t, expectedMockEntry, mockEntry)
	assert.False(t, checkNull)

	mockEntry, checkNull = parseReachabilityQuery(makeBuildIdQuery(buildIdsOfInterest, tq, false))
	expectedMockEntry = mockVisibilityEntry{
		taskQueue:              tq,
		buildIdsList:           []string{"'versioned:0'", "'versioned:1'", "'versioned:2'"},
		executionStatusRunning: false,
	}
	assert.Equal(t, expectedMockEntry, mockEntry)
	assert.False(t, checkNull)
}
