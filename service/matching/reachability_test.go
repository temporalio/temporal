package matching

import (
	"context"
	enumspb "go.temporal.io/api/enums/v1"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

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
	redirectRules := []*persistencespb.RedirectRule{
		{Rule: mkRedirectRule("1", "10")},
		{Rule: mkRedirectRule("2", "1")},
		{Rule: mkRedirectRule("3", "5")},
		{Rule: mkRedirectRule("4", "3")},
		{Rule: mkRedirectRule("5", "1")},
	}

	expectedUpstreamBuildIds := []string{"2", "5", "3", "4"}
	upstreamBuildIds := getUpstreamBuildIds("1", redirectRules, nil)

	for _, bid := range expectedUpstreamBuildIds {
		assert.Contains(t, upstreamBuildIds, bid)
	}
	assert.Equal(t, len(expectedUpstreamBuildIds), len(upstreamBuildIds))
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
	redirectRules := []*persistencespb.RedirectRule{
		{Rule: mkRedirectRule("1", "2")},
		{Rule: mkRedirectRule("2", "3")},
		{Rule: mkRedirectRule("3", "4")},
		{Rule: mkRedirectRule("3", "5")},
		{Rule: mkRedirectRule("5", "1")},
	}
	expectedUpstreamBuildIds := []string{"5", "3", "2"}
	upstreamBuildIds := getUpstreamBuildIds("1", redirectRules, nil)
	for _, bid := range expectedUpstreamBuildIds {
		assert.Contains(t, upstreamBuildIds, bid)
	}
	assert.Equal(t, len(expectedUpstreamBuildIds), len(upstreamBuildIds))

	/*
		e.g.
		Redirect Rules:
		1         2 <---
		^         |     \
		|         v      \
		5 <------ 3 ------> 4
	*/
	redirectRules = []*persistencespb.RedirectRule{
		{Rule: mkRedirectRule("2", "3")},
		{Rule: mkRedirectRule("3", "4")},
		{Rule: mkRedirectRule("3", "5")},
		{Rule: mkRedirectRule("4", "2")},
		{Rule: mkRedirectRule("5", "1")},
	}
	expectedUpstreamBuildIds = []string{"5", "3", "2", "4"}
	upstreamBuildIds = getUpstreamBuildIds("1", redirectRules, nil)
	for _, bid := range expectedUpstreamBuildIds {
		assert.Contains(t, upstreamBuildIds, bid)
	}
	assert.Equal(t, len(expectedUpstreamBuildIds), len(upstreamBuildIds))
}

func TestExistsBackloggedActivityOrWFAssignedTo(t *testing.T) {
	// todo after we have backlog info
}

/*
Assignment Rules:
[ (3, 50%), (2, nil) (1, nil) ]

Expect 3 and 2 are reachable, but not 1 since it is behind an unconditional rule.
*/
func TestIsReachableAssignmentRuleTarget(t *testing.T) {
	t.Parallel()
	assignmentRules := []*persistencespb.AssignmentRule{
		{Rule: mkAssignmentRule("3", mkNewAssignmentPercentageRamp(50))},
		{Rule: mkAssignmentRule("2", nil)},
		{Rule: mkAssignmentRule("1", nil)},
	}

	assert.True(t, isReachableAssignmentRuleTarget("3", assignmentRules))
	assert.True(t, isReachableAssignmentRuleTarget("2", assignmentRules))
	assert.False(t, isReachableAssignmentRuleTarget("1", assignmentRules))
	assert.False(t, isReachableAssignmentRuleTarget("0", assignmentRules))
}

func TestMakeBuildIdQuery(t *testing.T) {
	t.Parallel()
	tq := "test-query-tq"

	buildIdsOfInterest := []string{"0", "1", "2", ""}
	query := makeBuildIdQuery(buildIdsOfInterest, tq, true)
	expectedQuery := "TaskQueue = 'test-query-tq' AND (BuildIds IS NULL OR BuildIds IN ('assigned:0','assigned:1','assigned:2','unversioned')) AND ExecutionStatus = \"Running\""
	assert.Equal(t, expectedQuery, query)

	query = makeBuildIdQuery(buildIdsOfInterest, tq, false)
	expectedQuery = "TaskQueue = 'test-query-tq' AND (BuildIds IS NULL OR BuildIds IN ('versioned:0','versioned:1','versioned:2','unversioned')) AND ExecutionStatus != \"Running\""
	assert.Equal(t, expectedQuery, query)

	buildIdsOfInterest = []string{"0", "1", "2"}
	query = makeBuildIdQuery(buildIdsOfInterest, tq, true)
	expectedQuery = "TaskQueue = 'test-query-tq' AND BuildIds IN ('assigned:0','assigned:1','assigned:2') AND ExecutionStatus = \"Running\""
	assert.Equal(t, expectedQuery, query)

	query = makeBuildIdQuery(buildIdsOfInterest, tq, false)
	expectedQuery = "TaskQueue = 'test-query-tq' AND BuildIds IN ('versioned:0','versioned:1','versioned:2') AND ExecutionStatus != \"Running\""
	assert.Equal(t, expectedQuery, query)
}

func mkCountResponse(count int64) (*manager.CountWorkflowExecutionsResponse, error) {
	return &manager.CountWorkflowExecutionsResponse{
		Count:  count,
		Groups: nil,
	}, nil
}

// nothing in assignment rules for this test
func TestGetReachability_NoRules(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tq := "test-reachability-tq"
	nsID := namespace.ID("test-namespace-id")
	nsName := namespace.Name("test-namespace")
	ctrl := gomock.NewController(t)
	vm := manager.NewMockVisibilityManager(ctrl)
	rules := &persistencespb.VersioningData{}

	// 1. getReachability("") --> CLOSED_ONLY
	// Scenario: There is 1 closed workflow in visibility, unversioned with no build id --> BuildIds == NULL
	// Expect no matching open workflow executions
	reqOpen := makeBuildIdCountRequest(nsID, nsName, []string{""}, tq, true)
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), reqOpen).AnyTimes().Return(mkCountResponse(0))
	reqClosed := makeBuildIdCountRequest(nsID, nsName, []string{""}, tq, false)
	// Expect yes matching closed workflow execution
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), reqClosed).AnyTimes().Return(mkCountResponse(1))
	// Check reachability
	reachability, err := getBuildIdTaskReachability(ctx, rules, vm, nsID, nsName, tq, "")
	assert.Nil(t, err)
	assert.Equal(t, enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY, reachability)

	// 2. getReachability(1) --> UNREACHABLE

	// 3. populate mockStore with a versioned workflow assigned to build id 1
	// now you start a versioned workflow assigned to build id 1, and it is running
	// getReachability(1) --> REACHABLE
}

// nothing in visibility for this test
func TestGetReachability_WithActiveRules(t *testing.T) {
	// Assignment: [ A, D ]. Redirect: (A->B), (B->C)

	// getReachability(A) --> unreachable (redirect rule source)
	// getReachability(C) --> reachable (redirect rule target of reachable source)
	// getReachability(D) --> reachable (assignment rule target)
}

// nothing in visibility for this test
func TestGetReachability_WithDeletedRules(t *testing.T) {
	// recently-deleted redirect rule source --> reachable
	// recently-deleted assignment rule target --> reachable
	// deleted assignment rule target --> not reachable
	// recently-deleted redirect rule target of reachable source --> reachable
	// deleted redirect rule target of reachable source --> unreachable

	// Assignment: [ (C, active), (B, recently deleted), (A, deleted) ]. Redirect: (A->B, active), (C->B, recently deleted)

	// todo
	// getReachability(A) --> unreachable (active redirect rule source)
	// getReachability(A) --> unreachable (active redirect rule source)
	// getReachability(C) --> reachable (redirect rule target of reachable source)
	// getReachability(D) --> reachable (assignment rule target)
}
