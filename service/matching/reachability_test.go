package matching

import (
	"context"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	"testing"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	commonclock "go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
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
	createTs := hlc.Zero(1)
	rc := &reachabilityCalculator{
		redirectRules: []*persistencespb.RedirectRule{
			mkRedirectRulePersistence(mkRedirectRule("1", "10"), createTs, nil),
			mkRedirectRulePersistence(mkRedirectRule("2", "1"), createTs, nil),
			mkRedirectRulePersistence(mkRedirectRule("3", "5"), createTs, nil),
			mkRedirectRulePersistence(mkRedirectRule("4", "3"), createTs, nil),
			mkRedirectRulePersistence(mkRedirectRule("5", "1"), createTs, nil),
		},
	}

	expectedUpstreamBuildIds := []string{"2", "5", "3", "4"}
	upstreamBuildIds := rc.getUpstreamBuildIds("1", true)

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
	createTs := hlc.Zero(1)
	rc := &reachabilityCalculator{
		redirectRules: []*persistencespb.RedirectRule{
			mkRedirectRulePersistence(mkRedirectRule("1", "2"), createTs, nil),
			mkRedirectRulePersistence(mkRedirectRule("2", "3"), createTs, nil),
			mkRedirectRulePersistence(mkRedirectRule("3", "4"), createTs, nil),
			mkRedirectRulePersistence(mkRedirectRule("3", "5"), createTs, nil),
			mkRedirectRulePersistence(mkRedirectRule("5", "1"), createTs, nil),
		},
	}
	expectedUpstreamBuildIds := []string{"5", "3", "2"}
	upstreamBuildIds := rc.getUpstreamBuildIds("1", true)
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
	rc = &reachabilityCalculator{
		redirectRules: []*persistencespb.RedirectRule{
			mkRedirectRulePersistence(mkRedirectRule("2", "3"), createTs, nil),
			mkRedirectRulePersistence(mkRedirectRule("3", "4"), createTs, nil),
			mkRedirectRulePersistence(mkRedirectRule("3", "5"), createTs, nil),
			mkRedirectRulePersistence(mkRedirectRule("4", "2"), createTs, nil),
			mkRedirectRulePersistence(mkRedirectRule("5", "1"), createTs, nil),
		},
	}
	expectedUpstreamBuildIds = []string{"5", "3", "2", "4"}
	upstreamBuildIds = rc.getUpstreamBuildIds("1", true)
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
	createTs := hlc.Zero(1)
	deleteTs := hlc.Next(createTs, commonclock.NewRealTimeSource())
	rc := &reachabilityCalculator{
		assignmentRules: []*persistencespb.AssignmentRule{
			mkAssignmentRulePersistence(mkAssignmentRule("3", mkNewAssignmentPercentageRamp(50)), createTs, nil),
			mkAssignmentRulePersistence(mkAssignmentRule("2.5", nil), createTs, deleteTs),
			mkAssignmentRulePersistence(mkAssignmentRule("2", nil), createTs, nil),
			mkAssignmentRulePersistence(mkAssignmentRule("1", nil), createTs, nil),
		},
	}

	assert.True(t, rc.isReachableActiveAssignmentRuleTarget("3"))
	assert.False(t, rc.isReachableActiveAssignmentRuleTarget("2.5"))
	assert.True(t, rc.isReachableActiveAssignmentRuleTarget("2"))
	assert.False(t, rc.isReachableActiveAssignmentRuleTarget("1"))
	assert.False(t, rc.isReachableActiveAssignmentRuleTarget("0"))
}

func TestMakeBuildIdQuery(t *testing.T) {
	t.Parallel()
	rc := &reachabilityCalculator{
		taskQueue: "test-query-tq",
	}

	buildIdsOfInterest := []string{"0", "1", "2", ""}
	query := rc.makeBuildIdQuery(buildIdsOfInterest, true)
	expectedQuery := "TaskQueue = 'test-query-tq' AND (BuildIds IS NULL OR BuildIds IN ('assigned:0','assigned:1','assigned:2','unversioned')) AND ExecutionStatus = \"Running\""
	assert.Equal(t, expectedQuery, query)

	query = rc.makeBuildIdQuery(buildIdsOfInterest, false)
	expectedQuery = "TaskQueue = 'test-query-tq' AND (BuildIds IS NULL OR BuildIds IN ('versioned:0','versioned:1','versioned:2','unversioned')) AND ExecutionStatus != \"Running\""
	assert.Equal(t, expectedQuery, query)

	buildIdsOfInterest = []string{"0", "1", "2"}
	query = rc.makeBuildIdQuery(buildIdsOfInterest, true)
	expectedQuery = "TaskQueue = 'test-query-tq' AND BuildIds IN ('assigned:0','assigned:1','assigned:2') AND ExecutionStatus = \"Running\""
	assert.Equal(t, expectedQuery, query)

	query = rc.makeBuildIdQuery(buildIdsOfInterest, false)
	expectedQuery = "TaskQueue = 'test-query-tq' AND BuildIds IN ('versioned:0','versioned:1','versioned:2') AND ExecutionStatus != \"Running\""
	assert.Equal(t, expectedQuery, query)
}

func mkCountResponse(count int64) (*manager.CountWorkflowExecutionsResponse, error) {
	return &manager.CountWorkflowExecutionsResponse{
		Count:  count,
		Groups: nil,
	}, nil
}

// nothing in assignment rules for this test --> buildIdsOfInterest list will always just contain the original build id
func TestGetReachability_WithVisibility_WithoutRules(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	vm := manager.NewMockVisibilityManager(ctrl)

	rc := &reachabilityCalculator{
		visibilityMgr:   vm,
		nsID:            namespace.ID("test-namespace-id"),
		nsName:          namespace.Name("test-namespace"),
		taskQueue:       "test-reachability-tq",
		assignmentRules: nil,
		redirectRules:   nil,
	}

	// 1. getReachability("") --> CLOSED_ONLY
	// Scenario:
	//	- There is 1 closed workflow in visibility, unversioned with no build id --> BuildIds == NULL
	// Expect no matching open workflow executions
	reqOpen := rc.makeBuildIdCountRequest([]string{""}, true)
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), reqOpen).AnyTimes().Return(mkCountResponse(0))
	// Expect yes matching closed workflow execution
	reqClosed := rc.makeBuildIdCountRequest([]string{""}, false)
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), reqClosed).AnyTimes().Return(mkCountResponse(1))
	// Check reachability
	reachability, err := rc.getReachability(ctx, "")
	assert.Nil(t, err)
	assert.Equal(t, enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY, reachability)

	// 2. getReachability(1) --> UNREACHABLE
	// Scenario:
	//	- There is 1 closed workflow in visibility, unversioned with no build id --> BuildIds == NULL
	// Expect no matching open workflow executions
	reqOpen = rc.makeBuildIdCountRequest([]string{"1"}, true)
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), reqOpen).AnyTimes().Return(mkCountResponse(0))
	// Expect no matching closed workflow execution
	reqClosed = rc.makeBuildIdCountRequest([]string{"1"}, false)
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), reqClosed).AnyTimes().Return(mkCountResponse(0))
	// Check reachability
	reachability, err = rc.getReachability(ctx, "1")
	assert.Nil(t, err)
	assert.Equal(t, enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, reachability)

	// 3. getReachability(1) --> REACHABLE
	// Scenario:
	//	- There is 1 closed workflow in visibility, unversioned with no build id --> BuildIds == NULL
	//	- There is one running workflow execution assigned to build id 2
	// Expect yes matching open workflow executions
	reqOpen = rc.makeBuildIdCountRequest([]string{"2"}, true)
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), reqOpen).AnyTimes().Return(mkCountResponse(1))
	// Expect no matching closed workflow execution
	reqClosed = rc.makeBuildIdCountRequest([]string{"2"}, false)
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), reqClosed).AnyTimes().Return(mkCountResponse(0))
	// Check reachability
	reachability, err = rc.getReachability(ctx, "2")
	assert.Nil(t, err)
	assert.Equal(t, enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, reachability)
}

func TestGetReachability_WithoutVisibility_WithRules(t *testing.T) {
	// Assignment: [ (D, 50%), (A, nil) ]
	// Redirect: (A->B), (B->C), (F->G)
	t.Parallel()
	ctx := context.Background()
	createTs := hlc.Zero(1)

	// make a visibility manager with no records
	ctrl := gomock.NewController(t)
	vm := manager.NewMockVisibilityManager(ctrl)
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).AnyTimes().Return(mkCountResponse(0))

	rc := &reachabilityCalculator{
		visibilityMgr: vm,
		nsID:          namespace.ID("test-namespace-id"),
		nsName:        namespace.Name("test-namespace"),
		taskQueue:     "test-reachability-tq",
		assignmentRules: []*persistencespb.AssignmentRule{
			mkAssignmentRulePersistence(mkAssignmentRule("D", mkNewAssignmentPercentageRamp(50)), createTs, nil),
			mkAssignmentRulePersistence(mkAssignmentRule("A", nil), createTs, nil),
		},
		redirectRules: []*persistencespb.RedirectRule{
			mkRedirectRulePersistence(mkRedirectRule("A", "B"), createTs, nil),
			mkRedirectRulePersistence(mkRedirectRule("B", "C"), createTs, nil),
			mkRedirectRulePersistence(mkRedirectRule("F", "G"), createTs, nil),
		},
	}

	// 1. getReachability(A) --> unreachable (assignment rule target, and also redirect rule source)
	reachability, err := rc.getReachability(ctx, "A")
	assert.Nil(t, err)
	assert.Equal(t, enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, reachability)

	// 2. getReachability(C) --> reachable (redirect rule target of reachable source)
	reachability, err = rc.getReachability(ctx, "C")
	assert.Nil(t, err)
	assert.Equal(t, enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, reachability)

	// 3. getReachability(D) --> reachable (assignment rule target, nothing else)
	reachability, err = rc.getReachability(ctx, "D")
	assert.Nil(t, err)
	assert.Equal(t, enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, reachability)

	// 4. getReachability(G) --> unreachable (redirect rule target of unreachable source [F not reachable by rules or visibility])
	reachability, err = rc.getReachability(ctx, "G")
	assert.Nil(t, err)
	assert.Equal(t, enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, reachability)
}

// todo: test reachability of rules that are only reachable by the buildIdsOfInterest list
// todo: test deleted rules
func TestGetReachability_WithVisibility_WithDeletedRules(t *testing.T) {
	t.Skip()
	// recently-deleted redirect rule source --> reachable
	// recently-deleted assignment rule target --> reachable only if we include recently-deleted rules for non-visibility buildIdsOfInterest list
	// deleted assignment rule target --> not reachable
	// recently-deleted redirect rule target of reachable source --> reachable only if we include recently-deleted rules for non-visibility buildIdsOfInterest list
	// deleted redirect rule target of reachable source --> unreachable

	// Assignment: [ (C, active), (B, recently deleted), (A, deleted) ]. Redirect: (A->B, active), (C->B, recently deleted)

	// getReachability(A) --> unreachable (active redirect rule source)
	// getReachability(A) --> unreachable (active redirect rule source)
	// getReachability(C) --> reachable (redirect rule target of reachable source)
	// getReachability(D) --> reachable (assignment rule target)
}
