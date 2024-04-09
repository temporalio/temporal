package matching

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	commonclock "go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

const testBuildIdVisibilityGracePeriod = 2 * time.Minute

func TestGetBuildIdsOfInterest_NoCycle(t *testing.T) {
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

	expectedUpstreamBuildIds := []string{"2", "5", "3", "4", "1"}
	upstreamBuildIds := rc.getBuildIdsOfInterest("1", true)

	for _, bid := range expectedUpstreamBuildIds {
		assert.Contains(t, upstreamBuildIds, bid)
	}
	assert.Equal(t, len(expectedUpstreamBuildIds), len(upstreamBuildIds))
}

func TestGetBuildIdsOfInterest_WithCycle(t *testing.T) {
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
	expectedUpstreamBuildIds := []string{"5", "3", "2", "1"}
	upstreamBuildIds := rc.getBuildIdsOfInterest("1", true)
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
	expectedUpstreamBuildIds = []string{"5", "3", "2", "4", "1"}
	upstreamBuildIds = rc.getBuildIdsOfInterest("1", true)
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

	assert.True(t, rc.isReachableActiveAssignmentRuleTargetOrDefault("3"))
	assert.False(t, rc.isReachableActiveAssignmentRuleTargetOrDefault("2.5"))
	assert.True(t, rc.isReachableActiveAssignmentRuleTargetOrDefault("2"))
	assert.False(t, rc.isReachableActiveAssignmentRuleTargetOrDefault("1"))
	assert.False(t, rc.isReachableActiveAssignmentRuleTargetOrDefault("0"))
}

func TestGetDefaultBuildId(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "", getDefaultBuildId([]*persistencespb.AssignmentRule{}))

	createTs := hlc.Zero(1)
	deleteTs := hlc.Next(createTs, commonclock.NewRealTimeSource())
	assignmentRules := []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(mkAssignmentRule("3", mkNewAssignmentPercentageRamp(50)), createTs, nil),
		mkAssignmentRulePersistence(mkAssignmentRule("2.5", nil), createTs, deleteTs),
		mkAssignmentRulePersistence(mkAssignmentRule("2", nil), createTs, nil),
		mkAssignmentRulePersistence(mkAssignmentRule("1", nil), createTs, nil),
	}
	assert.Equal(t, "2", getDefaultBuildId(assignmentRules))
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

// nothing in assignment rules for this test --> buildIdsOfInterest list will always just contain the original build id
func TestGetReachability_WithVisibility_WithoutRules(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rc := mkTestReachabilityCalculator()

	// Visibility: [ (NULL, closed), (A, open) ]
	setVisibilityExpect(t, rc, []string{""}, 0, 1)
	setVisibilityExpect(t, rc, []string{"A"}, 1, 0)
	setVisibilityExpect(t, rc, []string{"B"}, 0, 0)

	// getReachability("") --> reachable (it's the default build id)
	getAndCheckReachability(t, ctx, rc, "", enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE)

	// getReachability("") --> closed_workflows_only (now that "" is not default)
	rc.assignmentRules = []*persistencespb.AssignmentRule{mkAssignmentRulePersistence(mkAssignmentRule("A", nil), nil, nil)}
	getAndCheckReachability(t, ctx, rc, "", enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY)
	rc.assignmentRules = nil // remove rule for rest of test

	// getReachability(A) --> reachable (open workflow in visibility)
	getAndCheckReachability(t, ctx, rc, "A", enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE)

	// getReachability(B) --> unreachable (not mentioned in rules or visibility)
	getAndCheckReachability(t, ctx, rc, "B", enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE)
}

func TestGetReachability_WithoutVisibility_WithRules(t *testing.T) {
	// Assignment: [ (D, 50%), (A, nil) ]
	// Redirect: [ (A->B), (B->C), (F->G) ]
	t.Parallel()
	ctx := context.Background()
	createTs := hlc.Zero(1)
	rc := mkTestReachabilityCalculator()
	rc.assignmentRules = []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(mkAssignmentRule("D", mkNewAssignmentPercentageRamp(50)), createTs, nil),
		mkAssignmentRulePersistence(mkAssignmentRule("A", nil), createTs, nil),
	}
	rc.redirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("A", "B"), createTs, nil),
		mkRedirectRulePersistence(mkRedirectRule("B", "C"), createTs, nil),
		mkRedirectRulePersistence(mkRedirectRule("F", "G"), createTs, nil),
	}
	setVisibilityExpectEmptyAlways(t, rc)

	// getReachability(A) --> unreachable (assignment rule target, and also redirect rule source)
	getAndCheckReachability(t, ctx, rc, "A", enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE)

	// getReachability(C) --> reachable (redirect rule target of reachable source)
	getAndCheckReachability(t, ctx, rc, "C", enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE)

	// getReachability(D) --> reachable (assignment rule target, nothing else)
	getAndCheckReachability(t, ctx, rc, "D", enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE)

	// getReachability(G) --> unreachable (redirect rule target of unreachable source [F not reachable by rules or visibility])
	getAndCheckReachability(t, ctx, rc, "G", enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE)
}

// test reachability of build ids that are only reachable by the buildIdsOfInterest list + visibility
func TestGetReachability_WithVisibility_WithRules(t *testing.T) {
	// Visibility: [ (A, closed), (B, running) ]
	// Redirect: [ (A->C, active), (B->D, active) ]
	t.Parallel()
	ctx := context.Background()
	createTs := hlc.Zero(1)
	rc := mkTestReachabilityCalculator()
	rc.redirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("A", "C"), createTs, nil),
		mkRedirectRulePersistence(mkRedirectRule("B", "D"), createTs, nil),
	}

	// getReachability(C) --> closed_workflows_only (via upstream closed wf execution A)
	setVisibilityExpect(t, rc, []string{"C", "A"}, 0, 1)
	getAndCheckReachability(t, ctx, rc, "C", enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY)

	// getReachability(D) --> reachable (via upstream running wf execution B)
	setVisibilityExpect(t, rc, []string{"D", "B"}, 1, 0)
	getAndCheckReachability(t, ctx, rc, "D", enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE)
}

// test reachability of build ids that are only reachable by buildIdsOfInterest + visibility
// test rules with deletion time within and before the visibility update delay
func TestGetReachability_WithVisibility_WithDeletedRules(t *testing.T) {
	// Visibility: [ (A, closed), (B, running), (X, closed), (Y, running)]
	// Redirect: (A->C, recently-deleted), (B->D, recently-deleted), (X->Z, deleted), (Y->ZZ, deleted)
	t.Parallel()
	ctx := context.Background()
	rc := mkTestReachabilityCalculator()

	// start time 3x rc.buildIdVisibilityGracePeriod ago
	timesource := commonclock.NewEventTimeSource().Update(time.Now().Add(-3*rc.buildIdVisibilityGracePeriod + time.Second))
	createTs := hlc.Next(hlc.Zero(1), timesource)
	timesource.Advance(rc.buildIdVisibilityGracePeriod)
	oldDeleteTs := hlc.Next(createTs, timesource)
	timesource.Advance(rc.buildIdVisibilityGracePeriod)
	recentDeleteTs := hlc.Next(oldDeleteTs, timesource) // this should be within the grace period by 1s
	rc.redirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("A", "C"), createTs, recentDeleteTs),
		mkRedirectRulePersistence(mkRedirectRule("B", "D"), createTs, recentDeleteTs),
		mkRedirectRulePersistence(mkRedirectRule("X", "Z"), createTs, oldDeleteTs),
		mkRedirectRulePersistence(mkRedirectRule("Y", "ZZ"), createTs, oldDeleteTs),
	}

	// getReachability(C) --> closed_workflows_only (via upstream closed wf execution A, rule included due to recent delete)
	setVisibilityExpect(t, rc, []string{"C", "A"}, 0, 1)
	getAndCheckReachability(t, ctx, rc, "C", enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY)

	// getReachability(D) --> reachable (via upstream running wf execution B, rule included due to recent delete)
	setVisibilityExpect(t, rc, []string{"D", "B"}, 1, 0)
	getAndCheckReachability(t, ctx, rc, "D", enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE)

	// getReachability(Z) --> unreachable (despite upstream closed wf execution X, rule excluded due to old delete)
	setVisibilityExpect(t, rc, []string{"Z"}, 0, 0)
	getAndCheckReachability(t, ctx, rc, "Z", enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE)

	// getReachability(ZZ) --> unreachable (despite upstream running wf execution Y, rule excluded due to recent delete)
	setVisibilityExpect(t, rc, []string{"ZZ"}, 0, 0)
	getAndCheckReachability(t, ctx, rc, "ZZ", enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE)
}

// test reachability via deleted rules within the rule propagation delay
func TestGetReachability_WithoutVisibility_WithDeletedRules(t *testing.T) {
	t.Skip()
	// todo carly
}

func getAndCheckReachability(t *testing.T,
	ctx context.Context,
	rc *reachabilityCalculator,
	buildId string,
	expectedReachability enumspb.BuildIdTaskReachability) {
	reachability, err := rc.getReachability(ctx, buildId)
	assert.Nil(t, err)
	assert.Equal(t, expectedReachability, reachability)
}

func setVisibilityExpect(t *testing.T,
	rc *reachabilityCalculator,
	buildIdsOfInterest []string,
	countOpen, countClosed int64) {
	vm := manager.NewMockVisibilityManager(gomock.NewController(t))
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), rc.makeBuildIdCountRequest(buildIdsOfInterest, true)).AnyTimes().Return(mkCountResponse(countOpen))
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), rc.makeBuildIdCountRequest(buildIdsOfInterest, false)).AnyTimes().Return(mkCountResponse(countClosed))
	rc.visibilityMgr = vm
}

func setVisibilityExpectEmptyAlways(t *testing.T,
	rc *reachabilityCalculator) {
	vm := manager.NewMockVisibilityManager(gomock.NewController(t))
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).AnyTimes().Return(mkCountResponse(0))
	rc.visibilityMgr = vm
}

func mkCountResponse(count int64) (*manager.CountWorkflowExecutionsResponse, error) {
	return &manager.CountWorkflowExecutionsResponse{
		Count:  count,
		Groups: nil,
	}, nil
}

func mkTestReachabilityCalculator() *reachabilityCalculator {
	return &reachabilityCalculator{
		nsID:                         "test-namespace-id",
		nsName:                       "test-namespace",
		taskQueue:                    "test-reachability-tq",
		buildIdVisibilityGracePeriod: testBuildIdVisibilityGracePeriod,
	}
}
