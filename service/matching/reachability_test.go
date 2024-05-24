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
	"slices"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	commonclock "go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

const (
	testBuildIdVisibilityGracePeriod  = 2 * time.Minute
	testReachabilityCacheOpenWFsTTL   = 2 * time.Millisecond
	testReachabilityCacheClosedWFsTTL = 4 * time.Millisecond
)

type testReachabilityCalculator struct {
	rc      *reachabilityCalculator
	capture *metricstest.Capture
}

func TestGetBuildIdsOfInterest(t *testing.T) {
	t.Parallel()
	trc := mkTestReachabilityCalculatorWithEmptyVisibility(t)
	rc := trc.rc

	// start time 3x rc.buildIdVisibilityGracePeriod ago
	timesource := commonclock.NewEventTimeSource().Update(time.Now().Add(-3*rc.buildIdVisibilityGracePeriod + time.Second))
	createTs := hlc.Next(hlc.Zero(1), timesource)
	timesource.Advance(rc.buildIdVisibilityGracePeriod)
	oldDeleteTs := hlc.Next(createTs, timesource)
	timesource.Advance(rc.buildIdVisibilityGracePeriod)
	recentDeleteTs := hlc.Next(oldDeleteTs, timesource) // this should be within the grace period by 1s
	/*
		e.g.
		Redirect Rules:
		10
		^
		|
		1 <------ 2
		^
		|
		5 <------ 3 <------ 4 (recently-deleted) <------ 6 (old-deleted) <------ 7 (recently-deleted)
	*/
	rc.redirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("1", "10"), createTs, nil),
		mkRedirectRulePersistence(mkRedirectRule("2", "1"), createTs, nil),
		mkRedirectRulePersistence(mkRedirectRule("3", "5"), createTs, nil),
		mkRedirectRulePersistence(mkRedirectRule("4", "3"), createTs, recentDeleteTs),
		mkRedirectRulePersistence(mkRedirectRule("5", "1"), createTs, nil),
		mkRedirectRulePersistence(mkRedirectRule("6", "4"), createTs, oldDeleteTs),
		mkRedirectRulePersistence(mkRedirectRule("7", "6"), createTs, recentDeleteTs),
	}

	// getBuildIdsOfInterest(1, deletedRuleInclusionPeriod=nil)
	buildIdsOfInterest := rc.getBuildIdsOfInterest("1", time.Duration(0))
	expectedBuildIdsOfInterest := []string{"2", "5", "3", "1"}
	slices.Sort(expectedBuildIdsOfInterest)
	slices.Sort(buildIdsOfInterest)
	assert.Equal(t, expectedBuildIdsOfInterest, buildIdsOfInterest)

	// getBuildIdsOfInterest(1, deletedRuleInclusionPeriod=rc.buildIdVisibilityGracePeriod)
	buildIdsOfInterest = rc.getBuildIdsOfInterest("1", rc.buildIdVisibilityGracePeriod)
	expectedBuildIdsOfInterest = []string{"2", "5", "3", "4", "1"}
	slices.Sort(expectedBuildIdsOfInterest)
	slices.Sort(buildIdsOfInterest)
	assert.Equal(t, expectedBuildIdsOfInterest, buildIdsOfInterest)

	// getBuildIdsOfInterest(6, deletedRuleInclusionPeriod=nil)
	buildIdsOfInterest = rc.getBuildIdsOfInterest("6", time.Duration(0))
	expectedBuildIdsOfInterest = []string{"6"}
	slices.Sort(expectedBuildIdsOfInterest)
	slices.Sort(buildIdsOfInterest)
	assert.Equal(t, expectedBuildIdsOfInterest, buildIdsOfInterest)

	// getBuildIdsOfInterest(6, deletedRuleInclusionPeriod=rc.buildIdVisibilityGracePeriod)
	buildIdsOfInterest = rc.getBuildIdsOfInterest("6", rc.buildIdVisibilityGracePeriod)
	expectedBuildIdsOfInterest = []string{"7", "6"}
	slices.Sort(expectedBuildIdsOfInterest)
	slices.Sort(buildIdsOfInterest)
	assert.Equal(t, expectedBuildIdsOfInterest, buildIdsOfInterest)
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
	expectedQuery := "TaskQueue = 'test-query-tq' AND (BuildIds IS NULL OR BuildIds IN ('unversioned','assigned:0','assigned:1','assigned:2')) AND ExecutionStatus = \"Running\""
	assert.Equal(t, expectedQuery, query)

	query = rc.makeBuildIdQuery(buildIdsOfInterest, false)
	expectedQuery = "TaskQueue = 'test-query-tq' AND (BuildIds IS NULL OR BuildIds IN ('unversioned','versioned:0','versioned:1','versioned:2')) AND ExecutionStatus != \"Running\""
	assert.Equal(t, expectedQuery, query)

	buildIdsOfInterest = []string{"0", "1", "2"}
	query = rc.makeBuildIdQuery(buildIdsOfInterest, true)
	expectedQuery = "TaskQueue = 'test-query-tq' AND BuildIds IN ('assigned:0','assigned:1','assigned:2') AND ExecutionStatus = \"Running\""
	assert.Equal(t, expectedQuery, query)

	query = rc.makeBuildIdQuery(buildIdsOfInterest, false)
	expectedQuery = "TaskQueue = 'test-query-tq' AND BuildIds IN ('versioned:0','versioned:1','versioned:2') AND ExecutionStatus != \"Running\""
	assert.Equal(t, expectedQuery, query)
}

// nothing in assignment rules for this test --> buildIdsOfInterest list will always just contain the original build ID
func TestGetReachability_WithVisibility_WithoutRules(t *testing.T) {
	// Visibility: [ (NULL, closed), (A, open) ]
	t.Parallel()
	ctx := context.Background()
	trc := mkTestReachabilityCalculatorWithEmptyVisibility(t)
	rc := trc.rc

	// reachability("") --> reachable (it's the default build ID)
	checkReachability(ctx, t, rc, "", enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, checkedRuleTargetsForUpstream)

	// reachability("") --> closed_workflows_only (now that "" is not default)
	rc.assignmentRules = []*persistencespb.AssignmentRule{mkAssignmentRulePersistence(mkAssignmentRule("A", nil), nil, nil)}
	setVisibilityExpect(t, rc, []string{""}, 0, 1)
	checkReachability(ctx, t, rc, "", enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY, checkedClosedWorkflowExecutionsForUpstreamMiss)
	rc.assignmentRules = nil // remove rule for rest of test

	// reachability(A) --> reachable (open workflow in visibility)
	setVisibilityExpect(t, rc, []string{"A"}, 1, 0)
	checkReachability(ctx, t, rc, "A", enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, checkedOpenWorkflowExecutionsForUpstreamMiss)

	// reachability(B) --> unreachable (not mentioned in rules or visibility)
	setVisibilityExpect(t, rc, []string{"B"}, 0, 0)
	checkReachability(ctx, t, rc, "B", enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, checkedClosedWorkflowExecutionsForUpstreamMiss)
}

func TestGetReachability_WithoutVisibility_WithRules(t *testing.T) {
	// Assignment: [ (D, 50%), (A, nil) ]
	// Redirect: [ (A->B), (B->C), (F->G) ]
	t.Parallel()
	ctx := context.Background()
	createTs := hlc.Zero(1)
	trc := mkTestReachabilityCalculatorWithEmptyVisibility(t)
	rc := trc.rc
	rc.assignmentRules = []*persistencespb.AssignmentRule{
		mkAssignmentRulePersistence(mkAssignmentRule("D", mkNewAssignmentPercentageRamp(50)), createTs, nil),
		mkAssignmentRulePersistence(mkAssignmentRule("A", nil), createTs, nil),
	}
	rc.redirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("A", "B"), createTs, nil),
		mkRedirectRulePersistence(mkRedirectRule("B", "C"), createTs, nil),
		mkRedirectRulePersistence(mkRedirectRule("F", "G"), createTs, nil),
	}

	// reachability(A) --> unreachable (assignment rule target, and also redirect rule source)
	checkReachability(ctx, t, rc, "A", enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, checkedRuleSourcesForInput)

	// reachability(C) --> reachable (redirect rule target of reachable source)
	checkReachability(ctx, t, rc, "C", enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, checkedRuleTargetsForUpstream)

	// reachability(D) --> reachable (assignment rule target, nothing else)
	checkReachability(ctx, t, rc, "D", enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, checkedRuleTargetsForUpstream)

	// reachability(G) --> unreachable (redirect rule target of unreachable source [F not reachable by rules or visibility])
	checkReachability(ctx, t, rc, "G", enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, checkedClosedWorkflowExecutionsForUpstreamMiss)
}

// test reachability of build ids that are only reachable by the buildIdsOfInterest list + visibility
func TestGetReachability_WithVisibility_WithRules(t *testing.T) {
	// Visibility: [ (A, closed), (B, running) ]
	// Redirect: [ (A->C, active), (B->D, active) ]
	t.Parallel()
	ctx := context.Background()
	createTs := hlc.Zero(1)
	trc := mkTestReachabilityCalculatorWithEmptyVisibility(t)
	rc := trc.rc
	rc.redirectRules = []*persistencespb.RedirectRule{
		mkRedirectRulePersistence(mkRedirectRule("A", "C"), createTs, nil),
		mkRedirectRulePersistence(mkRedirectRule("B", "D"), createTs, nil),
	}

	// reachability(C) --> closed_workflows_only (via upstream closed wf execution A)
	setVisibilityExpect(t, rc, []string{"C", "A"}, 0, 1)
	checkReachability(ctx, t, rc, "C", enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY, checkedClosedWorkflowExecutionsForUpstreamMiss)

	// reachability(D) --> reachable (via upstream running wf execution B)
	setVisibilityExpect(t, rc, []string{"D", "B"}, 1, 0)
	checkReachability(ctx, t, rc, "D", enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, checkedOpenWorkflowExecutionsForUpstreamMiss)
}

// test reachability of build ids that are only reachable by buildIdsOfInterest + visibility
// test rules with deletion time within and before the visibility update delay
func TestGetReachability_WithVisibility_WithDeletedRules(t *testing.T) {
	// Visibility: [ (A, closed), (B, running), (X, closed), (Y, running)]
	// Redirect: (A->C, recently-deleted), (B->D, recently-deleted), (X->Z, deleted), (Y->ZZ, deleted)
	t.Parallel()
	ctx := context.Background()
	trc := mkTestReachabilityCalculatorWithEmptyVisibility(t)
	rc := trc.rc

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

	// reachability(C) --> closed_workflows_only (via upstream closed wf execution A, rule included due to recent delete)
	setVisibilityExpect(t, rc, []string{"C", "A"}, 0, 1)
	checkReachability(ctx, t, rc, "C", enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY, checkedClosedWorkflowExecutionsForUpstreamMiss)

	// reachability(D) --> reachable (via upstream running wf execution B, rule included due to recent delete)
	setVisibilityExpect(t, rc, []string{"D", "B"}, 1, 0)
	checkReachability(ctx, t, rc, "D", enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, checkedOpenWorkflowExecutionsForUpstreamMiss)

	// reachability(Z) --> unreachable (despite upstream closed wf execution X, rule excluded due to old delete)
	setVisibilityExpect(t, rc, []string{"Z"}, 0, 0)
	checkReachability(ctx, t, rc, "Z", enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, checkedClosedWorkflowExecutionsForUpstreamMiss)

	// reachability(ZZ) --> unreachable (despite upstream running wf execution Y, rule excluded due to recent delete)
	setVisibilityExpect(t, rc, []string{"ZZ"}, 0, 0)
	checkReachability(ctx, t, rc, "ZZ", enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, checkedClosedWorkflowExecutionsForUpstreamMiss)
}

// test reachability via deleted rules within the rule propagation delay
func TestGetReachability_WithoutVisibility_WithDeletedRules(t *testing.T) {
	t.Skip()
	// todo carly
}

func checkReachability(ctx context.Context,
	t *testing.T,
	rc *reachabilityCalculator,
	buildId string,
	expectedReachability enumspb.BuildIdTaskReachability,
	expectedExitPoint reachabilityExitPoint,
) {
	// check that getBuildIdTaskReachability works (generates expected reachability and logs expected exit point)
	metricsHandler := metricstest.NewCaptureHandler()
	metricsCapture := metricsHandler.StartCapture()
	logger := log.NewTestLogger()
	reachability, err := getBuildIdTaskReachability(ctx, rc, metricsHandler, logger, buildId)
	assert.Nil(t, err)
	assert.Equal(t, expectedReachability, reachability)
	snapshot := metricsCapture.Snapshot()
	counterRecordings := snapshot[metrics.ReachabilityExitPointCounter.Name()]
	assert.Equal(t, len(counterRecordings), 1)
	assert.Equal(t, int64(1), counterRecordings[0].Value.(int64))
	assert.Equal(t, reachabilityExitPoint2TagValue[expectedExitPoint], counterRecordings[0].Tags[reachabilityExitPointTagName])

	// check that rc.run works (don't check exit point this time because cache will be warm)
	reachability, _, err = rc.run(ctx, buildId)
	assert.Nil(t, err)
	assert.Equal(t, expectedReachability, reachability)
}

// setVisibilityExpect resets the cache and sets the expected visibility results for the mock visibility inside it
func setVisibilityExpect(t *testing.T,
	rc *reachabilityCalculator,
	buildIdsOfInterest []string,
	countOpen, countClosed int64) {
	queryOpen := rc.makeBuildIdQuery(buildIdsOfInterest, true)
	queryClosed := rc.makeBuildIdQuery(buildIdsOfInterest, false)
	vm := manager.NewMockVisibilityManager(gomock.NewController(t))
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), rc.makeBuildIdCountRequest(queryOpen)).MaxTimes(2).Return(mkCountResponse(countOpen))
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), rc.makeBuildIdCountRequest(queryClosed)).MaxTimes(2).Return(mkCountResponse(countClosed))
	rc.cache.visibilityMgr = vm
}

func mkCountResponse(count int64) (*manager.CountWorkflowExecutionsResponse, error) {
	return &manager.CountWorkflowExecutionsResponse{
		Count:  count,
		Groups: nil,
	}, nil
}

func mkTestReachabilityCalculatorWithEmptyVisibility(t *testing.T) *testReachabilityCalculator {
	cacheMetricsHandler := metricstest.NewCaptureHandler()
	vm := manager.NewMockVisibilityManager(gomock.NewController(t))
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).AnyTimes().Return(mkCountResponse(0))

	return &testReachabilityCalculator{
		rc: &reachabilityCalculator{
			nsID:                         "test-namespace-id",
			nsName:                       "test-namespace",
			taskQueue:                    "test-reachability-tq",
			buildIdVisibilityGracePeriod: testBuildIdVisibilityGracePeriod,
			cache: newReachabilityCache(
				cacheMetricsHandler,
				vm,
				testReachabilityCacheOpenWFsTTL,
				testReachabilityCacheClosedWFsTTL,
			),
		},
		capture: cacheMetricsHandler.StartCapture(),
	}
}
