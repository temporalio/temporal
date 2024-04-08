package matching

import (
	"context"
	"fmt"
	"go.temporal.io/server/api/clock/v1"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/types/known/durationpb"
	"strings"

	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/worker_versioning"
)

func getBuildIdTaskReachability(
	ctx context.Context,
	data *persistencespb.VersioningData,
	visibilityMgr manager.VisibilityManager,
	nsID namespace.ID,
	nsName namespace.Name,
	taskQueue,
	buildId string,
) (enumspb.BuildIdTaskReachability, error) {
	assignmentRules := data.GetAssignmentRules()
	redirectRules := data.GetRedirectRules()

	// 1. Easy UNREACHABLE case
	if isActiveRedirectRuleSource(buildId, redirectRules) {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, nil
	}

	// Gather list of all build ids that could point to buildId
	buildIdsOfInterest := append(getUpstreamBuildIds(buildId, redirectRules, false), buildId)

	// 2. Cases for REACHABLE
	// 2a. If buildId is assignable to new tasks
	for _, bid := range buildIdsOfInterest {
		if isReachableActiveAssignmentRuleTarget(bid, assignmentRules) {
			return enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, nil
		}
	}

	// 2b. If buildId could be reached from the backlog
	if existsBacklog, err := existsBackloggedActivityOrWFTaskAssignedToAny(ctx, nsID, nsName, taskQueue, buildIdsOfInterest); err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	} else if existsBacklog {
		return enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, nil
	}

	// Note: The below cases are not applicable to activity-only task queues, since we don't record those in visibility

	// Gather list of all build ids that could point to buildId, now including deleted rules to account for the delay in updating visibility
	buildIdsOfInterest = append(getUpstreamBuildIds(buildId, redirectRules, true), buildId)

	// 2c. If buildId is assignable to tasks from open workflows
	existsOpenWFAssignedToBuildId, err := existsWFAssignedToAny(ctx, visibilityMgr, makeBuildIdCountRequest(nsID, nsName, buildIdsOfInterest, taskQueue, true))
	if err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	}
	if existsOpenWFAssignedToBuildId {
		return enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, nil
	}

	// 3. Cases for CLOSED_WORKFLOWS_ONLY
	existsClosedWFAssignedToBuildId, err := existsWFAssignedToAny(ctx, visibilityMgr, makeBuildIdCountRequest(nsID, nsName, buildIdsOfInterest, taskQueue, false))
	if err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	}
	if existsClosedWFAssignedToBuildId {
		return enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY, nil
	}

	// 4. Otherwise, UNREACHABLE
	return enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, nil
}

// getUpstreamBuildIds returns a list of build ids that point to the given buildId in the graph of redirect rules.
// It considers rules if the deletion time is within versioningReachabilityDeletedRuleInclusionPeriod
// This list will be used to query visibility, and it;s
// It is important to avoid false-negative reachability results, so we need to be sure that we
func getUpstreamBuildIds(
	buildId string,
	redirectRules []*persistencespb.RedirectRule,
	includeRecentlyDeleted bool) []string {
	return getUpstreamHelper(buildId, redirectRules, includeRecentlyDeleted, nil)
}

func getUpstreamHelper(
	buildId string,
	redirectRules []*persistencespb.RedirectRule,
	includeRecentlyDeleted bool,
	visited []string) []string {
	var upstream []string
	visited = append(visited, buildId)
	directSources := getSourcesForTarget(buildId, redirectRules, includeRecentlyDeleted)

	for _, src := range directSources {
		if !slices.Contains(visited, src) {
			upstream = append(upstream, src)
			upstream = append(upstream, getUpstreamHelper(src, redirectRules, includeRecentlyDeleted, visited)...)
		}
	}

	// dedupe
	upstreamUnique := make(map[string]bool)
	for _, bid := range upstream {
		upstreamUnique[bid] = true
	}
	upstream = make([]string, 0)
	for k := range upstreamUnique {
		upstream = append(upstream, k)
	}
	return upstream
}

// getSourcesForTarget gets the first-degree sources for any redirect rule targeting buildId
func getSourcesForTarget(buildId string, redirectRules []*persistencespb.RedirectRule, includeRecentlyDeleted bool) []string {
	var sources []string
	for _, rr := range redirectRules {
		if rr.GetRule().GetTargetBuildId() == buildId &&
			withinDeletedRuleInclusionPeriod(rr.GetDeleteTimestamp(), durationpb.New(versioningReachabilityDeletedRuleInclusionPeriod)) {
			sources = append(sources, rr.GetRule().GetSourceBuildId())
		}
	}

	return sources
}

func withinDeletedRuleInclusionPeriod(clk *clock.HybridLogicalClock, period *durationpb.Duration) bool {
	return clk == nil || (period != nil && hlc.Since(clk) <= period.AsDuration())
}

func existsBackloggedActivityOrWFTaskAssignedToAny(ctx context.Context,
	nsID namespace.ID,
	nsName namespace.Name,
	taskQueue string,
	buildIdsOfInterest []string,
) (bool, error) {
	// todo backlog
	return false, nil
}

func isReachableActiveAssignmentRuleTarget(buildId string, assignmentRules []*persistencespb.AssignmentRule) bool {
	for _, r := range getActiveAssignmentRules(assignmentRules) {
		if r.GetRule().GetTargetBuildId() == buildId {
			return true
		}
		if r.GetRule().GetPercentageRamp() == nil {
			// rules after an unconditional rule will not be reached
			break
		}
	}
	return false
}

func existsWFAssignedToAny(
	ctx context.Context,
	visibilityMgr manager.VisibilityManager,
	countRequest *manager.CountWorkflowExecutionsRequest,
) (bool, error) {
	countResponse, err := visibilityMgr.CountWorkflowExecutions(ctx, countRequest)
	if err != nil {
		return false, err
	}
	return countResponse.Count > 0, nil
}

func makeBuildIdCountRequest(
	nsID namespace.ID,
	nsName namespace.Name,
	buildIdsOfInterest []string,
	taskQueue string,
	open bool,
) *manager.CountWorkflowExecutionsRequest {
	return &manager.CountWorkflowExecutionsRequest{
		NamespaceID: nsID,
		Namespace:   nsName,
		Query:       makeBuildIdQuery(buildIdsOfInterest, taskQueue, open),
	}
}

func makeBuildIdQuery(
	buildIdsOfInterest []string,
	taskQueue string,
	open bool,
) string {
	escapedTaskQueue := sqlparser.String(sqlparser.NewStrVal([]byte(taskQueue)))
	var statusFilter string
	var escapedBuildIds []string
	var includeNull bool
	if open {
		statusFilter = fmt.Sprintf(` AND %s = "Running"`, searchattribute.ExecutionStatus)
		// want: currently assigned to that build-id
		// (b1, b2) --> (assigned:b1, assigned:b2)
		// (b1, b2, "") --> (assigned:b1, assigned:b2, unversioned, null)
		// ("") --> (unversioned, null)
		for _, bid := range buildIdsOfInterest {
			if bid == "" {
				escapedBuildIds = append(escapedBuildIds, sqlparser.String(sqlparser.NewStrVal([]byte(worker_versioning.UnversionedSearchAttribute))))
				includeNull = true
			} else {
				escapedBuildIds = append(escapedBuildIds, sqlparser.String(sqlparser.NewStrVal([]byte(worker_versioning.AssignedBuildIdSearchAttribute(bid)))))
			}
		}
	} else {
		statusFilter = fmt.Sprintf(` AND %s != "Running"`, searchattribute.ExecutionStatus)
		// want: closed AT that build id, and once used that build id
		// (b1, b2) --> (versioned:b1, versioned:b2)
		// (b1, b2, "") --> (versioned:b1, versioned:b2, unversioned, null)
		// ("") --> (unversioned, null)
		for _, bid := range buildIdsOfInterest {
			if bid == "" {
				escapedBuildIds = append(escapedBuildIds, sqlparser.String(sqlparser.NewStrVal([]byte(worker_versioning.UnversionedSearchAttribute))))
				includeNull = true
			} else {
				escapedBuildIds = append(escapedBuildIds, sqlparser.String(sqlparser.NewStrVal([]byte(worker_versioning.VersionedBuildIdSearchAttribute(bid)))))
			}
		}
	}
	buildIdsFilter := fmt.Sprintf("%s IN (%s)", searchattribute.BuildIds, strings.Join(escapedBuildIds, ","))
	if includeNull {
		buildIdsFilter = fmt.Sprintf("(%s IS NULL OR %s)", searchattribute.BuildIds, buildIdsFilter)
	}
	return fmt.Sprintf("%s = %s AND %s%s", searchattribute.TaskQueue, escapedTaskQueue, buildIdsFilter, statusFilter)
}
