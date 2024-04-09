package matching

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/api/clock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/worker_versioning"
)

type reachabilityCalculator struct {
	visibilityMgr                manager.VisibilityManager
	nsID                         namespace.ID
	nsName                       namespace.Name
	taskQueue                    string
	assignmentRules              []*persistencespb.AssignmentRule
	redirectRules                []*persistencespb.RedirectRule
	buildIdVisibilityGracePeriod time.Duration
}

func getBuildIdTaskReachability(
	ctx context.Context,
	data *persistencespb.VersioningData,
	visibilityMgr manager.VisibilityManager,
	nsID,
	nsName,
	taskQueue,
	buildId string,
	buildIdVisibilityGracePeriod time.Duration,
) (enumspb.BuildIdTaskReachability, error) {
	rc := &reachabilityCalculator{
		visibilityMgr:                visibilityMgr,
		nsID:                         namespace.ID(nsID),
		nsName:                       namespace.Name(nsName),
		taskQueue:                    taskQueue,
		assignmentRules:              data.GetAssignmentRules(),
		redirectRules:                data.GetRedirectRules(),
		buildIdVisibilityGracePeriod: buildIdVisibilityGracePeriod,
	}

	return rc.getReachability(ctx, buildId)
}

func (rc *reachabilityCalculator) getReachability(ctx context.Context, buildId string) (enumspb.BuildIdTaskReachability, error) {
	// 1. Easy UNREACHABLE case
	if isActiveRedirectRuleSource(buildId, rc.redirectRules) {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, nil
	}

	// Gather list of all build ids that could point to buildId
	buildIdsOfInterest := rc.getBuildIdsOfInterest(buildId, false)

	// 2. Cases for REACHABLE
	// 2a. If buildId is assignable to new tasks
	for _, bid := range buildIdsOfInterest {
		if rc.isReachableActiveAssignmentRuleTargetOrDefault(bid) {
			return enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, nil
		}
	}

	// 2b. If buildId could be reached from the backlog
	if existsBacklog, err := rc.existsBackloggedActivityOrWFTaskAssignedToAny(ctx, buildIdsOfInterest); err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	} else if existsBacklog {
		return enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, nil
	}

	// Note: The below cases are not applicable to activity-only task queues, since we don't record those in visibility

	// Gather list of all build ids that could point to buildId, now including deleted rules to account for the delay in updating visibility
	buildIdsOfInterest = rc.getBuildIdsOfInterest(buildId, true)

	// 2c. If buildId is assignable to tasks from open workflows
	existsOpenWFAssignedToBuildId, err := rc.existsWFAssignedToAny(ctx, rc.makeBuildIdCountRequest(buildIdsOfInterest, true))
	if err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	}
	if existsOpenWFAssignedToBuildId {
		return enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, nil
	}

	// 3. Cases for CLOSED_WORKFLOWS_ONLY
	existsClosedWFAssignedToBuildId, err := rc.existsWFAssignedToAny(ctx, rc.makeBuildIdCountRequest(buildIdsOfInterest, false))
	if err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	}
	if existsClosedWFAssignedToBuildId {
		return enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY, nil
	}

	// 4. Otherwise, UNREACHABLE
	return enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, nil
}

// getBuildIdsOfInterest returns a list of build ids that point to the given buildId in the graph of redirect rules
// and adds the given build id to that list.
// It considers rules if the deletion time is within versioningReachabilityDeletedRuleInclusionPeriod
// This list will be used to query visibility, and it;s
// It is important to avoid false-negative reachability results, so we need to be sure that we
func (rc *reachabilityCalculator) getBuildIdsOfInterest(
	buildId string,
	includeRecentlyDeleted bool) []string {
	return append(rc.getUpstreamHelper(buildId, includeRecentlyDeleted, nil), buildId)
}

func (rc *reachabilityCalculator) getUpstreamHelper(
	buildId string,
	includeRecentlyDeleted bool,
	visited []string) []string {
	var upstream []string
	visited = append(visited, buildId)
	directSources := rc.getSourcesForTarget(buildId, includeRecentlyDeleted)

	for _, src := range directSources {
		if !slices.Contains(visited, src) {
			upstream = append(upstream, src)
			upstream = append(upstream, rc.getUpstreamHelper(src, includeRecentlyDeleted, visited)...)
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
func (rc *reachabilityCalculator) getSourcesForTarget(buildId string, includeRecentlyDeleted bool) []string {
	var sources []string
	for _, rr := range rc.redirectRules {
		if rr.GetRule().GetTargetBuildId() == buildId &&
			((!includeRecentlyDeleted && rr.GetDeleteTimestamp() == nil) ||
				(includeRecentlyDeleted && rc.withinDeletedRuleInclusionPeriod(rr.GetDeleteTimestamp()))) {
			sources = append(sources, rr.GetRule().GetSourceBuildId())
		}
	}

	return sources
}

func (rc *reachabilityCalculator) withinDeletedRuleInclusionPeriod(clk *clock.HybridLogicalClock) bool {
	return clk == nil || hlc.Since(clk) <= rc.buildIdVisibilityGracePeriod
}

func (rc *reachabilityCalculator) existsBackloggedActivityOrWFTaskAssignedToAny(ctx context.Context, buildIdsOfInterest []string) (bool, error) {
	// todo backlog
	return false, nil
}

func (rc *reachabilityCalculator) isReachableActiveAssignmentRuleTargetOrDefault(buildId string) bool {
	foundUnconditionalRule := false
	for _, r := range getActiveAssignmentRules(rc.assignmentRules) {
		if r.GetRule().GetTargetBuildId() == buildId {
			return true
		}
		if r.GetRule().GetPercentageRamp() == nil {
			// rules after an unconditional rule will not be reached
			foundUnconditionalRule = true
			break
		}
	}
	if !foundUnconditionalRule && buildId == "" {
		// unversioned is the default, and is reachable
		return true
	}
	return false
}

func (rc *reachabilityCalculator) existsWFAssignedToAny(
	ctx context.Context,
	countRequest *manager.CountWorkflowExecutionsRequest,
) (bool, error) {
	countResponse, err := rc.visibilityMgr.CountWorkflowExecutions(ctx, countRequest)
	if err != nil {
		return false, err
	}
	return countResponse.Count > 0, nil
}

func (rc *reachabilityCalculator) makeBuildIdCountRequest(
	buildIdsOfInterest []string,
	open bool,
) *manager.CountWorkflowExecutionsRequest {
	slices.Sort(buildIdsOfInterest)
	return &manager.CountWorkflowExecutionsRequest{
		NamespaceID: rc.nsID,
		Namespace:   rc.nsName,
		Query:       rc.makeBuildIdQuery(buildIdsOfInterest, open),
	}
}

func (rc *reachabilityCalculator) makeBuildIdQuery(
	buildIdsOfInterest []string,
	open bool,
) string {
	escapedTaskQueue := sqlparser.String(sqlparser.NewStrVal([]byte(rc.taskQueue)))
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

// getDefaultBuildId gets the build id mentioned in the first unconditional Assignment Rule.
// If there is no default Build ID, the result for the unversioned queue will be returned.
// This should only be called on the root.
func getDefaultBuildId(assignmentRules []*persistencespb.AssignmentRule) string {
	for _, ar := range getActiveAssignmentRules(assignmentRules) {
		if isUnconditional(ar.GetRule()) {
			return ar.GetRule().GetTargetBuildId()
		}
	}
	return ""
}
