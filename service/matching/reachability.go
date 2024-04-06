package matching

import (
	"context"
	"fmt"
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
	nsID,
	nsName,
	taskQueue,
	buildId string,
) (enumspb.BuildIdTaskReachability, error) {
	assignmentRules := data.GetAssignmentRules()
	redirectRules := data.GetRedirectRules()

	// 1. Easy UNREACHABLE case
	if isRedirectRuleSource(buildId, redirectRules) {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, nil
	}

	// Gather list of all build ids that could point to buildId -> upstreamBuildIds
	upstreamBuildIds := getUpstreamBuildIds(buildId, redirectRules)
	buildIdsOfInterest := append(upstreamBuildIds, buildId)

	// 2. Cases for REACHABLE
	// 2a. If buildId is assignable to new tasks
	for _, bid := range buildIdsOfInterest {
		if isReachableAssignmentRuleTarget(bid, assignmentRules) {
			return enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, nil
		}
	}

	// 2b. If buildId could be reached from the backlog
	if existsBacklog, err := existsBackloggedActivityOrWFAssignedToAny(ctx, nsID, nsName, taskQueue, buildIdsOfInterest); err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	} else if existsBacklog {
		return enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, nil
	}

	// Note: The below cases are not applicable to activity-only task queues, since we don't record those in visibility

	// 2c. If buildId is assignable to tasks from open workflows
	existsOpenWFAssignedToBuildId, err := existsWFAssignedToAny(ctx, visibilityMgr, nsID, nsName, makeBuildIdQuery(buildIdsOfInterest, taskQueue, true))
	if err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	}
	if existsOpenWFAssignedToBuildId {
		return enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, nil
	}

	// 3. Cases for CLOSED_WORKFLOWS_ONLY
	existsClosedWFAssignedToBuildId, err := existsWFAssignedToAny(ctx, visibilityMgr, nsID, nsName, makeBuildIdQuery(buildIdsOfInterest, taskQueue, false))
	if err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	}
	if existsClosedWFAssignedToBuildId {
		return enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY, nil
	}

	// 4. Otherwise, UNREACHABLE
	return enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, nil
}

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

Assignment Rules:
[ (3, 50%), (2, nil) ]
*/
func getUpstreamBuildIds(
	buildId string,
	redirectRules []*persistencespb.RedirectRule) []string {
	var upstream []string
	directSources := getSourcesForTarget(buildId, redirectRules)

	for _, src := range directSources {
		upstream = append(upstream, src)
		upstream = append(upstream, getUpstreamBuildIds(src, redirectRules)...)
	}

	return upstream
}

// getSourcesForTarget gets the first-degree sources for any redirect rule targeting buildId
func getSourcesForTarget(buildId string, redirectRules []*persistencespb.RedirectRule) []string {
	var sources []string
	for _, rr := range redirectRules {
		if rr.GetRule().GetTargetBuildId() == buildId {
			sources = append(sources, rr.GetRule().GetSourceBuildId())
		}
	}

	return sources
}

func existsBackloggedActivityOrWFAssignedToAny(ctx context.Context,
	nsID,
	nsName,
	taskQueue string,
	buildIdsOfInterest []string,
) (bool, error) {
	// todo backlog
	return false, nil
}

func isReachableAssignmentRuleTarget(buildId string, assignmentRules []*persistencespb.AssignmentRule) bool {
	for _, r := range assignmentRules {
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
	nsID,
	nsName,
	query string,
) (bool, error) {
	countResponse, err := visibilityMgr.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID(nsID),
		Namespace:   namespace.Name(nsName),
		Query:       query,
	})
	if err != nil {
		return false, err
	}
	return countResponse.Count > 0, nil
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
