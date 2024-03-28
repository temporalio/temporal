package matching

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

// getReachability specifies which category of tasks may reach a versioned worker of a certain Build ID.
//   - BUILD_ID_TASK_REACHABILITY_UNSPECIFIED: Task reachability is not reported
//   - BUILD_ID_TASK_REACHABILITY_REACHABLE: Build ID may be used by new workflows or activities (base on versioning rules), or there are open workflows or backlogged activities assigned to it.
//   - BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY: Build ID does not have open workflows and is not reachable by new workflows, but MAY have closed workflows within the namespace retention period. Not applicable to activity-only task queues.
//   - BUILD_ID_TASK_REACHABILITY_UNREACHABLE: Build ID is not used for new executions, nor it has been used by any existing execution within the retention period.
func getReachability(e *matchingEngineImpl, ctx context.Context, buildId string) (enumspb.BuildIdTaskReachability, error) {
	// todo carly
	/*
		def getReachability(buildId) enumspb.BuildIdTaskReachability {
			if assignableToNewTasks(buildId) || ∃ openWFAssignedTo(buildId) || ∃ backloggedActivitiesAssignedTo(buildId) {
				return BUILD_ID_TASK_REACHABILITY_REACHABLE
			} else if ∃ closedWFAssignedTo(buildId) {
				// not sure what "MAY" means here
				// not sure why "Not applicable to activity-only task queues"
				return BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY
			} else {
				return BUILD_ID_TASK_REACHABILITY_UNREACHABLE
			}
		}

		def assignableToNewTasks(buildId) bool {
			if buildId is in an assignment rule, return true
			if buildId is the target of a redirect rule and the source id for that redirect rule is reachable*, return true
				*Q: what type of reachable?
			else return false
		}

		def openWFAssignedTo(buildId) bool {
			query visibility for open WF executions with "assigned:buildId"
			if len(resp.Executions) > 0, return true
		}

		def backloggedActivitiesAssignedTo(buildId) bool {
			// no clue how to find this out
		}

		def closedWFAssignedTo(buildId) bool {
			query visibility for closed WF executions with "assigned:buildId"
			if len(resp.Executions) > 0, return true
		}
	*/

	openResp, err := e.visibilityManager.ListOpenWorkflowExecutionsByVersion(ctx, &manager.ListWorkflowExecutionsByVersionSARequest{
		ListWorkflowExecutionsRequest: &manager.ListWorkflowExecutionsRequest{},
		VersionSearchAttribute:        "todo",
	})
	if err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	}
	closedResp, err := e.visibilityManager.ListClosedWorkflowExecutionsByVersion(ctx, &manager.ListWorkflowExecutionsByVersionSARequest{
		ListWorkflowExecutionsRequest: &manager.ListWorkflowExecutionsRequest{},
		VersionSearchAttribute:        "todo",
	})
	if err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	}

	return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, nil
}
