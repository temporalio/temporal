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
	"go.temporal.io/server/common/util"
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

	return rc.run(ctx, buildId)
}

func (rc *reachabilityCalculator) run(ctx context.Context, buildId string) (enumspb.BuildIdTaskReachability, error) {
	// 1. Easy UNREACHABLE case
	if isActiveRedirectRuleSource(buildId, rc.redirectRules) {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, nil
	}

	// Gather list of all build ids that could point to buildId
	buildIdsOfInterest := rc.getBuildIdsOfInterest(buildId, time.Duration(0))

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
	buildIdsOfInterest = rc.getBuildIdsOfInterest(buildId, rc.buildIdVisibilityGracePeriod)

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

// getBuildIdsOfInterest returns a list of build ids that point to the given buildId in the graph
// of redirect rules and adds the given build ID to that list.
// It considers rules if the deletion time is nil or within the given deletedRuleInclusionPeriod.
func (rc *reachabilityCalculator) getBuildIdsOfInterest(
	buildId string,
	deletedRuleInclusionPeriod time.Duration) []string {

	withinRuleInclusionPeriod := func(clk *clock.HybridLogicalClock) bool {
		if clk == nil {
			return true
		}
		return hlc.Since(clk) <= deletedRuleInclusionPeriod
	}

	includedRules := util.FilterSlice(slices.Clone(rc.redirectRules), func(rr *persistencespb.RedirectRule) bool {
		return rr.DeleteTimestamp == nil || withinRuleInclusionPeriod(rr.DeleteTimestamp)
	})

	return append(getUpstreamBuildIds(buildId, includedRules), buildId)
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
		// want: closed AT that build ID, and once used that build ID
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

// getDefaultBuildId gets the build ID mentioned in the first unconditional Assignment Rule.
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
