// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2024 Uber Technologies, Inc.
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

package deployment

import (
	"context"
	"fmt"
	"time"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/worker_versioning"
)

const (
	reachabilityCacheOpenWFsTTL   = time.Minute
	reachabilityCacheClosedWFsTTL = 10 * time.Minute
	reachabilityCacheMaxSize      = 10000
)

// We update the reachability search attribute on workflow task completion and when there is
// an UpdateWorkflowExecutionOptions request that changes the workflow's effective behavior
// or deployment. We do not update the reachability search attribute if the search attribute
// has not changed. We also do not update the reachability search attribute if a workflow has
// started transitioning to a new deployment, because only on task completion do we know whether
// the transition has succeeded or failed.
func getDeploymentReachability(
	ctx context.Context,
	nsId, nsName, seriesName, buildId string,
	isCurrent bool,
	rc reachabilityCache,
) (enumspb.DeploymentReachability, time.Time, error) {
	// 1a. Reachable by new unpinned workflows
	if isCurrent {
		// TODO (carly) part 2: still return reachable if the deployment just became not current, but workflows started on it are not yet in reachability
		return enumspb.DEPLOYMENT_REACHABILITY_REACHABLE, time.Now(), nil
	}

	// 2a. Reachable by open pinned workflows
	countRequest := makeCountRequest(nsId, nsName, seriesName, buildId, true)
	exists, lastUpdateTime, err := rc.Get(ctx, countRequest, true)
	if err != nil {
		return enumspb.DEPLOYMENT_REACHABILITY_UNSPECIFIED, time.Time{}, err
	}
	if exists {
		return enumspb.DEPLOYMENT_REACHABILITY_REACHABLE, lastUpdateTime, nil
	}

	// 3. Reachable by closed pinned workflows
	countRequest = makeCountRequest(nsId, nsName, seriesName, buildId, false)
	exists, lastUpdateTime, err = rc.Get(ctx, countRequest, false)
	if err != nil {
		return enumspb.DEPLOYMENT_REACHABILITY_UNSPECIFIED, time.Time{}, err
	}
	if exists {
		return enumspb.DEPLOYMENT_REACHABILITY_CLOSED_WORKFLOWS_ONLY, lastUpdateTime, nil
	}

	// 4. Unreachable
	return enumspb.DEPLOYMENT_REACHABILITY_UNREACHABLE, time.Now(), nil
}

func makeCountRequest(
	namespaceId, namespaceName, seriesName, buildId string, open bool,
) manager.CountWorkflowExecutionsRequest {
	return manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID(namespaceId),
		Namespace:   namespace.Name(namespaceName),
		Query:       makeDeploymentQuery(seriesName, buildId, open),
	}
}

func makeDeploymentQuery(seriesName, buildID string, open bool) string {
	var statusFilter string
	deploymentFilter := fmt.Sprintf("= '%s'", worker_versioning.PinnedBuildIdSearchAttribute(&deploymentpb.Deployment{
		SeriesName: seriesName,
		BuildId:    buildID,
	}))
	if open {
		statusFilter = "= 'Running'"
	} else {
		statusFilter = "!= 'Running'"
	}
	// todo (carly) part 2: handle null search attribute / unversioned deployment
	return fmt.Sprintf("%s %s AND %s %s", searchattribute.BuildIds, deploymentFilter, searchattribute.ExecutionStatus, statusFilter)
}

/*
In-memory Reachability Cache of Visibility Queries and Results
*/

type reachabilityCache struct {
	openWFCache    cache.Cache
	closedWFCache  cache.Cache // these are separate due to allow for different TTL
	metricsHandler metrics.Handler
	visibilityMgr  manager.VisibilityManager
}

type reachabilityCacheValue struct {
	exists         bool
	lastUpdateTime time.Time
}

func newReachabilityCache(
	handler metrics.Handler,
	visibilityMgr manager.VisibilityManager,
	reachabilityCacheOpenWFExecutionTTL,
	reachabilityCacheClosedWFExecutionTTL time.Duration,
) reachabilityCache {
	return reachabilityCache{
		openWFCache:    cache.New(reachabilityCacheMaxSize, &cache.Options{TTL: reachabilityCacheOpenWFExecutionTTL}),
		closedWFCache:  cache.New(reachabilityCacheMaxSize, &cache.Options{TTL: reachabilityCacheClosedWFExecutionTTL}),
		metricsHandler: handler,
		visibilityMgr:  visibilityMgr,
	}
}

// Get retrieves the Workflow Count existence value and update time based on the query-string key.
func (c *reachabilityCache) Get(
	ctx context.Context,
	countRequest manager.CountWorkflowExecutionsRequest,
	open bool,
) (exists bool, lastUpdateTime time.Time, err error) {
	// try cache
	var result interface{}
	if open {
		result = c.openWFCache.Get(countRequest)
	} else {
		result = c.closedWFCache.Get(countRequest)
	}
	if result != nil {
		// there's no reason that the cache would ever contain a non-reachabilityCacheValue, but just in case, treat non-bool as a miss
		val, ok := result.(reachabilityCacheValue)
		if ok {
			return val.exists, val.lastUpdateTime, nil
		}
	}

	// cache was cold, ask visibility and put result in cache
	countResponse, err := c.visibilityMgr.CountWorkflowExecutions(ctx, &countRequest)
	if err != nil {
		return false, time.Time{}, err
	}
	exists = countResponse.Count > 0
	lastUpdateTime = time.Now()
	c.Put(countRequest, reachabilityCacheValue{exists, lastUpdateTime}, open)
	return exists, lastUpdateTime, nil
}

// Put adds an element to the cache.
func (c *reachabilityCache) Put(key manager.CountWorkflowExecutionsRequest, val reachabilityCacheValue, open bool) {
	if open {
		c.openWFCache.Put(key, val)
	} else {
		c.closedWFCache.Put(key, val)
	}
}
