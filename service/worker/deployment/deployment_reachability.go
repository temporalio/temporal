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
	"github.com/temporalio/sqlparser"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	visibility_manager "go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/worker_versioning"
	"time"
)

const (
	reachabilityCacheOpenWFsTTL   = time.Minute
	reachabilityCacheClosedWFsTTL = 10 * time.Minute
	reachabilityCacheMaxSize      = 10000
)

/*
const (
	// Reachability level is not specified.
	DEPLOYMENT_REACHABILITY_UNSPECIFIED DeploymentReachability = 0
	// The deployment is reachable by new and/or open workflows. The deployment cannot be
	// decommissioned safely.
	DEPLOYMENT_REACHABILITY_REACHABLE DeploymentReachability = 1
	// The deployment is not reachable by new or open workflows, but might be still needed by
	// Queries sent to closed workflows. The deployment can be decommissioned safely if user does
	// not query closed workflows.
	DEPLOYMENT_REACHABILITY_CLOSED_WORKFLOWS_ONLY DeploymentReachability = 2
	// The deployment is not reachable by any workflow because all the workflows who needed this
	// deployment went out of retention period. The deployment can be decommissioned safely.
	DEPLOYMENT_REACHABILITY_UNREACHABLE DeploymentReachability = 3
)
*/

func getDeploymentReachability(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	seriesName, buildID, currentBuildID string,
	currentBuildIDValidTime time.Time,
	cache reachabilityCache,
) (enumspb.DeploymentReachability, time.Time, error) {
	// 1a. Reachable by new unpinned workflows
	if buildID == currentBuildID { // add if buildID is ramping, once we have ramp
		return enumspb.DEPLOYMENT_REACHABILITY_REACHABLE, currentBuildIDValidTime, nil
	}

	// 2a. Reachable by open pinned workflows
	countRequest := visibility_manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespaceEntry.ID(),
		Namespace:   namespaceEntry.Name(),
		Query:       makeDeploymentQuery(seriesName, buildID, true),
	}
	exists, lastUpdateTime, err := cache.Get(ctx, countRequest, true)
	if err != nil {
		return enumspb.DEPLOYMENT_REACHABILITY_UNSPECIFIED, time.Time{}, err
	}
	if exists {
		return enumspb.DEPLOYMENT_REACHABILITY_REACHABLE, lastUpdateTime, nil
	}

	// 3. Reachable by closed pinned workflows
	countRequest = visibility_manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespaceEntry.ID(),
		Namespace:   namespaceEntry.Name(),
		Query:       makeDeploymentQuery(seriesName, buildID, false),
	}
	exists, lastUpdateTime, err = cache.Get(ctx, countRequest, false)
	if err != nil {
		return enumspb.DEPLOYMENT_REACHABILITY_UNSPECIFIED, time.Time{}, err
	}
	if exists {
		return enumspb.DEPLOYMENT_REACHABILITY_CLOSED_WORKFLOWS_ONLY, lastUpdateTime, nil
	}

	// 4. Unreachable
	return enumspb.DEPLOYMENT_REACHABILITY_UNREACHABLE, time.Now(), nil
}

func makeDeploymentQuery(seriesName, buildID string, open bool) string {
	var statusFilter string
	deploymentFilter := sqlparser.String(sqlparser.NewStrVal([]byte(
		worker_versioning.ReachabilityBuildIdSearchAttribute(enumspb.VERSIONING_BEHAVIOR_PINNED, &deploymentpb.Deployment{
			SeriesName: seriesName,
			BuildId:    buildID,
		}),
	)))
	if open {
		statusFilter = `= "Running"`
	} else {
		statusFilter = `!= "Running"`
	}
	// todo (carly): handle null / unversioned
	return fmt.Sprintf("%s = %s AND %s %s", searchattribute.BuildIds, deploymentFilter, searchattribute.ExecutionStatus, statusFilter)
}

/*
In-memory Reachability Cache of Visibility Queries and Results
*/

type reachabilityCache struct {
	openWFCache    cache.Cache
	closedWFCache  cache.Cache // these are separate due to allow for different TTL
	metricsHandler metrics.Handler
	visibilityMgr  visibility_manager.VisibilityManager
}

type reachabilityCacheValue struct {
	exists         bool
	lastUpdateTime time.Time
}

func newReachabilityCache(
	handler metrics.Handler,
	visibilityMgr visibility_manager.VisibilityManager,
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
	countRequest visibility_manager.CountWorkflowExecutionsRequest,
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
func (c *reachabilityCache) Put(key visibility_manager.CountWorkflowExecutionsRequest, val reachabilityCacheValue, open bool) {
	if open {
		c.openWFCache.Put(key, val)
	} else {
		c.closedWFCache.Put(key, val)
	}
}
