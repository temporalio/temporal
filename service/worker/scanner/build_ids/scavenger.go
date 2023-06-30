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

package build_ids

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/worker_versioning"
)

const (
	BuildIdScavangerWorkflowName = "build-id-scavenger"
	BuildIdScavangerActivityName = "scavenge-build-ids"

	BuildIdScavengerWFID          = "temporal-sys-build-id-scavenger"
	BuildIdScavengerTaskQueueName = "temporal-sys-build-id-scavenger-taskqueue-0"
)

var (
	BuildIdScavengerWFStartOptions = client.StartWorkflowOptions{
		ID:                    BuildIdScavengerWFID,
		TaskQueue:             BuildIdScavengerTaskQueueName,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		CronSchedule:          "0 */12 * * *",
	}
)

type (
	BuildIdScavangerInput struct {
		NamespaceListPageSize int
		TaskQueueListPageSize int
	}

	Activities struct {
		logger             log.Logger
		taskManager        persistence.TaskManager
		metadataManager    persistence.MetadataManager
		visibilityManager  manager.VisibilityManager
		namespaceRegistry  namespace.Registry
		matchingClient     matchingservice.MatchingServiceClient
		currentClusterName string
		// Minimum duration since a build id was last default in its containing set for it to be considered for removal.
		// If a build id was still default recently, there may be:
		// 1. workers with that identifier processing tasks
		// 2. workflows with that identifier that have yet to be indexed in visibility
		// The scavenger should allow enough time to pass before cleaning these build ids.
		removableBuildIdDurationSinceDefault dynamicconfig.DurationPropertyFn
		buildIdScavengerVisibilityRPS        dynamicconfig.FloatPropertyFn
	}

	heartbeatDetails struct {
		NamespaceIdx           int
		TaskQueueIdx           int
		NamespaceNextPageToken []byte
		TaskQueueNextPageToken []byte
	}
)

func NewActivities(
	logger log.Logger,
	taskManager persistence.TaskManager,
	metadataManager persistence.MetadataManager,
	visibilityManager manager.VisibilityManager,
	namespaceRegistry namespace.Registry,
	matchingClient matchingservice.MatchingServiceClient,
	currentClusterName string,
	removableBuildIdDurationSinceDefault dynamicconfig.DurationPropertyFn,
	buildIdScavengerVisibilityRPS dynamicconfig.FloatPropertyFn,
) *Activities {
	return &Activities{
		logger:                               logger,
		taskManager:                          taskManager,
		metadataManager:                      metadataManager,
		visibilityManager:                    visibilityManager,
		namespaceRegistry:                    namespaceRegistry,
		matchingClient:                       matchingClient,
		currentClusterName:                   currentClusterName,
		removableBuildIdDurationSinceDefault: removableBuildIdDurationSinceDefault,
		buildIdScavengerVisibilityRPS:        buildIdScavengerVisibilityRPS,
	}
}

// BuildIdScavangerWorkflow scans all task queue user data entries in all namespaces and cleans up unused build ids.
// This workflow is a wrapper around the long running ScavengeBuildIds activity.
func BuildIdScavangerWorkflow(ctx workflow.Context, input BuildIdScavangerInput) error {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		// Give the activity enough time to scan the entire namespace
		StartToCloseTimeout: 6 * time.Hour,
		HeartbeatTimeout:    30 * time.Second,
	})
	return workflow.ExecuteActivity(activityCtx, BuildIdScavangerActivityName, input).Get(ctx, nil)
}

func (a *Activities) setDefaults(input *BuildIdScavangerInput) {
	if input.NamespaceListPageSize == 0 {
		input.NamespaceListPageSize = 100
	}
	if input.TaskQueueListPageSize == 0 {
		input.TaskQueueListPageSize = 100
	}
}

func (a *Activities) recordHeartbeat(ctx context.Context, heartbeat heartbeatDetails) {
	activity.RecordHeartbeat(ctx, heartbeat)
}

// ScavengeBuildIds scans all task queue user data entries in all namespaces and cleans up unused build ids.
func (a *Activities) ScavengeBuildIds(ctx context.Context, input BuildIdScavangerInput) error {
	a.setDefaults(&input)

	var heartbeat heartbeatDetails
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &heartbeat); err != nil {
			return temporal.NewNonRetryableApplicationError("failed to load previous heartbeat details", "TypeError", err)
		}
	}
	rateLimiter := quotas.NewDefaultOutgoingRateLimiter(quotas.RateFn(a.buildIdScavengerVisibilityRPS))
	for {
		nsResponse, err := a.metadataManager.ListNamespaces(ctx, &persistence.ListNamespacesRequest{
			PageSize:       input.NamespaceListPageSize,
			NextPageToken:  heartbeat.NamespaceNextPageToken,
			IncludeDeleted: false, // Don't care about deleted namespaces.
		})
		if err != nil {
			return err
		}
		for heartbeat.NamespaceIdx < len(nsResponse.Namespaces) {
			nsId := nsResponse.Namespaces[heartbeat.NamespaceIdx].Namespace.Info.Id
			if err := a.processNamespaceEntry(ctx, rateLimiter, input, &heartbeat, nsId); err != nil {
				return err
			}
			heartbeat.NamespaceIdx++
			a.recordHeartbeat(ctx, heartbeat)
		}
		heartbeat.NamespaceIdx = 0
		heartbeat.NamespaceNextPageToken = nsResponse.NextPageToken
		if len(heartbeat.NamespaceNextPageToken) == 0 {
			break
		}
		a.recordHeartbeat(ctx, heartbeat)
	}
	return nil
}

func (a *Activities) processNamespaceEntry(
	ctx context.Context,
	rateLimiter quotas.RateLimiter,
	input BuildIdScavangerInput,
	heartbeat *heartbeatDetails,
	nsId string,
) error {
	ns, err := a.namespaceRegistry.GetNamespaceByID(namespace.ID(nsId))
	if err != nil {
		return err
	}
	// Only the active cluster for this namespace should perform the cleanup.
	if !ns.ActiveInCluster(a.currentClusterName) {
		return nil
	}
	for {
		tqResponse, err := a.taskManager.ListTaskQueueUserDataEntries(ctx, &persistence.ListTaskQueueUserDataEntriesRequest{
			NamespaceID:   nsId,
			PageSize:      input.TaskQueueListPageSize,
			NextPageToken: heartbeat.TaskQueueNextPageToken,
		})
		if err != nil {
			return err
		}
		for heartbeat.TaskQueueIdx < len(tqResponse.Entries) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			entry := tqResponse.Entries[heartbeat.TaskQueueIdx]
			if err := a.processUserDataEntry(ctx, rateLimiter, *heartbeat, ns, entry); err != nil {
				// Intentionally don't fail the activity on single entry.
				a.logger.Error("Failed to update task queue user data",
					tag.WorkflowNamespace(ns.Name().String()),
					tag.WorkflowTaskQueueName(entry.TaskQueue),
					tag.Error(err))
			}
			heartbeat.TaskQueueIdx++
			a.recordHeartbeat(ctx, *heartbeat)
		}
		heartbeat.TaskQueueIdx = 0
		heartbeat.TaskQueueNextPageToken = tqResponse.NextPageToken
		if len(heartbeat.TaskQueueNextPageToken) == 0 {
			break
		}
		a.recordHeartbeat(ctx, *heartbeat)
	}
	return nil
}

func (a *Activities) processUserDataEntry(
	ctx context.Context,
	rateLimiter quotas.RateLimiter,
	heartbeat heartbeatDetails,
	ns *namespace.Namespace,
	entry *persistence.TaskQueueUserDataEntry,
) error {
	buildIdsToRemove, err := a.findBuildIdsToRemove(ctx, rateLimiter, heartbeat, ns, entry)
	if err != nil {
		return err
	}
	if len(buildIdsToRemove) == 0 {
		return nil
	}
	_, err = a.matchingClient.UpdateWorkerBuildIdCompatibility(ctx, &matchingservice.UpdateWorkerBuildIdCompatibilityRequest{
		NamespaceId: ns.ID().String(),
		TaskQueue:   entry.TaskQueue,
		Operation: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_RemoveBuildIds_{
			RemoveBuildIds: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_RemoveBuildIds{
				KnownUserDataVersion: entry.UserData.Version,
				BuildIds:             buildIdsToRemove,
			},
		},
	})
	return err
}

// Queries visibility for each build id in versioning data and returns a list of those that are safe for removal.
func (a *Activities) findBuildIdsToRemove(
	ctx context.Context,
	rateLimiter quotas.RateLimiter,
	heartbeat heartbeatDetails,
	ns *namespace.Namespace,
	entry *persistence.TaskQueueUserDataEntry,
) ([]string, error) {
	versioningData := entry.UserData.Data.GetVersioningData()
	var buildIdsToRemove []string
	for setIdx, set := range versioningData.GetVersionSets() {
		setActive := len(set.BuildIds)
		for buildIdIdx, buildId := range set.BuildIds {
			if buildId.State == persistencespb.STATE_DELETED {
				setActive--
				continue
			}
			buildIdIsSetDefault := buildIdIdx == len(set.BuildIds)-1
			setIsQueueDefault := setIdx == len(versioningData.VersionSets)-1
			// Don't remove if build id is the queue default of there's another active build id in this set.
			if buildIdIsSetDefault && (setIsQueueDefault || setActive > 1) {
				continue
			}
			timeSinceWasDefault := time.Since(hybrid_logical_clock.UTC(*buildId.BecameDefaultTimestamp))
			if timeSinceWasDefault < a.removableBuildIdDurationSinceDefault() {
				continue
			}

			if err := rateLimiter.Wait(ctx); err != nil {
				return buildIdsToRemove, err
			}
			exists, err := worker_versioning.WorkflowsExistForBuildId(ctx, a.visibilityManager, ns, entry.TaskQueue, buildId.Id)
			if err != nil {
				return buildIdsToRemove, err
			}
			a.recordHeartbeat(ctx, heartbeat)
			if !exists {
				a.logger.Info("Found build id to remove",
					tag.WorkflowNamespace(ns.Name().String()),
					tag.WorkflowTaskQueueName(entry.TaskQueue),
					tag.BuildId(buildId.Id),
				)
				buildIdsToRemove = append(buildIdsToRemove, buildId.Id)
				setActive--
			}
		}
	}

	return buildIdsToRemove, nil
}
