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
	"fmt"
	"math"
	"time"

	"github.com/xwb1989/sqlparser"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/searchattribute"
)

const (
	BuildIdScavangerWorkflowName = "build-id-scavenger"
	BuildIdScavangerActivityName = "scavenge-build-ids"
)

type (
	BuildIdScavangerInput struct {
		VisibilityRPS         float64
		NamespaceListPageSize int
		TaskQueueListPageSize int
	}

	Activities struct {
		logger            log.Logger
		taskManager       persistence.TaskManager
		metadataManager   persistence.MetadataManager
		visibilityManager manager.VisibilityManager
		namespaceRegistry namespace.Registry
		timeSource        clock.TimeSource
		matchingClient    matchingservice.MatchingServiceClient
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
	timeSource clock.TimeSource,
	matchingClient matchingservice.MatchingServiceClient,
) *Activities {
	return &Activities{
		logger:            logger,
		taskManager:       taskManager,
		metadataManager:   metadataManager,
		visibilityManager: visibilityManager,
		namespaceRegistry: namespaceRegistry,
		timeSource:        timeSource,
		matchingClient:    matchingClient,
	}
}

// BuildIdScavangerWorkflow scans all task queue user data entries in all namespaces and cleans up unused build ids.
// This workflow is a wrapper around the long running ScavengeBuildIds activity.
func BuildIdScavangerWorkflow(ctx workflow.Context, input BuildIdScavangerInput) error {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		// Give the activity enough time to scan the entire namespace
		StartToCloseTimeout: 6 * time.Hour,
		HeartbeatTimeout:    10 * time.Second,
	})
	return workflow.ExecuteActivity(activityCtx, BuildIdScavangerActivityName, input).Get(ctx, nil)
}

func setDefaults(input *BuildIdScavangerInput) {
	if input.NamespaceListPageSize == 0 {
		input.NamespaceListPageSize = 100
	}
	if input.TaskQueueListPageSize == 0 {
		input.TaskQueueListPageSize = 100
	}
	if input.VisibilityRPS == 0 {
		input.VisibilityRPS = 1
	}
}

// ScavengeBuildIds scans all task queue user data entries in all namespaces and cleans up unused build ids.
func (a *Activities) ScavengeBuildIds(ctx context.Context, input BuildIdScavangerInput) error {
	setDefaults(&input)

	var heartbeat heartbeatDetails
	if err := activity.GetHeartbeatDetails(ctx, &heartbeat); err != nil {
		return err
	}
	rateLimiter := quotas.NewRateLimiter(input.VisibilityRPS, int(math.Ceil(input.VisibilityRPS)))
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
			ns, err := a.namespaceRegistry.GetNamespaceByID(namespace.ID(nsId))
			if err != nil {
				return err
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
					entry := tqResponse.Entries[heartbeat.TaskQueueIdx]
					buildIdsRemoved, err := a.processUserDataEntry(ctx, rateLimiter, heartbeat, ns, entry)
					if err != nil {
						return err
					}
					if len(buildIdsRemoved) > 0 {
						// We don't need to keep the tombstones around if we're not replicating them.
						if !ns.IsGlobalNamespace() {
							clearTombstones(entry.UserData.Data.GetVersioningData())
						}

						_, err := a.matchingClient.UpdateTaskQueueUserData(ctx, &matchingservice.UpdateTaskQueueUserDataRequest{
							NamespaceId: nsId,
							TaskQueue:   entry.TaskQueue,
							UserData: &persistencespb.VersionedTaskQueueUserData{
								Version: entry.UserData.Version + 1,
								Data:    entry.UserData.Data,
							},
							BuildIdsRemoved: buildIdsRemoved,
						})
						if err != nil {
							a.logger.Error("Failed to update task queue user data", tag.Error(err))
							continue
						}
						if ns.IsGlobalNamespace() {
							_, err = a.matchingClient.ReplicateTaskQueueUserData(ctx, &matchingservice.ReplicateTaskQueueUserDataRequest{
								NamespaceId: nsId,
								TaskQueue:   entry.TaskQueue,
								UserData:    entry.UserData.Data,
							})
							if err != nil {
								a.logger.Error("Failed to replicate task queue user data", tag.Error(err))
								continue
							}
							// Only clear tombstones after they have been replicated.
							if clearTombstones(entry.UserData.Data.VersioningData) {
								_, err := a.matchingClient.UpdateTaskQueueUserData(ctx, &matchingservice.UpdateTaskQueueUserDataRequest{
									NamespaceId: nsId,
									TaskQueue:   entry.TaskQueue,
									UserData: &persistencespb.VersionedTaskQueueUserData{
										Version: entry.UserData.Version + 2,
										Data:    entry.UserData.Data,
									},
									BuildIdsRemoved: buildIdsRemoved,
								})
								if err != nil {
									a.logger.Error("Failed to perform second task queue user data update", tag.Error(err))
									continue
								}
							}
						}
					}
					heartbeat.TaskQueueIdx++
					activity.RecordHeartbeat(ctx, heartbeat)
				}
				heartbeat.TaskQueueIdx = 0
				heartbeat.TaskQueueNextPageToken = tqResponse.NextPageToken
				if len(heartbeat.TaskQueueNextPageToken) == 0 {
					break
				}
				activity.RecordHeartbeat(ctx, heartbeat)
			}
			heartbeat.NamespaceIdx++
			activity.RecordHeartbeat(ctx, heartbeat)
		}
		heartbeat.NamespaceIdx = 0
		heartbeat.NamespaceNextPageToken = nsResponse.NextPageToken
		if len(heartbeat.NamespaceNextPageToken) == 0 {
			break
		}
		activity.RecordHeartbeat(ctx, heartbeat)
	}
	return nil
}

// Process a single user data entry. Queries visibility for each build id and updates build id state with STATE_DELETED
// (tombstone) for every build id that can safely be deleted.
// Returns a list of build ids that were removed.
func (a *Activities) processUserDataEntry(
	ctx context.Context,
	rateLimiter quotas.RateLimiter,
	heartbeat heartbeatDetails,
	ns *namespace.Namespace,
	entry *persistence.TaskQueueUserDataEntry,
) ([]string, error) {
	clk := hlc.Next(*entry.UserData.Data.Clock, a.timeSource)
	versioningData := entry.UserData.Data.GetVersioningData()
	var buildIdsRemoved []string
	for setIdx, set := range versioningData.GetVersionSets() {
		setActive := len(set.BuildIds)
		for buildIdIdx, buildId := range set.BuildIds {
			if buildId.State == persistencespb.STATE_DELETED {
				setActive--
				continue
			}
			// Set default
			if buildIdIdx == len(set.BuildIds)-1 {
				// Can't delete the queue default
				if setIdx == len(versioningData.VersionSets)-1 {
					continue
				}
				// There's another active build id in this set
				if setActive > 1 {
					continue
				}
			}

			escapedBuildId := sqlparser.String(sqlparser.NewStrVal([]byte(common.VersionedBuildIdSearchAttribute(buildId.Id))))
			query := fmt.Sprintf("%s = %s", searchattribute.BuildIds, escapedBuildId)

			if err := rateLimiter.Wait(ctx); err != nil {
				return buildIdsRemoved, err
			}
			response, err := a.visibilityManager.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
				NamespaceID: ns.ID(),
				Namespace:   ns.Name(),
				Query:       query,
			})
			if err != nil {
				return buildIdsRemoved, err
			}
			activity.RecordHeartbeat(ctx, heartbeat)
			if response.Count == 0 {
				a.logger.Info("Deleting build id",
					tag.NewStringTag("namespace", ns.Name().String()),
					tag.NewStringTag("task-queue", entry.TaskQueue),
					tag.NewStringTag("build-id", buildId.Id),
				)
				buildId.State = persistencespb.STATE_DELETED
				buildId.StateUpdateTimestamp = &clk
				entry.UserData.Data.Clock = &clk
				buildIdsRemoved = append(buildIdsRemoved, buildId.Id)
				setActive--
			}
		}
	}

	return buildIdsRemoved, nil
}

// Clear all tombstone build ids (with STATE_DELETED) from versioning data.
// Returns true if any tombstones were cleared.
func clearTombstones(versioningData *persistencespb.VersioningData) bool {
	cleared := false
	for setIdx := 0; setIdx < len(versioningData.GetVersionSets()); {
		set := versioningData.VersionSets[setIdx]
		for buildIdIdx := 0; buildIdIdx < len(set.BuildIds); {
			buildId := set.BuildIds[buildIdIdx]
			if buildId.State == persistencespb.STATE_DELETED {
				set.BuildIds = removeIndex(set.BuildIds, buildIdIdx)
				cleared = true
			} else {
				buildIdIdx++
			}
		}
		if len(set.BuildIds) == 0 {
			versioningData.VersionSets = removeIndex(versioningData.VersionSets, setIdx)
			cleared = true
		} else {
			setIdx++
		}
	}
	return cleared
}

func removeIndex[T any](s []T, index int) []T {
	ret := make([]T, 0, len(s)-1)
	ret = append(ret, s[:index]...)
	return append(ret, s[index+1:]...)
}
