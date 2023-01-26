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

package reapplyevents

import (
	"context"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/shard"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

func Invoke(
	ctx context.Context,
	namespaceUUID namespace.ID,
	workflowID string,
	runID string,
	reapplyEvents []*historypb.HistoryEvent,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	workflowResetter ndc.WorkflowResetter,
	eventsReapplier ndc.EventsReapplier,
) error {
	if shard.GetConfig().SkipReapplicationByNamespaceID(namespaceUUID.String()) {
		return nil
	}

	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(namespaceUUID.String()))
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.ID()

	return api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			"",
		),
		func(workflowContext api.WorkflowContext) (*api.UpdateWorkflowAction, error) {
			context := workflowContext.GetContext()
			mutableState := workflowContext.GetMutableState()
			// Filter out reapply event from the same cluster
			toReapplyEvents := make([]*historypb.HistoryEvent, 0, len(reapplyEvents))
			lastWriteVersion, err := mutableState.GetLastWriteVersion()
			if err != nil {
				return nil, err
			}

			for _, event := range reapplyEvents {
				if event.GetVersion() == lastWriteVersion {
					// The reapply is from the same cluster. Ignoring.
					continue
				}
				dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
				if mutableState.IsResourceDuplicated(dedupResource) {
					// already apply the signal
					continue
				}

				toReapplyEvents = append(toReapplyEvents, event)
			}
			if len(toReapplyEvents) == 0 {
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}

			if !mutableState.IsWorkflowExecutionRunning() {
				// need to reset target workflow (which is also the current workflow)
				// to accept events to be reapplied
				baseRunID := mutableState.GetExecutionState().GetRunId()
				resetRunID := uuid.New()
				baseRebuildLastEventID := mutableState.GetPreviousStartedEventID()

				// TODO when https://github.com/uber/cadence/issues/2420 is finished, remove this block,
				//  since cannot reapply event to a finished workflow which had no workflow tasks started
				if baseRebuildLastEventID == common.EmptyEventID {
					shard.GetLogger().Warn("cannot reapply event to a finished workflow with no workflow task",
						tag.WorkflowNamespaceID(namespaceID.String()),
						tag.WorkflowID(workflowID),
					)
					shard.GetMetricsHandler().Counter(metrics.EventReapplySkippedCount.GetMetricName()).Record(
						1,
						metrics.OperationTag(metrics.HistoryReapplyEventsScope))
					return &api.UpdateWorkflowAction{
						Noop:               true,
						CreateWorkflowTask: false,
					}, nil
				}

				baseVersionHistories := mutableState.GetExecutionInfo().GetVersionHistories()
				baseCurrentVersionHistory, err := versionhistory.GetCurrentVersionHistory(baseVersionHistories)
				if err != nil {
					return nil, err
				}
				baseRebuildLastEventVersion, err := versionhistory.GetVersionHistoryEventVersion(baseCurrentVersionHistory, baseRebuildLastEventID)
				if err != nil {
					return nil, err
				}
				baseCurrentBranchToken := baseCurrentVersionHistory.GetBranchToken()
				baseNextEventID := mutableState.GetNextEventID()

				err = workflowResetter.ResetWorkflow(
					ctx,
					namespaceID,
					workflowID,
					baseRunID,
					baseCurrentBranchToken,
					baseRebuildLastEventID,
					baseRebuildLastEventVersion,
					baseNextEventID,
					resetRunID.String(),
					uuid.New().String(),
					ndc.NewWorkflow(
						ctx,
						shard.GetNamespaceRegistry(),
						shard.GetClusterMetadata(),
						context,
						mutableState,
						wcache.NoopReleaseFn,
					),
					ndc.EventsReapplicationResetWorkflowReason,
					toReapplyEvents,
					enumspb.RESET_REAPPLY_TYPE_SIGNAL,
				)
				switch err.(type) {
				case *serviceerror.InvalidArgument:
					// no-op. Usually this is due to reset workflow with pending child workflows
					shard.GetLogger().Warn("Cannot reset workflow. Ignoring reapply events.", tag.Error(err))
				case nil:
					// no-op
				default:
					return nil, err
				}
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}

			postActions := &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: true,
			}
			if mutableState.IsWorkflowPendingOnWorkflowTaskBackoff() {
				// Do not create workflow task when the workflow has first workflow task backoff and execution is not started yet
				postActions.CreateWorkflowTask = false
			}
			reappliedEvents, err := eventsReapplier.ReapplyEvents(
				ctx,
				mutableState,
				toReapplyEvents,
				runID,
			)
			if err != nil {
				shard.GetLogger().Error("failed to re-apply stale events", tag.Error(err))
				return nil, err
			}
			if len(reappliedEvents) == 0 {
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}
			return postActions, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)
}
