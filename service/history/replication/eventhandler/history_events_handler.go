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

package eventhandler

import (
	"context"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	workflowpb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence/versionhistory"
)

//go:generate mockgen -copyright_file ../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination history_events_handler_mock.go

// Local vs Remote
// Local and Remote are introduced to handle the case:
//
// wf_1 was originally started in cluster_A, and was migrated to cluster_B.
// Then wf_1 data was deleted in cluster_A. Then we want to migrate the wf_1 data back to cluster_A.
// in this case, local events are the event 1 to the last events that are generated in cluster_A (event_version % failover_incremental == cluster_A's initial_failover_version)
// remote events are events thereafter.
// For local events, we should use ImportWorkflowExecution API to process and for remote events, we should use ReplicateHistoryEvents API to process.
type (
	// HistoryEventsHandler is to handle all cases that add history events from remote cluster to current cluster.
	// so it can be used by:
	// 1. ExecutableHistoryTask to replicate events
	// 2. When HistoryResender trying to add events (currently triggered by RetryReplication error, which still need to handle import case)
	HistoryEventsHandler interface {
		HandleHistoryEvents(
			ctx context.Context,
			sourceClusterName string,
			workflowKey definition.WorkflowKey,
			baseExecutionInfo *workflowpb.BaseExecutionInfo,
			versionHistoryItems []*historyspb.VersionHistoryItem,
			historyEvents [][]*historypb.HistoryEvent,
			newEvents []*historypb.HistoryEvent,
			newRunID string,
		) error
	}

	historyEventsHandlerImpl struct {
		clusterMetadata     cluster.Metadata
		localEventsHandler  LocalGeneratedEventsHandler
		remoteEventsHandler RemoteGeneratedEventsHandler
	}
)

func NewHistoryEventsHandler(
	clusterMetadata cluster.Metadata,
	localHandler LocalGeneratedEventsHandler,
	remoteHandler RemoteGeneratedEventsHandler,
) HistoryEventsHandler {
	return &historyEventsHandlerImpl{
		clusterMetadata,
		localHandler,
		remoteHandler,
	}
}

func (h *historyEventsHandlerImpl) HandleHistoryEvents(
	ctx context.Context,
	sourceClusterName string,
	workflowKey definition.WorkflowKey,
	baseExecutionInfo *workflowpb.BaseExecutionInfo,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	historyEvents [][]*historypb.HistoryEvent,
	newEvents []*historypb.HistoryEvent,
	newRunID string,
) error {
	if len(historyEvents) == 0 {
		return serviceerror.NewInvalidArgument("Empty batches")
	}
	localEvents, remoteEvents, err := h.splitBatchesToLocalAndRemote(historyEvents, versionHistoryItems)
	if err != nil {
		return err
	}

	if len(localEvents) != 0 {
		if err := h.localEventsHandler.HandleLocalGeneratedHistoryEvents(
			ctx,
			sourceClusterName,
			workflowKey,
			versionHistoryItems,
			localEvents,
		); err != nil {
			return err
		}
	}
	if len(remoteEvents) != 0 {
		if err := h.remoteEventsHandler.HandleRemoteGeneratedHistoryEvents(
			ctx,
			workflowKey,
			baseExecutionInfo,
			versionHistoryItems,
			remoteEvents,
			newEvents,
			newRunID,
		); err != nil {
			return err
		}
	}
	return nil
}

func (h *historyEventsHandlerImpl) splitBatchesToLocalAndRemote(
	eventsBatches [][]*historypb.HistoryEvent,
	versionHistoryItems []*historyspb.VersionHistoryItem,
) (local [][]*historypb.HistoryEvent, remote [][]*historypb.HistoryEvent, err error) {
	for _, batch := range eventsBatches {
		if len(batch) == 0 {
			return nil, nil, serviceerror.NewInvalidArgument("Empty batch")
		}
	}
	localVersionHistory, _ := versionhistory.SplitVersionHistoryByLastLocalGeneratedItem(versionHistoryItems, h.clusterMetadata.GetClusterID(), h.clusterMetadata.GetFailoverVersionIncrement())
	if len(localVersionHistory) == 0 {
		return nil, eventsBatches, nil
	}
	lastLocalEventId := localVersionHistory[len(localVersionHistory)-1].EventId
	firstBatch := eventsBatches[0]
	lastBatch := eventsBatches[len(eventsBatches)-1]

	if lastBatch[len(lastBatch)-1].EventId <= lastLocalEventId {
		return eventsBatches, nil, nil
	}
	if firstBatch[0].EventId > lastLocalEventId {
		return nil, eventsBatches, nil
	}
	lastLocalBatchIndex := -1
	for index, batch := range eventsBatches {
		if batch[len(batch)-1].EventId == lastLocalEventId {
			lastLocalBatchIndex = index
			break
		}
	}
	if lastLocalBatchIndex == -1 {
		return nil, nil, serviceerror.NewInternal("No boundary events found") // if this happens, means the events are not consecutive and we have bug somewhere
	}
	return eventsBatches[:lastLocalBatchIndex+1], eventsBatches[lastLocalBatchIndex+1:], nil
}
