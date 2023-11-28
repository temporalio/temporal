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
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/replication"
)

//go:generate mockgen -copyright_file ../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination history_events_handler_mock.go

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
		) error
	}

	historyEventsHandlerImpl struct {
		replication.ProcessToolBox
		PastEventsHandler
		FutureEventsHandler
	}
)

func NewHistoryEventsHandler(toolBox replication.ProcessToolBox, pastHandler PastEventsHandler, futureHandler FutureEventsHandler) HistoryEventsHandler {
	return &historyEventsHandlerImpl{
		toolBox,
		pastHandler,
		futureHandler,
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
) error {
	if len(historyEvents) == 0 {
		return serviceerror.NewInvalidArgument("Empty batches")
	}
	pastEvents, futureEvents, err := h.parseBatchesToPastAndFuture(historyEvents, versionHistoryItems)
	if err != nil {
		return err
	}

	if len(pastEvents) != 0 {
		if err := h.PastEventsHandler.HandlePastEvents(
			ctx,
			sourceClusterName,
			workflowKey,
			versionHistoryItems,
			pastEvents,
		); err != nil {
			return err
		}
	}
	if len(futureEvents) != 0 {
		if err := h.FutureEventsHandler.HandleFutureEvents(
			ctx,
			workflowKey,
			baseExecutionInfo,
			versionHistoryItems,
			futureEvents,
			newEvents,
		); err != nil {
			return err
		}
	}
	return nil
}

func (h *historyEventsHandlerImpl) parseBatchesToPastAndFuture(
	eventsBatches [][]*historypb.HistoryEvent,
	versionHistoryItems []*historyspb.VersionHistoryItem,
) (past [][]*historypb.HistoryEvent, future [][]*historypb.HistoryEvent, err error) {
	for _, batch := range eventsBatches {
		if len(batch) == 0 {
			return nil, nil, serviceerror.NewInvalidArgument("Empty batch")
		}
	}
	pastVersionHistory, _ := versionhistory.ParseVersionHistoryToPastAndFuture(versionHistoryItems, h.ClusterMetadata.GetClusterID(), h.ClusterMetadata.GetFailoverVersionIncrement())
	if len(pastVersionHistory) == 0 {
		return nil, eventsBatches, nil
	}
	lastPastEventId := pastVersionHistory[len(pastVersionHistory)-1].EventId
	firstBatch := eventsBatches[0]
	lastBatch := eventsBatches[len(eventsBatches)-1]

	if lastBatch[len(lastBatch)-1].EventId <= lastPastEventId {
		return eventsBatches, nil, nil
	}
	if firstBatch[0].EventId > lastPastEventId {
		return nil, eventsBatches, nil
	}
	lastPastBatchIndex := -1
	for index, batch := range eventsBatches {
		if batch[len(batch)-1].EventId == lastPastEventId {
			lastPastBatchIndex = index
			break
		}
	}
	if lastPastBatchIndex == -1 {
		return nil, nil, serviceerror.NewInvalidArgument("No boundary events found") // if this happens, means the events are not consecutive and we have bug somewhere
	}
	return eventsBatches[:lastPastBatchIndex+1], eventsBatches[lastPastBatchIndex+1:], nil
}
