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

package replication

import (
	"context"
	"time"

	"go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	workflowpb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/shard"
)

const (
	historyImportBlobSize = 16
	historyImportPageSize = 256 * 1024 // 256K
)

type (
	// HistoryEventsHandler is to handle all cases that add history events from remote cluster to current cluster.
	// so it can be used by:
	// 1. ExecutableHistoryTask to replicate events
	// 2. When HistoryResender trying to add events (currently triggered by RetryReplication error, which still need to handle import case)
	HistoryEventsHandler interface {
		HandleHistoryEvents(
			ctx context.Context,
			workflowKey definition.WorkflowKey,
			remoteCluster string,
			metricsTag string,
			baseExecutionInfo *workflowpb.BaseExecutionInfo,
			versionHistoryItems []*historyspb.VersionHistoryItem,
			historyEvents []*historypb.HistoryEvent,
			newEvents []*historypb.HistoryEvent,
		) error
	}

	historyEventsHandlerImpl struct {
		ProcessToolBox
	}
)

func NewHistoryEventsHandler(toolBox ProcessToolBox) HistoryEventsHandler {
	return &historyEventsHandlerImpl{
		toolBox,
	}
}

func (h *historyEventsHandlerImpl) HandleHistoryEvents(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	sourceClusterName string,
	metricsTag string,
	baseExecutionInfo *workflowpb.BaseExecutionInfo,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	historyEvents []*historypb.HistoryEvent,
	newEvents []*historypb.HistoryEvent,
) error {
	shardContext, err := h.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
	)
	if err != nil {
		return err
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}

	pastVersionHistory, _ := versionhistory.ParseVersionHistoryToPastAndFuture(versionHistoryItems, h.ClusterMetadata.GetClusterID(), h.ClusterMetadata.GetFailoverVersionIncrement())
	if len(pastVersionHistory) != 0 {
		boundaryVersionHistoryItem := pastVersionHistory[len(pastVersionHistory)-1]
		pastEvents, futureEvents, err := h.parseEventsToPastAndFuture(historyEvents, boundaryVersionHistoryItem.EventId)
		if err != nil {
			return err
		}
		if len(pastEvents) != 0 {
			err = h.handlePastEvents(ctx, engine, sourceClusterName, metricsTag, workflowKey, pastEvents, versionHistoryItems, boundaryVersionHistoryItem)
			if err != nil {
				return err
			}
		}
		if len(futureEvents) != 0 {
			return engine.ReplicateHistoryEvents(
				ctx,
				workflowKey,
				baseExecutionInfo,
				versionHistoryItems,
				[][]*historypb.HistoryEvent{futureEvents},
				newEvents,
			)
		} else {
			return nil
		}
	}

	return engine.ReplicateHistoryEvents(
		ctx,
		workflowKey,
		baseExecutionInfo,
		versionHistoryItems,
		[][]*historypb.HistoryEvent{historyEvents},
		newEvents,
	)
}

func (h *historyEventsHandlerImpl) parseEventsToPastAndFuture(events []*historypb.HistoryEvent, boundaryEventId int64) ([]*historypb.HistoryEvent, []*historypb.HistoryEvent, error) {
	if events[len(events)-1].EventId < boundaryEventId {
		return events, nil, nil
	}
	if events[0].EventId > boundaryEventId {
		return nil, events, nil
	}
	boundaryIndex := -1
	for index, item := range events {
		if item.EventId == boundaryEventId {
			boundaryIndex = index
			break
		}
	}
	if boundaryIndex == -1 {
		return nil, nil, serviceerror.NewInvalidArgument("No boundary events found") // if this happens, means the events are not consecutive and we have bug somewhere
	}
	return events[:boundaryIndex+1], events[boundaryIndex+1:], nil
}

func (h *historyEventsHandlerImpl) handlePastEvents(
	ctx context.Context,
	engine shard.Engine,
	sourceClusterName string,
	metricsTag string,
	workflowKey definition.WorkflowKey,
	pastEvents []*historypb.HistoryEvent,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	boundaryVersionHistoryItem *historyspb.VersionHistoryItem,
) error {
	eventBlob, err := h.EventSerializer.SerializeEvents(pastEvents, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return err
	}
	versionHistory := versionhistory.NewVersionHistory([]byte{}, versionHistoryItems)

	response, err := h.invokeImportWorkflowExecutionCall(ctx, engine, workflowKey, []*common.DataBlob{eventBlob}, versionHistory, nil)
	if err != nil {
		return err
	}
	if !response.EventsApplied { // means events already exist before importing, no more action needed
		return nil
	}

	lastPastEvent := pastEvents[len(pastEvents)-1]
	if lastPastEvent.EventId == boundaryVersionHistoryItem.EventId { // means all past events are imported, we just need to commit.
		response, err = h.invokeImportWorkflowExecutionCall(ctx, engine, workflowKey, []*common.DataBlob{}, versionHistory, response.Token) // commit
		if err != nil {
			return err
		}
		return nil
	}
	// otherwise we need to import events all the way until boundaryEvent and then commit

	//nextEventVersion, err := versionhistory.GetVersionHistoryEventVersion(versionHistory, nextEventId)
	if err != nil {
		return err
	}
	return h.importEvents(
		ctx,
		sourceClusterName,
		metricsTag,
		engine,
		workflowKey,
		lastPastEvent.EventId,
		lastPastEvent.Version,
		boundaryVersionHistoryItem.EventId,
		boundaryVersionHistoryItem.Version,
		response.Token,
	)
}

func (h *historyEventsHandlerImpl) importEvents(
	ctx context.Context,
	remoteCluster string,
	metricsTag string,
	engine shard.Engine,
	workflowKey definition.WorkflowKey,
	startEventId int64,
	startEventVersion int64,
	endEventId int64,
	endEventVersion int64,
	token []byte,
) error {
	h.MetricsHandler.Counter(metrics.ClientRequests.GetMetricName()).Record(
		1,
		metrics.OperationTag(metricsTag+"Import"),
	)
	startTime := time.Now().UTC()
	defer func() {
		h.MetricsHandler.Timer(metrics.ClientLatency.GetMetricName()).Record(
			time.Since(startTime),
			metrics.OperationTag(metricsTag+"Import"),
		)
	}()

	historyIterator := h.ProcessToolBox.HistoryPaginatedFetcher.GetSingleWorkflowHistoryPaginatedIterator(
		ctx,
		remoteCluster,
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
		workflowKey.RunID,
		startEventId,
		startEventVersion,
		endEventId,
		endEventVersion,
	)

	blobs := []*common.DataBlob{}
	blobSize := 0
	var versionHistory *historyspb.VersionHistory

	for historyIterator.HasNext() {
		batch, err := historyIterator.Next()
		if err != nil {
			h.Logger.Error("failed to get history events",
				tag.WorkflowNamespaceID(workflowKey.NamespaceID),
				tag.WorkflowID(workflowKey.WorkflowID),
				tag.WorkflowRunID(workflowKey.RunID),
				tag.Error(err))
			return err
		}

		if versionHistory != nil && !versionhistory.IsVersionHistoryItemsInSameBranch(versionHistory.Items, batch.VersionHistory.Items) {
			return serviceerror.NewInternal("History Branch changed during importing")
		}
		versionHistory = batch.VersionHistory

		blobSize++
		blobs = append(blobs, batch.RawEventBatch)
		if blobSize >= historyImportBlobSize || len(blobs) >= historyImportPageSize {
			response, err := h.invokeImportWorkflowExecutionCall(ctx, engine, workflowKey, blobs, versionHistory, token)
			if err != nil {
				return err
			}
			blobs = []*common.DataBlob{}
			blobSize = 0
			token = response.Token
		}
	}
	if len(blobs) != 0 {
		response, err := h.invokeImportWorkflowExecutionCall(ctx, engine, workflowKey, blobs, versionHistory, token)
		if err != nil {
			return err
		}
		token = response.Token
	}

	// call with empty event blob to commit the import
	blobs = []*common.DataBlob{}
	response, err := h.invokeImportWorkflowExecutionCall(ctx, engine, workflowKey, blobs, versionHistory, token)
	if err != nil || len(response.Token) != 0 {
		h.Logger.Error("failed to commit import action",
			tag.WorkflowNamespaceID(workflowKey.NamespaceID),
			tag.WorkflowID(workflowKey.WorkflowID),
			tag.WorkflowRunID(workflowKey.RunID),
			tag.Error(err))
		return serviceerror.NewInternal("Failed to commit import transaction")
	}
	return nil
}

func (h *historyEventsHandlerImpl) invokeImportWorkflowExecutionCall(
	ctx context.Context,
	historyEngine shard.Engine,
	workflowKey definition.WorkflowKey,
	historyBatches []*common.DataBlob,
	versionHistory *historyspb.VersionHistory,
	token []byte,
) (*historyservice.ImportWorkflowExecutionResponse, error) {
	request := &historyservice.ImportWorkflowExecutionRequest{
		NamespaceId: workflowKey.NamespaceID,
		Execution: &common.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
		HistoryBatches: historyBatches,
		VersionHistory: versionHistory,
		Token:          token,
	}
	response, err := historyEngine.ImportWorkflowExecution(ctx, request)
	if err != nil {
		h.Logger.Error("failed to import events",
			tag.WorkflowNamespaceID(workflowKey.NamespaceID),
			tag.WorkflowID(workflowKey.WorkflowID),
			tag.WorkflowRunID(workflowKey.RunID),
			tag.Error(err))
		return nil, err
	}
	return response, nil
}
