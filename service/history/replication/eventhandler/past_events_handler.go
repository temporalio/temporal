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

	"go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
)

//go:generate mockgen -copyright_file ../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination past_events_handler_mock.go

const (
	historyImportBlobSize = 16
	historyImportPageSize = 256 * 1024 // 256K
)

type (
	PastEventsHandler interface {
		HandlePastEvents(
			ctx context.Context,
			remoteCluster string,
			workflowKey definition.WorkflowKey,
			versionHistoryItems []*historyspb.VersionHistoryItem,
			pastEvents [][]*historypb.HistoryEvent,
		) error
	}

	pastEventsHandlerImpl struct {
		replication.ProcessToolBox
	}
)

func NewPastEventsHandler(toolBox replication.ProcessToolBox) PastEventsHandler {
	return &pastEventsHandlerImpl{
		toolBox,
	}
}

func (h *pastEventsHandlerImpl) HandlePastEvents(
	ctx context.Context,
	sourceClusterName string,
	workflowKey definition.WorkflowKey,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	pastEvents [][]*historypb.HistoryEvent,
) error {
	shardContext, err := h.ShardController.GetShardByNamespaceWorkflow(namespace.ID(workflowKey.NamespaceID), workflowKey.WorkflowID)
	if err != nil {
		return err
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	versionHistory := versionhistory.NewVersionHistory([]byte{}, versionHistoryItems)
	pastEventsBlobs := make([]*common.DataBlob, len(pastEvents))

	for index, batch := range pastEvents {
		blob, err := h.EventSerializer.SerializeEvents(batch, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return err
		}
		pastEventsBlobs[index] = blob
	}
	response, err := h.invokeImportWorkflowExecutionCall(ctx, engine, workflowKey, pastEventsBlobs, versionHistory, nil)
	if err != nil {
		return err
	}
	if !response.EventsApplied { // means past events were already existing before importing, no more action needed
		return nil
	}

	pastVersionHistory, _ := versionhistory.ParseVersionHistoryToPastAndFuture(versionHistoryItems, h.ClusterMetadata.GetClusterID(), h.ClusterMetadata.GetFailoverVersionIncrement())

	lastBatch := pastEvents[len(pastEvents)-1]
	lastPastEvent := lastBatch[len(lastBatch)-1]

	if lastPastEvent.EventId == pastVersionHistory[len(pastVersionHistory)-1].EventId {
		// all past events were imported successfully
		_, err := h.invokeImportWorkflowExecutionCall(ctx, engine, workflowKey, nil, versionHistory, response.Token)
		if err != nil {
			return err
		}
		return nil
	}
	return h.importEvents(
		ctx,
		sourceClusterName,
		engine,
		workflowKey,
		lastPastEvent.EventId,
		lastPastEvent.Version,
		pastVersionHistory[len(pastVersionHistory)-1].EventId,
		pastVersionHistory[len(pastVersionHistory)-1].Version,
		response.Token,
	)
}

func (h *pastEventsHandlerImpl) importEvents(
	ctx context.Context,
	remoteCluster string,
	engine shard.Engine,
	workflowKey definition.WorkflowKey,
	startEventId int64,
	startEventVersion int64,
	endEventId int64,
	endEventVersion int64,
	token []byte,
) error {
	// Todo: change this to use inclusive/inclusive api once it is available
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

func (h *pastEventsHandlerImpl) invokeImportWorkflowExecutionCall(
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
