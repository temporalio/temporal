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
	common2 "go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/shard"
)

//go:generate mockgen -copyright_file ../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination local_events_handler_mock.go

const (
	historyImportBlobSize = 16
	historyImportPageSize = 256 * 1024 // 256K
)

type (
	LocalGeneratedEventsHandler interface {
		HandleLocalGeneratedHistoryEvents(
			ctx context.Context,
			remoteCluster string,
			workflowKey definition.WorkflowKey,
			versionHistoryItems []*historyspb.VersionHistoryItem,
			localEvents [][]*historypb.HistoryEvent,
		) error
	}

	localEventsHandlerImpl struct {
		clusterMetadata         cluster.Metadata
		shardController         shard.Controller
		logger                  log.Logger
		eventSerializer         serialization.Serializer
		historyPaginatedFetcher HistoryPaginatedFetcher
	}
)

func NewLocalEventsHandler(
	clusterMetadata cluster.Metadata,
	shardController shard.Controller,
	logger log.Logger,
	eventSerializer serialization.Serializer,
	historyPaginatedFetcher HistoryPaginatedFetcher,
) LocalGeneratedEventsHandler {
	return &localEventsHandlerImpl{
		clusterMetadata:         clusterMetadata,
		shardController:         shardController,
		logger:                  logger,
		eventSerializer:         eventSerializer,
		historyPaginatedFetcher: historyPaginatedFetcher,
	}
}

// HandleLocalGeneratedHistoryEvents current implementation is using Import API which requires transactional importing.
// So when this API is called, it will try to import all local history events in one transaction.
// i.e. From version history, local events are [1,100]. When this API is called with events[10,11], it will try to import
// [10,11] and if success, it will fetch [12,100] from source cluster and also import them, then commit the transaction.
func (h *localEventsHandlerImpl) HandleLocalGeneratedHistoryEvents(
	ctx context.Context,
	sourceClusterName string,
	workflowKey definition.WorkflowKey,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	localEvents [][]*historypb.HistoryEvent,
) error {
	shardContext, err := h.shardController.GetShardByNamespaceWorkflow(namespace.ID(workflowKey.NamespaceID), workflowKey.WorkflowID)
	if err != nil {
		return err
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	versionHistory := versionhistory.NewVersionHistory([]byte{}, versionHistoryItems)
	localEventsBlobs := make([]*common.DataBlob, len(localEvents))
	localVersionHistory, _ := versionhistory.SplitVersionHistoryByLastLocalGeneratedItem(versionHistoryItems, h.clusterMetadata.GetClusterID(), h.clusterMetadata.GetFailoverVersionIncrement())

	for index, batch := range localEvents {
		blob, err := h.eventSerializer.SerializeEvents(batch, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return err
		}
		localEventsBlobs[index] = blob
	}
	_, err = engine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: workflowKey.NamespaceID,
		Execution: &common.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
	})

	switch err.(type) {
	case nil:
	case *serviceerror.NotFound:
		// if mutable state not found, we import from beginning
		return h.importEvents(
			ctx,
			sourceClusterName,
			engine,
			workflowKey,
			common2.FirstEventID,
			localVersionHistory[0].Version,
			localVersionHistory[len(localVersionHistory)-1].EventId,
			localVersionHistory[len(localVersionHistory)-1].Version,
			nil,
		)
	default:
		return err
	}
	response, err := h.invokeImportWorkflowExecutionCall(ctx, engine, workflowKey, localEventsBlobs, versionHistory, nil)
	if err != nil {
		return err
	}
	if !response.EventsApplied { // means local events were already existing before importing, no more action needed
		return nil
	}

	lastBatch := localEvents[len(localEvents)-1]
	lastEvent := lastBatch[len(lastBatch)-1]

	if lastEvent.EventId == localVersionHistory[len(localVersionHistory)-1].EventId {
		// all local events were imported successfully, we call commit to finish the transaction
		_, err := h.invokeImportWorkflowExecutionCall(ctx, engine, workflowKey, nil, versionHistory, response.Token)
		if err != nil {
			return err
		}
		return nil
	}
	nextEventId := lastEvent.EventId + 1
	nextEventVersion, err := versionhistory.GetVersionHistoryEventVersion(versionHistory, nextEventId)
	if err != nil {
		return err
	}
	return h.importEvents(
		ctx,
		sourceClusterName,
		engine,
		workflowKey,
		nextEventId,
		nextEventVersion,
		localVersionHistory[len(localVersionHistory)-1].EventId,
		localVersionHistory[len(localVersionHistory)-1].Version,
		response.Token,
	)
}

func (h *localEventsHandlerImpl) importEvents(
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
	historyIterator := h.historyPaginatedFetcher.GetSingleWorkflowHistoryPaginatedIterator(
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
			h.logger.Error("failed to get history events",
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
		events, err := h.eventSerializer.DeserializeEvents(batch.RawEventBatch)
		if err != nil {
			return err
		}

		if blobSize >= historyImportBlobSize ||
			len(blobs) >= historyImportPageSize ||
			h.isLastEventAtHistoryBoundary(events[len(events)-1], versionHistory) { // Import API only take events that has same version
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
		h.logger.Error("failed to commit import action",
			tag.WorkflowNamespaceID(workflowKey.NamespaceID),
			tag.WorkflowID(workflowKey.WorkflowID),
			tag.WorkflowRunID(workflowKey.RunID),
			tag.Error(err))
		return serviceerror.NewInternal("Failed to commit import transaction")
	}
	return nil
}

func (h *localEventsHandlerImpl) isLastEventAtHistoryBoundary(
	lastLocalEvent *historypb.HistoryEvent,
	versionHistory *historyspb.VersionHistory,
) bool {
	for _, item := range versionHistory.Items {
		if item.EventId == lastLocalEvent.EventId {
			return true
		}
	}
	return false
}

func (h *localEventsHandlerImpl) invokeImportWorkflowExecutionCall(
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
		h.logger.Error("failed to import events",
			tag.WorkflowNamespaceID(workflowKey.NamespaceID),
			tag.WorkflowID(workflowKey.WorkflowID),
			tag.WorkflowRunID(workflowKey.RunID),
			tag.Error(err))
		return nil, err
	}
	return response, nil
}
