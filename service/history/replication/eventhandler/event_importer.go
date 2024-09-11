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

//go:generate mockgen -copyright_file ../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination event_importer_mock.go

import (
	"context"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	common2 "go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/observability/log"
	"go.temporal.io/server/common/observability/log/tag"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/shard"
)

const (
	historyImportBlobSize = 16
	historyImportPageSize = 256 * 1024 // 256K
)

type (
	EventImporter interface {
		ImportHistoryEventsFromBeginning(
			ctx context.Context,
			remoteCluster string,
			workflowKey definition.WorkflowKey,
			endEventId int64, // inclusive
			endEventVersion int64,
		) error
	}

	eventImporterImpl struct {
		historyFetcher HistoryPaginatedFetcher
		engineProvider historyEngineProvider
		serializer     serialization.Serializer
		logger         log.Logger
	}
)

func NewEventImporter(
	historyFetcher HistoryPaginatedFetcher,
	engineProvider historyEngineProvider,
	serializer serialization.Serializer,
	logger log.Logger,
) EventImporter {
	return &eventImporterImpl{
		historyFetcher: historyFetcher,
		engineProvider: engineProvider,
		serializer:     serializer,
		logger:         logger,
	}
}

//nolint:revive // cognitive complexity 30 (> max enabled 25)
func (e *eventImporterImpl) ImportHistoryEventsFromBeginning(
	ctx context.Context,
	remoteCluster string,
	workflowKey definition.WorkflowKey,
	endEventId int64,
	endEventVersion int64,
) error {
	historyIterator := e.historyFetcher.GetSingleWorkflowHistoryPaginatedIteratorInclusive(
		ctx,
		remoteCluster,
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
		workflowKey.RunID,
		common2.EmptyEventID,
		common2.EmptyVersion,
		endEventId,
		endEventVersion,
	)
	engine, err := e.engineProvider(ctx, namespace.ID(workflowKey.NamespaceID), workflowKey.WorkflowID)
	if err != nil {
		return err
	}

	var blobs []*common.DataBlob
	blobSize := 0
	var token []byte
	var versionHistory *historyspb.VersionHistory
	eventsVersion := common2.EmptyVersion
	importFn := func() error {
		res, err := invokeImportWorkflowExecutionCall(ctx, engine, workflowKey, blobs, versionHistory, token, e.logger)
		if err != nil {
			return err
		}
		token = res.Token
		blobs = []*common.DataBlob{}
		blobSize = 0
		eventsVersion = common2.EmptyVersion
		return nil
	}
	for historyIterator.HasNext() {
		batch, err := historyIterator.Next()
		if err != nil {
			e.logger.Error("failed to get history events",
				tag.WorkflowNamespaceID(workflowKey.NamespaceID),
				tag.WorkflowID(workflowKey.WorkflowID),
				tag.WorkflowRunID(workflowKey.RunID),
				tag.Error(err))
			return err
		}

		if versionHistory != nil && !versionhistory.IsVersionHistoryItemsInSameBranch(versionHistory.Items, batch.VersionHistory.Items) {
			return serviceerror.NewInternal("History Branch changed during importing")
		}
		events, err := e.serializer.DeserializeEvents(batch.RawEventBatch)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return serviceerror.NewInternal("Empty events received when importing")
		}
		if eventsVersion != common2.EmptyVersion && eventsVersion != events[0].GetVersion() {
			if err := importFn(); err != nil {
				return err
			}
		}
		versionHistory = batch.VersionHistory
		eventsVersion = events[0].GetVersion()
		blobSize += len(batch.RawEventBatch.Data)
		blobs = append(blobs, batch.RawEventBatch)

		if blobSize >= historyImportPageSize || len(blobs) >= historyImportBlobSize {
			if err := importFn(); err != nil {
				return err
			}
		}
	}
	if len(blobs) != 0 {
		err = importFn()
		if err != nil {
			return err
		}
	}

	// call with empty event blob to commit the import
	response, err := invokeImportWorkflowExecutionCall(ctx, engine, workflowKey, blobs, versionHistory, token, e.logger)
	if err != nil || len(response.Token) != 0 {
		e.logger.Error("failed to commit import action",
			tag.WorkflowNamespaceID(workflowKey.NamespaceID),
			tag.WorkflowID(workflowKey.WorkflowID),
			tag.WorkflowRunID(workflowKey.RunID),
			tag.Error(err))
		return serviceerror.NewInternal("Failed to commit import transaction")
	}
	return nil
}

func invokeImportWorkflowExecutionCall(
	ctx context.Context,
	historyEngine shard.Engine,
	workflowKey definition.WorkflowKey,
	historyBatches []*common.DataBlob,
	versionHistory *historyspb.VersionHistory,
	token []byte,
	logger log.Logger,
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
		return nil, serviceerror.NewInternal("Failed to import events")
	}
	return response, nil
}
