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
	historyspb "go.temporal.io/server/api/history/v1"
	workflowpb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/shard"
)

//go:generate mockgen -copyright_file ../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination remote_events_handler_mock.go

type (
	RemoteGeneratedEventsHandler interface {
		HandleRemoteGeneratedHistoryEvents(
			ctx context.Context,
			workflowKey definition.WorkflowKey,
			baseExecutionInfo *workflowpb.BaseExecutionInfo,
			versionHistoryItems []*historyspb.VersionHistoryItem,
			historyEvents [][]*historypb.HistoryEvent,
			newEvents []*historypb.HistoryEvent,
			newRunID string,
		) error
		ResendRemoteGeneratedHistoryEvents(
			ctx context.Context,
			remoteClusterName string,
			namespaceID namespace.ID,
			workflowID string,
			runID string,
			startEventID int64,
			startEventVersion int64,
			endEventID int64,
			endEventVersion int64,
		) error
	}

	futureEventsHandlerImpl struct {
		shardController         shard.Controller
		historyPaginatedFetcher HistoryPaginatedFetcher
		eventSerializer         serialization.Serializer
	}
)

func NewRemoteGeneratedEventsHandler(shardController shard.Controller) RemoteGeneratedEventsHandler {
	return &futureEventsHandlerImpl{
		shardController: shardController,
	}
}

func (f futureEventsHandlerImpl) HandleRemoteGeneratedHistoryEvents(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	baseExecutionInfo *workflowpb.BaseExecutionInfo,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	historyEvents [][]*historypb.HistoryEvent,
	newEvents []*historypb.HistoryEvent,
	newRunID string,
) error {
	shardContext, err := f.shardController.GetShardByNamespaceWorkflow(
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
	return engine.ReplicateHistoryEvents(
		ctx,
		workflowKey,
		baseExecutionInfo,
		versionHistoryItems,
		historyEvents,
		newEvents,
		newRunID,
	)
}

func (f futureEventsHandlerImpl) ResendRemoteGeneratedHistoryEvents(
	ctx context.Context,
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	startEventID int64,
	startEventVersion int64,
	endEventID int64,
	endEventVersion int64,
) error {
	historyEventIterator := f.historyPaginatedFetcher.GetSingleWorkflowHistoryPaginatedIterator(
		ctx,
		remoteClusterName,
		namespaceID,
		workflowID,
		runID,
		startEventID,
		startEventVersion,
		endEventID,
		endEventVersion)
	shardContext, err := f.shardController.GetShardByNamespaceWorkflow(
		namespaceID,
		workflowID,
	)
	if err != nil {
		return err
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	for historyEventIterator.HasNext() {
		historyBatch, err := historyEventIterator.Next()
		if err != nil {
			return err
		}
		events, err := f.eventSerializer.DeserializeEvents(historyBatch.RawEventBatch)
		if err != nil {
			return err
		}
		err = engine.ReplicateHistoryEvents(
			ctx,
			definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
			nil,
			historyBatch.VersionHistory.GetItems(),
			[][]*historypb.HistoryEvent{events},
			nil,
			"",
		)
		if err != nil {
			return err
		}
	}
	return nil
}
