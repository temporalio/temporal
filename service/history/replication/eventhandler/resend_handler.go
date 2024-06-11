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
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/shard"
)

type (
	ResendHandler interface {
		ResendHistoryEvents(
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
	resendHandlerImpl struct {
		namespaceRegistry     namespace.Registry
		clientBean            client.Bean
		serializer            serialization.Serializer
		historyEngineProvider func(ctx context.Context, namespaceId namespace.ID, workflowId string) (shard.Engine, error)
		rereplicationTimeout  dynamicconfig.DurationPropertyFnWithNamespaceIDFilter
		remoteHistoryFetcher  HistoryPaginatedFetcher
		logger                log.Logger
		clusterMetadata       cluster.Metadata
	}
)

func NewResendHandler(
	namespaceRegistry namespace.Registry,
	clientBean client.Bean,
	serializer serialization.Serializer,
	clusterMetadata cluster.Metadata,
	historyEngineProvider func(ctx context.Context, namespaceId namespace.ID, workflowId string) (shard.Engine, error),
	rereplicationTimeout dynamicconfig.DurationPropertyFnWithNamespaceIDFilter,
	remoteHistoryFetcher HistoryPaginatedFetcher,
	logger log.Logger,
) ResendHandler {
	return &resendHandlerImpl{
		namespaceRegistry:     namespaceRegistry,
		clientBean:            clientBean,
		serializer:            serializer,
		historyEngineProvider: historyEngineProvider,
		rereplicationTimeout:  rereplicationTimeout,
		remoteHistoryFetcher:  remoteHistoryFetcher,
		logger:                logger,
		clusterMetadata:       clusterMetadata,
	}
}

func (r *resendHandlerImpl) ResendHistoryEvents(
	ctx context.Context,
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	startEventID int64, // exclusive
	startEventVersion int64,
	endEventID int64, // exclusive
	endEventVersion int64,
) error {
	versionHistory, err := r.remoteHistoryFetcher.GetWorkflowVersionHistory(
		ctx,
		remoteClusterName,
		namespaceID,
		workflowID,
		runID,
		startEventID,
		startEventVersion,
		endEventID,
		endEventVersion,
	)
	if err != nil {
		return err
	}
	startEventIDInclusive := startEventID + 1
	startEventVersionInclusive, err := versionhistory.GetVersionHistoryEventVersion(versionHistory, startEventID+1)
	if err != nil {
		return nil
	}
	var endEventIDInclusive, endEventVersionInclusive int64
	if endEventID == common.EmptyEventID {
		lastVersionItem, err := versionhistory.GetLastVersionHistoryItem(versionHistory)
		if err != nil {
			return err
		}
		endEventIDInclusive = lastVersionItem.GetEventId()
		endEventVersionInclusive = lastVersionItem.GetVersion()
	} else {
		endEventIDInclusive = endEventID - 1
		endEventVersionInclusive, err = versionhistory.GetVersionHistoryEventVersion(versionHistory, endEventID-1)
		if err != nil {
			return nil
		}
	}
	localVersionHistory, remoteVersionHistory := versionhistory.SplitVersionHistoryByLastLocalGeneratedItem(versionHistory.GetItems(), r.clusterMetadata.GetClusterID(), r.clusterMetadata.GetFailoverVersionIncrement())
	// resend request has local generated events portion, handle them first
	if len(localVersionHistory) != 0 && startEventIDInclusive <= localVersionHistory[len(localVersionHistory)-1].EventId {
		localLastItem := localVersionHistory[len(localVersionHistory)-1]
		err = r.resendLocalGeneratedHistoryEvents(
			ctx, remoteClusterName, namespaceID, workflowID, runID, startEventIDInclusive, startEventVersionInclusive, localLastItem.EventId, localLastItem.Version)
		if err != nil {
			return err
		}
		if len(remoteVersionHistory) == 0 {
			return nil
		}
		startEventIDInclusive = remoteVersionHistory[0].EventId
		startEventVersionInclusive = remoteVersionHistory[0].Version
	}
	if startEventIDInclusive > endEventIDInclusive {
		return nil
	}

	return r.resendRemoteGeneratedHistoryEvents(
		ctx,
		remoteClusterName,
		namespaceID,
		workflowID,
		runID,
		startEventIDInclusive,
		startEventVersionInclusive,
		endEventIDInclusive,
		endEventVersionInclusive,
	)
}

func (r *resendHandlerImpl) resendLocalGeneratedHistoryEvents(
	ctx context.Context,
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	startEventID int64, // inclusive
	startEventVersion int64,
	endEventID int64, // inclusive
	endEventVersion int64,
) error {
	engine, err := r.historyEngineProvider(ctx, namespaceID, workflowID)
	if err != nil {
		return err
	}
	return importEvents(
		ctx,
		remoteClusterName,
		engine,
		definition.WorkflowKey{
			NamespaceID: namespaceID.String(),
			WorkflowID:  workflowID,
			RunID:       runID,
		},
		startEventID,
		startEventVersion,
		endEventID,
		endEventVersion,
		nil,
		r.remoteHistoryFetcher,
		r.logger,
		r.serializer,
	)
}

func (r *resendHandlerImpl) resendRemoteGeneratedHistoryEvents(
	ctx context.Context,
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	startEventID int64, // inclusive
	startEventVersion int64,
	endEventID int64, // inclusive
	endEventVersion int64,
) error {
	historyEventIterator := r.remoteHistoryFetcher.GetSingleWorkflowHistoryPaginatedIterator(
		ctx,
		remoteClusterName,
		namespaceID,
		workflowID,
		runID,
		startEventID,
		startEventVersion,
		endEventID,
		endEventVersion)

	engine, err := r.historyEngineProvider(ctx, namespaceID, workflowID)
	if err != nil {
		return err
	}
	for historyEventIterator.HasNext() {
		historyBatch, err := historyEventIterator.Next()
		if err != nil {
			return err
		}
		events, err := r.serializer.DeserializeEvents(historyBatch.RawEventBatch)
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
