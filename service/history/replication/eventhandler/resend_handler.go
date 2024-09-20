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

//go:generate mockgen -copyright_file ../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination resend_handler_mock.go

import (
	"context"
	"fmt"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
)

type (
	historyEngineProvider func(ctx context.Context, namespaceId namespace.ID, workflowId string) (shard.Engine, error)
	ResendHandler         interface {
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
		namespaceRegistry    namespace.Registry
		clientBean           client.Bean
		serializer           serialization.Serializer
		engineProvider       historyEngineProvider
		remoteHistoryFetcher HistoryPaginatedFetcher
		eventImporter        EventImporter
		logger               log.Logger
		clusterMetadata      cluster.Metadata
		config               *configs.Config
	}
)

func NewResendHandler(
	namespaceRegistry namespace.Registry,
	clientBean client.Bean,
	serializer serialization.Serializer,
	clusterMetadata cluster.Metadata,
	historyEngineProvider func(ctx context.Context, namespaceId namespace.ID, workflowId string) (shard.Engine, error),
	remoteHistoryFetcher HistoryPaginatedFetcher,
	importer EventImporter,
	logger log.Logger,
	config *configs.Config,
) ResendHandler {
	return &resendHandlerImpl{
		namespaceRegistry:    namespaceRegistry,
		clientBean:           clientBean,
		serializer:           serializer,
		engineProvider:       historyEngineProvider,
		remoteHistoryFetcher: remoteHistoryFetcher,
		eventImporter:        importer,
		logger:               logger,
		clusterMetadata:      clusterMetadata,
		config:               config,
	}
}

// ResendHistoryEvents is used to retrieve history events from remote and apply to current(passive) cluster. Mostly handle 3 cases:
//
//	1.For normal out-of-order delivery case i.e. passive has event from 1-10, got history replication task for event 20 to 25, then it should call this API with
//		  (10, 20). In this case, event (10, 20) should not include any local generated events, otherwise it is data lose on passive side.
//	2.For a special out-of-order delivery case(force replication of running workflow) i.e. passive side does not have any data for this workflow and gets history replication task for event 10
//	  i. If the event 10 is local generated event, it should be handled by history_event_handler, no resend should be triggered, otherwise it is bug.
//	  ii.The only valid case for the resend portion includes local generated events is when i.e. passive does not have any events. Local generated events are [1,10], active side
//		 has events [1,20](workflow made some progress on active side), then passive side get replication tasks for event 11 (any event from 11 to 20 is valid).
//	 3.when processing passive tasks. In this case, (startEventId,endEventId) should not include any local generated events.
//	   For example, passive cluster version is 1, passive timer task schedule event is 10, version is 1(generated by passive cluster), the task executor should
//	   call this api with (10, EmptyEventID), indicating resend all events after EventID 10. if resend portion has any local portion, it means passive cluster has data lose.
func (r *resendHandlerImpl) ResendHistoryEvents(
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
	historyEventIterator := r.remoteHistoryFetcher.GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		ctx,
		remoteClusterName,
		namespaceID,
		workflowID,
		runID,
		startEventID,
		startEventVersion,
		endEventID,
		endEventVersion)
	var firstBatch *HistoryBatch
	var err error
	if historyEventIterator.HasNext() {
		firstBatch, err = historyEventIterator.Next()
		if err != nil {
			return err
		}
	}
	if firstBatch == nil {
		return serviceerror.NewInvalidArgument("Empty batch received from remote during resend")
	}
	versionHistory := firstBatch.VersionHistory
	localVersionHistory, remoteVersionHistory := versionhistory.SplitVersionHistoryByLastLocalGeneratedItem(versionHistory.GetItems(), r.clusterMetadata.GetClusterID(), r.clusterMetadata.GetFailoverVersionIncrement())

	if len(remoteVersionHistory) == 0 {
		// rule out case 2-i
		return serviceerror.NewInvalidArgument("Invalid Resend Request.")
	}

	// most common case: no need to handle local portion
	if len(localVersionHistory) == 0 || startEventID >= localVersionHistory[len(localVersionHistory)-1].EventId {
		return r.replicateRemoteGeneratedEvents(
			ctx,
			namespaceID,
			workflowID,
			runID,
			firstBatch, // This is an optimization to reduce a network call as we already have the first batch
			historyEventIterator,
		)
	}

	if startEventID != common.EmptyEventID {
		// make sure resend is requesting from the first event when requesting local generated portion
		return serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid Resend Request: expecting to resend from first event for local generated portion, but startEventID is %v", startEventID))
	}
	lastLocalItem := localVersionHistory[len(localVersionHistory)-1]
	err = r.resendLocalGeneratedHistoryEvents(ctx, remoteClusterName, namespaceID, workflowID, runID, lastLocalItem.EventId, lastLocalItem.Version)
	if err != nil {
		return err
	}
	// no remote portion to resend
	if endEventID == localVersionHistory[len(localVersionHistory)-1].EventId+1 {
		return nil
	}
	return r.resendRemoteGeneratedHistoryEvents(
		ctx,
		remoteClusterName,
		namespaceID,
		workflowID,
		runID,
		localVersionHistory[len(localVersionHistory)-1].EventId,
		localVersionHistory[len(localVersionHistory)-1].Version,
		endEventID,
		endEventVersion,
	)
}

func (r *resendHandlerImpl) resendLocalGeneratedHistoryEvents(
	ctx context.Context,
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	endEventID int64, // inclusive
	endEventVersion int64,
) error {
	return r.eventImporter.ImportHistoryEventsFromBeginning(
		ctx,
		remoteClusterName,
		definition.WorkflowKey{
			NamespaceID: namespaceID.String(),
			WorkflowID:  workflowID,
			RunID:       runID,
		},
		endEventID,
		endEventVersion,
	)
}

func (r *resendHandlerImpl) resendRemoteGeneratedHistoryEvents(
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
	historyEventIterator := r.remoteHistoryFetcher.GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		ctx,
		remoteClusterName,
		namespaceID,
		workflowID,
		runID,
		startEventID,
		startEventVersion,
		endEventID,
		endEventVersion)
	return r.replicateRemoteGeneratedEvents(ctx, namespaceID, workflowID, runID, nil, historyEventIterator)
}

//nolint:revive // cognitive complexity 35 (> max enabled 25)
func (r *resendHandlerImpl) replicateRemoteGeneratedEvents(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	headBatch *HistoryBatch,
	historyBatchIterator collection.Iterator[*HistoryBatch],
) error {
	engine, err := r.engineProvider(ctx, namespaceID, workflowID)
	if err != nil {
		return err
	}
	var eventsBatch [][]*historypb.HistoryEvent
	var versionHistory []*historyspb.VersionHistoryItem
	const EmptyVersion = int64(-1) // 0 is a valid event version when namespace is local
	eventsVersion := EmptyVersion
	if headBatch != nil {
		events, err := r.serializer.DeserializeEvents(headBatch.RawEventBatch)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return serviceerror.NewInvalidArgument("Empty batch received from remote during resend")
		}
		eventsVersion = events[0].GetVersion()
		eventsBatch = append(eventsBatch, events)
		versionHistory = headBatch.VersionHistory.GetItems()
	}

	applyFn := func() error {
		err := engine.ReplicateHistoryEvents(
			ctx,
			definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
			nil,
			versionHistory,
			eventsBatch,
			nil,
			"",
		)
		if err != nil {
			r.logger.Error("failed to replicate events",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.Error(err))
			return err
		}
		eventsBatch = nil
		versionHistory = nil
		eventsVersion = EmptyVersion
		return nil
	}

	for historyBatchIterator.HasNext() {
		batch, err := historyBatchIterator.Next()
		if err != nil {
			r.logger.Error("failed to get history events",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.Error(err))
			return err
		}
		events, err := r.serializer.DeserializeEvents(batch.RawEventBatch)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return serviceerror.NewInvalidArgument("Empty batch received from remote during resend")
		}
		if len(eventsBatch) != 0 && len(versionHistory) != 0 {
			if !versionhistory.IsEqualVersionHistoryItems(versionHistory, batch.VersionHistory.Items) ||
				(eventsVersion != EmptyVersion && eventsVersion != events[0].Version) {
				err := applyFn()
				if err != nil {
					return err
				}
			}
		}
		eventsBatch = append(eventsBatch, events)
		versionHistory = batch.VersionHistory.Items
		eventsVersion = events[0].Version
		if len(eventsBatch) >= r.config.ReplicationResendMaxBatchCount() {
			err := applyFn()
			if err != nil {
				return err
			}
		}
	}
	if len(eventsBatch) > 0 {
		err := applyFn()
		if err != nil {
			return err
		}
	}
	return nil
}
