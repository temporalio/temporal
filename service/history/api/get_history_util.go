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

package api

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
)

func GetRawHistory(
	ctx context.Context,
	shard shard.Context,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	firstEventID int64,
	nextEventID int64,
	pageSize int32,
	token []byte,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
	branchToken []byte,
) ([]*commonpb.DataBlob, []byte, error) {
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), shard.GetConfig().NumberOfShards)
	logger := shard.GetLogger()
	rawHistory, size, nextToken, err := persistence.ReadFullPageRawEvents(
		ctx, shard.GetExecutionManager(),
		&persistence.ReadHistoryBranchRequest{
			BranchToken:   branchToken,
			MinEventID:    firstEventID,
			MaxEventID:    nextEventID,
			PageSize:      int(pageSize),
			NextPageToken: token,
			ShardID:       shardID,
		},
	)

	if err != nil {
		return nil, nil, err
	}

	allEvents := make([]*historyspb.StrippedHistoryEvent, 0)
	var lastEventID int64
	for _, blob := range rawHistory {
		events, err := shard.GetPayloadSerializer().DeserializeStrippedEvents(blob)
		if err != nil {
			return nil, nil, err
		}
		err = persistence.ValidateBatch(events, branchToken, lastEventID, logger)
		if err != nil {
			return nil, nil, err
		}
		allEvents = append(allEvents, events...)
		lastEventID = events[len(events)-1].GetEventId()
	}
	if err = persistence.VerifyHistoryIsComplete(
		allEvents,
		firstEventID,
		nextEventID-1,
		len(token) == 0,
		len(nextToken) == 0,
		int(pageSize),
	); err != nil {
		metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, logger).WithTags(metrics.OperationTag(metrics.HistoryGetHistoryScope))
		metrics.ServiceErrIncompleteHistoryCounter.With(metricsHandler).Record(1)
		logger.Error("getHistory: incomplete history",
			tag.WorkflowBranchToken(branchToken),
			tag.Error(err))
	}

	metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, shard.GetLogger()).WithTags(metrics.OperationTag(metrics.HistoryGetHistoryScope))
	metrics.HistorySize.With(metricsHandler).Record(int64(size))

	if len(nextToken) == 0 && transientWorkflowTaskInfo != nil {
		if err := validateTransientWorkflowTaskEvents(nextEventID, transientWorkflowTaskInfo); err != nil {
			logger := shard.GetLogger()
			metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, logger).WithTags(metrics.OperationTag(metrics.HistoryGetRawHistoryScope))
			metrics.ServiceErrIncompleteHistoryCounter.With(metricsHandler).Record(1)
			logger.Error("getHistory error",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
			return nil, nil, err
		}

		if len(transientWorkflowTaskInfo.HistorySuffix) > 0 {
			blob, err := shard.GetPayloadSerializer().SerializeEvents(transientWorkflowTaskInfo.HistorySuffix, enumspb.ENCODING_TYPE_PROTO3)
			if err != nil {
				return nil, nil, err
			}
			rawHistory = append(rawHistory, blob)
		}
	}
	return rawHistory, nextToken, nil
}

func GetHistory(
	ctx context.Context,
	shard shard.Context,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	firstEventID int64,
	nextEventID int64,
	pageSize int32,
	nextPageToken []byte,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
	branchToken []byte,
	persistenceVisibilityMgr manager.VisibilityManager,
) (*historypb.History, []byte, error) {

	var size int
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), shard.GetConfig().NumberOfShards)
	var err error
	var historyEvents []*historypb.HistoryEvent
	historyEvents, size, nextPageToken, err = persistence.ReadFullPageEvents(ctx, shard.GetExecutionManager(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      int(pageSize),
		NextPageToken: nextPageToken,
		ShardID:       shardID,
	})
	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.DataLoss, *serialization.DeserializationError, *serialization.SerializationError:
		// log event
		shard.GetLogger().Error("encountered data corruption event",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.WorkflowID(execution.GetWorkflowId()),
			tag.WorkflowRunID(execution.GetRunId()),
			tag.Error(err),
		)
		return nil, nil, err
	default:
		return nil, nil, err
	}

	logger := shard.GetLogger()
	metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, logger).WithTags(metrics.OperationTag(metrics.HistoryGetHistoryScope))
	metrics.HistorySize.With(metricsHandler).Record(int64(size))

	if len(nextPageToken) == 0 && transientWorkflowTaskInfo != nil {
		if err := validateTransientWorkflowTaskEvents(nextEventID, transientWorkflowTaskInfo); err != nil {
			metrics.ServiceErrIncompleteHistoryCounter.With(metricsHandler).Record(1)
			logger.Error("getHistory error",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
		}
		// Append the transient workflow task events once we are done enumerating everything from the events table
		historyEvents = append(historyEvents, transientWorkflowTaskInfo.HistorySuffix...)
	}

	if err := ProcessOutgoingSearchAttributes(
		shard.GetNamespaceRegistry(),
		shard.GetSearchAttributesProvider(),
		shard.GetSearchAttributesMapperProvider(),
		historyEvents,
		namespaceID,
		persistenceVisibilityMgr); err != nil {
		return nil, nil, err
	}

	executionHistory := &historypb.History{
		Events: historyEvents,
	}
	return executionHistory, nextPageToken, nil
}

func GetHistoryReverse(
	ctx context.Context,
	shard shard.Context,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	nextEventID int64,
	lastFirstTxnID int64,
	pageSize int32,
	nextPageToken []byte,
	branchToken []byte,
	persistenceVisibilityMgr manager.VisibilityManager,
) (*historypb.History, []byte, int64, error) {
	var size int
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), shard.GetConfig().NumberOfShards)
	var err error
	var historyEvents []*historypb.HistoryEvent

	historyEvents, size, nextPageToken, err = persistence.ReadFullPageEventsReverse(ctx, shard.GetExecutionManager(), &persistence.ReadHistoryBranchReverseRequest{
		BranchToken:            branchToken,
		MaxEventID:             nextEventID,
		LastFirstTransactionID: lastFirstTxnID,
		PageSize:               int(pageSize),
		NextPageToken:          nextPageToken,
		ShardID:                shardID,
	})

	logger := shard.GetLogger()
	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.DataLoss:
		// log event
		logger.Error("encountered data loss event", tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowID(execution.GetWorkflowId()), tag.WorkflowRunID(execution.GetRunId()))
		return nil, nil, 0, err
	default:
		return nil, nil, 0, err
	}

	metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, logger).WithTags(metrics.OperationTag(metrics.HistoryGetHistoryReverseScope))
	metrics.HistorySize.With(metricsHandler).Record(int64(size))

	if err := ProcessOutgoingSearchAttributes(
		shard.GetNamespaceRegistry(),
		shard.GetSearchAttributesProvider(),
		shard.GetSearchAttributesMapperProvider(),
		historyEvents,
		namespaceID,
		persistenceVisibilityMgr); err != nil {
		return nil, nil, 0, err
	}

	executionHistory := &historypb.History{
		Events: historyEvents,
	}

	var newNextEventID int64
	if len(historyEvents) > 0 {
		newNextEventID = historyEvents[len(historyEvents)-1].EventId - 1
	} else {
		newNextEventID = nextEventID
	}

	return executionHistory, nextPageToken, newNextEventID, nil
}

func ProcessOutgoingSearchAttributes(
	nsRegistry namespace.Registry,
	saProvider searchattribute.Provider,
	saMapperProvider searchattribute.MapperProvider,
	events []*historypb.HistoryEvent,
	namespaceId namespace.ID,
	persistenceVisibilityMgr manager.VisibilityManager,
) error {
	ns, err := nsRegistry.GetNamespaceName(namespaceId)
	if err != nil {
		return err
	}
	saTypeMap, err := saProvider.GetSearchAttributes(persistenceVisibilityMgr.GetIndexName(), false)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(consts.ErrUnableToGetSearchAttributesMessage, err))
	}
	for _, event := range events {
		var searchAttributes *commonpb.SearchAttributes
		switch event.EventType {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
			searchAttributes = event.GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes()
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
			searchAttributes = event.GetWorkflowExecutionContinuedAsNewEventAttributes().GetSearchAttributes()
		case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
			searchAttributes = event.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetSearchAttributes()
		case enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
			searchAttributes = event.GetUpsertWorkflowSearchAttributesEventAttributes().GetSearchAttributes()
		}
		if searchAttributes != nil {
			searchattribute.ApplyTypeMap(searchAttributes, saTypeMap)
			aliasedSas, err := searchattribute.AliasFields(saMapperProvider, searchAttributes, ns.String())
			if err != nil {
				return err
			}
			if aliasedSas != searchAttributes {
				searchAttributes.IndexedFields = aliasedSas.IndexedFields
			}
		}
	}

	return nil
}

func validateTransientWorkflowTaskEvents(
	eventIDOffset int64,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
) error {
	for i, event := range transientWorkflowTaskInfo.HistorySuffix {
		expectedEventID := eventIDOffset + int64(i)
		if event.GetEventId() != expectedEventID {
			return serviceerror.NewInternal(
				fmt.Sprintf(
					"invalid transient workflow task at position %v; expected event ID %v, found event ID %v",
					i,
					expectedEventID,
					event.GetEventId()))
		}
	}

	return nil
}
