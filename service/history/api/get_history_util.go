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
	nextPageToken []byte,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
	branchToken []byte,
) ([]*commonpb.DataBlob, []byte, error) {
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), shard.GetConfig().NumberOfShards)

	persistenceExecutionManager := shard.GetExecutionManager()
	resp, err := persistenceExecutionManager.ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      int(pageSize),
		NextPageToken: nextPageToken,
		ShardID:       shardID,
	})
	if err != nil {
		return nil, nil, err
	}

	rawHistory := resp.HistoryEventBlobs

	if len(resp.NextPageToken) == 0 && transientWorkflowTaskInfo != nil {
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

		for _, event := range transientWorkflowTaskInfo.HistorySuffix {
			blob, err := shard.GetPayloadSerializer().SerializeEvent(event, enumspb.ENCODING_TYPE_PROTO3)
			if err != nil {
				return nil, nil, err
			}
			rawHistory = append(rawHistory, blob)
		}
	}

	return rawHistory, resp.NextPageToken, nil
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
	isFirstPage := len(nextPageToken) == 0
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
	case *serviceerror.DataLoss:
		// log event
		shard.GetLogger().Error("encountered data loss event",
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

	isLastPage := len(nextPageToken) == 0
	if err := verifyHistoryIsComplete(
		historyEvents,
		firstEventID,
		nextEventID-1,
		isFirstPage,
		isLastPage,
		int(pageSize)); err != nil {
		metrics.ServiceErrIncompleteHistoryCounter.With(metricsHandler).Record(1)
		logger.Error("getHistory: incomplete history",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.WorkflowID(execution.GetWorkflowId()),
			tag.WorkflowRunID(execution.GetRunId()),
			tag.Error(err))
	}

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

	if err := ProcessOutgoingSearchAttributes(shard, historyEvents, namespaceID, persistenceVisibilityMgr); err != nil {
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

	if err := ProcessOutgoingSearchAttributes(shard, historyEvents, namespaceID, persistenceVisibilityMgr); err != nil {
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
	shard shard.Context,
	events []*historypb.HistoryEvent,
	namespaceId namespace.ID,
	persistenceVisibilityMgr manager.VisibilityManager,
) error {
	namespace, err := shard.GetNamespaceRegistry().GetNamespaceName(namespaceId)
	if err != nil {
		return err
	}
	saTypeMap, err := shard.GetSearchAttributesProvider().GetSearchAttributes(persistenceVisibilityMgr.GetIndexName(), false)
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
			aliasedSas, err := searchattribute.AliasFields(shard.GetSearchAttributesMapperProvider(), searchAttributes, namespace.String())
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

func verifyHistoryIsComplete(
	events []*historypb.HistoryEvent,
	expectedFirstEventID int64,
	expectedLastEventID int64,
	isFirstPage bool,
	isLastPage bool,
	pageSize int,
) error {

	nEvents := len(events)
	if nEvents == 0 {
		if isLastPage {
			// we seem to be returning a non-nil pageToken on the lastPage which
			// in turn cases the client to call getHistory again - only to find
			// there are no more events to consume - bail out if this is the case here
			return nil
		}
		return serviceerror.NewDataLoss("History contains zero events.")
	}

	firstEventID := events[0].GetEventId()
	lastEventID := events[nEvents-1].GetEventId()

	if !isFirstPage { // at least one page of history has been read previously
		if firstEventID <= expectedFirstEventID {
			// not first page and no events have been read in the previous pages - not possible
			return serviceerror.NewDataLoss(fmt.Sprintf("Invalid history: expected first eventID to be > %v but got %v", expectedFirstEventID, firstEventID))
		}
		expectedFirstEventID = firstEventID
	}

	if !isLastPage {
		// estimate lastEventID based on pageSize. This is a lower bound
		// since the persistence layer counts "batch of events" as a single page
		expectedLastEventID = expectedFirstEventID + int64(pageSize) - 1
	}

	nExpectedEvents := expectedLastEventID - expectedFirstEventID + 1

	if firstEventID == expectedFirstEventID &&
		((isLastPage && lastEventID == expectedLastEventID && int64(nEvents) == nExpectedEvents) ||
			(!isLastPage && lastEventID >= expectedLastEventID && int64(nEvents) >= nExpectedEvents)) {
		return nil
	}

	return serviceerror.NewDataLoss(fmt.Sprintf("Incomplete history: expected events [%v-%v] but got events [%v-%v] of length %v: isFirstPage=%v,isLastPage=%v,pageSize=%v",
		expectedFirstEventID,
		expectedLastEventID,
		firstEventID,
		lastEventID,
		nEvents,
		isFirstPage,
		isLastPage,
		pageSize))
}
