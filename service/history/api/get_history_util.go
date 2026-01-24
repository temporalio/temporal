package api

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"google.golang.org/protobuf/proto"
)

func GetRawHistory(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	firstEventID int64,
	nextEventID int64,
	pageSize int32,
	token []byte,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
	branchToken []byte,
) (_ []*commonpb.DataBlob, _ []byte, retError error) {
	defer func() {
		var dataLossErr *serviceerror.DataLoss
		if errors.As(retError, &dataLossErr) {
			if shardContext.GetConfig().EnableDataLossMetrics() {
				persistence.EmitDataLossMetric(
					shardContext.GetMetricsHandler(),
					namespaceName.String(),
					execution.GetWorkflowId(),
					execution.GetRunId(),
					"GetRawHistory",
					retError,
				)
			}
		}
	}()

	logger := shardContext.GetLogger()
	rawHistory, size, nextToken, err := persistence.ReadFullPageRawEvents(
		ctx, shardContext.GetExecutionManager(),
		&persistence.ReadHistoryBranchRequest{
			BranchToken:   branchToken,
			MinEventID:    firstEventID,
			MaxEventID:    nextEventID,
			PageSize:      int(pageSize),
			NextPageToken: token,
			ShardID:       shardContext.GetShardID(),
		},
	)

	if err != nil {
		return nil, nil, err
	}

	allEvents := make([]*historyspb.StrippedHistoryEvent, 0)
	var lastEventID int64
	for _, blob := range rawHistory {
		events, err := shardContext.GetPayloadSerializer().DeserializeStrippedEvents(blob)
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
	var firstEvent, lastEvent *historyspb.StrippedHistoryEvent
	if len(allEvents) > 0 {
		firstEvent = allEvents[0]
		lastEvent = allEvents[len(allEvents)-1]
	}
	if err = VerifyHistoryIsComplete(
		logger,
		firstEvent,
		lastEvent,
		len(allEvents),
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

	metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, shardContext.GetLogger()).WithTags(metrics.OperationTag(metrics.HistoryGetHistoryScope))
	metrics.HistorySize.With(metricsHandler).Record(int64(size))

	if len(nextToken) == 0 && transientWorkflowTaskInfo != nil {
		if err := validateTransientWorkflowTaskEvents(nextEventID, transientWorkflowTaskInfo); err != nil {
			logger := shardContext.GetLogger()
			metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, logger).WithTags(metrics.OperationTag(metrics.HistoryGetRawHistoryScope))
			metrics.ServiceErrIncompleteHistoryCounter.With(metricsHandler).Record(1)
			logger.Error("getHistory error",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
			return nil, nil, err
		}

		if len(transientWorkflowTaskInfo.GetHistorySuffix()) > 0 {
			blob, err := shardContext.GetPayloadSerializer().SerializeEvents(transientWorkflowTaskInfo.GetHistorySuffix())
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
	shardContext historyi.ShardContext,
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	firstEventID int64,
	nextEventID int64,
	pageSize int32,
	nextPageToken []byte,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
	branchToken []byte,
	persistenceVisibilityMgr manager.VisibilityManager,
) (history *historypb.History, token []byte, retError error) {
	defer func() {
		var dataLossErr *serviceerror.DataLoss
		var serializationErr *serialization.DeserializationError
		var deserializationErr *serialization.SerializationError
		if errors.As(retError, &dataLossErr) || errors.As(retError, &serializationErr) || errors.As(retError, &deserializationErr) {
			// log event
			shardContext.GetLogger().Error("encountered data loss event in GetHistory",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowNamespace(namespaceName.String()),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(retError),
			)
			if shardContext.GetConfig().EnableDataLossMetrics() {
				persistence.EmitDataLossMetric(
					shardContext.GetMetricsHandler(),
					namespaceName.String(),
					execution.GetWorkflowId(),
					execution.GetRunId(),
					"api_get_history",
					retError,
				)
			}
		}
	}()

	var size int
	isFirstPage := len(nextPageToken) == 0
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), shardContext.GetConfig().NumberOfShards)
	var err error
	var historyEvents []*historypb.HistoryEvent
	historyEvents, size, nextPageToken, err = persistence.ReadFullPageEvents(ctx, shardContext.GetExecutionManager(), &persistence.ReadHistoryBranchRequest{
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

	logger := shardContext.GetLogger()
	metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, logger).WithTags(metrics.OperationTag(metrics.HistoryGetHistoryScope))
	metrics.HistorySize.With(metricsHandler).Record(int64(size))

	isLastPage := len(nextPageToken) == 0
	var firstEvent, lastEvent *historyspb.StrippedHistoryEvent
	if len(historyEvents) > 0 {
		firstEvent = historyspb.StrippedHistoryEvent_builder{
			EventId: historyEvents[0].GetEventId(),
		}.Build()
		lastEvent = historyspb.StrippedHistoryEvent_builder{
			EventId: historyEvents[len(historyEvents)-1].GetEventId(),
		}.Build()
	}
	if err := VerifyHistoryIsComplete(
		shardContext.GetLogger(),
		firstEvent,
		lastEvent,
		len(historyEvents),
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
		historyEvents = append(historyEvents, transientWorkflowTaskInfo.GetHistorySuffix()...)
	}

	if err := ProcessOutgoingSearchAttributes(
		shardContext.GetSearchAttributesProvider(),
		shardContext.GetSearchAttributesMapperProvider(),
		historyEvents,
		namespaceName,
		persistenceVisibilityMgr); err != nil {
		return nil, nil, err
	}

	executionHistory := historypb.History_builder{
		Events: historyEvents,
	}.Build()
	return executionHistory, nextPageToken, nil
}

func GetHistoryReverse(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	nextEventID int64,
	lastFirstTxnID int64,
	pageSize int32,
	nextPageToken []byte,
	branchToken []byte,
	persistenceVisibilityMgr manager.VisibilityManager,
) (history *historypb.History, token []byte, lastFirstTransactionID int64, retError error) {
	ns, err := shardContext.GetNamespaceRegistry().GetNamespaceName(namespaceID)
	if err != nil {
		return nil, nil, 0, err
	}

	defer func() {
		var dataLossErr *serviceerror.DataLoss
		var serializationErr *serialization.DeserializationError
		var deserializationErr *serialization.SerializationError
		if errors.As(retError, &dataLossErr) || errors.As(retError, &serializationErr) || errors.As(retError, &deserializationErr) {
			shardContext.GetLogger().Error("encountered data loss event in GetHistoryReverse",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowNamespace(ns.String()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(retError),
			)
			if shardContext.GetConfig().EnableDataLossMetrics() {
				persistence.EmitDataLossMetric(
					shardContext.GetMetricsHandler(),
					ns.String(),
					execution.GetWorkflowId(),
					execution.GetRunId(),
					"api_get_history_reverse",
					retError,
				)
			}
		}
	}()

	var size int
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), shardContext.GetConfig().NumberOfShards)
	var historyEvents []*historypb.HistoryEvent

	historyEvents, size, nextPageToken, err = persistence.ReadFullPageEventsReverse(ctx, shardContext.GetExecutionManager(), &persistence.ReadHistoryBranchReverseRequest{
		BranchToken:            branchToken,
		MaxEventID:             nextEventID,
		LastFirstTransactionID: lastFirstTxnID,
		PageSize:               int(pageSize),
		NextPageToken:          nextPageToken,
		ShardID:                shardID,
	})

	if err != nil {
		return nil, nil, 0, err
	}

	logger := shardContext.GetLogger()
	metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, logger).WithTags(metrics.OperationTag(metrics.HistoryGetHistoryReverseScope))
	metrics.HistorySize.With(metricsHandler).Record(int64(size))

	if err := ProcessOutgoingSearchAttributes(
		shardContext.GetSearchAttributesProvider(),
		shardContext.GetSearchAttributesMapperProvider(),
		historyEvents,
		ns,
		persistenceVisibilityMgr); err != nil {
		return nil, nil, 0, err
	}

	executionHistory := historypb.History_builder{
		Events: historyEvents,
	}.Build()

	var newNextEventID int64
	if len(historyEvents) > 0 {
		newNextEventID = historyEvents[len(historyEvents)-1].GetEventId() - 1
	} else {
		newNextEventID = nextEventID
	}

	return executionHistory, nextPageToken, newNextEventID, nil
}

func ProcessOutgoingSearchAttributes(
	saProvider searchattribute.Provider,
	saMapperProvider searchattribute.MapperProvider,
	events []*historypb.HistoryEvent,
	ns namespace.Name,
	persistenceVisibilityMgr manager.VisibilityManager,
) error {
	saTypeMap, err := saProvider.GetSearchAttributes(persistenceVisibilityMgr.GetIndexName(), false)
	if err != nil {
		return serviceerror.NewUnavailablef(consts.ErrUnableToGetSearchAttributesMessage, err)
	}
	for _, event := range events {
		var searchAttributes *commonpb.SearchAttributes
		switch event.GetEventType() {
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
				searchAttributes.SetIndexedFields(aliasedSas.GetIndexedFields())
			}
		}
	}

	return nil
}

func validateTransientWorkflowTaskEvents(
	eventIDOffset int64,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
) error {
	for i, event := range transientWorkflowTaskInfo.GetHistorySuffix() {
		expectedEventID := eventIDOffset + int64(i)
		if event.GetEventId() != expectedEventID {
			return serviceerror.NewInternalf(
				"invalid transient workflow task at position %v; expected event ID %v, found event ID %v",
				i,
				expectedEventID,
				event.GetEventId())
		}
	}

	return nil
}

func VerifyHistoryIsComplete(
	logger log.Logger,
	firstEvent *historyspb.StrippedHistoryEvent,
	lastEvent *historyspb.StrippedHistoryEvent,
	eventCount int,
	expectedFirstEventID int64,
	expectedLastEventID int64,
	isFirstPage bool,
	isLastPage bool,
	pageSize int,
) error {

	if eventCount == 0 {
		if isLastPage {
			// we seem to be returning a non-nil pageToken on the lastPage which
			// in turn cases the client to call getHistory again - only to find
			// there are no more events to consume - bail out if this is the case here
			return nil
		}
		return softassert.UnexpectedDataLoss(logger, "History contains zero events", nil)
	}

	if !isFirstPage { // at least one page of history has been read previously
		if firstEvent.GetEventId() <= expectedFirstEventID {
			// not first page and no events have been read in the previous pages - not possible
			return softassert.UnexpectedDataLoss(logger,
				"Invalid history: expected first eventID to be greater than last seen",
				fmt.Errorf("expected > %v but got %v", expectedFirstEventID, firstEvent.GetEventId()))
		}
		expectedFirstEventID = firstEvent.GetEventId()
	}

	if !isLastPage {
		// estimate lastEventID based on pageSize. This is a lower bound
		// since the persistence layer counts "batch of events" as a single page
		expectedLastEventID = expectedFirstEventID + int64(pageSize) - 1
	}

	nExpectedEvents := expectedLastEventID - expectedFirstEventID + 1

	if firstEvent.GetEventId() == expectedFirstEventID &&
		((isLastPage && lastEvent.GetEventId() == expectedLastEventID && int64(eventCount) == nExpectedEvents) ||
			(!isLastPage && lastEvent.GetEventId() >= expectedLastEventID && int64(eventCount) >= nExpectedEvents)) {
		return nil
	}

	return softassert.UnexpectedDataLoss(logger,
		"Incomplete history",
		fmt.Errorf("expected events [%v-%v] but got events [%v-%v] of length %v: isFirstPage=%v,isLastPage=%v,pageSize=%v",
			expectedFirstEventID,
			expectedLastEventID,
			firstEvent.GetEventId(),
			lastEvent.GetEventId(),
			eventCount,
			isFirstPage,
			isLastPage,
			pageSize))
}

// ProcessInternalRawHistory processes history in the field response.History.
// History service can send history events in response.History.Events. In that case, process the events and move them
// to response.Response.History. Usually this is done by history service but when history.sendRawHistoryBetweenInternalServices
// is enabled, history service sends raw history events to frontend without any processing.
func ProcessInternalRawHistory(
	requestContext context.Context,
	saProvider searchattribute.Provider,
	saMapperProvider searchattribute.MapperProvider,
	response *historyservice.GetWorkflowExecutionHistoryResponse,
	visibilityManager manager.VisibilityManager,
	versionChecker headers.VersionChecker,
	ns namespace.Name,
	isCloseEventOnly bool,
) error {
	if response == nil || !response.HasHistory() {
		return nil
	}
	response.GetResponse().SetHistory(response.GetHistory())
	if isCloseEventOnly && len(response.GetResponse().GetHistory().GetEvents()) > 0 {
		response.GetResponse().GetHistory().SetEvents(response.GetResponse().GetHistory().GetEvents()[len(response.GetResponse().GetHistory().GetEvents())-1:])
	}
	err := ProcessOutgoingSearchAttributes(
		saProvider,
		saMapperProvider,
		response.GetResponse().GetHistory().GetEvents(),
		ns,
		visibilityManager,
	)
	if err != nil {
		return err
	}
	err = FixFollowEvents(requestContext, versionChecker, isCloseEventOnly, response.GetResponse().GetHistory())
	if err != nil {
		return err
	}
	return nil
}

func FixFollowEvents(
	ctx context.Context,
	versionChecker headers.VersionChecker,
	isCloseEventOnly bool,
	history *historypb.History,
) error {
	// Backwards-compatibility fix for retry events after #1866: older SDKs don't know how to "follow"
	// subsequent runs linked in WorkflowExecutionFailed or TimedOut events, so they'll get the wrong result
	// when trying to "get" the result of a workflow run. (This applies to cron runs also but "get" on a cron
	// workflow isn't really sensible.)
	//
	// To handle this in a backwards-compatible way, we'll pretend the completion event is actually
	// ContinuedAsNew, if it's Failed or TimedOut. We want to do this only when the client is looking for a
	// completion event, and not when it's getting the history to display for other purposes. The best signal
	// for that purpose is `isCloseEventOnly`. (We can't use `isLongPoll` also because in some cases, older
	// versions of the Java SDK don't set that flag.)
	//
	// TODO: We can remove this once we no longer support SDK versions prior to around September 2021.
	// Revisit this once we have an SDK deprecation policy.
	followsNextRunId := versionChecker.ClientSupportsFeature(ctx, headers.FeatureFollowsNextRunID)
	if isCloseEventOnly && !followsNextRunId && len(history.GetEvents()) > 0 {
		lastEvent := history.GetEvents()[len(history.GetEvents())-1]
		fakeEvent, err := makeFakeContinuedAsNewEvent(ctx, lastEvent)
		if err != nil {
			return err
		}
		if fakeEvent != nil {
			history.GetEvents()[len(history.GetEvents())-1] = fakeEvent
		}
	}
	return nil
}

func makeFakeContinuedAsNewEvent(
	_ context.Context,
	lastEvent *historypb.HistoryEvent,
) (*historypb.HistoryEvent, error) {
	switch lastEvent.GetEventType() { // nolint:exhaustive
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		if lastEvent.GetWorkflowExecutionCompletedEventAttributes().GetNewExecutionRunId() == "" {
			return nil, nil
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		if lastEvent.GetWorkflowExecutionFailedEventAttributes().GetNewExecutionRunId() == "" {
			return nil, nil
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		if lastEvent.GetWorkflowExecutionTimedOutEventAttributes().GetNewExecutionRunId() == "" {
			return nil, nil
		}
	default:
		return nil, nil
	}

	// We need to replace the last event with a continued-as-new event that has at least the
	// NewExecutionRunId field. We don't actually need any other fields, since that's the only one
	// the client looks at in this case, but copy the last result or failure from the real completed
	// event just so it's clear what the result was.
	newAttrs := &historypb.WorkflowExecutionContinuedAsNewEventAttributes{}
	switch lastEvent.GetEventType() { // nolint:exhaustive
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		attrs := lastEvent.GetWorkflowExecutionCompletedEventAttributes()
		newAttrs.SetNewExecutionRunId(attrs.GetNewExecutionRunId())
		newAttrs.SetLastCompletionResult(attrs.GetResult())
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		attrs := lastEvent.GetWorkflowExecutionFailedEventAttributes()
		newAttrs.SetNewExecutionRunId(attrs.GetNewExecutionRunId())
		newAttrs.SetFailure(attrs.GetFailure())
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		attrs := lastEvent.GetWorkflowExecutionTimedOutEventAttributes()
		newAttrs.SetNewExecutionRunId(attrs.GetNewExecutionRunId())
		newAttrs.SetFailure(failure.NewTimeoutFailure("workflow timeout", enumspb.TIMEOUT_TYPE_START_TO_CLOSE))
	}

	return historypb.HistoryEvent_builder{
		EventId:   lastEvent.GetEventId(),
		EventTime: lastEvent.GetEventTime(),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Version:   lastEvent.GetVersion(),
		TaskId:    lastEvent.GetTaskId(),
		WorkflowExecutionContinuedAsNewEventAttributes: proto.ValueOrDefault(newAttrs),
	}.Build(), nil
}
