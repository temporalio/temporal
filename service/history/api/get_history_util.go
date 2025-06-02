package api

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func GetRawHistory(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	firstEventID int64,
	nextEventID int64,
	pageSize int32,
	token []byte,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
	branchToken []byte,
) ([]*commonpb.DataBlob, []byte, error) {
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), shardContext.GetConfig().NumberOfShards)
	logger := shardContext.GetLogger()
	rawHistory, size, nextToken, err := persistence.ReadFullPageRawEvents(
		ctx, shardContext.GetExecutionManager(),
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

	// If
	//    there are no more events in DB (i.e., page token is empty),
	//    and transient/speculative WFT events are present (i.e., such WFT is in MS),
	//    and WF is still running (i.e., last event is not completion event),
	//    and the client supports transient/speculative WFT events in the history (i.e., not UI or CLI),
	// then add transient/speculative events to the history.
	if len(nextToken) == 0 && len(transientWorkflowTaskInfo.GetHistorySuffix()) > 0 && !isWorkflowCompletionEvent(lastEvent) && clientSupportsTranOrSpecEvents(ctx) {
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

		blob, err := shardContext.GetPayloadSerializer().SerializeEvents(transientWorkflowTaskInfo.HistorySuffix, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, nil, err
		}
		rawHistory = append(rawHistory, blob)
	}
	return rawHistory, nextToken, nil
}

func GetHistory(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	firstEventID int64,
	nextEventID int64,
	pageSize int32,
	nextPageToken []byte,
	tranOrSpecEvents *historyspb.TransientWorkflowTaskInfo,
	branchToken []byte,
	persistenceVisibilityMgr manager.VisibilityManager,
) (*historypb.History, []byte, error) {

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
	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.DataLoss, *serialization.DeserializationError, *serialization.SerializationError:
		// log event
		shardContext.GetLogger().Error("encountered data corruption event",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.WorkflowID(execution.GetWorkflowId()),
			tag.WorkflowRunID(execution.GetRunId()),
			tag.Error(err),
		)
		return nil, nil, err
	default:
		return nil, nil, err
	}

	logger := shardContext.GetLogger()
	metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, logger).WithTags(metrics.OperationTag(metrics.HistoryGetHistoryScope))
	metrics.HistorySize.With(metricsHandler).Record(int64(size))

	isLastPage := len(nextPageToken) == 0
	var firstEvent, lastEvent *historyspb.StrippedHistoryEvent
	if len(historyEvents) > 0 {
		firstEvent = &historyspb.StrippedHistoryEvent{
			EventId:   historyEvents[0].GetEventId(),
			EventType: historyEvents[0].GetEventType(),
		}
		lastEvent = &historyspb.StrippedHistoryEvent{
			EventId:   historyEvents[len(historyEvents)-1].GetEventId(),
			EventType: historyEvents[len(historyEvents)-1].GetEventType(),
		}
	}
	if err := VerifyHistoryIsComplete(
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

	// If
	//    there are no more events in DB (i.e., page token is empty),
	//    and transient/speculative WFT events are present (i.e., such WFT is in MS),
	//    and WF is still running (i.e., last event is not completion event),
	//    and the client supports transient/speculative WFT events in the history (i.e., not UI or CLI),
	// then add transient/speculative events to the history.
	if len(nextPageToken) == 0 && len(tranOrSpecEvents.GetHistorySuffix()) > 0 && !isWorkflowCompletionEvent(lastEvent) && clientSupportsTranOrSpecEvents(ctx) {
		if err := validateTransientWorkflowTaskEvents(nextEventID, tranOrSpecEvents); err != nil {
			metrics.ServiceErrIncompleteHistoryCounter.With(metricsHandler).Record(1)
			logger.Error("getHistory error",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
		}
		historyEvents = append(historyEvents, tranOrSpecEvents.HistorySuffix...)
	}

	ns, err := shardContext.GetNamespaceRegistry().GetNamespaceName(namespaceID)
	if err != nil {
		return nil, nil, err
	}
	if err := ProcessOutgoingSearchAttributes(
		shardContext.GetSearchAttributesProvider(),
		shardContext.GetSearchAttributesMapperProvider(),
		historyEvents,
		ns,
		persistenceVisibilityMgr); err != nil {
		return nil, nil, err
	}

	executionHistory := &historypb.History{
		Events: historyEvents,
	}
	return executionHistory, nextPageToken, nil
}

// GetHistoryReverse doesn't accept transientWorkflowTaskInfo and doesn't attach transient/speculative WFT events,
// because it is used by UI only. When support for these events is added for GetHistory API, it might be added here too.
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
) (*historypb.History, []byte, int64, error) {
	var size int
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), shardContext.GetConfig().NumberOfShards)
	var err error
	var historyEvents []*historypb.HistoryEvent

	historyEvents, size, nextPageToken, err = persistence.ReadFullPageEventsReverse(ctx, shardContext.GetExecutionManager(), &persistence.ReadHistoryBranchReverseRequest{
		BranchToken:            branchToken,
		MaxEventID:             nextEventID,
		LastFirstTransactionID: lastFirstTxnID,
		PageSize:               int(pageSize),
		NextPageToken:          nextPageToken,
		ShardID:                shardID,
	})

	logger := shardContext.GetLogger()
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

	ns, err := shardContext.GetNamespaceRegistry().GetNamespaceName(namespaceID)
	if err != nil {
		return nil, nil, 0, err
	}
	if err := ProcessOutgoingSearchAttributes(
		shardContext.GetSearchAttributesProvider(),
		shardContext.GetSearchAttributesMapperProvider(),
		historyEvents,
		ns,
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
		return serviceerror.NewDataLoss("History contains zero events.")
	}

	if !isFirstPage { // at least one page of history has been read previously
		if firstEvent.GetEventId() <= expectedFirstEventID {
			// not first page and no events have been read in the previous pages - not possible
			return serviceerror.NewDataLossf("Invalid history: expected first eventID to be > %v but got %v", expectedFirstEventID, firstEvent.GetEventId())
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

	return serviceerror.NewDataLossf("Incomplete history: expected events [%v-%v] but got events [%v-%v] of length %v: isFirstPage=%v,isLastPage=%v,pageSize=%v",
		expectedFirstEventID,
		expectedLastEventID,
		firstEvent.GetEventId(),
		lastEvent.GetEventId(),
		eventCount,
		isFirstPage,
		isLastPage,
		pageSize)
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
	if response == nil || response.History == nil {
		return nil
	}
	response.Response.History = response.History
	if isCloseEventOnly && len(response.Response.History.Events) > 0 {
		response.Response.History.Events = response.Response.History.Events[len(response.Response.History.Events)-1:]
	}
	err := ProcessOutgoingSearchAttributes(
		saProvider,
		saMapperProvider,
		response.Response.History.Events,
		ns,
		visibilityManager,
	)
	if err != nil {
		return err
	}
	err = FixFollowEvents(requestContext, versionChecker, isCloseEventOnly, response.Response.History)
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
	if isCloseEventOnly && !followsNextRunId && len(history.Events) > 0 {
		lastEvent := history.Events[len(history.Events)-1]
		fakeEvent, err := makeFakeContinuedAsNewEvent(ctx, lastEvent)
		if err != nil {
			return err
		}
		if fakeEvent != nil {
			history.Events[len(history.Events)-1] = fakeEvent
		}
	}
	return nil
}

func clientSupportsTranOrSpecEvents(
	ctx context.Context,
) bool {
	// clientVersion is the second return value.
	clientName, _ := headers.GetClientNameAndVersion(ctx)
	switch clientName {
	// Currently, CLI and UI don't support transient/speculative WFT events in the history,
	// because they might disappear and appear again. This support can be added later though.
	// Adjust a version after CLI and UI start to support it.
	case headers.ClientNameCLI:
		// ver, err := semver.Parse(clientVersion)
		// if err != nil {
		// 	return false
		// }
		// // TODO: Change "first.supported.version" to specific version (i.e. 1.20.0) when support for transient/speculative WFT events is added.
		// return ver.GT(semver.MustParse("first.supported.version"))
		return false
	case headers.ClientNameUI:
		// ver, err := semver.Parse(clientVersion)
		// if err != nil {
		// 	return false
		// }
		// // TODO: Change "first.supported.version" to specific version (i.e. 2.20.0) when support for transient/speculative WFT events is added.
		// return ver.GT(semver.MustParse("first.supported.version"))
		return false
	default:
		return true
	}
}

func isWorkflowCompletionEvent(event *historyspb.StrippedHistoryEvent) bool {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return true
	default:
		return false
	}
}

func makeFakeContinuedAsNewEvent(
	_ context.Context,
	lastEvent *historypb.HistoryEvent,
) (*historypb.HistoryEvent, error) {
	switch lastEvent.EventType { // nolint:exhaustive
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
	switch lastEvent.EventType { // nolint:exhaustive
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		attrs := lastEvent.GetWorkflowExecutionCompletedEventAttributes()
		newAttrs.NewExecutionRunId = attrs.NewExecutionRunId
		newAttrs.LastCompletionResult = attrs.Result
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		attrs := lastEvent.GetWorkflowExecutionFailedEventAttributes()
		newAttrs.NewExecutionRunId = attrs.NewExecutionRunId
		newAttrs.Failure = attrs.Failure
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		attrs := lastEvent.GetWorkflowExecutionTimedOutEventAttributes()
		newAttrs.NewExecutionRunId = attrs.NewExecutionRunId
		newAttrs.Failure = failure.NewTimeoutFailure("workflow timeout", enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	}

	return &historypb.HistoryEvent{
		EventId:   lastEvent.EventId,
		EventTime: lastEvent.EventTime,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Version:   lastEvent.Version,
		TaskId:    lastEvent.TaskId,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
			WorkflowExecutionContinuedAsNewEventAttributes: newAttrs,
		},
	}, nil
}
