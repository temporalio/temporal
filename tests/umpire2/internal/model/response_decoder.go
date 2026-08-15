package model

import (
	"strconv"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/fact"
)

type responseDecoder struct {
	deliveryMu sync.Mutex
	deliveries map[string]deliveryAssociation
}

type deliveryAssociation struct {
	namespaceID string
	callbackID  string
}

func newResponseDecoder() *responseDecoder {
	return &responseDecoder{deliveries: make(map[string]deliveryAssociation)}
}

// fromResponse creates a fact from a gRPC request+response pair, or nil if unrecognized.
func (d *responseDecoder) First(req, resp any, namespaceID string) umpire.Fact {
	facts := decodeResponseFacts(req, resp, namespaceID)
	if len(facts) == 0 {
		return nil
	}
	return facts[0]
}

// fromResponses creates every fact from a gRPC request+response pair.
func (d *responseDecoder) Decode(req, resp any, namespaceID string) []umpire.Fact {
	facts := decodeResponseFacts(req, resp, namespaceID)
	switch request := req.(type) {
	case *workflowservice.PollNexusTaskQueueRequest:
		response, ok := resp.(*workflowservice.PollNexusTaskQueueResponse)
		if !ok || len(response.GetTaskToken()) == 0 || len(facts) == 0 {
			return facts
		}
		callback, ok := facts[0].(*fact.NexusCallbackObservation)
		if !ok || callback.Malformed || callback.CallbackID == "" {
			return facts
		}
		deliveryID, err := fact.NexusTaskDeliveryID(response.GetTaskToken())
		if err != nil {
			return facts
		}
		d.deliveryMu.Lock()
		if _, exists := d.deliveries[deliveryID]; !exists {
			d.deliveries[deliveryID] = deliveryAssociation{namespaceID: callback.NamespaceID, callbackID: callback.CallbackID}
		}
		d.deliveryMu.Unlock()
		_ = request
		return facts
	case *workflowservice.RespondNexusTaskCompletedRequest:
		if _, ok := resp.(*workflowservice.RespondNexusTaskCompletedResponse); !ok {
			return facts
		}
		deliveryID, err := fact.NexusTaskDeliveryID(request.GetTaskToken())
		if err != nil || request.GetResponse().GetStartOperation() == nil {
			return facts
		}
		d.deliveryMu.Lock()
		association, found := d.deliveries[deliveryID]
		d.deliveryMu.Unlock()
		if !found {
			return facts
		}
		if observed := fact.NewNexusStartResponse(association.namespaceID, association.callbackID, deliveryID, request.GetResponse().GetStartOperation()); observed != nil {
			facts = append(facts, observed)
		}
	default:
		return facts
	}
	return facts
}

// Purge removes ephemeral delivery-to-callback correlations for one namespace.
func (d *responseDecoder) Purge(namespaceID string) {
	d.deliveryMu.Lock()
	defer d.deliveryMu.Unlock()
	for deliveryID, association := range d.deliveries {
		if association.namespaceID == namespaceID {
			delete(d.deliveries, deliveryID)
		}
	}
}

func decodeResponseFacts(req, resp any, namespaceID string) []umpire.Fact {
	switch req := req.(type) {
	case *workflowservice.DescribeActivityExecutionRequest:
		response, ok := resp.(*workflowservice.DescribeActivityExecutionResponse)
		if !ok || response.GetInfo() == nil {
			return nil
		}
		return []umpire.Fact{fact.NewActivityExecutionSnapshot(namespaceID, req.GetActivityId(), response.GetInfo().GetStatus(), response.GetInfo().GetLinks())}
	case *workflowservice.DescribeNexusOperationExecutionRequest:
		response, ok := resp.(*workflowservice.DescribeNexusOperationExecutionResponse)
		if !ok || response.GetInfo() == nil {
			return nil
		}
		snapshot := fact.NewNexusOperationExecutionSnapshot(namespaceID, req.GetOperationId(), response.GetInfo().GetLinks())
		snapshot.CancellationState = response.GetInfo().GetCancellationInfo().GetState()
		snapshot.CancellationFailure = response.GetInfo().GetCancellationInfo().GetLastAttemptFailure().GetMessage()
		return []umpire.Fact{snapshot}
	case *workflowservice.PollNexusTaskQueueRequest:
		response, ok := resp.(*workflowservice.PollNexusTaskQueueResponse)
		if !ok || response.GetRequest().GetStartOperation() == nil {
			return nil
		}
		return []umpire.Fact{fact.NewNexusCallbackObservation(namespaceID, response.GetRequest().GetStartOperation())}
	case *workflowservice.StartWorkflowExecutionRequest:
		response, ok := resp.(*workflowservice.StartWorkflowExecutionResponse)
		if !ok || response.GetRunId() == "" {
			return nil
		}
		facts := make([]umpire.Fact, 0, len(req.GetCompletionCallbacks()))
		for _, callback := range req.GetCompletionCallbacks() {
			facts = append(facts, fact.NewWorkflowCallbackAttachment(namespaceID, req.GetWorkflowId(), response.GetRunId(), req.GetRequestId(), callback))
		}
		return facts
	case *workflowservice.GetWorkflowExecutionHistoryRequest:
		response, ok := resp.(*workflowservice.GetWorkflowExecutionHistoryResponse)
		if !ok || response.GetHistory() == nil {
			return nil
		}
		startToClose := make(map[int64]time.Duration)
		var facts []umpire.Fact
		var handlerStartTime time.Time
		handlerRunID := req.GetExecution().GetRunId()
		for _, event := range response.GetHistory().GetEvents() {
			attributes := event.GetWorkflowExecutionStartedEventAttributes()
			if attributes == nil {
				continue
			}
			if event.GetEventTime() != nil {
				handlerStartTime = event.GetEventTime().AsTime()
			}
			if handlerRunID == "" {
				handlerRunID = attributes.GetOriginalExecutionRunId()
			}
			break
		}
		for _, event := range response.GetHistory().GetEvents() {
			var callbacks []*commonpb.Callback
			switch attributes := event.GetWorkflowExecutionStartedEventAttributes(); {
			case attributes != nil:
				callbacks = attributes.GetCompletionCallbacks()
			case event.GetWorkflowExecutionOptionsUpdatedEventAttributes() != nil:
				callbacks = event.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetAttachedCompletionCallbacks()
			default:
				continue
			}
			for _, callback := range callbacks {
				facts = append(facts, fact.NewWorkflowCallbackAttachmentFromHistory(
					namespaceID,
					req.GetExecution().GetWorkflowId(),
					handlerRunID,
					handlerStartTime,
					event,
					callback,
				))
			}
		}
		for _, event := range response.GetHistory().GetEvents() {
			if attributes := event.GetNexusOperationScheduledEventAttributes(); attributes != nil {
				startToClose[event.GetEventId()] = attributes.GetStartToCloseTimeout().AsDuration()
			}
		}
		for _, event := range response.GetHistory().GetEvents() {
			attributes := event.GetNexusOperationCancelRequestFailedEventAttributes()
			if attributes == nil {
				continue
			}
			facts = append(facts, fact.NewNexusOperationCancelRequestFailed(
				namespaceID,
				req.GetExecution().GetWorkflowId(),
				strconv.FormatInt(attributes.GetScheduledEventId(), 10),
				strconv.FormatInt(attributes.GetRequestedEventId(), 10),
				attributes.GetFailure().GetMessage(),
			))
		}
		for _, event := range response.GetHistory().GetEvents() {
			attributes := event.GetNexusOperationTimedOutEventAttributes()
			if attributes == nil {
				continue
			}
			cause := attributes.GetFailure().GetCause()
			facts = append(facts, fact.NewNexusOperationHistorySnapshot(
				namespaceID,
				req.GetExecution().GetWorkflowId(),
				strconv.FormatInt(attributes.GetScheduledEventId(), 10),
				startToClose[attributes.GetScheduledEventId()],
				cause.GetTimeoutFailureInfo().GetTimeoutType(),
				cause.GetMessage(),
			))
		}
		for _, event := range response.GetHistory().GetEvents() {
			if started := fact.NewNexusOperationStartedHistory(namespaceID, req.GetExecution().GetWorkflowId(), event); started != nil {
				facts = append(facts, started)
			}
		}
		for _, event := range response.GetHistory().GetEvents() {
			if terminal := fact.NewNexusOperationTerminal(namespaceID, req.GetExecution().GetWorkflowId(), event); terminal != nil {
				facts = append(facts, terminal)
			}
		}
		return facts
	case *adminservice.DescribeMutableStateRequest:
		response, ok := resp.(*adminservice.DescribeMutableStateResponse)
		if !ok || response.GetDatabaseMutableState() == nil || req.GetExecution().GetWorkflowId() == "" {
			return nil
		}
		return []umpire.Fact{fact.NewWorkflowNexusStorageSnapshot(namespaceID, req.GetExecution().GetWorkflowId(), response.GetDatabaseMutableState())}
	case *matchingservice.PollWorkflowTaskQueueRequest:
		r, ok := resp.(*matchingservice.PollWorkflowTaskQueueResponse)
		if !ok || r == nil || len(r.GetTaskToken()) == 0 {
			return nil // no task was returned
		}
		m := &fact.WorkflowTaskPolled{Request: req, TaskReturned: true}
		tqName := req.GetPollRequest().GetTaskQueue().GetName()
		wfID := r.GetWorkflowExecution().GetWorkflowId()
		runID := r.GetWorkflowExecution().GetRunId()
		if tqName != "" && wfID != "" {
			wtID := umpire.NewEntityID(WorkflowTaskType, tqName+":"+wfID+":"+runID)
			tqID := umpire.NewEntityID(TaskQueueType, tqName)
			ancestors := []umpire.EntityID{tqID}
			if nsID := req.GetNamespaceId(); nsID != "" {
				ancestors = []umpire.EntityID{umpire.NewEntityID(fact.NamespaceType, nsID), tqID}
			}
			m.EntityPath = &umpire.EntityPath{EntityID: wtID, Ancestors: ancestors}
		}
		return []umpire.Fact{m}
	default:
		return nil
	}
}
