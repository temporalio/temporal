package fact

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/components/nexusoperations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const invalidCallbackTokenClass = "invalid-callback-token"

// NexusCallbackObservation carries a non-secret callback identity and its completion target.
type NexusCallbackObservation struct {
	NamespaceID        string
	CallbackID         string
	OperationID        string
	OperationRunID     string
	OperationRequestID string
	HandlerRequestID   string
	Malformed          bool
	ErrorClass         string
	EntityPath         *umpire.EntityPath
}

// NexusStartResponse is one successful response delivery correlated to its callback identity.
type NexusStartResponse struct {
	NamespaceID         string
	CallbackID          string
	DeliveryID          string
	ResponseKind        string
	ResponseFingerprint string
	ObservedAt          time.Time
	EntityPath          *umpire.EntityPath
}

func (*NexusStartResponse) Name() string                       { return "NexusStartResponse" }
func (e *NexusStartResponse) TargetEntity() *umpire.EntityPath { return e.EntityPath }

// NexusTaskDeliveryID returns a labeled digest without retaining the task token.
func NexusTaskDeliveryID(taskToken []byte) (string, error) {
	if len(taskToken) == 0 {
		return "", errors.New("nexus task token is empty")
	}
	return umpire.CanonicalProtoDigest("nexus-task-delivery", wrapperspb.Bytes(taskToken))
}

// NewNexusStartResponse normalizes one successful start response.
func NewNexusStartResponse(namespaceID, callbackID, deliveryID string, response *nexuspb.StartOperationResponse) *NexusStartResponse {
	if namespaceID == "" || callbackID == "" || deliveryID == "" || response == nil {
		return nil
	}
	kind := ""
	switch response.GetVariant().(type) {
	case *nexuspb.StartOperationResponse_SyncSuccess:
		kind = "sync_success"
	case *nexuspb.StartOperationResponse_AsyncSuccess:
		kind = "async_success"
	case *nexuspb.StartOperationResponse_OperationError:
		kind = "operation_error"
	case *nexuspb.StartOperationResponse_Failure:
		kind = "failure"
	default:
		return nil
	}
	fingerprint, err := umpire.CanonicalProtoDigest("nexus-start-response", response)
	if err != nil {
		return nil
	}
	return &NexusStartResponse{
		NamespaceID:         namespaceID,
		CallbackID:          callbackID,
		DeliveryID:          deliveryID,
		ResponseKind:        kind,
		ResponseFingerprint: fingerprint,
		ObservedAt:          time.Now(),
		EntityPath:          nsPath(namespaceID, umpire.NewEntityID(CallbackType, callbackID)),
	}
}

// NewNexusHTTPStartResponse normalizes a successful direct HTTP handler response.
func NewNexusHTTPStartResponse(namespaceID, callbackID, requestID string, response *nexuspb.StartOperationResponse) *NexusStartResponse {
	if requestID == "" {
		requestID = callbackID
	}
	deliveryID, err := umpire.CanonicalProtoDigest("nexus-http-delivery", wrapperspb.String(requestID))
	if err != nil {
		return nil
	}
	return NewNexusStartResponse(namespaceID, callbackID, deliveryID, response)
}

func (*NexusCallbackObservation) Name() string                       { return "NexusCallbackObservation" }
func (e *NexusCallbackObservation) TargetEntity() *umpire.EntityPath { return e.EntityPath }

// WorkflowCallbackAttachment carries the handler execution selected for one callback.
type WorkflowCallbackAttachment struct {
	NamespaceID              string
	CallbackID               string
	HandlerWorkflowID        string
	HandlerRunID             string
	HandlerWorkflowStartTime time.Time
	AttachmentEventTime      time.Time
	AttachmentEventID        int64
	ReferenceKind            string
	ReferenceValue           string
	ReferencedEventType      enumspb.EventType
	RequestID                string
	Malformed                bool
	ErrorClass               string
	EntityPath               *umpire.EntityPath
}

func (*WorkflowCallbackAttachment) Name() string                       { return "WorkflowCallbackAttachment" }
func (e *WorkflowCallbackAttachment) TargetEntity() *umpire.EntityPath { return e.EntityPath }

// NexusOperationTerminal is a canonical terminal history observation.
type NexusOperationTerminal struct {
	EventTimeCarrier
	NamespaceID      string
	WorkflowID       string
	ScheduledEventID string
	EventID          string
	Kind             string
	ResultDigest     string
	FailureDigest    string
	LinkDigests      []string
	LinkEndpoints    []string
	Malformed        bool
	ErrorClass       string
	EntityPath       *umpire.EntityPath
}

func (*NexusOperationTerminal) Name() string                       { return "NexusOperationTerminal" }
func (e *NexusOperationTerminal) TargetEntity() *umpire.EntityPath { return e.EntityPath }

// WorkflowNexusStorageSnapshot is explicit HSM and CHASM operation storage observed from mutable state.
type WorkflowNexusStorageSnapshot struct {
	NamespaceID       string
	WorkflowID        string
	HSMOperationIDs   []string
	CHASMOperationIDs []string
	OperationIDs      []string
	EntityPath        *umpire.EntityPath
}

func (*WorkflowNexusStorageSnapshot) Name() string                       { return "WorkflowNexusStorageSnapshot" }
func (e *WorkflowNexusStorageSnapshot) TargetEntity() *umpire.EntityPath { return e.EntityPath }

// NewNexusCallbackObservation normalizes one Nexus start callback without retaining routing secrets.
func NewNexusCallbackObservation(namespaceID string, start *nexuspb.StartOperationRequest) *NexusCallbackObservation {
	observed := &NexusCallbackObservation{NamespaceID: namespaceID}
	if start == nil {
		observed.Malformed = true
		observed.ErrorClass = invalidCallbackTokenClass
		return observed
	}
	observed.HandlerRequestID = start.GetRequestId()
	callback := nexusCallback(start.GetCallback(), start.GetCallbackHeader())
	callbackID, err := callbackDigest(callback)
	if err != nil {
		observed.Malformed = true
		observed.ErrorClass = "invalid-callback"
		return observed
	}
	observed.CallbackID = callbackID
	if observed.CallbackID != "" {
		observed.EntityPath = nsPath(namespaceID, umpire.NewEntityID(CallbackType, observed.CallbackID))
	}
	token, err := commonnexus.DecodeCallbackToken(callbackToken(start.GetCallbackHeader()))
	if err != nil {
		observed.Malformed = true
		observed.ErrorClass = invalidCallbackTokenClass
		return observed
	}
	completion, err := (&commonnexus.CallbackTokenGenerator{}).DecodeCompletion(token)
	if err != nil {
		observed.Malformed = true
		observed.ErrorClass = invalidCallbackTokenClass
		return observed
	}
	tokenNamespaceID, operationID, runID, err := commonnexus.CompletionTarget(completion)
	if err != nil || tokenNamespaceID == "" || operationID == "" || runID == "" || namespaceID != "" && tokenNamespaceID != namespaceID {
		observed.Malformed = true
		observed.ErrorClass = invalidCallbackTokenClass
		return observed
	}
	if observed.NamespaceID == "" {
		observed.NamespaceID = tokenNamespaceID
		observed.EntityPath = nsPath(tokenNamespaceID, umpire.NewEntityID(CallbackType, observed.CallbackID))
	}
	observed.OperationID = operationID
	observed.OperationRunID = runID
	observed.OperationRequestID = completion.GetRequestId()
	return observed
}

// NewWorkflowCallbackAttachment normalizes a callback attached to the selected workflow run.
func NewWorkflowCallbackAttachment(namespaceID, workflowID, runID, requestID string, callback *commonpb.Callback) *WorkflowCallbackAttachment {
	observed := &WorkflowCallbackAttachment{
		NamespaceID:       namespaceID,
		HandlerWorkflowID: workflowID,
		HandlerRunID:      runID,
		RequestID:         requestID,
	}
	var err error
	observed.CallbackID, err = callbackDigest(callback)
	if err != nil {
		observed.Malformed = true
		observed.ErrorClass = "invalid-callback"
		return observed
	}
	observed.EntityPath = nsPath(namespaceID, umpire.NewEntityID(CallbackType, observed.CallbackID))
	return observed
}

// NewWorkflowCallbackAttachmentFromHistory normalizes the public event reference owning an attachment.
func NewWorkflowCallbackAttachmentFromHistory(
	namespaceID string,
	workflowID string,
	runID string,
	handlerStartTime time.Time,
	event *historypb.HistoryEvent,
	callback *commonpb.Callback,
) *WorkflowCallbackAttachment {
	observed := NewWorkflowCallbackAttachment(namespaceID, workflowID, runID, "", callback)
	observed.HandlerWorkflowStartTime = handlerStartTime
	if event == nil {
		observed.Malformed = true
		observed.ErrorClass = "invalid-callback-reference"
		return observed
	}
	observed.AttachmentEventID = event.GetEventId()
	if event.GetEventTime() != nil {
		observed.AttachmentEventTime = event.GetEventTime().AsTime()
	}
	switch attributes := event.GetWorkflowExecutionStartedEventAttributes(); {
	case attributes != nil:
		observed.ReferenceKind = "event"
		observed.ReferenceValue = strconv.FormatInt(event.GetEventId(), 10)
		observed.ReferencedEventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
	case event.GetWorkflowExecutionOptionsUpdatedEventAttributes() != nil:
		options := event.GetWorkflowExecutionOptionsUpdatedEventAttributes()
		observed.RequestID = options.GetAttachedRequestId()
		observed.ReferenceKind = "request"
		observed.ReferenceValue = options.GetAttachedRequestId()
		observed.ReferencedEventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED
		if observed.ReferenceValue == "" {
			observed.Malformed = true
			observed.ErrorClass = "invalid-callback-reference"
		}
	default:
		observed.Malformed = true
		observed.ErrorClass = "invalid-callback-reference"
	}
	return observed
}

func nexusCallback(url string, header map[string]string) *commonpb.Callback {
	routingHeader := map[string]string{}
	for key, value := range header {
		if strings.EqualFold(key, commonnexus.CallbackTokenHeader) {
			routingHeader[commonnexus.CallbackTokenHeader] = value
		}
	}
	return &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: url, Header: routingHeader}}}
}

func callbackToken(header map[string]string) string {
	for key, value := range header {
		if strings.EqualFold(key, commonnexus.CallbackTokenHeader) {
			return value
		}
	}
	return ""
}

func callbackDigest(callback *commonpb.Callback) (string, error) {
	if callback == nil || callback.GetNexus() == nil {
		return "", errors.New("callback has no Nexus routing identity")
	}
	routing := nexusCallback(callback.GetNexus().GetUrl(), callback.GetNexus().GetHeader())
	return umpire.CanonicalProtoDigest("callback", routing)
}

// NewNexusOperationTerminal returns a canonical terminal fact, or nil for a non-terminal event.
func NewNexusOperationTerminal(namespaceID, workflowID string, event *historypb.HistoryEvent) *NexusOperationTerminal {
	if event == nil {
		return nil
	}
	observed := &NexusOperationTerminal{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		EventID:     strconv.FormatInt(event.GetEventId(), 10),
		Kind:        event.GetEventType().String(),
	}
	if event.GetEventTime() != nil {
		observed.SetEventTime(event.GetEventTime().AsTime())
	}
	var result, failure proto.Message
	switch attributes := event.GetNexusOperationCompletedEventAttributes(); {
	case attributes != nil:
		observed.ScheduledEventID = strconv.FormatInt(attributes.GetScheduledEventId(), 10)
		result = attributes.GetResult()
	case event.GetNexusOperationFailedEventAttributes() != nil:
		failed := event.GetNexusOperationFailedEventAttributes()
		observed.ScheduledEventID = strconv.FormatInt(failed.GetScheduledEventId(), 10)
		failure = failed.GetFailure()
	case event.GetNexusOperationCanceledEventAttributes() != nil:
		canceled := event.GetNexusOperationCanceledEventAttributes()
		observed.ScheduledEventID = strconv.FormatInt(canceled.GetScheduledEventId(), 10)
		failure = canceled.GetFailure()
	case event.GetNexusOperationTimedOutEventAttributes() != nil:
		timedOut := event.GetNexusOperationTimedOutEventAttributes()
		observed.ScheduledEventID = strconv.FormatInt(timedOut.GetScheduledEventId(), 10)
		failure = timedOut.GetFailure()
	default:
		return nil
	}
	if result != nil {
		var err error
		observed.ResultDigest, err = umpire.CanonicalProtoDigest("result", result)
		if err != nil {
			observed.Malformed = true
			observed.ErrorClass = "invalid-terminal-payload"
		}
	}
	if failure != nil {
		var err error
		observed.FailureDigest, err = umpire.CanonicalProtoDigest("failure", failure)
		if err != nil {
			observed.Malformed = true
			observed.ErrorClass = "invalid-terminal-payload"
		}
	}
	for _, link := range event.GetLinks() {
		digest, err := umpire.CanonicalProtoDigest("link", link)
		if err == nil {
			observed.LinkDigests = append(observed.LinkDigests, digest)
		} else {
			observed.Malformed = true
			observed.ErrorClass = "invalid-terminal-link"
		}
		if endpoint := linkEndpoint(link); endpoint != "" {
			observed.LinkEndpoints = append(observed.LinkEndpoints, endpoint)
		}
	}
	slices.Sort(observed.LinkDigests)
	slices.Sort(observed.LinkEndpoints)
	observed.LinkEndpoints = slices.Compact(observed.LinkEndpoints)
	self := umpire.NewEntityID(NexusOperationType, workflowID+":"+observed.ScheduledEventID)
	observed.EntityPath = nsPath(namespaceID, self, umpire.NewEntityID(WorkflowType, workflowID))
	return observed
}

func linkEndpoint(link *commonpb.Link) string {
	if activity := link.GetActivity(); activity != nil {
		return fmt.Sprintf("activity:%s/%s/%s", activity.GetNamespace(), activity.GetActivityId(), activity.GetRunId())
	}
	if operation := link.GetNexusOperation(); operation != nil {
		return fmt.Sprintf("nexus-operation:%s/%s/%s", operation.GetNamespace(), operation.GetOperationId(), operation.GetRunId())
	}
	if workflowEvent := link.GetWorkflowEvent(); workflowEvent != nil {
		reference := ""
		if eventRef := workflowEvent.GetEventRef(); eventRef != nil {
			reference = strconv.FormatInt(eventRef.GetEventId(), 10)
		} else if requestRef := workflowEvent.GetRequestIdRef(); requestRef != nil {
			reference = requestRef.GetRequestId()
		}
		return fmt.Sprintf("workflow-event:%s/%s/%s/%s", workflowEvent.GetNamespace(), workflowEvent.GetWorkflowId(), workflowEvent.GetRunId(), reference)
	}
	return ""
}

// NewWorkflowNexusStorageSnapshot inventories currently stored Nexus operations.
func NewWorkflowNexusStorageSnapshot(namespaceID, workflowID string, state *persistencespb.WorkflowMutableState) *WorkflowNexusStorageSnapshot {
	observed := &WorkflowNexusStorageSnapshot{NamespaceID: namespaceID, WorkflowID: workflowID}
	if state != nil {
		machines := state.GetExecutionInfo().GetSubStateMachinesByType()[nexusoperations.OperationMachineType]
		for operationID := range machines.GetMachinesById() {
			observed.HSMOperationIDs = append(observed.HSMOperationIDs, operationID)
		}
		for key := range state.GetChasmNodes() {
			if operationID, found := strings.CutPrefix(key, "Operations#"); found && operationID != "" {
				observed.CHASMOperationIDs = append(observed.CHASMOperationIDs, operationID)
			}
		}
	}
	slices.Sort(observed.HSMOperationIDs)
	slices.Sort(observed.CHASMOperationIDs)
	observed.OperationIDs = append(observed.OperationIDs, observed.HSMOperationIDs...)
	observed.OperationIDs = append(observed.OperationIDs, observed.CHASMOperationIDs...)
	slices.Sort(observed.OperationIDs)
	observed.OperationIDs = slices.Compact(observed.OperationIDs)
	observed.EntityPath = nsPath(namespaceID, umpire.NewEntityID(WorkflowType, workflowID))
	return observed
}
