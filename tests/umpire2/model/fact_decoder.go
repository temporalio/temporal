package model

import (
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
)

// FactDecoder decodes gRPC requests and OTEL spans into canonical events.
type FactDecoder struct {
	spanFacts    map[string][]func() fact.SpanFact
	requestFacts []func() fact.RequestFact
}

// NewFactDecoder creates a new event decoder.
func NewFactDecoder() *FactDecoder {
	d := &FactDecoder{spanFacts: make(map[string][]func() fact.SpanFact)}

	d.registerSpanFact(func() fact.SpanFact { return &fact.WorkflowTaskStored{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.WorkflowTaskDiscarded{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.WorkflowTerminated{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.SpeculativeWorkflowTaskScheduled{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.WorkflowExecutionCompleted{} })
	// The same completion event also yields a run-precise fact (keyed by RunID) for the WorkflowRun
	// entity; both are emitted for the event (see ImportSpan). Its Name() is its own identity, so it
	// is registered under the OTEL event name explicitly.
	d.registerSpanFactAs(telemetry.EventWorkflowExecutionCompleted, func() fact.SpanFact { return &fact.WorkflowRunCompleted{} })
	// A run is also observed at start, with its lineage (first / previous run) for the run graph.
	d.registerSpanFactAs(telemetry.EventWorkflowExecutionStarted, func() fact.SpanFact { return &fact.WorkflowRunStarted{} })
	// A run that closes via continue-as-new reaches a continued_as_new terminal (not just started).
	d.registerSpanFactAs(telemetry.EventWorkflowExecutionContinuedAsNew, func() fact.SpanFact { return &fact.WorkflowRunContinuedAsNew{} })

	d.registerSpanFact(func() fact.SpanFact { return &fact.NexusOperationScheduled{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.NexusOperationAttemptFailed{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.NexusOperationStarted{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.NexusOperationSucceeded{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.NexusOperationFailed{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.NexusOperationCanceled{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.NexusOperationTimedOut{} })

	// Generic CHASM transition telemetry — routes real CHASM Nexus operations (its
	// Name() is the OTEL event name, so it is decoder-only; routing is by TargetEntity).
	d.registerSpanFact(func() fact.SpanFact { return &fact.ChasmTransition{} })

	d.registerRequestFact(func() fact.RequestFact { return &fact.WorkflowTaskAdded{} })
	d.registerRequestFact(func() fact.RequestFact { return &fact.WorkflowTaskPolled{} })
	d.registerRequestFact(func() fact.RequestFact { return &fact.WorkflowStarted{} })
	return d
}

func (d *FactDecoder) registerSpanFact(factory func() fact.SpanFact) {
	d.registerSpanFactAs(factory().Name(), factory)
}

// registerSpanFactAs registers a span fact under an explicit OTEL event name, decoupling the event
// it decodes from its own Name() (identity). Used when several facts derive from one event.
func (d *FactDecoder) registerSpanFactAs(eventName string, factory func() fact.SpanFact) {
	d.spanFacts[eventName] = append(d.spanFacts[eventName], factory)
}

func (d *FactDecoder) registerRequestFact(factory func() fact.RequestFact) {
	d.requestFacts = append(d.requestFacts, factory)
}

// ImportRequest converts a gRPC request to a fact, or nil if unrecognized.
func (d *FactDecoder) ImportRequest(request any) umpire.Fact {
	for _, factory := range d.requestFacts {
		f := factory()
		if f.ImportRequest(request) {
			return f
		}
	}
	return nil
}

// ImportResponse converts a gRPC request+response pair to a fact, or nil.
func (d *FactDecoder) ImportResponse(req, resp any, namespaceID string) umpire.Fact {
	return fromResponse(req, resp, namespaceID)
}

// ImportResponses converts a gRPC request+response pair to every recognized fact.
func (d *FactDecoder) ImportResponses(req, resp any, namespaceID string) []umpire.Fact {
	return fromResponses(req, resp, namespaceID)
}

// ImportRejection converts a rejected gRPC request (request + error + resolved namespace id) to a
// fact, or nil if the request/rejection is not modeled. See fact.NexusOperationRejected and
// UMPIRE_ERR.md.
func (d *FactDecoder) ImportRejection(req any, err error, namespaceID string) umpire.Fact {
	f := &fact.NexusOperationRejected{}
	if f.ImportRejection(req, err, namespaceID) {
		return f
	}
	return nil
}

// fromResponse creates a fact from a gRPC request+response pair, or nil if unrecognized.
func fromResponse(req, resp any, namespaceID string) umpire.Fact {
	facts := fromResponses(req, resp, namespaceID)
	if len(facts) == 0 {
		return nil
	}
	return facts[0]
}

// fromResponses creates every fact from a gRPC request+response pair.
func fromResponses(req, resp any, namespaceID string) []umpire.Fact {
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

// ImportSpan extracts umpire facts from a ReadOnlySpan's span events.
// This is called synchronously from the SpanProcessor's OnEnd callback.
func (d *FactDecoder) ImportSpan(span sdktrace.ReadOnlySpan) []umpire.Fact {
	var facts []umpire.Fact
	for _, ev := range span.Events() {
		factories, ok := d.spanFacts[ev.Name]
		if !ok {
			continue
		}
		attrs := attribute.NewSet(ev.Attributes...)
		for _, factory := range factories {
			f := factory()
			if f.ImportSpanEvent(attrs) {
				facts = append(facts, f)
			}
		}
	}
	return facts
}
