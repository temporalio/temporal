package model

import (
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
)

// FactDecoder decodes gRPC requests and OTEL spans into canonical events.
type FactDecoder struct {
	spanFacts    map[string][]func() fact.SpanFact
	requestFacts []func() fact.RequestFact
	responses    *responseDecoder
}

// NewFactDecoder creates a new event decoder.
func NewFactDecoder() *FactDecoder {
	d := &FactDecoder{
		spanFacts: make(map[string][]func() fact.SpanFact),
		responses: newResponseDecoder(),
	}

	d.registerSpanFact(func() fact.SpanFact { return &fact.WorkflowTaskStored{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.WorkflowTaskDiscarded{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.WorkflowTerminated{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.SpeculativeWorkflowTaskScheduled{} })
	d.registerSpanFactAs(telemetry.EventWorkflowExecutionClosed, func() fact.SpanFact { return &fact.WorkflowExecutionClosed{} })
	d.registerSpanFactAs(telemetry.EventWorkflowExecutionClosed, func() fact.SpanFact { return &fact.WorkflowRunClosed{} })
	// A run is also observed at start, with its lineage (first / previous run) for the run graph.
	d.registerSpanFactAs(telemetry.EventWorkflowExecutionStarted, func() fact.SpanFact { return &fact.WorkflowRunStarted{} })

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
	return d.responses.First(req, resp, namespaceID)
}

// ImportResponses converts a gRPC request+response pair to every recognized fact.
func (d *FactDecoder) ImportResponses(req, resp any, namespaceID string) []umpire.Fact {
	return d.responses.Decode(req, resp, namespaceID)
}

// PurgeNamespace removes ephemeral delivery-to-callback correlations for one namespace.
func (d *FactDecoder) PurgeNamespace(namespaceID string) {
	d.responses.Purge(namespaceID)
}

// ImportRejection converts a rejected gRPC request (request + error + resolved namespace id) to a
// fact, or nil if the request/rejection is not modeled. See fact.NexusOperationRejected and
// UMPIRE.md.
func (d *FactDecoder) ImportRejection(req any, err error, namespaceID string) umpire.Fact {
	f := &fact.NexusOperationRejected{}
	if f.ImportRejection(req, err, namespaceID) {
		return f
	}
	return nil
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
				if eventTimed, ok := f.(fact.EventTimeFact); ok {
					eventTimed.SetEventTime(ev.Time)
				}
				facts = append(facts, f)
			}
		}
	}
	return facts
}
