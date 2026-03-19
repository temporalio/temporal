package entity

import (
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	matchingservice "go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/fact"
)

// FactDecoder decodes gRPC requests and OTEL spans into canonical events.
type FactDecoder struct {
	spanFacts    map[string]func() fact.SpanFact
	requestFacts []func() fact.RequestFact
}

// NewFactDecoder creates a new event decoder.
func NewFactDecoder() *FactDecoder {
	d := &FactDecoder{spanFacts: make(map[string]func() fact.SpanFact)}

	d.registerSpanFact(func() fact.SpanFact { return &fact.WorkflowTaskStored{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.WorkflowTaskDiscarded{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.WorkflowUpdateAborted{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.WorkflowTerminated{} })
	d.registerSpanFact(func() fact.SpanFact { return &fact.SpeculativeWorkflowTaskScheduled{} })

	d.registerRequestFact(func() fact.RequestFact { return &fact.WorkflowTaskAdded{} })
	d.registerRequestFact(func() fact.RequestFact { return &fact.WorkflowTaskPolled{} })
	d.registerRequestFact(func() fact.RequestFact { return &fact.ActivityTaskAdded{} })
	d.registerRequestFact(func() fact.RequestFact { return &fact.ActivityTaskPolled{} })
	d.registerRequestFact(func() fact.RequestFact { return &fact.WorkflowStarted{} })
	d.registerRequestFact(func() fact.RequestFact { return &fact.WorkflowTaskCompleted{} })
	d.registerRequestFact(func() fact.RequestFact { return &fact.WorkflowUpdateRequested{} })

	return d
}

func (d *FactDecoder) registerSpanFact(factory func() fact.SpanFact) {
	probe := factory()
	d.spanFacts[probe.Name()] = factory
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
func (d *FactDecoder) ImportResponse(req, resp any) umpire.Fact {
	return fromResponse(req, resp)
}

// fromResponse creates a fact from a gRPC request+response pair, or nil if unrecognized.
func fromResponse(req, resp any) umpire.Fact {
	switch req := req.(type) {
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
			m.Identity = &umpire.Identity{EntityID: wtID, ParentID: &tqID}
		}
		return m
	default:
		return nil
	}
}

// ImportSpan extracts umpire facts from a ReadOnlySpan's span events.
// This is called synchronously from the SpanProcessor's OnEnd callback.
func (d *FactDecoder) ImportSpan(span sdktrace.ReadOnlySpan) []umpire.Fact {
	var events []umpire.Fact
	for _, ev := range span.Events() {
		factory, ok := d.spanFacts[ev.Name]
		if !ok {
			continue
		}
		attrs := attribute.NewSet(ev.Attributes...)
		f := factory()
		if f.ImportSpanEvent(attrs) {
			events = append(events, f)
		}
	}
	return events
}
