package chasm

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/telemetry"
)

// ErrInvalidTransition is returned from [Transition.Apply] on an invalid state transition.
var ErrInvalidTransition = serviceerror.NewFailedPrecondition("invalid transition")

// A StateMachine is anything that can get and set a comparable state S and re-generate tasks based on current state.
// It is meant to be used with [Transition] objects to safely transition their state on a given event.
type StateMachine[S comparable] interface {
	StateMachineState() S
	SetStateMachineState(S)
}

// Transition represents a state machine transition for a machine of type SM with state S and event E.
type Transition[S comparable, SM StateMachine[S], E any] struct {
	// Source states that are valid for this transition.
	Sources []S
	// Destination state to transition to.
	Destination S
	// Function to apply the transition. Mutate the state machine object here and schedule tasks.
	apply func(SM, MutableContext, E) error
}

// NewTransition creates a new [Transition] from the given source states to a destination state for a given event.
// The apply function is called after verifying the transition is possible but before setting the destination state,
// so it can inspect the current (source) state.
func NewTransition[S comparable, SM StateMachine[S], E any](src []S, dst S, apply func(SM, MutableContext, E) error) Transition[S, SM, E] {
	return Transition[S, SM, E]{
		Sources:     src,
		Destination: dst,
		apply:       apply,
	}
}

// Possible returns a boolean indicating whether the transition is possible for the current state.
func (t Transition[S, SM, E]) Possible(sm SM) bool {
	return slices.Contains(t.Sources, sm.StateMachineState())
}

// Apply applies a transition event to the given state machine changing the state machine's state to the transition's
// Destination on success. The apply function is called before the state is changed, so it can inspect the current
// (source) state.
func (t Transition[S, SM, E]) Apply(sm SM, ctx MutableContext, event E) (retErr error) {
	prevState := sm.StateMachineState()

	// Defer to always emit the transition telemetry event. The event carries the
	// component's identity (execution key, Go type, and path) so an out-of-band
	// observer — e.g. the umpire test Monitor — can attribute the transition to a
	// specific component instance from this one generic event type.
	if telemetry.DebugMode() {
		defer func() {
			ek := ctx.ExecutionKey()
			attrs := []attribute.KeyValue{
				telemetry.AttrChasmTransitionSource.String(fmt.Sprintf("%v", prevState)),
				telemetry.AttrChasmTransitionDestination.String(fmt.Sprintf("%v", t.Destination)),
				telemetry.AttrChasmTransitionEvent.String(fmt.Sprintf("%T", event)),
				telemetry.AttrChasmComponentType.String(fmt.Sprintf("%T", sm)),
				telemetry.AttrNamespaceID.String(ek.NamespaceID),
				telemetry.AttrWorkflowID.String(ek.BusinessID),
				telemetry.AttrRunID.String(ek.RunID),
			}
			if comp, ok := any(sm).(Component); ok {
				if ref, err := ctx.structuredRef(comp); err == nil {
					attrs = append(attrs, telemetry.AttrChasmComponentPath.String(strings.Join(ref.componentPath, "/")))
				}
			}
			// A component may enrich its transition telemetry with its own attributes
			// (e.g. an attempt count) — one generic event, optionally richer per component.
			if enr, ok := any(sm).(interface {
				TransitionTelemetryAttributes() []attribute.KeyValue
			}); ok {
				attrs = append(attrs, enr.TransitionTelemetryAttributes()...)
			}
			if retErr != nil {
				attrs = append(attrs, attribute.String("chasm.transition.error", retErr.Error()))
			}
			// Prefer the ambient span (keeps the event correlated with the enclosing
			// trace). CHASM transitions often run in background task processing with no
			// recording ambient span, so fall back to a fresh span from the global
			// tracer — a no-op unless a TracerProvider is configured (e.g. an observer
			// wiring its own SpanProcessor).
			span := trace.SpanFromContext(ctx.goContext())
			if !span.IsRecording() {
				var created trace.Span
				_, created = otel.Tracer("go.temporal.io/server/chasm").Start(context.Background(), telemetry.EventChasmTransition)
				span = created
				defer created.End()
			}
			span.AddEvent(telemetry.EventChasmTransition, trace.WithAttributes(attrs...))
		}()
	}

	if !t.Possible(sm) {
		return fmt.Errorf("%w from %v", ErrInvalidTransition, prevState)
	}

	if err := t.apply(sm, ctx, event); err != nil {
		return err
	}
	sm.SetStateMachineState(t.Destination)
	return nil
}
