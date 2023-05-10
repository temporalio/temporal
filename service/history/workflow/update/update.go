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

package update

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	updatepb "go.temporal.io/api/update/v1"

	"go.temporal.io/server/common/future"
	"go.temporal.io/server/internal/effect"
)

type (
	// EventStore is the interface that an Update needs to read and write events
	// and to be notified when buffered writes have been flushed. It is the
	// expectation of this code that writes to EventStore will return before the
	// data has been made durable. Callbacks attached to the EventStore via
	// OnAfterCommit and OnAfterRollback *must* be called after the EventStore
	// state is successfully written or is discarded.
	EventStore interface {
		effect.Controller

		// AddWorkflowExecutionUpdateAcceptedEvent writes an update accepted
		// event. The data may not be durable when this function returns.
		AddWorkflowExecutionUpdateAcceptedEvent(
			updateID string,
			accpt *updatepb.Acceptance,
		) (*historypb.HistoryEvent, error)

		// AddWorkflowExecutionUpdateCompletedEvent writes an update completed
		// event. The data may not be durable when this function returns.
		AddWorkflowExecutionUpdateCompletedEvent(
			resp *updatepb.Response,
		) (*historypb.HistoryEvent, error)
	}

	// Update is a state machine for the update protocol. It reads and writes
	// messages from the go.temporal.io/api/update/v1 package. The update state
	// machine is straightforward except in that it provides "provisional"
	// in-between states where the update has received a message that has
	// updated its internal state but those updates have not been made visible
	// to clients yet (e.g. accepted or outcome futures have not been set yet).
	// The effects are bound to the EventStore's effect.Set and will be
	// triggered withen those effects are applied.
	Update struct {
		// accessed only while holding workflow lock
		id              string
		request         *protocolpb.Message // nil when not in stateRequested
		onComplete      func()
		instrumentation *instrumentation

		// these fields might be accessed while not holding the workflow lock
		state    state
		accepted future.Future[*failurepb.Failure]
		outcome  future.Future[*updatepb.Outcome]
	}

	updateOpt func(*Update)
)

// New creates a new Update instance with the provided ID that will call the
// onComplete callback when it completes.
func New(id string, onComplete func(), opts ...updateOpt) *Update {
	upd := &Update{
		id:              id,
		state:           stateAdmitted,
		onComplete:      onComplete,
		instrumentation: &noopInstrumentation,
		accepted:        future.NewFuture[*failurepb.Failure](),
		outcome:         future.NewFuture[*updatepb.Outcome](),
	}
	for _, opt := range opts {
		opt(upd)
	}
	return upd
}

func withInstrumentation(i *instrumentation) updateOpt {
	return func(u *Update) {
		u.instrumentation = i
	}
}

func newAccepted(id string, onComplete func(), opts ...updateOpt) *Update {
	upd := &Update{
		id:              id,
		state:           stateAccepted,
		onComplete:      onComplete,
		instrumentation: &noopInstrumentation,
		accepted:        future.NewReadyFuture[*failurepb.Failure](nil, nil),
		outcome:         future.NewFuture[*updatepb.Outcome](),
	}
	for _, opt := range opts {
		opt(upd)
	}
	return upd
}

func newCompleted(
	id string,
	fetchOutcome func(ctx context.Context) (*updatepb.Outcome, error),
	opts ...updateOpt,
) *Update {
	upd := &Update{
		id:              id,
		state:           stateCompleted,
		instrumentation: &noopInstrumentation,
		accepted:        future.NewReadyFuture[*failurepb.Failure](nil, nil),
		outcome:         lazyOutcome(fetchOutcome),
	}
	for _, opt := range opts {
		opt(upd)
	}
	return upd
}

// WaitOutcome observes this Update's completion, returning the Outcome when it
// is available. This call will block until the Outcome is known or the provided
// context.Context expires.
func (u *Update) WaitOutcome(ctx context.Context) (*updatepb.Outcome, error) {
	return u.outcome.Get(ctx)
}

// WaitAccepted blocks on the acceptance of this update, returning nil if has
// been accepted but not yet completed or the overall Outcome if the update has
// been completed (including completed by rejection). This call will block until
// the acceptance occurs or the provided context.Context expires.
func (u *Update) WaitAccepted(ctx context.Context) (*updatepb.Outcome, error) {
	if u.outcome.Ready() {
		// being complete implies being accepted, return the completed outcome
		// here because we can.
		return u.outcome.Get(ctx)
	}
	if _, err := u.accepted.Get(ctx); err != nil {
		return nil, err
	}
	return nil, nil
}

// OnMessage delivers a message to the Update state machine. The proto.Message
// parameter is expected to be one of *updatepb.Request, *updatepb.Response,
// *updatepb.Rejection, *updatepb.Acceptance, or a *protocolpb.Message whose
// Body field contains an instance from the same list. Writes to the EventStore
// occur synchronously but externally observable effects on this Update (e.g.
// emmitting an Outcome or an Accepted) are registered with the EventStore to be
// applied after the durable updates are committed. If the EventStore rolls
// back its effects, this state machine does the same.
func (u *Update) OnMessage(
	ctx context.Context,
	msg proto.Message,
	eventStore EventStore,
) error {
	if msg == nil {
		return invalidArgf("Update %q received nil message", u.id)
	}
	if protocolMsg, ok := msg.(*protocolpb.Message); ok {
		var dynbody types.DynamicAny
		if err := types.UnmarshalAny(protocolMsg.Body, &dynbody); err != nil {
			return err
		}
		msg = dynbody.Message
	}
	switch body := msg.(type) {
	case *updatepb.Request:
		return u.onRequestMsg(ctx, body, eventStore)
	case *updatepb.Acceptance:
		return u.onAcceptanceMsg(ctx, body, eventStore)
	case *updatepb.Rejection:
		return u.onRejectionMsg(ctx, body, eventStore)
	case *updatepb.Response:
		return u.onResponseMsg(ctx, body, eventStore)
	default:
		return invalidArgf("Message type %T not supported", body)
	}
}

// ReadOutgoingMessages loads any oubound messages from this Update state
// machine into the output slice provided.
func (u *Update) ReadOutgoingMessages(out *[]*protocolpb.Message) {
	if !u.state.Is(stateRequested) {
		// Update only sends messages to the workflow when it is in
		// stateRequested
		return
	}
	*out = append(*out, u.request)
}

// onRequestMsg works if the Update is in any state but if the state is anything
// other than stateAdmitted then it just early returns a nil error. This
// effectively gives us update request deduplication by update ID. If the Update
// is in stateAdmitted then it builds a protocolpb.Message that will be sent on
// ensuing calls to PollOutgoingMessages until the update is accepted.
func (u *Update) onRequestMsg(
	ctx context.Context,
	req *updatepb.Request,
	eventStore EventStore,
) error {
	if !u.state.Is(stateAdmitted) {
		return nil
	}
	if err := validateRequestMsg(u.id, req); err != nil {
		return err
	}
	u.instrumentation.CountRequestMsg()
	body, err := types.MarshalAny(req)
	if err != nil {
		return invalidArgf("could not marshal request: %v", err)
	}
	u.request = &protocolpb.Message{
		ProtocolInstanceId: u.id,
		Id:                 u.id + "/request",
		Body:               body,
	}
	u.setState(stateProvisionallyRequested)
	eventStore.OnAfterCommit(func(context.Context) { u.setState(stateRequested) })
	eventStore.OnAfterRollback(func(context.Context) { u.setState(stateAdmitted) })
	return nil
}

// onAcceptanceMsg expects the Update to be in stateRequested and returns an
// error if it finds otherwise. An event is written to the provided EventStore
// and on commit the accepted future is completed and the Update transitions to
// stateAccepted.
func (u *Update) onAcceptanceMsg(
	ctx context.Context,
	acpt *updatepb.Acceptance,
	eventStore EventStore,
) error {
	if err := u.checkState(acpt, stateRequested); err != nil {
		return err
	}
	if err := validateAcceptanceMsg(u.id, acpt); err != nil {
		return err
	}
	u.instrumentation.CountAcceptanceMsg()
	if _, err := eventStore.AddWorkflowExecutionUpdateAcceptedEvent(u.id, acpt); err != nil {
		return err
	}
	u.setState(stateProvisionallyAccepted)
	eventStore.OnAfterCommit(func(context.Context) {
		u.request = nil
		u.setState(stateAccepted)
		u.accepted.(*future.FutureImpl[*failurepb.Failure]).Set(nil, nil)
	})
	eventStore.OnAfterRollback(func(context.Context) { u.setState(stateRequested) })
	return nil
}

// onRejectionMsg expectes the Update stae to be in stateRequested and returns
// an error if it finds otherwise. On commit of buffered effects the state
// machine transitions to stateCompleted and the accepted and outcome futures
// are both completed with the failurepb.Failure value from the
// updatepb.Rejection input message.
func (u *Update) onRejectionMsg(
	ctx context.Context,
	rej *updatepb.Rejection,
	eventStore EventStore,
) error {
	if err := u.checkState(rej, stateRequested); err != nil {
		return err
	}
	if err := validateRejectionMsg(u.id, rej); err != nil {
		return err
	}
	u.instrumentation.CountRejectionMsg()
	u.setState(stateProvisionallyCompleted)
	eventStore.OnAfterCommit(func(context.Context) {
		u.request = nil
		u.setState(stateCompleted)
		outcome := updatepb.Outcome{
			Value: &updatepb.Outcome_Failure{Failure: rej.Failure},
		}
		u.accepted.(*future.FutureImpl[*failurepb.Failure]).Set(rej.Failure, nil)
		u.outcome.(*future.FutureImpl[*updatepb.Outcome]).Set(&outcome, nil)
		u.onComplete()
	})
	eventStore.OnAfterRollback(func(context.Context) { u.setState(stateRequested) })
	return nil
}

// onResponseMsg expectes the Update to be in either stateProvisionallyAccepted
// or stateAccepted and returns an error if it finds otherwise. On commit of
// buffered effects the state machine will transtion to stateCompleted and the
// outcome future is completed with the updatepb.Outcome from the
// updatepb.Response input message.
func (u *Update) onResponseMsg(
	ctx context.Context,
	res *updatepb.Response,
	eventStore EventStore,
) error {
	if err := u.checkState(res, stateProvisionallyAccepted, stateAccepted); err != nil {
		return err
	}
	if err := validateResponseMsg(u.id, res); err != nil {
		return err
	}
	if _, err := eventStore.AddWorkflowExecutionUpdateCompletedEvent(res); err != nil {
		return err
	}
	u.instrumentation.CountResponseMsg()
	prevState := u.setState(stateProvisionallyCompleted)
	eventStore.OnAfterCommit(func(context.Context) {
		u.setState(stateCompleted)
		u.outcome.(*future.FutureImpl[*updatepb.Outcome]).Set(res.GetOutcome(), nil)
		u.onComplete()
	})
	eventStore.OnAfterRollback(func(context.Context) { u.setState(prevState) })
	return nil
}

func (u *Update) completedOrProvisionallyCompleted() bool {
	return u.state.IsOneOf(stateCompleted, stateProvisionallyCompleted)
}

func (u *Update) hasOutgoingMessage() bool {
	return u.state.Is(stateRequested)
}

func (u *Update) checkState(msg proto.Message, allowedStates ...state) error {
	if u.state.IsOneOf(allowedStates...) {
		return nil
	}
	return invalidArgf("invalid state transition attempted: "+
		"received %T message while in state %q", msg, u.state)
}

// setState assigns the current state to a new value returning the original
// value.
func (u *Update) setState(newState state) state {
	currState := u.state.Load()
	u.instrumentation.StateChange(u.id, currState, newState)
	u.state.Set(newState)
	return currState
}
