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
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/server/common"
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
			acceptedRequestMessageId string,
			acceptedRequestSequencingEventId int64,
			acceptedRequest *updatepb.Request,
		) (*historypb.HistoryEvent, error)

		// AddWorkflowExecutionUpdateCompletedEvent writes an update completed
		// event. The data may not be durable when this function returns.
		AddWorkflowExecutionUpdateCompletedEvent(
			acceptedEventID int64,
			resp *updatepb.Response,
		) (*historypb.HistoryEvent, error)
	}

	// Update is a state machine for the update protocol. It reads and writes
	// messages from the go.temporal.io/api/update/v1 package. See the diagram
	// in service/history/workflow/update/README.md. The update state machine is
	// straightforward except in that it provides "provisional" in-between
	// states where the update has received a message that has modified its
	// internal state but those changes have not been made visible to clients
	// yet (e.g. accepted or outcome futures have not been set yet). The
	// observable changes are bound to the EventStore's effect.Controller and
	// will be triggered when those effects are applied. State transitions
	// (OnMessage calls) must be done while holding the workflow lock.
	Update struct {
		// accessed only while holding workflow lock
		id              string
		state           state
		request         *anypb.Any // of type *updatepb.Request, nil when not in stateRequested
		acceptedEventID int64
		onComplete      func()
		instrumentation *instrumentation

		// these fields might be accessed while not holding the workflow lock
		accepted future.Future[*failurepb.Failure]
		outcome  future.Future[*updatepb.Outcome]
	}

	updateOpt func(*Update)

	UpdateStatus struct {
		Stage   enumspb.UpdateWorkflowExecutionLifecycleStage
		Outcome *updatepb.Outcome
	}
)

// New creates a new Update instance with the provided ID that will call the
// onComplete callback when it completes.
func New(id string, opts ...updateOpt) *Update {
	upd := &Update{
		id:              id,
		state:           stateAdmitted,
		onComplete:      func() {},
		instrumentation: &noopInstrumentation,
		accepted:        future.NewFuture[*failurepb.Failure](),
		outcome:         future.NewFuture[*updatepb.Outcome](),
	}
	for _, opt := range opts {
		opt(upd)
	}
	return upd
}

func withCompletionCallback(cb func()) updateOpt {
	return func(u *Update) {
		u.onComplete = cb
	}
}

func withInstrumentation(i *instrumentation) updateOpt {
	return func(u *Update) {
		u.instrumentation = i
	}
}

func newAccepted(id string, acceptedEventID int64, opts ...updateOpt) *Update {
	upd := &Update{
		id:              id,
		state:           stateAccepted,
		acceptedEventID: acceptedEventID,
		onComplete:      func() {},
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
	outcomeFuture *future.ReadyFutureImpl[*updatepb.Outcome],
	opts ...updateOpt,
) *Update {
	upd := &Update{
		id:              id,
		state:           stateCompleted,
		onComplete:      func() {},
		instrumentation: &noopInstrumentation,
		accepted:        future.NewReadyFuture[*failurepb.Failure](nil, nil),
		outcome:         outcomeFuture,
	}
	for _, opt := range opts {
		opt(upd)
	}
	return upd
}

// WaitLifecycleStage waits until the Update has reached at least `waitStage` or
// a timeout. If the Update reaches `waitStage` with no timeout, the most
// advanced stage known to have been reached is returned, along with the outcome
// if any. If there is a timeout due to the supplied soft timeout, then
// unspecified stage and nil outcome are returned, without an error. If there is
// a timeout due to context deadline expiry, then the error is returned as usual.
func (u *Update) WaitLifecycleStage(
	ctx context.Context,
	waitStage enumspb.UpdateWorkflowExecutionLifecycleStage,
	softTimeout time.Duration) (UpdateStatus, error) {

	switch waitStage {
	case enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED:
		return u.waitLifecycleStage(ctx, u.WaitAccepted, softTimeout)
	case enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED:
		return u.waitLifecycleStage(ctx, u.WaitOutcome, softTimeout)
	default:
		err := serviceerror.NewInvalidArgument(fmt.Sprintf("%v is not implemented", waitStage))
		return UpdateStatus{enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, nil}, err
	}
}

func (u *Update) waitLifecycleStage(
	ctx context.Context,
	waitFn func(ctx context.Context) (UpdateStatus, error),
	softTimeout time.Duration) (UpdateStatus, error) {

	innerCtx, cancel := context.WithTimeout(context.Background(), softTimeout)
	defer cancel()
	status, err := waitFn(innerCtx)
	if ctx.Err() != nil {
		// Handle a context deadline expiry as usual.
		return UpdateStatus{enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, nil}, ctx.Err()
	}
	if innerCtx.Err() != nil {
		// Handle the deadline expiry as a violation of a soft deadline:
		// return non-error empty response.
		return UpdateStatus{enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, nil}, nil
	}
	return status, err
}

// Status returns an UpdateStatus containing the
// enumspb.UpdateWorkflowExecutionLifecycleStage corresponding to the current
// state of this Update, and the Outcome if it has one.
func (u *Update) Status() (UpdateStatus, error) {
	stage, err := u.state.LifecycleStage()
	if err != nil {
		return UpdateStatus{enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, nil}, err
	}
	var outcome *updatepb.Outcome
	if u.outcome.Ready() {
		outcome, err = u.outcome.Get(context.Background())
	}
	if err != nil {
		return UpdateStatus{enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, nil}, err
	}
	return UpdateStatus{stage, outcome}, err
}

// WaitOutcome observes this Update's completion, returning when the Outcome is
// available. This call will block until the Outcome is known or the provided
// context.Context expires. It is safe to call this method outside of workflow
// lock.
func (u *Update) WaitOutcome(ctx context.Context) (UpdateStatus, error) {
	outcome, err := u.outcome.Get(ctx)
	if err != nil {
		return UpdateStatus{enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, outcome}, err
	}
	return UpdateStatus{enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, outcome}, nil
}

// WaitAccepted blocks on the acceptance of this update, returning nil if has
// been accepted but not yet completed or the overall Outcome if the update has
// been completed (including completed by rejection). This call will block until
// the acceptance occurs or the provided context.Context expires.
// It is safe to call this method outside of workflow lock.
func (u *Update) WaitAccepted(ctx context.Context) (UpdateStatus, error) {
	if u.outcome.Ready() {
		// Being complete implies being accepted; return the completed outcome
		// here because we can.
		return u.WaitOutcome(ctx)
	}
	fail, err := u.accepted.Get(ctx)
	if err != nil {
		return UpdateStatus{enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, nil}, err
	}
	if fail != nil {
		outcome := &updatepb.Outcome{
			Value: &updatepb.Outcome_Failure{Failure: fail},
		}
		return UpdateStatus{enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, outcome}, nil
	}
	return UpdateStatus{enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, nil}, nil
}

// OnMessage delivers a message to the Update state machine. The proto.Message
// parameter is expected to be one of *updatepb.Request, *updatepb.Response,
// *updatepb.Rejection, *updatepb.Acceptance, or a *protocolpb.Message whose
// Body field contains an instance from the same list. Writes to the EventStore
// occur synchronously but externally observable effects on this Update (e.g.
// emitting an Outcome or an Accepted) are registered with the EventStore to be
// applied after the durable updates are committed. If the EventStore rolls back
// its effects, this state machine does the same.
//
// If you modify the state machine please update the diagram in
// service/history/workflow/update/README.md.
func (u *Update) OnMessage(
	ctx context.Context,
	msg proto.Message,
	eventStore EventStore,
) error {
	if msg == nil {
		return invalidArgf("Update %q received nil message", u.id)
	}

	if protocolMsg, ok := msg.(*protocolpb.Message); ok {
		var err error
		msg, err = protocolMsg.Body.UnmarshalNew()
		if err != nil {
			return err
		}
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

// ReadOutgoingMessages loads any outbound messages from this Update state
// machine into the output slice provided.
func (u *Update) ReadOutgoingMessages(out *[]*protocolpb.Message, sequencingID *protocolpb.Message_EventId) {
	if u.state != stateRequested {
		// Update only sends messages to the workflow when it is in
		// stateRequested
		return
	}

	reqMessage := &protocolpb.Message{
		ProtocolInstanceId: u.id,
		Id:                 u.outgoingMessageID(),
		SequencingId:       sequencingID,
		Body:               u.request,
	}

	*out = append(*out, reqMessage)
}

// outgoingMessageID returns the ID of the message that is used to send the Update to the worker.
func (u *Update) outgoingMessageID() string {
	return u.id + "/request"
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
	if u.state != stateAdmitted {
		return nil
	}
	if err := validateRequestMsg(u.id, req); err != nil {
		return err
	}
	u.instrumentation.CountRequestMsg()
	// Marshal update request here to return InvalidArgument to the API caller if it can't be marshaled.
	reqAny, err := anypb.New(req)
	if err != nil {
		return invalidArgf("unable to marshal request: %v", err)
	}
	u.request = reqAny
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
	if err := validateAcceptanceMsg(acpt); err != nil {
		return err
	}
	u.instrumentation.CountAcceptanceMsg()

	acceptedRequest := &updatepb.Request{}
	if err := u.request.UnmarshalTo(acceptedRequest); err != nil {
		return internalErrorf("unable to unmarshal original request: %v", err)
	}

	event, err := eventStore.AddWorkflowExecutionUpdateAcceptedEvent(
		u.id,
		u.outgoingMessageID(),
		acpt.AcceptedRequestSequencingEventId, // Only AcceptedRequestSequencingEventId from Acceptance message is used.
		acceptedRequest)
	if err != nil {
		return err
	}
	u.acceptedEventID = event.EventId
	u.setState(stateProvisionallyAccepted)
	eventStore.OnAfterCommit(func(context.Context) {
		u.request = nil
		u.setState(stateAccepted)
		u.accepted.(*future.FutureImpl[*failurepb.Failure]).Set(nil, nil)
	})
	eventStore.OnAfterRollback(func(context.Context) {
		u.acceptedEventID = common.EmptyEventID
		u.setState(stateRequested)
	})
	return nil
}

// onRejectionMsg expects the Update state to be stateRequested and returns
// an error otherwise. On commit of buffered effects the state
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
	if err := validateRejectionMsg(rej); err != nil {
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

// onResponseMsg expects the Update to be in either stateProvisionallyAccepted
// or stateAccepted and returns an error if it finds otherwise. On commit of
// buffered effects the state machine will transition to stateCompleted and the
// outcome future is completed with the updatepb.Outcome from the
// updatepb.Response input message.
func (u *Update) onResponseMsg(
	ctx context.Context,
	res *updatepb.Response,
	eventStore EventStore,
) error {
	if err := u.checkStateSet(res, stateSet(stateProvisionallyAccepted|stateAccepted)); err != nil {
		return err
	}
	if err := validateResponseMsg(u.id, res); err != nil {
		return err
	}
	if _, err := eventStore.AddWorkflowExecutionUpdateCompletedEvent(u.acceptedEventID, res); err != nil {
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

func (u *Update) hasBeenSeenByWorkflowExecution() bool {
	const unseen = stateAdmitted | stateProvisionallyRequested | stateRequested
	return !u.state.Matches(stateSet(unseen))
}

func (u *Update) hasOutgoingMessage() bool {
	return u.state == stateRequested
}

func (u *Update) checkState(msg proto.Message, expected state) error {
	return u.checkStateSet(msg, stateSet(expected))
}

func (u *Update) checkStateSet(msg proto.Message, allowed stateSet) error {
	if u.state.Matches(allowed) {
		return nil
	}
	u.instrumentation.CountInvalidStateTransition()
	return invalidArgf("invalid state transition attempted: "+
		"received %T message while in state %q", msg, u.state)
}

// setState assigns the current state to a new value returning the original
// value.
func (u *Update) setState(newState state) state {
	prevState := u.state
	u.state = newState
	u.instrumentation.StateChange(u.id, prevState, newState)
	return prevState
}
