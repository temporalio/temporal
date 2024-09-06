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
	"errors"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/utf8validator"
	"go.temporal.io/server/internal/effect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var (
	WorkflowUpdateAbortedErr = serviceerror.NewUnavailable("Workflow Update was aborted.")
)

type (
	// Update docs are at /docs/architecture/workflow-update.md.
	Update struct {
		// These fields must be accessed only while holding workflow lock.
		id    string
		state state
		// The `request` field holds the Update payload submitted with the original request. It is stored in the
		// Registry to be sent to the worker and then written to history in an UpdateAccepted event.
		// Therefore, it is nil when the Update in the Registry is in stateAccepted, stateCompleted etc., since then the
		// request has already been written to an UpdateAccepted event.
		// In addition, it is a nil when the Update in the Registry is in stateAdmitted AND it was created from an
		// UpdateInfo.AdmissionInfo entry in MutableState. In this case, the reason it is a nil is the following:
		// 1. The presence of the AdmissionInfo entry in MutableState implies that there is an UpdateAdmitted event in
		//    history. This event always contains the original request payload.
		// 2. Therefore, it is not necessary to write a second copy of the request payload to an UpdateAccepted event,
		//    so we don't *need* to load the request into the Registry.
		// 3. Furthermore, it is possible that many UpdateAdmitted events were created after a Reset or during conflict
		//    resolution. In that situation, we *must not* attempt to load all the payloads into the Registry.
		request         *anypb.Any // of type *updatepb.Request
		acceptedEventID int64
		onComplete      func()
		instrumentation *instrumentation
		admittedTime    time.Time

		// These fields might be accessed while not holding the workflow lock.
		accepted future.Future[*failurepb.Failure]
		outcome  future.Future[*updatepb.Outcome]
	}

	updateOpt func(*Update)
)

// New creates a new Update instance with the provided ID and set of options.
func New(id string, opts ...updateOpt) *Update {
	upd := &Update{
		id:              id,
		state:           stateCreated,
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

func newAdmitted(id string, request *anypb.Any, opts ...updateOpt) *Update {
	upd := &Update{
		id:              id,
		state:           stateAdmitted,
		request:         request,
		onComplete:      func() {},
		instrumentation: &noopInstrumentation,
		accepted:        future.NewFuture[*failurepb.Failure](),
		outcome:         future.NewFuture[*updatepb.Outcome](),
		admittedTime:    time.Now().UTC(),
	}
	for _, opt := range opts {
		opt(upd)
	}
	return upd
}

func newAccepted(id string, acceptedEventID int64, opts ...updateOpt) *Update {
	upd := &Update{
		id:              id,
		state:           stateAccepted,
		request:         nil,
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

// WaitLifecycleStage waits until the Update has reached waitStage or a timeout.
// If the Update reaches waitStage with no timeout, the outcome (if any) is returned.
// If there is a timeout due to the supplied soft timeout,
// then the most advanced stage known to have been reached is returned together with an empty outcome.
// If there is a timeout due to supplied context deadline expiry, then the error is returned.
// If waitStage is UNSPECIFIED, current, reached Status is returned immediately (even if ctx is expired).
func (u *Update) WaitLifecycleStage(
	ctx context.Context,
	waitStage enumspb.UpdateWorkflowExecutionLifecycleStage,
	softTimeout time.Duration,
) (*Status, error) {

	stCtx, stCancel := context.WithTimeout(ctx, softTimeout)
	defer stCancel()

	if u.outcome.Ready() || waitStage == enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED {
		outcome, err := u.outcome.Get(stCtx)
		if err == nil {
			return statusCompleted(outcome), nil
		}

		// If err is not nil (checked above), and is not coming from context,
		// then it means that the error is from the future itself.
		if ctx.Err() == nil && stCtx.Err() == nil {
			// Update uses registryClearedErr when Registry is cleared. This error has special handling here:
			//   abort waiting for COMPLETED stage and check if Update has reached ACCEPTED stage.
			// All other errors are returned to the caller.
			if !errors.Is(err, registryClearedErr) {
				return nil, err
			}
		}

		if ctx.Err() != nil {
			// Handle root context deadline expiry as normal error which is returned to the caller.
			metrics.WorkflowExecutionUpdateClientTimeout.With(u.instrumentation.metrics).Record(1)
			return nil, ctx.Err()
		}

		// Only get here if there is an error, and this error is stCtx.Err() (softTimeout has expired) or registryClearedErr.
		// In both cases, check if the Update has reached ACCEPTED stage.
	}

	// Update is not completed but maybe it is accepted.
	if u.accepted.Ready() || waitStage == enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED {
		// Using the same context which might be already expired, but if accepted future is ready,
		// then it will return immediately without checking context deadline.
		rejection, err := u.accepted.Get(stCtx)
		if err == nil {
			if rejection != nil {
				return statusRejected(rejection), nil
			}
			// Even if only ACCEPTED stage was requested, check if Update was completed on the same WFT,
			// and return Update result if it was.
			if u.outcome.Ready() {
				var outcome *updatepb.Outcome
				outcome, err = u.outcome.Get(stCtx)
				if err == nil {
					return statusCompleted(outcome), nil
				}
				// If outcome future returned an error, then ACCEPTED is the most advanced stage reached,
				// and it should be returned to the caller (because it was requested).
				// This can happen when Workflow completes after accepting but not completing Update.
			}
			return statusAccepted(), nil
		}

		// If err is not nil (checked above), and is not coming from context,
		// then it means that the error is from the future itself.
		if ctx.Err() == nil && stCtx.Err() == nil {
			// Update uses registryClearedErr when Registry is cleared. This error has special handling here:
			//   abort waiting for ACCEPTED stage and return Unavailable (retryable) error to the caller.
			// This error will be retried (by history service handler, or history service client in frontend,
			// or SDK, or user client). This will recreate Update in the Registry.
			// All other errors are returned to the caller.
			if !errors.Is(err, registryClearedErr) {
				return nil, err
			}
			return nil, WorkflowUpdateAbortedErr
		}

		if ctx.Err() != nil {
			// Handle root context deadline expiry as normal error which is returned to the caller.
			metrics.WorkflowExecutionUpdateClientTimeout.With(u.instrumentation.metrics).Record(1)
			return nil, ctx.Err()
		}
	}

	// If waitStage=COMPLETED or ACCEPTED and neither has been reached before the softTimeout has expired.
	if stCtx.Err() != nil {
		metrics.WorkflowExecutionUpdateServerTimeout.With(u.instrumentation.metrics).Record(1)
		return statusAdmitted(), nil
	}

	// Only get here if waitStage=ADMITTED or UNSPECIFIED and neither ACCEPTED nor COMPLETED are reached.
	// Return ADMITTED (as the most advanced stage reached) and empty outcome.
	return statusAdmitted(), nil
}

// abort fails Update futures with reason.Error() error (which will notify all waiters with error)
// and set state to stateAborted. It is a terminal state. Update can't be changed after it is aborted.
func (u *Update) abort(reason AbortReason) {
	u.instrumentation.countAborted()

	const preAcceptedStates = stateSet(stateCreated | stateProvisionallyAdmitted | stateAdmitted | stateSent | stateProvisionallyAccepted)
	if u.state.Matches(preAcceptedStates | stateSet(stateProvisionallyCompletedAfterAccepted)) {
		u.accepted.(*future.FutureImpl[*failurepb.Failure]).Set(nil, reason.Error())
		u.outcome.(*future.FutureImpl[*updatepb.Outcome]).Set(nil, reason.Error())
	}

	const preCompletedStates = stateSet(stateAccepted | stateProvisionallyCompleted)
	if u.state.Matches(preCompletedStates) {
		u.outcome.(*future.FutureImpl[*updatepb.Outcome]).Set(nil, reason.Error())
	}

	u.setState(stateAborted)
}

// Admit works if the Update is in any state, but if the state is anything
// other than stateCreated then it just early returns a nil error. This
// effectively gives us Update request deduplication by updateID. If the Update
// is in stateCreated then it builds a protocolpb.Message that will be sent
// when Send is called.
func (u *Update) Admit(
	req *updatepb.Request,
	eventStore EventStore, // Will be useful for durable admitted.
) error {
	if u.state != stateCreated {
		return nil
	}
	if err := validateRequestMsg(u.id, req); err != nil {
		return err
	}
	if !eventStore.CanAddEvent() {
		// There shouldn't be any waiters before Update is admitted (this func returns).
		// Call abort to seal the Update.
		u.abort(AbortReasonWorkflowCompleted)
		return AbortReasonWorkflowCompleted.Error()
	}

	u.instrumentation.countRequestMsg()

	// Marshal Update request here to return InvalidArgument to the API caller if it can't be marshaled.
	if err := utf8validator.Validate(req, utf8validator.SourceRPCRequest); err != nil {
		return invalidArgf("unable to validate utf-8 request: %v", err)
	}
	reqAny, err := anypb.New(req)
	if err != nil {
		return invalidArgf("unable to unmarshal request: %v", err)
	}
	u.request = reqAny

	prevState := u.setState(stateProvisionallyAdmitted)
	eventStore.OnAfterCommit(func(context.Context) {
		if u.state != stateProvisionallyAdmitted {
			return
		}
		u.setState(stateAdmitted)
		u.admittedTime = time.Now().UTC()
	})
	eventStore.OnAfterRollback(func(context.Context) {
		if u.state != stateProvisionallyAdmitted {
			return
		}
		u.setState(prevState)
		var timeZero time.Time
		u.admittedTime = timeZero
	})

	return nil
}

// OnProtocolMessage delivers a message to the Update state machine. The Body field of
// *protocolpb.Message parameter is expected to be one of *updatepb.Response,
// *updatepb.Rejection, *updatepb.Acceptance. Writes to the EventStore
// occur synchronously but externally observable effects on this Update (e.g.,
// emitting an Outcome or an Accepted) are registered with the EventStore to be
// applied after the durable Updates are committed. If the EventStore rolls back
// its effects, this state machine does the same.
//
// If you modify the state machine, please update the diagram in /docs/architecture/workflow-update.md.
func (u *Update) OnProtocolMessage(
	protocolMsg *protocolpb.Message,
	eventStore EventStore,
) error {
	if protocolMsg == nil {
		return invalidArgf("Update %s received nil message", u.id)
	}

	if protocolMsg.Body == nil {
		return invalidArgf("Update %s received message with nil body", u.id)
	}

	body, err := protocolMsg.Body.UnmarshalNew()
	if err != nil {
		return invalidArgf("unable to unmarshal request: %v", err)
	}
	err = utf8validator.Validate(body, utf8validator.SourceRPCRequest)
	if err != nil {
		return invalidArgf("unable to validate utf-8 request: %v", err)
	}

	// If no new events can be added to the event store (e.g., workflow is completed),
	// then only Rejection messages can be processed, because they don't create new events in the history.
	// All other message types abort Update.
	_, isRejection := body.(*updatepb.Rejection)
	shouldAbort := !(eventStore.CanAddEvent() || isRejection)
	if shouldAbort {
		u.abort(AbortReasonWorkflowCompleted)
		return nil
	}

	switch updMsg := body.(type) {
	case *updatepb.Acceptance:
		return u.onAcceptanceMsg(updMsg, eventStore)
	case *updatepb.Rejection:
		return u.onRejectionMsg(updMsg, eventStore)
	case *updatepb.Response:
		return u.onResponseMsg(updMsg, eventStore)
	default:
		return invalidArgf("Message type %T not supported", body)
	}
}

// needToSend returns true if outgoing message can be generated for the current Update state.
// If includeAlreadySent is set to true, then it will return true even if Update was already sent but not processed by worker.
func (u *Update) needToSend(includeAlreadySent bool) bool {
	if includeAlreadySent {
		return u.state.Matches(stateSet(stateAdmitted | stateSent))
	}
	return u.state.Matches(stateSet(stateAdmitted))
}

// Send move Update from stateAdmitted to stateSent and returns the message to be sent to worker.
// If Update is not in expected stateAdmitted, Send does nothing and returns nil.
// If includeAlreadySent is set to true, then Send will return a message even if Update was already sent but not processed by worker.
// If Update lacks a request, then return nil; the request will be communicated to the worker via an UpdateAdmitted event.
// Note: once Update moved to stateSent it never moves back to stateAdmitted.
func (u *Update) Send(
	includeAlreadySent bool,
	sequencingID *protocolpb.Message_EventId,
) *protocolpb.Message {
	if !u.needToSend(includeAlreadySent) {
		return nil
	}

	u.instrumentation.countSent()
	if u.state == stateSent {
		u.instrumentation.countSentAgain()
	}

	if u.state == stateAdmitted {
		u.setState(stateSent)
	}

	if u.request == nil {
		// This implies that the Update in the Registry derives from an UpdateAdmitted event exists;
		// this event (which contains the request payload) is how the Update request will be communicated to the worker.
		return nil
	}
	return &protocolpb.Message{
		ProtocolInstanceId: u.id,
		Id:                 u.outgoingMessageID(),
		SequencingId:       sequencingID,
		Body:               u.request,
	}
}

// isSent checks if Update was sent to worker.
func (u *Update) isSent() bool {
	return u.state.Matches(stateSet(stateSent))
}

// outgoingMessageID returns the ID of the message that is used to Send the Update to the worker.
func (u *Update) outgoingMessageID() string {
	return u.id + "/request"
}

// onAcceptanceMsg expects the Update to be in stateSent (or stateAdmitted) and returns an
// error if it finds otherwise. An event is written to the provided EventStore
// and on commit the accepted future is completed and the Update transitions to
// stateAccepted.
func (u *Update) onAcceptanceMsg(
	acpt *updatepb.Acceptance,
	eventStore EventStore,
) error {
	// Normally Update goes from stateAdmitted to stateSent and then to stateAccepted,
	// therefore, the only valid state here is stateSent.
	// But if Update Registry is cleared after Update was sent to the worker,
	// it will be recreated by retries in stateAdmitted, and then the worker can accept the previous (cleared) Update
	// with the same updateID. Because it is, in fact, the same Update, server should process this accepts message w/o error.
	// Therefore, stateAdmitted is also a valid state.
	if err := u.checkStateSet(acpt, stateSet(stateSent|stateAdmitted)); err != nil {
		return err
	}
	if err := validateAcceptanceMsg(acpt); err != nil {
		return err
	}
	u.instrumentation.countAcceptanceMsg()

	// If the in-registry Update lacks a request payload, this implies that there is an UpdateAdmitted event in
	// history. In this case, we write the UpdateAccepted event without a request payload, since the UpdateAdmitted
	// event has it.
	//
	// Thus, the following sequences of events are all possible, and SDK workers must handle them correctly:
	// UpdateAdmitted(requestPayload)
	// UpdateAdmitted(requestPayload)
	// ...
	// UpdateAccepted(nil)
	// UpdateAccepted(requestPayload)
	var acceptedRequest *updatepb.Request
	if u.request != nil {
		acceptedRequest = &updatepb.Request{}
		if err := u.request.UnmarshalTo(acceptedRequest); err != nil {
			return internalErrorf("unable to unmarshal original request: %v", err)
		}
	}
	// utf8validator: we just marshaled u.request ourself earlier, so we don't need to validate it for utf8 strings here

	event, err := eventStore.AddWorkflowExecutionUpdateAcceptedEvent(
		u.id,
		u.outgoingMessageID(),
		acpt.AcceptedRequestSequencingEventId,
		acceptedRequest)
	if err != nil {
		return err
	}
	u.acceptedEventID = event.EventId

	prevState := u.setState(stateProvisionallyAccepted)
	eventStore.OnAfterCommit(func(context.Context) {
		if !u.state.Matches(stateSet(stateProvisionallyAccepted | stateProvisionallyCompleted)) {
			return
		}
		u.request = nil

		// If the Update is accepted *and* completed in the same WFT, then its state has transitioned
		// from ProvisionallyAccepted to ProvisionallyCompleted in onResponseMsg by the
		// time we get here.
		// 
		// Now, to prevent a race condition in WaitLifecycleStage, the accepted future
		// cannot be set here right now, as it must be set *after* the outcome future.
		// 
		// So instead, the state is set to ProvisionallyCompletedAfterAccepted here,  
		// and onResponseMsg's OnAfterCommit callback will set the futures in the correct order.
		if u.state == stateProvisionallyCompleted {
			u.state = stateProvisionallyCompletedAfterAccepted
			return
		}
		u.setState(stateAccepted)
		u.accepted.(*future.FutureImpl[*failurepb.Failure]).Set(nil, nil)
	})
	eventStore.OnAfterRollback(func(context.Context) {
		if u.state != stateProvisionallyAccepted {
			return
		}
		u.acceptedEventID = common.EmptyEventID
		u.setState(prevState)
	})
	return nil
}

// onRejectionMsg expects the Update state to be stateSent (or stateAdmitted) and returns
// an error otherwise. On commit of buffered effects, the state
// machine transitions to stateCompleted and the accepted and outcome futures
// are both completed with the failurepb.Failure value from the updatepb.Rejection input message.
func (u *Update) onRejectionMsg(
	rej *updatepb.Rejection,
	effects effect.Controller,
) error {
	// See comment in onAcceptanceMsg about stateAdmitted.
	if err := u.checkStateSet(rej, stateSet(stateSent|stateAdmitted)); err != nil {
		return err
	}
	if err := validateRejectionMsg(rej); err != nil {
		return err
	}
	u.instrumentation.countRejectionMsg()
	return u.reject(rej.Failure, effects)
}

// rejects an Update with provided failure.
func (u *Update) reject(
	rejectionFailure *failurepb.Failure,
	effects effect.Controller,
) error {
	prevState := u.setState(stateProvisionallyCompleted)
	effects.OnAfterCommit(func(context.Context) {
		if u.state != stateProvisionallyCompleted {
			return
		}

		u.request = nil
		u.setState(stateCompleted)
		outcome := updatepb.Outcome{
			Value: &updatepb.Outcome_Failure{Failure: rejectionFailure},
		}
		u.accepted.(*future.FutureImpl[*failurepb.Failure]).Set(rejectionFailure, nil)
		u.outcome.(*future.FutureImpl[*updatepb.Outcome]).Set(&outcome, nil)
		u.onComplete()
	})
	effects.OnAfterRollback(func(context.Context) {
		if u.state != stateProvisionallyCompleted {
			return
		}
		u.setState(prevState)
	})
	return nil
}

// onResponseMsg expects the Update to be in either stateProvisionallyAccepted
// or stateAccepted and returns an error if it finds otherwise. On commit of
// buffered effects, the state machine will transition to stateCompleted, and the
// outcome future is completed with the updatepb.Outcome from the updatepb.Response input message.
func (u *Update) onResponseMsg(
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
	u.instrumentation.countResponseMsg()
	prevState := u.setState(stateProvisionallyCompleted)
	eventStore.OnAfterCommit(func(context.Context) {
		if !u.state.Matches(stateSet(stateProvisionallyCompleted | stateProvisionallyCompletedAfterAccepted)) {
			return
		}
		beforeCommitState := u.setState(stateCompleted)
		u.outcome.(*future.FutureImpl[*updatepb.Outcome]).Set(res.GetOutcome(), nil)
		if beforeCommitState == stateProvisionallyCompletedAfterAccepted {
			// If the Update is accepted *and* completed in the same WFT, then its state is ProvisionallyCompletedAfterAccepted here, set by onAcceptance's OnAfterCommit.
			//
			// To prevent a race condition in WaitLifecycleStage, the accepted future
		        // has not been set by OnAcceptance earlier, as it must be set *after* the outcome future.
		        // Now is the time to set it.
		        // 
			// Note that the Accepted state is skipped, and it transitions straight to Completed.
			u.accepted.(*future.FutureImpl[*failurepb.Failure]).Set(nil, nil)
		}
		u.onComplete()
	})
	eventStore.OnAfterRollback(func(context.Context) {
		if u.state != stateProvisionallyCompleted {
			return
		}
		u.setState(prevState)
	})
	return nil
}

func (u *Update) checkStateSet(msg proto.Message, allowed stateSet) error {
	if u.state.Matches(allowed) {
		return nil
	}
	u.instrumentation.invalidStateTransition(u.id, msg, u.state)
	return invalidArgf("invalid state transition attempted for Update %s: "+
		"received %T message while in state %s", u.id, msg, u.state)
}

// setState assigns the current state to a new value returning the original value.
func (u *Update) setState(newState state) state {
	prevState := u.state
	u.state = newState
	u.instrumentation.stateChange(u.id, prevState, newState)
	return prevState
}

func (u *Update) GetSize() int {
	size := len(u.id)
	size += proto.Size(u.request)
	if u.accepted.Ready() {
		res, _ := u.accepted.Get(context.Background())
		size += res.Size()
	}
	if u.outcome.Ready() {
		res, _ := u.outcome.Get(context.Background())
		size += res.Size()
	}
	return size
}
