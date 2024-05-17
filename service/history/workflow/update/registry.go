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
	"fmt"
	"math"
	"slices"

	"go.opentelemetry.io/otel/trace"
	enumspb "go.temporal.io/api/enums/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"google.golang.org/protobuf/types/known/anypb"

	updatespb "go.temporal.io/server/api/update/v1"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/utf8validator"
	"go.temporal.io/server/internal/effect"
)

type (
	// Registry maintains a set of updates that have been admitted to run
	// against a workflow execution.
	Registry interface {
		// FindOrCreate finds an existing Update or creates a new one. The second
		// return value (bool) indicates whether the Update returned already
		// existed and was found (true) or was not found and has been newly
		// created (false)
		FindOrCreate(ctx context.Context, protocolInstanceID string) (*Update, bool, error)

		// Find finds an existing update in this Registry but does not create a
		// new update if no update is found.
		Find(ctx context.Context, protocolInstanceID string) *Update

		// TryResurrect tries to resurrect the update from the protocol message,
		// whose body contains Acceptance or Rejection message.
		// It returns an error if some unexpected error happened, but if there is not
		// enough data in the message, it just returns a nil update.
		// If the update was successfully resurrected it is added to the registry in stateAdmitted.
		TryResurrect(ctx context.Context, acptOrRejMsg *protocolpb.Message) (*Update, error)

		// HasOutgoingMessages returns true if the registry has any Updates
		// for which outgoing message can be generated.
		// If includeAlreadySent is set to true then it will return true
		// even if update message was already sent but not processed by worker.
		HasOutgoingMessages(includeAlreadySent bool) bool

		// Send returns messages for all Updates that need to be sent to the worker.
		// If includeAlreadySent is set to true then messages will be created even
		// for updates which were already sent but not processed by worker.
		Send(ctx context.Context, includeAlreadySent bool, workflowTaskStartedEventID int64) []*protocolpb.Message

		// RejectUnprocessed reject all updates that are waiting for workflow task to be completed.
		// This method should be called after all messages from worker are handled to make sure
		// that worker processed (rejected or accepted) all updates that were delivered on the workflow task.
		RejectUnprocessed(ctx context.Context, effects effect.Controller) ([]string, error)

		// Abort all incomplete updates in the registry.
		Abort(reason AbortReason)

		Contains(protocolInstanceID string) bool

		// Clear registry and abort all waiters.
		Clear()

		// Len observes the number of incomplete updates in this Registry.
		Len() int

		// GetSize returns the size of the update object
		GetSize() int

		// FailoverVersion of mutable state at the time of registry creation.
		FailoverVersion() int64
	}

	// Store represents the update package's requirements for reading updates from the store.
	Store interface {
		VisitUpdates(visitor func(updID string, updInfo *updatespb.UpdateInfo))
		GetUpdateOutcome(ctx context.Context, updateID string) (*updatepb.Outcome, error)
		GetCurrentVersion() int64
		IsWorkflowExecutionRunning() bool
	}

	registry struct {
		updates         map[string]*Update
		store           Store
		instrumentation instrumentation
		maxInFlight     func() int
		maxTotal        func() int
		completedCount  int
		failoverVersion int64
	}

	Option func(*registry)
)

// WithInFlightLimit provides an optional limit to the number of incomplete
// updates that a Registry instance will allow.
func WithInFlightLimit(f func() int) Option {
	return func(r *registry) {
		r.maxInFlight = f
	}
}

// WithTotalLimit provides an optional limit to the total number of updates for workflow.
func WithTotalLimit(f func() int) Option {
	return func(r *registry) {
		r.maxTotal = f
	}
}

// WithLogger sets the log.Logger to be used by an UpdateRegistry and its
// Updates.
func WithLogger(l log.Logger) Option {
	return func(r *registry) {
		r.instrumentation.log = l
	}
}

// WithMetrics sets the metrics.Handler to be used by an UpdateRegistry and its
// Updates.
func WithMetrics(m metrics.Handler) Option {
	return func(r *registry) {
		r.instrumentation.metrics = m
	}
}

// WithTracerProvider sets the trace.TracerProvider (and by extension the
// trace.Tracer) to be used by an UpdateRegistry and its Updates.
func WithTracerProvider(t trace.TracerProvider) Option {
	return func(r *registry) {
		r.instrumentation.tracer = t.Tracer(libraryName)
	}
}

var _ Registry = (*registry)(nil)

func NewRegistry(
	store Store,
	opts ...Option,
) Registry {
	r := &registry{
		updates:         make(map[string]*Update),
		store:           store,
		instrumentation: noopInstrumentation,
		maxInFlight:     func() int { return math.MaxInt },
		maxTotal:        func() int { return math.MaxInt },
		failoverVersion: store.GetCurrentVersion(),
	}
	for _, opt := range opts {
		opt(r)
	}

	r.store.VisitUpdates(func(updID string, updInfo *updatespb.UpdateInfo) {
		if updInfo.GetAdmission() != nil {
			// An update entry in the registry may have a request payload: we use this to write the payload to an
			// UpdateAccepted event, in the event that the update is accepted. However, when populating the registry
			// from mutable state, we do not have access to update request payloads. In this situation it is correct
			// to create a registry entry in state Admitted with a nil payload for the following reason: the fact
			// that we have encountered an UpdateInfo in state Admitted in mutable state implies that there is an
			// UpdateAdmitted event in history; and when there is an UpdateAdmitted event in history, we will not
			// attempt to write the request payload to the UpdateAccepted event, since the request payload is
			// already present in the UpdateAdmitted event.
			r.updates[updID] = newAdmitted(
				updID,
				nil,
				r.remover(updID),
				withInstrumentation(&r.instrumentation),
			)
		} else if acc := updInfo.GetAcceptance(); acc != nil {
			u := newAccepted(
				updID,
				acc.EventId,
				r.remover(updID),
				withInstrumentation(&r.instrumentation),
			)
			if !r.store.IsWorkflowExecutionRunning() {
				// If workflow is completed, accepted update will never be completed.
				// This will return "workflow completed" error to the pollers of outcome of accepted updates.
				u.abort(AbortReasonWorkflowCompleted)
			}
			r.updates[updID] = u
		} else if updInfo.GetCompletion() != nil {
			r.completedCount++
		}
	})
	return r
}

func (r *registry) FindOrCreate(ctx context.Context, id string) (*Update, bool, error) {
	if upd := r.Find(ctx, id); upd != nil {
		return upd, true, nil
	}
	if err := r.checkLimits(ctx); err != nil {
		return nil, false, err
	}
	upd := New(id, r.remover(id), withInstrumentation(&r.instrumentation))
	r.updates[id] = upd
	return upd, false, nil
}

func (r *registry) TryResurrect(ctx context.Context, acptOrRejMsg *protocolpb.Message) (*Update, error) {
	if acptOrRejMsg == nil || acptOrRejMsg.Body == nil {
		return nil, nil
	}

	// Check only total limit here. This might add more than maxInFlight updates to registry,
	// but provides better developer experience.
	if err := r.checkTotalLimit(ctx); err != nil {
		return nil, err
	}

	body, err := acptOrRejMsg.Body.UnmarshalNew()
	if err != nil {
		return nil, invalidArgf("unable to unmarshal request: %v", err)
	}
	err = utf8validator.Validate(body, utf8validator.SourceRPCRequest)
	if err != nil {
		return nil, invalidArgf("unable to validate utf-8 request: %v", err)
	}
	var reqMsg *updatepb.Request
	switch updMsg := body.(type) {
	case *updatepb.Acceptance:
		reqMsg = updMsg.GetAcceptedRequest()
	case *updatepb.Rejection:
		reqMsg = updMsg.GetRejectedRequest()
		// Ignore all other message types.
	}
	if reqMsg == nil {
		return nil, nil
	}
	reqAny, err := anypb.New(reqMsg)
	if err != nil {
		return nil, invalidArgf("unable to unmarshal request body: %v", err)
	}

	updateID := acptOrRejMsg.ProtocolInstanceId
	upd := newAdmitted(
		updateID,
		reqAny,
		r.remover(updateID),
		withInstrumentation(&r.instrumentation),
	)
	r.updates[updateID] = upd
	return upd, nil
}

// Abort all incomplete updates in the registry.
func (r *registry) Abort(reason AbortReason) {
	for _, upd := range r.updates {
		upd.abort(reason)
	}
}

// Contains returns true iff the update ID exists in the registry.
func (r *registry) Contains(id string) bool {
	return r.updates[id] != nil
}

// RejectUnprocessed reject all updates that are waiting for workflow task to be completed.
// This method should be called after all messages from worker are handled to make sure
// that worker processed (rejected or accepted) all updates that were delivered on specific workflow task.
func (r *registry) RejectUnprocessed(
	ctx context.Context,
	effects effect.Controller,
) ([]string, error) {

	updatesToReject := r.filter(func(u *Update) bool { return u.isSent() })

	if len(updatesToReject) == 0 {
		return nil, nil
	}

	var rejectedUpdateIDs []string
	for _, upd := range updatesToReject {
		if err := upd.reject(ctx, unprocessedUpdateFailure, effects); err != nil {
			return nil, err
		}
		rejectedUpdateIDs = append(rejectedUpdateIDs, upd.id)
	}
	return rejectedUpdateIDs, nil
}

// HasOutgoingMessages returns true if the registry has any Updates
// for which outgoing message can be generated.
// If includeAlreadySent is set to true then it will return true
// even if update message was already sent but not processed by worker.
func (r *registry) HasOutgoingMessages(includeAlreadySent bool) bool {
	for _, upd := range r.updates {
		if upd.needToSend(includeAlreadySent) {
			return true
		}
	}
	return false
}

// Send returns messages for all Updates that need to be sent to the worker.
// If includeAlreadySent is set to true then messages will be created even
// for updates which were already sent but not processed by worker.
func (r *registry) Send(
	ctx context.Context,
	includeAlreadySent bool,
	workflowTaskStartedEventID int64,
) []*protocolpb.Message {

	// TODO (alex-update): currently sequencing_id is simply pointing to the
	//  event before WorkflowTaskStartedEvent. SDKs are supposed to respect this
	//  and process messages (specifically, updates) after event with that ID.
	//  In the future, sequencing_id could point to some specific event
	//  (specifically, signal) after which the update should be processed.
	//  Currently, it is not possible due to buffered events reordering on server
	//  and events reordering in some SDKs.
	sequencingEventID := &protocolpb.Message_EventId{EventId: workflowTaskStartedEventID - 1}

	var outgoingMessages []*protocolpb.Message

	var sortedUpdates []*Update
	for _, upd := range r.updates {
		sortedUpdates = append(sortedUpdates, upd)
	}
	slices.SortStableFunc(sortedUpdates, func(u1, u2 *Update) int { return u1.admittedTime.Compare(u2.admittedTime) })

	for _, upd := range sortedUpdates {
		outgoingMessage := upd.Send(ctx, includeAlreadySent, sequencingEventID)
		if outgoingMessage != nil {
			outgoingMessages = append(outgoingMessages, outgoingMessage)
		}
	}
	return outgoingMessages
}

// Clear registry and abort all waiters.
func (r *registry) Clear() {
	r.Abort(AbortReasonRegistryCleared)

	r.updates = nil
	r.completedCount = 0
}

func (r *registry) Len() int {
	return len(r.updates)
}

func (r *registry) remover(id string) updateOpt {
	return withCompletionCallback(
		func() {
			delete(r.updates, id)
			r.completedCount++
		},
	)
}

func (r *registry) checkLimits(ctx context.Context) error {
	if len(r.updates) >= r.maxInFlight() {
		r.instrumentation.countRateLimited()
		return &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: fmt.Sprintf("limit on number of concurrent in-flight updates has been reached (%v)", r.maxInFlight()),
		}
	}

	return r.checkTotalLimit(ctx)
}

func (r *registry) checkTotalLimit(_ context.Context) error {
	if len(r.updates)+r.completedCount >= r.maxTotal() {
		r.instrumentation.countTooMany()
		return serviceerror.NewFailedPrecondition(
			fmt.Sprintf("limit on number of total updates has been reached (%v)", r.maxTotal()),
		)
	}
	return nil
}

func (r *registry) Find(ctx context.Context, id string) *Update {
	if upd, ok := r.updates[id]; ok {
		return upd
	}

	// update not found in ephemeral state, but could have already completed so
	// check in registry storage
	updOutcome, err := r.store.GetUpdateOutcome(ctx, id)

	// Swallow NotFound error because it means that update doesn't exist.
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return nil
	}

	// Other errors go to the future of completed update because it means, that update exists, was found,
	// but there is something broken in it.

	// Completed, create the Update object but do not add to registry.
	// This should not happen often.
	fut := future.NewReadyFuture(updOutcome, err)
	return newCompleted(
		id,
		fut,
		withInstrumentation(&r.instrumentation),
	)
}

// filter returns a slice of all updates in the registry for which the
// provided predicate function returns true.
func (r *registry) filter(predicate func(u *Update) bool) []*Update {
	var res []*Update
	for _, upd := range r.updates {
		if predicate(upd) {
			res = append(res, upd)
		}
	}
	return res
}

func (r *registry) GetSize() int {
	var size int
	for key, update := range r.updates {
		size += len(key) + update.GetSize()
	}
	r.instrumentation.updateRegistrySize(size)
	return size
}

func (r *registry) FailoverVersion() int64 {
	return r.failoverVersion
}
