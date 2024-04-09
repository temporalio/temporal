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
	"sync"

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
		Find(ctx context.Context, protocolInstanceID string) (*Update, bool)
		// TODO: isn't the return `bool` always true when the *Update != nil?

		// HasOutgoingMessages returns true if the registry has any Updates
		// for which outgoing message can be generated.
		// If includeAlreadySent is set to true then it will return true
		// even if update message was already sent but not processed by worker.
		HasOutgoingMessages(includeAlreadySent bool) bool

		// Send returns messages for all Updates that need to be sent to the worker.
		// If includeAlreadySent is set to true then messages will be created even
		// for updates which were already sent but not processed by worker.
		Send(ctx context.Context, includeAlreadySent bool, workflowTaskStartedEventID int64, eventStore EventStore) []*protocolpb.Message

		// RejectUnprocessed reject all updates that are waiting for workflow task to be completed.
		// This method should be called after all messages from worker are handled to make sure
		// that worker processed (rejected or accepted) all updates that were delivered on the workflow task.
		RejectUnprocessed(ctx context.Context, eventStore EventStore) ([]string, error)

		// CancelIncomplete cancels all incomplete updates in the registry:
		//   - updates in stateCreated, stateAdmitted, or stateSent are rejected,
		//   - updates in stateAccepted are ignored (see CancelIncomplete() in update.go for details),
		//   - updates in stateCompleted are ignored.
		CancelIncomplete(ctx context.Context, reason CancelReason, eventStore EventStore) error

		// Len observes the number of incomplete updates in this Registry.
		Len() int

		// GetSize returns the size of the update object
		GetSize() int
	}

	// Store represents the update package's requirements for reading updates from the store.
	Store interface {
		VisitUpdates(visitor func(updID string, updInfo *updatespb.UpdateInfo))
		GetUpdateOutcome(ctx context.Context, updateID string) (*updatepb.Outcome, error)
	}

	registry struct {
		mu              sync.RWMutex
		updates         map[string]*Update
		getStoreFn      func() Store
		instrumentation instrumentation
		maxInFlight     func() int
		maxTotal        func() int
		completedCount  int
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
	getStoreFn func() Store,
	opts ...Option,
) Registry {
	r := &registry{
		updates:         make(map[string]*Update),
		getStoreFn:      getStoreFn,
		instrumentation: noopInstrumentation,
		maxInFlight:     func() int { return math.MaxInt },
		maxTotal:        func() int { return math.MaxInt },
	}
	for _, opt := range opts {
		opt(r)
	}

	getStoreFn().VisitUpdates(func(updID string, updInfo *updatespb.UpdateInfo) {
		if updInfo.GetRequest() != nil {
			// A update entry in the registry may have a request payload: we use this to write the payload to an
			// UpdateAccepted event, in the event that the update is accepted. However, when populating the registry
			// from mutable state, we do not have access to update request payloads. In this situation it is correct to
			// create a registry entry in state Admitted with a nil payload for the following reason: the fact that we
			// have encountered an UpdateInfo in state Admitted in mutable state implies that there is an
			// UpdateAdmitted event in history; and when there is an UpdateAdmitted event in history, we will not
			// attempt to write the request payload to the UpdateAccepted event, since the request payload is already
			// present in the UpdateAdmitted event.
			var request *anypb.Any
			r.updates[updID] = newAdmitted(
				updID,
				request,
				r.remover(updID),
				withInstrumentation(&r.instrumentation),
			)
		} else if acc := updInfo.GetAcceptance(); acc != nil {
			r.updates[updID] = newAccepted(
				updID,
				acc.EventId,
				r.remover(updID),
				withInstrumentation(&r.instrumentation),
			)
		} else if updInfo.GetCompletion() != nil {
			r.completedCount++
		}
	})
	return r
}

func (r *registry) FindOrCreate(ctx context.Context, id string) (*Update, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if upd, found := r.findLocked(ctx, id); found {
		return upd, true, nil
	}
	if err := r.admit(ctx); err != nil {
		return nil, false, err
	}
	upd := New(id, r.remover(id), withInstrumentation(&r.instrumentation))
	r.updates[id] = upd
	return upd, false, nil
}

func (r *registry) Find(ctx context.Context, id string) (*Update, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.findLocked(ctx, id)
}

// CancelIncomplete cancels all incomplete updates in the registry:
//   - updates in stateCreated, stateAdmitted, or stateSent are rejected,
//   - updates in stateAccepted are ignored (see CancelIncomplete() in update.go for details),
//   - updates in stateCompleted are ignored.
func (r *registry) CancelIncomplete(ctx context.Context, reason CancelReason, eventStore EventStore) error {
	incompleteUpdates := r.filter(func(u *Update) bool { return u.isIncomplete() })
	for _, upd := range incompleteUpdates {
		if err := upd.CancelIncomplete(ctx, reason, eventStore); err != nil {
			return err
		}
	}
	return nil
}

// RejectUnprocessed reject all updates that are waiting for workflow task to be completed.
// This method should be called after all messages from worker are handled to make sure
// that worker processed (rejected or accepted) all updates that were delivered on specific workflow task.
func (r *registry) RejectUnprocessed(
	ctx context.Context,
	eventStore EventStore,
) ([]string, error) {

	// Iterate over updates in the registry while holding read lock.
	// Call to reject() bellow will acquire write lock, thus it needs to be done outside the read lock.
	updatesToReject := r.filter(func(u *Update) bool { return u.isSent() })

	if len(updatesToReject) == 0 {
		return nil, nil
	}

	var rejectedUpdateIDs []string
	for _, upd := range updatesToReject {
		if err := upd.reject(ctx, unprocessedUpdateFailure, eventStore); err != nil {
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
	r.mu.RLock()
	defer r.mu.RUnlock()
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
	eventStore EventStore,
) []*protocolpb.Message {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// TODO (alex-update): currently sequencing_id is simply pointing to the
	//  event before WorkflowTaskStartedEvent. SDKs are supposed to respect this
	//  and process messages (specifically, updates) after event with that ID.
	//  In the future, sequencing_id could point to some specific event
	//  (specifically, signal) after which the update should be processed.
	//  Currently, it is not possible due to buffered events reordering on server
	//  and events reordering in some SDKs.
	sequencingEventID := &protocolpb.Message_EventId{EventId: workflowTaskStartedEventID - 1}

	var outgoingMessages []*protocolpb.Message
	for _, upd := range r.updates {
		outgoingMessage := upd.Send(ctx, includeAlreadySent, sequencingEventID, eventStore)
		if outgoingMessage != nil {
			outgoingMessages = append(outgoingMessages, outgoingMessage)
		}
	}
	return outgoingMessages
}

func (r *registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.updates)
}

func (r *registry) remover(id string) updateOpt {
	return withCompletionCallback(
		func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			delete(r.updates, id)
			r.completedCount++
		},
	)
}

func (r *registry) admit(ctx context.Context) error {
	if len(r.updates) >= r.maxInFlight() {
		return &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: fmt.Sprintf("limit on number of concurrent in-flight updates has been reached (%v)", r.maxInFlight()),
		}
	}

	if len(r.updates)+r.completedCount >= r.maxTotal() {
		return serviceerror.NewFailedPrecondition(
			fmt.Sprintf("limit on number of total updates has been reached (%v)", r.maxTotal()),
		)
	}

	return nil
}

func (r *registry) findLocked(ctx context.Context, id string) (*Update, bool) {
	if upd, ok := r.updates[id]; ok {
		return upd, true
	}

	// update not found in ephemeral state, but could have already completed so
	// check in registry storage
	updOutcome, err := r.getStoreFn().GetUpdateOutcome(ctx, id)

	// Swallow NotFound error because it means that update doesn't exist.
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return nil, false
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
	), true
}

// filter returns a slice of all updates in the registry for which the
// provided predicate function returns true. The registry is locked for reading
// while enumerating over the map. Resulted slice can be iterated w/o holding a lock.
func (r *registry) filter(predicate func(u *Update) bool) []*Update {
	var res []*Update
	r.mu.RLock()
	defer r.mu.RUnlock()
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
	return size
}
