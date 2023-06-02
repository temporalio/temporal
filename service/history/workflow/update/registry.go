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

	persistencespb "go.temporal.io/server/api/persistence/v1"
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

		// ReadOutgoingMessages polls each registered Update for outbound
		// messages and returns them.
		ReadOutgoingMessages(startedEventID int64) ([]*protocolpb.Message, error)

		// TerminateUpdates terminates all existing updates in the registry
		// and notifies update aPI callers with corresponding error.
		TerminateUpdates(ctx context.Context, eventStore EventStore)

		// HasOutgoing returns true if the registry has any Updates that want to
		// sent messages to a worker.
		HasOutgoing() bool

		// Len observes the number of incomplete updates in this Registry.
		Len() int
	}

	// UpdateStore represents the update package's requirements for reading updates from the store.
	UpdateStore interface {
		VisitUpdates(visitor func(updID string, updInfo *persistencespb.UpdateInfo))
		GetUpdateOutcome(ctx context.Context, updateID string) (*updatepb.Outcome, error)
	}

	RegistryImpl struct {
		mu              sync.RWMutex
		updates         map[string]*Update
		store           UpdateStore
		instrumentation instrumentation
		maxInFlight     func() int
		maxTotal        func() int
		completedCount  int
	}

	regOpt func(*RegistryImpl)
)

//revive:disable:unexported-return I *want* it to be unexported

// WithInFlightLimit provides an optional limit to the number of incomplete
// updates that a Registry instance will allow.
func WithInFlightLimit(f func() int) regOpt {
	return func(r *RegistryImpl) {
		r.maxInFlight = f
	}
}

// WithTotalLimit provides an optional limit to the total number of updates for workflow.
func WithTotalLimit(f func() int) regOpt {
	return func(r *RegistryImpl) {
		r.maxTotal = f
	}
}

// WithLogger sets the log.Logger to be used by an UpdateRegistry and its
// Updates.
func WithLogger(l log.Logger) regOpt {
	return func(r *RegistryImpl) {
		r.instrumentation.log = l
	}
}

// WithMetrics sets the metrics.Handler to be used by an UpdateRegistry and its
// Updates.
func WithMetrics(m metrics.Handler) regOpt {
	return func(r *RegistryImpl) {
		r.instrumentation.metrics = m
	}
}

// WithTracerProvider sets the trace.TracerProvider (and by extension the
// trace.Tracer) to be used by an UpdateRegistry and its Updates.
func WithTracerProvider(t trace.TracerProvider) regOpt {
	return func(r *RegistryImpl) {
		r.instrumentation.tracer = t.Tracer(libraryName)
	}
}

//revive:enable:unexported-return

var _ Registry = (*RegistryImpl)(nil)

func NewRegistry(store UpdateStore, opts ...regOpt) *RegistryImpl {
	r := &RegistryImpl{
		updates:         make(map[string]*Update),
		store:           store,
		instrumentation: noopInstrumentation,
		maxInFlight:     func() int { return math.MaxInt },
		maxTotal:        func() int { return math.MaxInt },
	}
	for _, opt := range opts {
		opt(r)
	}

	store.VisitUpdates(func(updID string, updInfo *persistencespb.UpdateInfo) {
		// need to eager load here so that Len and admit are correct.
		if updInfo.GetAcceptancePointer() != nil {
			r.updates[updID] = newAccepted(updID, r.remover(updID), withInstrumentation(&r.instrumentation))
		}
		if updInfo.GetCompletedPointer() != nil {
			r.completedCount++
		}
	})
	return r
}

func (r *RegistryImpl) FindOrCreate(ctx context.Context, id string) (*Update, bool, error) {
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

func (r *RegistryImpl) Find(ctx context.Context, id string) (*Update, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.findLocked(ctx, id)
}

func (r *RegistryImpl) TerminateUpdates(_ context.Context, _ EventStore) {
	// TODO (alex-update): implement
	// This method is not implemented and update API callers will just timeout.
	// In future, it should remove all existing updates and notify callers with better error.
}

func (r *RegistryImpl) HasOutgoing() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, upd := range r.updates {
		if upd.hasOutgoingMessage() {
			return true
		}
	}
	return false
}

func (r *RegistryImpl) ReadOutgoingMessages(
	startedEventID int64,
) ([]*protocolpb.Message, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*protocolpb.Message
	for _, upd := range r.updates {
		upd.ReadOutgoingMessages(&out)
	}

	// TODO (alex-update): currently sequencing_id is simply pointing to the
	// event before WorkflowTaskStartedEvent.  SDKs are supposed to respect this
	// and process messages (specifically, updates) after event with that ID.
	// In the future, sequencing_id could point to some specific event
	// (specifically, signal) after which the update should be processed.
	// Currently, it is not possible due to buffered events reordering on server
	// and events reordering in some SDKs.
	sequencingEventID := startedEventID - 1
	for _, msg := range out {
		msg.SequencingId = &protocolpb.Message_EventId{EventId: sequencingEventID}
	}
	return out, nil
}

func (r *RegistryImpl) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.updates)
}

func (r *RegistryImpl) remover(id string) updateOpt {
	return withCompletionCallback(
		func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			delete(r.updates, id)
			r.completedCount++
		},
	)
}

func (r *RegistryImpl) admit(ctx context.Context) error {
	if len(r.updates) >= r.maxInFlight() {
		return serviceerror.NewResourceExhausted(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			fmt.Sprintf("limit on number of concurrent in-flight updates has been reached (%v)", r.maxInFlight()),
		)
	}

	if len(r.updates)+r.completedCount >= r.maxTotal() {
		return serviceerror.NewFailedPrecondition(
			fmt.Sprintf("limit on number of total updates has been reached (%v)", r.maxTotal()),
		)
	}

	return nil
}

func (r *RegistryImpl) findLocked(ctx context.Context, id string) (*Update, bool) {

	if upd, ok := r.updates[id]; ok {
		return upd, true
	}

	// update not found in ephemeral state, but could have already completed so
	// check in registry storage
	updOutcome, err := r.store.GetUpdateOutcome(ctx, id)

	// Swallow NotFound error because it means that update doesn't exist.
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return nil, false
	}

	// Other errors goes to the future of completed update because it means, that update exists, was found,
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
