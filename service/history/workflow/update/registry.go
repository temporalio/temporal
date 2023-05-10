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
	"sync"

	"go.opentelemetry.io/otel/trace"
	protocolpb "go.temporal.io/api/protocol/v1"
	updatepb "go.temporal.io/api/update/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	Registry interface {
		// FindOrCreate finds an existing Update or creates a new one. The second
		// return value (bool) indicates whether the Update returned already
		// existed and was found (true) or was not found and has been newly
		// created (false)
		FindOrCreate(ctx context.Context, protocolInstanceID string) (*Update, bool, error)

		// Find finds an existing update in this Registry but does not create a
		// new update it is absent.
		Find(ctx context.Context, protocolInstanceID string) (*Update, bool)

		// ReadOutoundMessages polls each registered Update for outbound
		// messages and returns them.
		ReadOutgoingMessages(startedEventID int64) ([]*protocolpb.Message, error)

		// HasIncomplete returns true if the registry has Updates that have not
		// yet completed or are not at least provisionally complete.
		HasIncomplete() bool

		// HasOutgoing returns true if the registry has any Updates that want to
		// sent messages to a worker.
		HasOutgoing() bool

		// Len observes the number of updates in this Registry.
		Len() int
	}

	// UpdateStore represents the update package's requirements for writing
	// events and restoring ephemeral state from an event index.
	UpdateStore interface {
		GetAcceptedWorkflowExecutionUpdateIDs(context.Context) []string
		GetUpdateInfo(ctx context.Context, updateID string) (*persistencespb.UpdateInfo, bool)
		GetUpdateOutcome(ctx context.Context, updateID string) (*updatepb.Outcome, error)
	}

	RegistryImpl struct {
		mu              sync.RWMutex
		updates         map[string]*Update
		store           UpdateStore
		instrumentation instrumentation
	}

	regOpt func(*RegistryImpl)
)

//revive:disable:unexported-return I *want* it to be unexported

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
	}
	for _, opt := range opts {
		opt(r)
	}

	// need to eager load here so that HasIncomplete is correct.
	for _, id := range store.GetAcceptedWorkflowExecutionUpdateIDs(context.Background()) {
		r.updates[id] = newAccepted(id, r.remover(id), withInstrumentation(&r.instrumentation))
	}
	return r
}

func (r *RegistryImpl) FindOrCreate(ctx context.Context, id string) (*Update, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if upd, ok := r.findLocked(ctx, id); ok {
		return upd, true, nil
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

func (r *RegistryImpl) HasIncomplete() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, upd := range r.updates {
		if !upd.completedOrProvisionallyCompleted() {
			return true
		}
	}
	return false
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

func (r *RegistryImpl) remover(id string) func() {
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.updates, id)
	}
}

func (r *RegistryImpl) findLocked(ctx context.Context, id string) (*Update, bool) {
	upd, ok := r.updates[id]
	if ok {
		return upd, true
	}

	// update not found in ephemeral state, but could have already completed so
	// check in registry storage

	if info, ok := r.store.GetUpdateInfo(ctx, id); ok {
		if info.GetCompletedPointer() != nil {
			// Completed, create the Update object but do not add to registry. this
			// should not happen often.
			return newCompleted(id, func(ctx context.Context) (*updatepb.Outcome, error) {
				return r.store.GetUpdateOutcome(ctx, id)
			}, withInstrumentation(&r.instrumentation)), true
		}
	}
	return nil, false
}
