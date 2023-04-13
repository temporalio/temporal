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

package workflow

import (
	"context"
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/definition"
)

type (
	// ObserverSet encapsulates threadsafe management of a set of workflow
	// Observer instances. In general, instances of this type are not reentrant
	// so a caller should avoid doing things like calling ObserverSet.Remove
	// from within a call to ObserverSet.FindOrCreate.
	ObserverSet struct {
		mut     sync.Mutex
		entries map[definition.WorkflowKey]map[string]*rcObserver
	}

	// Observer represents the desire of an entity that lives longer than a
	// workflow Context object to observe the same. The use of the Observer
	// interface and ObserverSet registration utility promises that observers
	// will be connected/reconnected to workflow Context objects as they are
	// paged in and out of the workflow Cache.
	//
	// It is up to the specific implementation of Observer to attach itself to
	// the observed abstraction within the workflow Context and to transfer the
	// observed information out of the Context to some external entity.
	Observer interface {
		// Connect is called whenever the workflow.Context that this Observer
		// was created to track is loaded into memory in the workflow Cache.
		// Implementations should assume that the workflow Context lock is
		// already held but the caller when this function is called. If an
		// Observer implementation returns an error from a call to Connect, the
		// Observer will remain in the ObserverSet and Connect will be called
		// again the next time the observed workflow Context is loaded.
		Connect(context.Context, Context) error
	}

	// ObserverKey uniquely identifies an entity within a workflow.Context to be
	// observed (e.g. a WorkflowExecutionUpdate)
	ObserverKey struct {
		WFKey        definition.WorkflowKey
		ObservableID string
	}

	// ObserverReleaseFunc must be called by clients to release a reference to
	// an Observer that was created via a call FindOrCreate.
	ObserverReleaseFunc func()

	rcObserver struct {
		refCount atomic.Int32
		observer Observer
	}
)

func NewObserverSet() *ObserverSet {
	return &ObserverSet{
		entries: map[definition.WorkflowKey]map[string]*rcObserver{},
	}
}

// FindOrCreate looks up an existing Observer by key or if there is no such
// observer, creates a new one via the supplied ObserverConstructor. If a new
// Observer is constructed, the Connect method on the new instance is also
// invoked with the appropariate workflow.Context instance. Note that this
// causes workflow.Context objects to be paged into Cache only if there is not
// already an existing observer for a given ObserverKey. Callers must call the
// returned release function to relinquish a reference to the ref-counted
// Observer.
func (os *ObserverSet) FindOrCreate(
	ctx context.Context,
	key ObserverKey,
	lookupWFCtx func() (Context, func(error), error),
	newObserver func() (Observer, error),
) (Observer, ObserverReleaseFunc, error) {
	addRef := func(rco *rcObserver) (Observer, ObserverReleaseFunc, error) {
		releaser := func() {
			if rco.refCount.Add(-1) == 0 {
				os.Remove(key)
			}
		}
		rco.refCount.Add(1)
		return rco.observer, releaser, nil
	}

	os.mut.Lock()
	wfEntries, ok := os.entries[key.WFKey]
	if !ok {
		wfEntries = map[string]*rcObserver{}
		os.entries[key.WFKey] = wfEntries
	}

	if entry, ok := wfEntries[key.ObservableID]; ok {
		os.mut.Unlock()
		return addRef(entry)
	}
	os.mut.Unlock()

	wfctx, releaseWfCtx, err := lookupWFCtx()
	if err != nil {
		return nil, nil, err
	}
	defer releaseWfCtx(nil)

	obs, err := newObserver()
	if err != nil {
		return nil, nil, err
	}
	entry := &rcObserver{observer: obs}
	if err := obs.Connect(ctx, wfctx); err != nil {
		return nil, nil, err
	}

	os.mut.Lock()
	defer os.mut.Unlock()
	wfEntries[key.ObservableID] = entry
	return addRef(entry)
}

// ConnectAll calls Connect on all managed Observer instances that are observing
// the provided workflow.Context.
func (os *ObserverSet) ConnectAll(ctx context.Context, wfctx Context) {
	os.mut.Lock()
	defer os.mut.Unlock()
	for _, ent := range os.entries[wfctx.GetWorkflowKey()] {
		_ = ent.observer.Connect(ctx, wfctx)
	}
}

// Remove removes the referenced observer from the managed set. Does nothing if
// no matching observer is found.
func (os *ObserverSet) Remove(key ObserverKey) {
	os.mut.Lock()
	defer os.mut.Unlock()
	if wfEntries, ok := os.entries[key.WFKey]; ok {
		delete(wfEntries, key.ObservableID)
		if len(wfEntries) == 0 {
			delete(os.entries, key.WFKey)
		}
	}
}
