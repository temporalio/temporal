//go:build test_dep

package failpoint

import (
	"context"
	"time"

	"go.temporal.io/server/api/failpointservice/v1"
)

// Handle is a per-key view of a failpoint on a Registry. Its methods dispatch through the
// Registry's FailpointService methods, so the same Handle works for both in-process (local)
// registries and gRPC-backed (remote) registries.
//
// Tests that used to hold a *Failpoint[T] returned by NewFailpoint and call .Block / .Return /
// .Panic / .Wait / .Disable on it directly can now use a Handle and get the same ergonomics
// across in-process and subprocess nodes.
//
// The methods cover the per-key slice of FailpointService. Cross-failpoint ops
// (DisableAllFailpoints, ListFailpoints) stay on the Registry itself.
type Handle struct {
	registry Registry
	key      Key
}

// NewHandle constructs a Handle for the failpoint at the given key on the given Registry.
func NewHandle(registry Registry, key Key) *Handle {
	return &Handle{registry: registry, key: key}
}

// Block configures the failpoint to block until disabled. Mirrors InjectBlock.
func (h *Handle) Block(ctx context.Context, recurrence Recurrence) error {
	_, err := h.registry.InjectBlock(ctx, &failpointservice.InjectBlockRequest{
		Key:         h.key.name,
		NamespaceId: h.key.namespaceID.String(),
		Recurrence:  recurrence,
	})
	return err
}

// Return configures the failpoint to return an error with the given message. Mirrors
// InjectReturn.
func (h *Handle) Return(ctx context.Context, errMsg string, recurrence Recurrence) error {
	_, err := h.registry.InjectReturn(ctx, &failpointservice.InjectReturnRequest{
		Key:          h.key.name,
		NamespaceId:  h.key.namespaceID.String(),
		ErrorMessage: errMsg,
		Recurrence:   recurrence,
	})
	return err
}

// Panic configures the failpoint to panic with the given value. Mirrors InjectPanic.
func (h *Handle) Panic(ctx context.Context, panicValue string, recurrence Recurrence) error {
	_, err := h.registry.InjectPanic(ctx, &failpointservice.InjectPanicRequest{
		Key:         h.key.name,
		NamespaceId: h.key.namespaceID.String(),
		PanicValue:  panicValue,
		Recurrence:  recurrence,
	})
	return err
}

// Wait blocks until the failpoint's Block action is hit by application code, or ctx is done.
// Returns nil if it was hit, or the context error otherwise. Mirrors WaitForBlock.
func (h *Handle) Wait(ctx context.Context) error {
	_, err := h.registry.WaitForBlock(ctx, &failpointservice.WaitForBlockRequest{Key: h.key.name, NamespaceId: h.key.namespaceID.String()})
	return err
}

// Disable removes the failpoint from the Registry, unblocking any waiters. Returns an error if
// the underlying gRPC call fails (for local registries this is always nil).
//
// Disable uses a fresh, short-lived context internally so it stays callable from deferred
// cleanup paths even after the test's ctx is cancelled. The timeout is intentionally tight: if
// the subprocess is gone, we'd rather fail fast than block test teardown for seconds per failpoint.
func (h *Handle) Disable() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := h.registry.DisableFailpoint(ctx, &failpointservice.DisableFailpointRequest{Key: h.key.name, NamespaceId: h.key.namespaceID.String()})
	return err
}
