//go:build test_dep

package failpointserver

import (
	"context"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/failpointservice/v1"
	"go.temporal.io/testx/failpoint"
)

// Handle is a per-key view of a failpoint behind a FailpointService. The same Handle works for an
// in-process server (NewServer) and a gRPC-backed remote one (NewRemote).
//
// Tests that used to hold a *Failpoint[T] returned by NewFailpoint and call .Block / .Return /
// .Panic / .Wait / .Disable on it directly can now use a Handle and get the same ergonomics
// across in-process and subprocess nodes.
//
// The methods cover the per-key slice of FailpointService. Cross-failpoint ops
// (DisableAllFailpoints, ListFailpoints) stay on the service itself.
type Handle struct {
	service failpointservice.FailpointServiceServer
	key     failpoint.Key
}

// NewHandle constructs a Handle for the failpoint at the given key behind the given service.
func NewHandle(service failpointservice.FailpointServiceServer, key failpoint.Key) *Handle {
	return &Handle{service: service, key: key}
}

// Block configures the failpoint to block until disabled. Mirrors InjectBlock.
func (h *Handle) Block(ctx context.Context, recurrence failpoint.Recurrence) error {
	_, err := h.service.InjectBlock(ctx, &failpointservice.InjectBlockRequest{
		Key:         h.key.Name(),
		NamespaceId: h.key.NamespaceID(),
		Recurrence:  enumsspb.FailpointRecurrence(recurrence),
	})
	return err
}

// Return configures the failpoint to return an error with the given message. Mirrors
// InjectReturn.
func (h *Handle) Return(ctx context.Context, errMsg string, recurrence failpoint.Recurrence) error {
	_, err := h.service.InjectReturn(ctx, &failpointservice.InjectReturnRequest{
		Key:          h.key.Name(),
		NamespaceId:  h.key.NamespaceID(),
		ErrorMessage: errMsg,
		Recurrence:   enumsspb.FailpointRecurrence(recurrence),
	})
	return err
}

// Panic configures the failpoint to panic with the given value. Mirrors InjectPanic.
func (h *Handle) Panic(ctx context.Context, panicValue string, recurrence failpoint.Recurrence) error {
	_, err := h.service.InjectPanic(ctx, &failpointservice.InjectPanicRequest{
		Key:         h.key.Name(),
		NamespaceId: h.key.NamespaceID(),
		PanicValue:  panicValue,
		Recurrence:  enumsspb.FailpointRecurrence(recurrence),
	})
	return err
}

// Wait blocks until the failpoint's Block action is hit by application code, or ctx is done.
// Returns nil if it was hit, or the context error otherwise. Mirrors WaitForBlock.
func (h *Handle) Wait(ctx context.Context) error {
	_, err := h.service.WaitForBlock(ctx, &failpointservice.WaitForBlockRequest{Key: h.key.Name(), NamespaceId: h.key.NamespaceID()})
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
	_, err := h.service.DisableFailpoint(ctx, &failpointservice.DisableFailpointRequest{Key: h.key.Name(), NamespaceId: h.key.NamespaceID()})
	return err
}
