package failpointgrpc

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/failpointpb"
)

// Handle is a per-key view of a failpoint behind a FailpointService. The same Handle works for an
// in-process server (NewLocalClient of NewServer) and a remote one (failpointpb.NewFailpointServiceClient).
type Handle struct {
	client failpointpb.FailpointServiceClient
	key    failpoint.AnyKey
}

// NewHandle constructs a Handle for the failpoint at the given key behind the given service.
func NewHandle(client failpointpb.FailpointServiceClient, key failpoint.AnyKey) *Handle {
	return &Handle{client: client, key: key}
}

// errUnsupportedTrigger is returned for a trigger that the protocol can't carry.
var errUnsupportedTrigger = errors.New("the failpoint protocol supports only an unlimited trigger and Trigger{MaxFires: 1}")

// triggerRecurrence returns the recurrence of the protocol for the optional trigger of a Handle
// method: the failpoint fires on every execution without one, and once for Trigger{MaxFires: 1}.
func triggerRecurrence(trigger []failpoint.Trigger) (failpointpb.Recurrence, error) {
	if len(trigger) > 1 {
		return 0, errors.New("at most one trigger")
	}
	var t failpoint.Trigger
	if len(trigger) == 1 {
		t = trigger[0]
	}
	recurrence, ok := recurrenceOf(t)
	if !ok {
		return 0, errUnsupportedTrigger
	}
	return recurrence, nil
}

// Block configures the failpoint to block until disabled. Mirrors InjectBlock. The optional
// trigger is Trigger{MaxFires: 1} to hold one execution; the protocol has no other trigger.
func (h *Handle) Block(ctx context.Context, trigger ...failpoint.Trigger) error {
	recurrence, err := triggerRecurrence(trigger)
	if err != nil {
		return err
	}
	_, err = h.client.InjectBlock(ctx, &failpointpb.InjectBlockRequest{
		Name:       h.key.Name(),
		Scope:      h.key.Scope(),
		Recurrence: recurrence,
	})
	return err
}

// Return configures the failpoint to return an error with the given message. Mirrors
// InjectReturn. See Block for the optional trigger.
func (h *Handle) Return(ctx context.Context, errMsg string, trigger ...failpoint.Trigger) error {
	recurrence, err := triggerRecurrence(trigger)
	if err != nil {
		return err
	}
	_, err = h.client.InjectReturn(ctx, &failpointpb.InjectReturnRequest{
		Name:         h.key.Name(),
		Scope:        h.key.Scope(),
		ErrorMessage: errMsg,
		Recurrence:   recurrence,
	})
	return err
}

// Panic configures the failpoint to panic with the given value. Mirrors InjectPanic. See Block for
// the optional trigger.
func (h *Handle) Panic(ctx context.Context, panicValue string, trigger ...failpoint.Trigger) error {
	recurrence, err := triggerRecurrence(trigger)
	if err != nil {
		return err
	}
	_, err = h.client.InjectPanic(ctx, &failpointpb.InjectPanicRequest{
		Name:       h.key.Name(),
		Scope:      h.key.Scope(),
		PanicValue: panicValue,
		Recurrence: recurrence,
	})
	return err
}

// Wait waits until an execution reaches the block. It returns the error of WaitForBlock.
func (h *Handle) Wait(ctx context.Context) error {
	_, err := h.client.WaitForBlock(ctx, &failpointpb.WaitForBlockRequest{Name: h.key.Name(), Scope: h.key.Scope()})
	return err
}

// Disable removes the failpoint and releases the executions it holds. It uses its own one-second
// context, so it works in cleanup after the test's context is done.
func (h *Handle) Disable() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := h.client.DisableFailpoint(ctx, &failpointpb.DisableFailpointRequest{Name: h.key.Name(), Scope: h.key.Scope()})
	return err
}
