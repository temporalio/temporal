package nexusrpc_test

import (
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

type asyncWithCancelHandler struct {
	expectHeader bool
	nexus.UnimplementedHandler
}

func (h *asyncWithCancelHandler) StartOperation(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	return &nexus.HandlerStartOperationResultAsync{
		OperationToken: "a/sync",
	}, nil
}

func (h *asyncWithCancelHandler) CancelOperation(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
	if service != testService {
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "unexpected service: %s", service)
	}
	if operation != "f/o/o" {
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "expected operation to be 'foo', got: %s", operation)
	}
	if token != "a/sync" {
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "expected operation ID to be 'async', got: %s", token)
	}
	if h.expectHeader && options.Header.Get("foo") != "bar" {
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid 'foo' request header")
	}
	if options.Header.Get("User-Agent") != "temporalio/server" {
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid 'User-Agent' header: %q", options.Header.Get("User-Agent"))
	}
	return nil
}

func TestCancel_HandleFromStart(t *testing.T) {
	ctx, client, teardown := setup(t, &asyncWithCancelHandler{expectHeader: true})
	defer teardown()

	result, err := client.StartOperation(ctx, "f/o/o", nil, nexus.StartOperationOptions{})
	require.NoError(t, err)
	handle := result.Pending
	require.NotNil(t, handle)
	err = handle.Cancel(ctx, nexus.CancelOperationOptions{
		Header: nexus.Header{"foo": "bar"},
	})
	require.NoError(t, err)
}

func TestCancel_HandleFromClient(t *testing.T) {
	ctx, client, teardown := setup(t, &asyncWithCancelHandler{})
	defer teardown()

	handle, err := client.NewOperationHandle("f/o/o", "a/sync")
	require.NoError(t, err)
	err = handle.Cancel(ctx, nexus.CancelOperationOptions{})
	require.NoError(t, err)
}

type echoTimeoutAsyncWithCancelHandler struct {
	expectedTimeout time.Duration
	nexus.UnimplementedHandler
}

func (h *echoTimeoutAsyncWithCancelHandler) StartOperation(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	return &nexus.HandlerStartOperationResultAsync{
		OperationToken: "timeout",
	}, nil
}

func (h *echoTimeoutAsyncWithCancelHandler) CancelOperation(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
	deadline, set := ctx.Deadline()
	if h.expectedTimeout > 0 && !set {
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "expected operation to have timeout set but context has no deadline")
	}
	if h.expectedTimeout <= 0 && set {
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "expected operation to have no timeout but context has deadline set")
	}
	timeout := time.Until(deadline)
	if timeout > h.expectedTimeout {
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "operation has timeout (%s) greater than expected (%s)", timeout.String(), h.expectedTimeout.String())
	}
	return nil
}

func TestCancel_ContextDeadlinePropagated(t *testing.T) {
	ctx, client, teardown := setup(t, &echoTimeoutAsyncWithCancelHandler{expectedTimeout: testTimeout})
	defer teardown()

	handle, err := client.NewOperationHandle("foo", "timeout")
	require.NoError(t, err)
	err = handle.Cancel(ctx, nexus.CancelOperationOptions{})
	require.NoError(t, err)
}

func TestCancel_RequestTimeoutHeaderOverridesContextDeadline(t *testing.T) {
	timeout := 100 * time.Millisecond
	// relies on ctx returned here having default testTimeout set greater than expected timeout
	ctx, client, teardown := setup(t, &echoTimeoutAsyncWithCancelHandler{expectedTimeout: timeout})
	defer teardown()

	handle, err := client.NewOperationHandle("foo", "timeout")
	require.NoError(t, err)
	err = handle.Cancel(ctx, nexus.CancelOperationOptions{Header: nexus.Header{nexus.HeaderRequestTimeout: nexusrpc.FormatDuration(timeout)}})
	require.NoError(t, err)
}

func TestCancel_TimeoutNotPropagated(t *testing.T) {
	_, client, teardown := setup(t, &echoTimeoutAsyncWithCancelHandler{})
	defer teardown()

	handle, err := client.NewOperationHandle("foo", "timeout")
	require.NoError(t, err)
	err = handle.Cancel(context.Background(), nexus.CancelOperationOptions{})
	require.NoError(t, err)
}
