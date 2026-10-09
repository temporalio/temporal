package callback

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/nexus/nexusrpc"
	test "go.temporal.io/server/common/testing"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeHost keeps its callbacks and metadata separately, as a Host pairing a child component with its
// parent's metadata does.
type fakeHost struct {
	callbacks chasm.Map[string, *Callback]
	metadata  *callbackspb.CallbackMetadata
	lifecycle chasm.LifecycleState
}

var _ Host = (*fakeHost)(nil)

func newFakeHost() *fakeHost {
	return &fakeHost{lifecycle: chasm.LifecycleStateRunning}
}

func (h *fakeHost) CompletionCallbacks() chasm.Map[string, *Callback] {
	if h.callbacks == nil {
		h.callbacks = make(chasm.Map[string, *Callback])
	}
	return h.callbacks
}

func (h *fakeHost) CompletionCallbackMetadata() *callbackspb.CallbackMetadata {
	if h.metadata == nil {
		h.metadata = &callbackspb.CallbackMetadata{}
	}
	return h.metadata
}

func (h *fakeHost) LifecycleState(chasm.Context) chasm.LifecycleState {
	return h.lifecycle
}

func (h *fakeHost) GetNexusCompletion(chasm.Context, string) (nexusrpc.CompleteOperationOptions, error) {
	return nexusrpc.CompleteOperationOptions{}, nil
}

func nexusCallbacks(urls ...string) []*commonpb.Callback {
	cbs := make([]*commonpb.Callback, len(urls))
	for i, url := range urls {
		cbs[i] = &commonpb.Callback{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{Url: url}}}
	}
	return cbs
}

func sizeOf(cbs []*commonpb.Callback) int {
	var size int
	for _, cb := range cbs {
		size += cb.Size()
	}
	return size
}

func TestUsage(t *testing.T) {
	t.Parallel()

	require.Equal(t, Usage{}, UsageOf(nil))

	usage := UsageOf(&callbackspb.CallbackMetadata{TotalCallbacksCount: 2, TotalCallbacksSize: 100})
	require.Equal(t, Usage{Count: 2, TotalSize: 100}, usage)

	cbs := nexusCallbacks("http://cb-1", "http://cb-2")
	usage.Reserve(cbs)
	require.Equal(t, Usage{Count: 4, TotalSize: 100 + sizeOf(cbs)}, usage)
}

func TestCheckLimits(t *testing.T) {
	t.Parallel()
	cfg := test.NewCallbacksValidatorConfig()
	cfg.MaxCallbacksPerExecution = func(string) int { return 2 }
	validator := test.NewCallbacksValidator(t, cfg)

	require.NoError(t, ValidateAdditions("ns", Usage{Count: 1}, nexusCallbacks("http://cb-1"), validator))

	err := ValidateAdditions("ns", Usage{Count: 2}, nexusCallbacks("http://cb-1"), validator)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
}

func TestAttach(t *testing.T) {
	t.Parallel()

	t.Run("AttachesAndTracksTotals", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		host := newFakeHost()
		cbs := nexusCallbacks("http://cb-1", "http://cb-2")

		require.NoError(t, Attach(ctx, host, "req-1", timestamppb.Now(), cbs))
		require.Len(t, host.callbacks, 2)
		require.Equal(t, int64(2), host.metadata.GetTotalCallbacksCount())
		require.Equal(t, int64(sizeOf(cbs)), host.metadata.GetTotalCallbacksSize())
		for idx := range cbs {
			cb := host.callbacks[completionCallbackID("req-1", idx)].Get(ctx)
			require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, cb.GetStatus())
		}
		first := host.callbacks[completionCallbackID("req-1", 0)].Get(ctx).GetRequestId()
		second := host.callbacks[completionCallbackID("req-1", 1)].Get(ctx).GetRequestId()
		require.NotEqual(t, "req-1", first)
		require.NotEqual(t, first, second)
	})

	t.Run("ReattachingARequestIsANoOp", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		host := newFakeHost()
		cbs := nexusCallbacks("http://cb-1")

		require.NoError(t, Attach(ctx, host, "req-1", timestamppb.Now(), cbs))
		require.NoError(t, Attach(ctx, host, "req-1", timestamppb.Now(), cbs))
		require.Len(t, host.callbacks, 1)
		require.Equal(t, int64(1), host.metadata.GetTotalCallbacksCount())
	})

	t.Run("SkipsLimitsAndLifecycle", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		host := newFakeHost()
		host.lifecycle = chasm.LifecycleStateCompleted

		require.NoError(t, Attach(ctx, host, "req-1", timestamppb.Now(), nexusCallbacks("http://cb-1")))
		require.Len(t, host.callbacks, 1)
	})

	t.Run("NoCallbacksLeavesHostUntouched", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		host := newFakeHost()

		require.NoError(t, Attach(ctx, host, "req-1", timestamppb.Now(), nil))
		require.Nil(t, host.callbacks)
		require.Nil(t, host.metadata)
	})

	t.Run("WithReusedRequestID", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		host := newFakeHost()
		cbs := nexusCallbacks("http://cb-1", "http://cb-2")

		require.NoError(t, Attach(ctx, host, "req-1", timestamppb.Now(), cbs, WithReusedRequestID()))
		for idx := range cbs {
			cb := host.callbacks[completionCallbackID("req-1", idx)].Get(ctx)
			require.Equal(t, "req-1", cb.GetRequestId())
		}
	})
}

func TestValidateAndAttach(t *testing.T) {
	t.Parallel()
	validator := test.NewCallbacksValidator(t, test.NewCallbacksValidatorConfig())

	t.Run("RejectsAMissingRequestID", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		err := ValidateAndAttach(ctx, newFakeHost(), "", timestamppb.Now(), nexusCallbacks("http://cb-1"), "ns", validator)
		require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
	})

	t.Run("RejectsAClosedHost", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		host := newFakeHost()
		host.lifecycle = chasm.LifecycleStateCompleted

		err := ValidateAndAttach(ctx, host, "req-1", timestamppb.Now(), nexusCallbacks("http://cb-1"), "ns", validator)
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.Empty(t, host.callbacks)
	})

	t.Run("RetryAfterCloseSucceeds", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		host := newFakeHost()
		cbs := nexusCallbacks("http://cb-1")

		require.NoError(t, ValidateAndAttach(ctx, host, "req-1", timestamppb.Now(), cbs, "ns", validator))
		host.lifecycle = chasm.LifecycleStateCompleted
		require.NoError(t, ValidateAndAttach(ctx, host, "req-1", timestamppb.Now(), cbs, "ns", validator))
		require.Len(t, host.callbacks, 1)
	})

	t.Run("CountsAttachedCallbacksAgainstTheLimits", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		cfg := test.NewCallbacksValidatorConfig()
		cfg.MaxCallbacksPerExecution = func(string) int { return 2 }
		limited := test.NewCallbacksValidator(t, cfg)
		host := newFakeHost()

		require.NoError(t, ValidateAndAttach(ctx, host, "req-1", timestamppb.Now(), nexusCallbacks("http://cb-1"), "ns", limited))
		err := ValidateAndAttach(ctx, host, "req-2", timestamppb.Now(), nexusCallbacks("http://cb-2", "http://cb-3"), "ns", limited)
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.Len(t, host.callbacks, 1)
		require.Equal(t, int64(1), host.metadata.GetTotalCallbacksCount())
	})
}
