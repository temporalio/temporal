package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	commoncallbacks "go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testCallbackValidator returns a Validator enforcing the aggregate limits these tests exercise.
// The per-callback limits are set out of the way, since only the aggregate checks are under test.
func testCallbackValidator(t *testing.T, maxCount, maxSourceContextSize int) commoncallbacks.Validator {
	t.Helper()
	v, err := commoncallbacks.NewValidator(commoncallbacks.ValidatorConfig{
		MaxCallbacksPerExecution:                  func(string) int { return maxCount },
		MaxIDLengthLimit:                          func() int { return 1000 },
		URLMaxLength:                              func(string) int { return 1000 },
		HeaderMaxSize:                             func(string) int { return 1000 },
		EndpointRules:                             func(string) commoncallbacks.AddressMatchRules { return commoncallbacks.AddressMatchRules{} },
		MaxServiceNameLength:                      func(string) int { return 1000 },
		MaxOperationNameLength:                    func(string) int { return 1000 },
		NexusHandlerSourceContextMaxSize:          func(string) int { return 1024 * 1024 },
		NexusHandlerSourceContextAggregateMaxSize: func(string) int { return maxSourceContextSize },
	})
	require.NoError(t, err)
	return v
}

// callbackTestContext returns a mutable context carrying the namespace entry that the callback
// limit checks resolve their dynamic config against.
func callbackTestContext() *chasm.MockMutableContext {
	return &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNamespaceEntry: func() *namespace.Namespace {
				return namespace.NewLocalNamespaceForTest(
					&persistencespb.NamespaceInfo{Id: "test-namespace-id", Name: "test-namespace"},
					&persistencespb.NamespaceConfig{},
					"test-cluster",
				)
			},
		},
	}
}

// nexusHandlerCallback returns a NexusHandler-variant callback whose source context serializes to
// roughly sourceCtxSize bytes.
func nexusHandlerCallback(sourceCtxSize int) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_NexusHandler_{NexusHandler: &commonpb.Callback_NexusHandler{
			TaskQueueName: "completions",
			Service:       "Adapter",
			Operation:     "Deliver",
			SourceContext: &commonpb.Payload{Data: make([]byte, sourceCtxSize)},
		}},
	}
}

// The frontend bounds the source context carried by one request. Only this check bounds what
// accumulates across the many requests that can attach callbacks to a running workflow, and it
// counts the workflow's own callbacks together with those on its updates.
func TestWorkflowCallbackSourceContextLimit(t *testing.T) {
	callbackValidator := testCallbackValidator(t, 10, 1500)
	eventTime := timestamppb.Now()

	t.Run("RejectsASingleOversizedRequest", func(t *testing.T) {
		ctx := callbackTestContext()
		wf := &Workflow{}

		err := wf.AddCompletionCallbacks(ctx, eventTime, "req-1", []*commonpb.Callback{
			nexusHandlerCallback(1600),
		}, callbackValidator)
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
		require.ErrorContains(t, err, "cannot attach more than 1500 bytes of callback source_context")
		require.Empty(t, wf.Callbacks)
	})

	t.Run("RejectsExceedingTheLimitWithAlreadyAttachedCallbacks", func(t *testing.T) {
		ctx := callbackTestContext()
		wf := &Workflow{}

		require.NoError(t, wf.AddCompletionCallbacks(ctx, eventTime, "req-1", []*commonpb.Callback{
			nexusHandlerCallback(900),
		}, callbackValidator))

		err := wf.AddCompletionCallbacks(ctx, eventTime, "req-2", []*commonpb.Callback{
			nexusHandlerCallback(900),
		}, callbackValidator)
		// req-2 is within the limit on its own, so only the accumulated total can have rejected it.
		require.ErrorContains(t, err, "cannot attach more than 1500 bytes of callback source_context")
		require.Len(t, wf.Callbacks, 1)
	})

	// An update's callbacks count against the same budget as the workflow's own.
	t.Run("CountsCallbacksAttachedToUpdates", func(t *testing.T) {
		ctx := callbackTestContext()
		wf := &Workflow{}

		require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, eventTime, "update-1", "req-1",
			[]*commonpb.Callback{nexusHandlerCallback(900)}, callbackValidator, 10))

		err := wf.AddCompletionCallbacks(ctx, eventTime, "req-2", []*commonpb.Callback{
			nexusHandlerCallback(900),
		}, callbackValidator)
		require.ErrorContains(t, err, "cannot attach more than 1500 bytes of callback source_context")
		require.Empty(t, wf.Callbacks)
	})

	t.Run("AllowsCallbacksWithinTheLimit", func(t *testing.T) {
		ctx := callbackTestContext()
		wf := &Workflow{}

		require.NoError(t, wf.AddCompletionCallbacks(ctx, eventTime, "req-1", []*commonpb.Callback{
			nexusHandlerCallback(700),
			nexusHandlerCallback(700),
		}, callbackValidator))
		require.Len(t, wf.Callbacks, 2)
	})
}
