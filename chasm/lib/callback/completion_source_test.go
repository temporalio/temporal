package callback

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeCompletionSource struct {
	chasm.UnimplementedComponent
}

func (fakeCompletionSource) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (fakeCompletionSource) GetNexusCompletion(chasm.Context, string) (nexusrpc.CompleteOperationOptions, error) {
	return nexusrpc.CompleteOperationOptions{}, nil
}

// TestLoadInvocationArgsResolvesCompletionSource verifies that an outbound invocation carries the
// completion source reported by the framework for the callback's parent.
func TestLoadInvocationArgsResolvesCompletionSource(t *testing.T) {
	source := &fakeCompletionSource{}

	var fqnRequestedFor chasm.Component
	ctx := &chasm.MockContext{
		HandleComponentFqn: func(component chasm.Component) string {
			fqnRequestedFor = component
			return "somelib.somecomponent"
		},
		HandleExecutionKey: func() chasm.ExecutionKey {
			return chasm.ExecutionKey{BusinessID: "business-id", RunID: "run-id"}
		},
	}

	cb := &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId:        "request-id",
			RegistrationTime: timestamppb.Now(),
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{Url: "http://localhost/callback"},
				},
			},
		},
		CompletionSource: chasm.NewMockParentPtr[CompletionSource](source),
	}

	invocable, err := cb.loadInvocationArgs(ctx, nil)
	require.NoError(t, err)

	outbound, ok := invocable.(invocableOutbound)
	require.True(t, ok, "expected an outbound invocation for an http callback URL")
	require.Equal(t, "somelib.somecomponent", outbound.completionSource)
	require.Same(t, source, fqnRequestedFor, "the FQN must be resolved for the callback's parent")
}
