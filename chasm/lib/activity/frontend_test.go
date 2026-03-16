package activity

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

// stubActivityServiceClient is a test double that implements activitypb.ActivityServiceClient.
// Only methods under test are overridden; all others panic if called.
type stubActivityServiceClient struct {
	activitypb.ActivityServiceClient
	deleteActivityExecution func(ctx context.Context, in *activitypb.DeleteActivityExecutionRequest, opts ...grpc.CallOption) (*activitypb.DeleteActivityExecutionResponse, error)
}

func (c *stubActivityServiceClient) DeleteActivityExecution(ctx context.Context, in *activitypb.DeleteActivityExecutionRequest, opts ...grpc.CallOption) (*activitypb.DeleteActivityExecutionResponse, error) {
	return c.deleteActivityExecution(ctx, in, opts...)
}

func TestDeleteActivityExecution_Disabled(t *testing.T) {
	h := &frontendHandler{
		config: &Config{
			Enabled:          func(string) bool { return false },
			MaxIDLengthLimit: func() int { return defaultMaxIDLengthLimit },
		},
	}
	_, err := h.DeleteActivityExecution(context.Background(), &workflowservice.DeleteActivityExecutionRequest{
		Namespace:  "test-namespace",
		ActivityId: defaultActivityID,
	})
	require.ErrorIs(t, err, ErrStandaloneActivityDisabled)
}

func TestDeleteActivityExecution_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().
		GetNamespaceID(namespace.Name("test-namespace")).
		Return(namespace.ID("test-namespace-id"), nil)

	var capturedReq *activitypb.DeleteActivityExecutionRequest
	client := &stubActivityServiceClient{
		deleteActivityExecution: func(_ context.Context, in *activitypb.DeleteActivityExecutionRequest, _ ...grpc.CallOption) (*activitypb.DeleteActivityExecutionResponse, error) {
			capturedReq = in
			return &activitypb.DeleteActivityExecutionResponse{}, nil
		},
	}

	h := &frontendHandler{
		config: &Config{
			Enabled:          func(string) bool { return true },
			MaxIDLengthLimit: func() int { return defaultMaxIDLengthLimit },
		},
		client:            client,
		namespaceRegistry: nsRegistry,
	}

	_, err := h.DeleteActivityExecution(context.Background(), &workflowservice.DeleteActivityExecutionRequest{
		Namespace:  "test-namespace",
		ActivityId: defaultActivityID,
	})
	require.NoError(t, err)
	require.NotNil(t, capturedReq)
	require.Equal(t, "test-namespace-id", capturedReq.GetNamespaceId())
	require.Equal(t, defaultActivityID, capturedReq.GetFrontendRequest().GetActivityId())
}

func TestDeleteActivityExecution_NamespaceNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().
		GetNamespaceID(namespace.Name("test-namespace")).
		Return(namespace.EmptyID, errors.New("namespace not found"))

	h := &frontendHandler{
		config: &Config{
			Enabled:          func(string) bool { return true },
			MaxIDLengthLimit: func() int { return defaultMaxIDLengthLimit },
		},
		namespaceRegistry: nsRegistry,
	}

	_, err := h.DeleteActivityExecution(context.Background(), &workflowservice.DeleteActivityExecutionRequest{
		Namespace:  "test-namespace",
		ActivityId: defaultActivityID,
	})
	require.Error(t, err)
}

func TestDeleteActivityExecution_ValidationError(t *testing.T) {
	h := &frontendHandler{
		config: &Config{
			Enabled:          func(string) bool { return true },
			MaxIDLengthLimit: func() int { return defaultMaxIDLengthLimit },
		},
	}

	_, err := h.DeleteActivityExecution(context.Background(), &workflowservice.DeleteActivityExecutionRequest{
		Namespace: "test-namespace",
		// ActivityId intentionally empty
	})

	var invalidArgErr *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgErr)
	require.Equal(t, "activity ID is required", invalidArgErr.Message)
}
