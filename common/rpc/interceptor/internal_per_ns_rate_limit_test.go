package interceptor

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/grpc"
)

const (
	internalPerNSEnabledNamespace = "enabled-namespace"
	scheduleWorkflowID            = primitives.ScheduleWorkflowIDPrefix + "my-schedule"
)

type recordingNamespaceRateLimitInterceptor struct {
	NamespaceRateLimitInterceptor
	numTokens []int
	err       error
}

func (r *recordingNamespaceRateLimitInterceptor) InterceptN(
	ctx context.Context,
	req any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
	numToken int,
) (any, error) {
	r.numTokens = append(r.numTokens, numToken)
	if r.err != nil {
		return nil, r.err
	}
	return handler(ctx, req)
}

func TestInternalPerNSNamespaceRateLimitInterceptorClassification(t *testing.T) {
	t.Parallel()

	serializer := tasktoken.NewSerializer()
	taskToken := func(t *testing.T, workflowID string) []byte {
		token, err := serializer.Serialize(&tokenspb.Task{WorkflowId: workflowID})
		require.NoError(t, err)
		return token
	}
	queryTaskToken := func(t *testing.T, taskQueue string) []byte {
		token, err := serializer.SerializeQueryTaskToken(&tokenspb.QueryTask{TaskQueue: taskQueue})
		require.NoError(t, err)
		return token
	}

	testCases := []struct {
		name         string
		request      func(t *testing.T) any
		wantInternal bool
	}{
		{
			name: "internal workflow task poll",
			request: func(*testing.T) any {
				return &workflowservice.PollWorkflowTaskQueueRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
				}
			},
			wantInternal: true,
		},
		{
			name: "internal sticky workflow task poll",
			request: func(*testing.T) any {
				return &workflowservice.PollWorkflowTaskQueueRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskQueue: &taskqueuepb.TaskQueue{
						Name:       "sticky-queue",
						Kind:       enumspb.TASK_QUEUE_KIND_STICKY,
						NormalName: primitives.PerNSWorkerTaskQueue,
					},
				}
			},
			wantInternal: true,
		},
		{
			name: "internal activity task poll",
			request: func(*testing.T) any {
				return &workflowservice.PollActivityTaskQueueRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
				}
			},
			wantInternal: true,
		},
		{
			name: "customer workflow task poll",
			request: func(*testing.T) any {
				return &workflowservice.PollWorkflowTaskQueueRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskQueue: &taskqueuepb.TaskQueue{Name: "customer-queue"},
				}
			},
		},
		{
			name: "internal poll in namespace without the split",
			request: func(*testing.T) any {
				return &workflowservice.PollWorkflowTaskQueueRequest{
					Namespace: "other-namespace",
					TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
				}
			},
		},
		{
			name: "internal workflow history read",
			request: func(*testing.T) any {
				return &workflowservice.GetWorkflowExecutionHistoryRequest{
					Namespace: internalPerNSEnabledNamespace,
					Execution: &commonpb.WorkflowExecution{WorkflowId: scheduleWorkflowID},
				}
			},
			wantInternal: true,
		},
		{
			name: "customer workflow history read",
			request: func(*testing.T) any {
				return &workflowservice.GetWorkflowExecutionHistoryRequest{
					Namespace: internalPerNSEnabledNamespace,
					Execution: &commonpb.WorkflowExecution{WorkflowId: "customer-workflow"},
				}
			},
		},
		{
			name: "internal workflow task completion",
			request: func(t *testing.T) any {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskToken: taskToken(t, scheduleWorkflowID),
				}
			},
			wantInternal: true,
		},
		{
			name: "customer workflow task completion",
			request: func(t *testing.T) any {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskToken: taskToken(t, "customer-workflow"),
				}
			},
		},
		{
			name: "undecodable workflow task token",
			request: func(*testing.T) any {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskToken: []byte("not-a-token"),
				}
			},
		},
		{
			name: "internal workflow task failure",
			request: func(t *testing.T) any {
				return &workflowservice.RespondWorkflowTaskFailedRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskToken: taskToken(t, scheduleWorkflowID),
				}
			},
			wantInternal: true,
		},
		{
			name: "internal activity heartbeat",
			request: func(t *testing.T) any {
				return &workflowservice.RecordActivityTaskHeartbeatRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskToken: taskToken(t, scheduleWorkflowID),
				}
			},
			wantInternal: true,
		},
		{
			name: "internal activity completion",
			request: func(t *testing.T) any {
				return &workflowservice.RespondActivityTaskCompletedRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskToken: taskToken(t, scheduleWorkflowID),
				}
			},
			wantInternal: true,
		},
		{
			name: "internal activity failure",
			request: func(t *testing.T) any {
				return &workflowservice.RespondActivityTaskFailedRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskToken: taskToken(t, scheduleWorkflowID),
				}
			},
			wantInternal: true,
		},
		{
			name: "internal activity cancellation",
			request: func(t *testing.T) any {
				return &workflowservice.RespondActivityTaskCanceledRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskToken: taskToken(t, scheduleWorkflowID),
				}
			},
			wantInternal: true,
		},
		{
			name: "standalone activity completion",
			request: func(t *testing.T) any {
				return &workflowservice.RespondActivityTaskCompletedRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskToken: taskToken(t, ""),
				}
			},
		},
		{
			name: "internal query task response",
			request: func(t *testing.T) any {
				return &workflowservice.RespondQueryTaskCompletedRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskToken: queryTaskToken(t, primitives.PerNSWorkerTaskQueue),
				}
			},
			wantInternal: true,
		},
		{
			name: "internal query task response from a non-root partition",
			request: func(t *testing.T) any {
				return &workflowservice.RespondQueryTaskCompletedRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskToken: queryTaskToken(t, partitionRPCName(primitives.PerNSWorkerTaskQueue, 3)),
				}
			},
			wantInternal: true,
		},
		{
			name: "customer query task response from a non-root partition",
			request: func(t *testing.T) any {
				return &workflowservice.RespondQueryTaskCompletedRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskToken: queryTaskToken(t, partitionRPCName("customer-queue", 3)),
				}
			},
		},
		{
			name: "sticky query task response",
			request: func(t *testing.T) any {
				return &workflowservice.RespondQueryTaskCompletedRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskToken: queryTaskToken(t, "sticky-queue"),
				}
			},
		},
		{
			name: "customer query task response",
			request: func(t *testing.T) any {
				return &workflowservice.RespondQueryTaskCompletedRequest{
					Namespace: internalPerNSEnabledNamespace,
					TaskToken: queryTaskToken(t, "customer-queue"),
				}
			},
		},
		{
			name: "scheduled start of a customer workflow",
			request: func(*testing.T) any {
				return &workflowservice.StartWorkflowExecutionRequest{
					Namespace:  internalPerNSEnabledNamespace,
					WorkflowId: "scheduled-customer-workflow",
				}
			},
		},
		{
			name: "signal to an internal workflow",
			request: func(*testing.T) any {
				return &workflowservice.SignalWorkflowExecutionRequest{
					Namespace:         internalPerNSEnabledNamespace,
					WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: scheduleWorkflowID},
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			regular := &recordingNamespaceRateLimitInterceptor{}
			internalPerNS := &recordingNamespaceRateLimitInterceptor{}
			i := NewInternalPerNSNamespaceRateLimitInterceptor(
				regular,
				internalPerNS,
				func(ns string) bool { return ns == internalPerNSEnabledNamespace },
			)

			handlerCalls := 0
			_, err := i.Intercept(
				context.Background(),
				tc.request(t),
				&grpc.UnaryServerInfo{},
				func(context.Context, any) (any, error) {
					handlerCalls++
					return nil, nil
				},
			)
			require.NoError(t, err)
			require.Equal(t, 1, handlerCalls)

			want, other := regular, internalPerNS
			if tc.wantInternal {
				want, other = internalPerNS, regular
			}
			require.Equal(t, []int{NamespaceRateLimitDefaultToken}, want.numTokens)
			require.Empty(t, other.numTokens)
		})
	}
}

func TestInternalPerNSNamespaceRateLimitInterceptorPassesTokenCount(t *testing.T) {
	t.Parallel()

	regular := &recordingNamespaceRateLimitInterceptor{}
	internalPerNS := &recordingNamespaceRateLimitInterceptor{}
	i := NewInternalPerNSNamespaceRateLimitInterceptor(
		regular,
		internalPerNS,
		func(string) bool { return true },
	)
	handler := func(context.Context, any) (any, error) { return nil, nil }

	_, err := i.InterceptN(
		context.Background(),
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: internalPerNSEnabledNamespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: scheduleWorkflowID},
		},
		&grpc.UnaryServerInfo{},
		handler,
		3,
	)
	require.NoError(t, err)
	_, err = i.InterceptN(
		context.Background(),
		&workflowservice.StartWorkflowExecutionRequest{Namespace: internalPerNSEnabledNamespace},
		&grpc.UnaryServerInfo{},
		handler,
		5,
	)
	require.NoError(t, err)

	require.Equal(t, []int{3}, internalPerNS.numTokens)
	require.Equal(t, []int{5}, regular.numTokens)
}

func TestInternalPerNSNamespaceRateLimitInterceptorDistinguishesThrottlingBudget(t *testing.T) {
	t.Parallel()

	internalRequest := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: internalPerNSEnabledNamespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: scheduleWorkflowID},
	}
	customerRequest := &workflowservice.StartWorkflowExecutionRequest{Namespace: internalPerNSEnabledNamespace}
	handlerErr := errors.New("handler error")

	testCases := []struct {
		name        string
		request     any
		limiterErr  error
		expectedErr error
	}{
		{
			name:        "internal budget throttles",
			request:     internalRequest,
			limiterErr:  ErrNamespaceRateLimitServerBusy,
			expectedErr: ErrInternalPerNSNamespaceRateLimitServerBusy,
		},
		{
			name:        "regular budget throttles",
			request:     customerRequest,
			limiterErr:  ErrNamespaceRateLimitServerBusy,
			expectedErr: ErrNamespaceRateLimitServerBusy,
		},
		{
			name:        "other internal errors pass through",
			request:     internalRequest,
			limiterErr:  handlerErr,
			expectedErr: handlerErr,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			i := NewInternalPerNSNamespaceRateLimitInterceptor(
				&recordingNamespaceRateLimitInterceptor{err: tc.limiterErr},
				&recordingNamespaceRateLimitInterceptor{err: tc.limiterErr},
				func(string) bool { return true },
			)
			_, err := i.Intercept(
				context.Background(),
				tc.request,
				&grpc.UnaryServerInfo{},
				func(context.Context, any) (any, error) { return nil, nil },
			)
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func partitionRPCName(taskQueue string, partition int) string {
	return tqid.UnsafeTaskQueueFamily("", taskQueue).
		TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).
		NormalPartition(partition).
		RpcName()
}
