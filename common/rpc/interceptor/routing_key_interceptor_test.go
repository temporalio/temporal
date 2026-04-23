package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasktoken"
	"google.golang.org/grpc"
)

func TestRoutingKeyInterceptor_AllMethods(t *testing.T) {
	extractor := NewRoutingKeyExtractor()
	logger := log.NewTestLogger()
	interceptor := NewRoutingKeyInterceptor(
		[]RoutingKeyExtractorFunc{WorkflowServiceExtractor(extractor)},
		logger,
	)

	serializer := tasktoken.NewSerializer()
	createTaskToken := func(workflowID string) []byte {
		taskToken := &tokenspb.Task{WorkflowId: workflowID}
		tokenBytes, err := serializer.Serialize(taskToken)
		require.NoError(t, err)
		return tokenBytes
	}

	// Test all methods in methodToPattern
	testCases := []struct {
		methodName         string
		request            any
		expectedRoutingKey namespace.RoutingKey
	}{
		// PatternWorkflowID methods (direct WorkflowId field)
		{
			methodName:         "StartWorkflowExecution",
			request:            &workflowservice.StartWorkflowExecutionRequest{WorkflowId: "wf-id"},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "SignalWithStartWorkflowExecution",
			request:            &workflowservice.SignalWithStartWorkflowExecutionRequest{WorkflowId: "wf-id"},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "PauseWorkflowExecution",
			request:            &workflowservice.PauseWorkflowExecutionRequest{WorkflowId: "wf-id"},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "UnpauseWorkflowExecution",
			request:            &workflowservice.UnpauseWorkflowExecutionRequest{WorkflowId: "wf-id"},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "RecordActivityTaskHeartbeatById",
			request:            &workflowservice.RecordActivityTaskHeartbeatByIdRequest{WorkflowId: "wf-id"},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "RespondActivityTaskCompletedById",
			request:            &workflowservice.RespondActivityTaskCompletedByIdRequest{WorkflowId: "wf-id"},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "RespondActivityTaskCanceledById",
			request:            &workflowservice.RespondActivityTaskCanceledByIdRequest{WorkflowId: "wf-id"},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "RespondActivityTaskFailedById",
			request:            &workflowservice.RespondActivityTaskFailedByIdRequest{WorkflowId: "wf-id"},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},

		// PatternWorkflowExecution methods (GetWorkflowExecution().GetWorkflowId())
		{
			methodName:         "DeleteWorkflowExecution",
			request:            &workflowservice.DeleteWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "RequestCancelWorkflowExecution",
			request:            &workflowservice.RequestCancelWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "ResetWorkflowExecution",
			request:            &workflowservice.ResetWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "SignalWorkflowExecution",
			request:            &workflowservice.SignalWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "TerminateWorkflowExecution",
			request:            &workflowservice.TerminateWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "UpdateWorkflowExecution",
			request:            &workflowservice.UpdateWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "UpdateWorkflowExecutionOptions",
			request:            &workflowservice.UpdateWorkflowExecutionOptionsRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},

		// PatternExecution methods (GetExecution().GetWorkflowId())
		{
			methodName:         "DescribeWorkflowExecution",
			request:            &workflowservice.DescribeWorkflowExecutionRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "GetWorkflowExecutionHistory",
			request:            &workflowservice.GetWorkflowExecutionHistoryRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "GetWorkflowExecutionHistoryReverse",
			request:            &workflowservice.GetWorkflowExecutionHistoryReverseRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "QueryWorkflow",
			request:            &workflowservice.QueryWorkflowRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "ResetStickyTaskQueue",
			request:            &workflowservice.ResetStickyTaskQueueRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "ResetActivity",
			request:            &workflowservice.ResetActivityRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "PauseActivity",
			request:            &workflowservice.PauseActivityRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "UnpauseActivity",
			request:            &workflowservice.UnpauseActivityRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "UpdateActivityOptions",
			request:            &workflowservice.UpdateActivityOptionsRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "TriggerWorkflowRule",
			request:            &workflowservice.TriggerWorkflowRuleRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},

		// PatternTaskToken methods (TaskToken deserialization)
		{
			methodName:         "RecordActivityTaskHeartbeat",
			request:            &workflowservice.RecordActivityTaskHeartbeatRequest{TaskToken: createTaskToken("wf-id")},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "RespondActivityTaskCompleted",
			request:            &workflowservice.RespondActivityTaskCompletedRequest{TaskToken: createTaskToken("wf-id")},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "RespondActivityTaskCanceled",
			request:            &workflowservice.RespondActivityTaskCanceledRequest{TaskToken: createTaskToken("wf-id")},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "RespondActivityTaskFailed",
			request:            &workflowservice.RespondActivityTaskFailedRequest{TaskToken: createTaskToken("wf-id")},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "RespondWorkflowTaskCompleted",
			request:            &workflowservice.RespondWorkflowTaskCompletedRequest{TaskToken: createTaskToken("wf-id")},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},
		{
			methodName:         "RespondWorkflowTaskFailed",
			request:            &workflowservice.RespondWorkflowTaskFailedRequest{TaskToken: createTaskToken("wf-id")},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},

		// PatternMultiOperation
		{
			methodName: "ExecuteMultiOperation",
			request: &workflowservice.ExecuteMultiOperationRequest{
				Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
					{
						Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
							StartWorkflow: &workflowservice.StartWorkflowExecutionRequest{WorkflowId: "wf-id"},
						},
					},
				},
			},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-id"},
		},

		// task queue name
		{
			methodName:         "UpdateTaskQueueConfig",
			request:            &workflowservice.UpdateTaskQueueConfigRequest{TaskQueue: "test-task-queue"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-task-queue"},
		},

		// task queue name (from TaskQueue message)
		{
			methodName:         "ListTaskQueuePartitions",
			request:            &workflowservice.ListTaskQueuePartitionsRequest{TaskQueue: &taskqueuepb.TaskQueue{Name: "test-task-queue"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-task-queue"},
		},

		// deployment name
		{
			methodName:         "DescribeWorkerDeployment",
			request:            &workflowservice.DescribeWorkerDeploymentRequest{DeploymentName: "test-deployment"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-deployment"},
		},
		{
			methodName:         "DeleteWorkerDeployment",
			request:            &workflowservice.DeleteWorkerDeploymentRequest{DeploymentName: "test-deployment"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-deployment"},
		},
		{
			methodName:         "SetWorkerDeploymentCurrentVersion",
			request:            &workflowservice.SetWorkerDeploymentCurrentVersionRequest{DeploymentName: "test-deployment"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-deployment"},
		},
		{
			methodName:         "SetWorkerDeploymentManager",
			request:            &workflowservice.SetWorkerDeploymentManagerRequest{DeploymentName: "test-deployment"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-deployment"},
		},
		{
			methodName:         "SetWorkerDeploymentRampingVersion",
			request:            &workflowservice.SetWorkerDeploymentRampingVersionRequest{DeploymentName: "test-deployment"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-deployment"},
		},

		// deployment name (from WorkerDeploymentVersion message)
		{
			methodName:         "DescribeWorkerDeploymentVersion",
			request:            &workflowservice.DescribeWorkerDeploymentVersionRequest{DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "test-deployment"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-deployment"},
		},
		{
			methodName:         "DeleteWorkerDeploymentVersion",
			request:            &workflowservice.DeleteWorkerDeploymentVersionRequest{DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "test-deployment"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-deployment"},
		},
		{
			methodName:         "UpdateWorkerDeploymentVersionMetadata",
			request:            &workflowservice.UpdateWorkerDeploymentVersionMetadataRequest{DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "test-deployment"}},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-deployment"},
		},

		// namespace
		{
			methodName:         "FetchWorkerConfig",
			request:            &workflowservice.FetchWorkerConfigRequest{Namespace: "test-namespace"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-namespace"},
		},
		{
			methodName:         "UpdateWorkerConfig",
			request:            &workflowservice.UpdateWorkerConfigRequest{Namespace: "test-namespace"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-namespace"},
		},
		{
			methodName:         "DescribeWorker",
			request:            &workflowservice.DescribeWorkerRequest{Namespace: "test-namespace"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-namespace"},
		},
		{
			methodName:         "RecordWorkerHeartbeat",
			request:            &workflowservice.RecordWorkerHeartbeatRequest{Namespace: "test-namespace"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-namespace"},
		},
		// workflow ID (from UpdateRef)
		{
			methodName: "PollWorkflowExecutionUpdate",
			request: &workflowservice.PollWorkflowExecutionUpdateRequest{
				UpdateRef: &updatepb.UpdateRef{
					WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "test-workflow-id"},
				},
			},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-workflow-id"},
		},

		// PatternPollerGroupID methods (GetPollerGroupId())
		{
			methodName:         "PollWorkflowTaskQueue",
			request:            &workflowservice.PollWorkflowTaskQueueRequest{PollerGroupId: "test-poller-group-id"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-poller-group-id", Strategy: namespace.RoutingStrategyPollerGroup},
		},
		{
			methodName:         "PollActivityTaskQueue",
			request:            &workflowservice.PollActivityTaskQueueRequest{PollerGroupId: "test-poller-group-id"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-poller-group-id", Strategy: namespace.RoutingStrategyPollerGroup},
		},
		{
			methodName:         "PollNexusTaskQueue",
			request:            &workflowservice.PollNexusTaskQueueRequest{PollerGroupId: "test-poller-group-id"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-poller-group-id", Strategy: namespace.RoutingStrategyPollerGroup},
		},
		{
			methodName:         "RespondQueryTaskCompleted",
			request:            &workflowservice.RespondQueryTaskCompletedRequest{PollerGroupId: "test-poller-group-id"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-poller-group-id", Strategy: namespace.RoutingStrategyPollerGroup},
		},
		{
			methodName:         "RespondNexusTaskCompleted",
			request:            &workflowservice.RespondNexusTaskCompletedRequest{PollerGroupId: "test-poller-group-id"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-poller-group-id", Strategy: namespace.RoutingStrategyPollerGroup},
		},
		{
			methodName:         "RespondNexusTaskFailed",
			request:            &workflowservice.RespondNexusTaskFailedRequest{PollerGroupId: "test-poller-group-id"},
			expectedRoutingKey: namespace.RoutingKey{ID: "test-poller-group-id", Strategy: namespace.RoutingStrategyPollerGroup},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.methodName, func(t *testing.T) {
			capturedRoutingKey := namespace.RoutingKey{}
			handler := func(ctx context.Context, req any) (any, error) {
				capturedRoutingKey = GetRoutingKeyFromContext(ctx)
				return nil, nil
			}

			info := &grpc.UnaryServerInfo{
				FullMethod: api.WorkflowServicePrefix + tc.methodName,
			}

			_, err := interceptor.Intercept(context.Background(), tc.request, info, handler)
			require.NoError(t, err)
			require.Equal(t, tc.expectedRoutingKey, capturedRoutingKey)
		})
	}

	// Verify we tested all methods in methodToPattern
	require.Len(t, testCases, len(methodToPattern), "test cases should cover all methods in methodToPattern")
}

func TestRoutingKeyInterceptor_SkipsNonWorkflowServiceAndUnmappedMethods(t *testing.T) {
	extractor := NewRoutingKeyExtractor()
	logger := log.NewTestLogger()
	interceptor := NewRoutingKeyInterceptor(
		[]RoutingKeyExtractorFunc{WorkflowServiceExtractor(extractor)},
		logger,
	)

	testCases := []struct {
		name       string
		fullMethod string
		request    any
	}{
		{
			name:       "NonWorkflowServiceAPI",
			fullMethod: "/temporal.api.operatorservice.v1.OperatorService/AddSearchAttributes",
			request:    &workflowservice.StartWorkflowExecutionRequest{WorkflowId: "should-not-extract"},
		},
		{
			name:       "UnmappedMethod",
			fullMethod: api.WorkflowServicePrefix + "ListNamespaces",
			request:    &workflowservice.ListNamespacesRequest{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			capturedRoutingKey := namespace.RoutingKey{}
			handler := func(ctx context.Context, req any) (any, error) {
				capturedRoutingKey = GetRoutingKeyFromContext(ctx)
				return nil, nil
			}

			info := &grpc.UnaryServerInfo{FullMethod: tc.fullMethod}
			_, err := interceptor.Intercept(context.Background(), tc.request, info, handler)
			require.NoError(t, err)
			require.Empty(t, capturedRoutingKey)
		})
	}
}

func TestRoutingKeyInterceptor_EdgeCases(t *testing.T) {
	extractor := NewRoutingKeyExtractor()
	logger := log.NewTestLogger()
	interceptor := NewRoutingKeyInterceptor(
		[]RoutingKeyExtractorFunc{WorkflowServiceExtractor(extractor)},
		logger,
	)

	testCases := []struct {
		name               string
		methodName         string
		request            any
		expectedRoutingKey namespace.RoutingKey
	}{
		{
			name:               "NilWorkflowExecution",
			methodName:         "TerminateWorkflowExecution",
			request:            &workflowservice.TerminateWorkflowExecutionRequest{WorkflowExecution: nil},
			expectedRoutingKey: namespace.RoutingKey{},
		},
		{
			name:               "InvalidTaskToken",
			methodName:         "RespondActivityTaskCompleted",
			request:            &workflowservice.RespondActivityTaskCompletedRequest{TaskToken: []byte("invalid")},
			expectedRoutingKey: namespace.RoutingKey{},
		},
		{
			name:               "EmptyMultiOperations",
			methodName:         "ExecuteMultiOperation",
			request:            &workflowservice.ExecuteMultiOperationRequest{Operations: nil},
			expectedRoutingKey: namespace.RoutingKey{},
		},
		{
			name:       "MultiOperation_UpdateWorkflowFallback",
			methodName: "ExecuteMultiOperation",
			request: &workflowservice.ExecuteMultiOperationRequest{
				Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
					{
						Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
							UpdateWorkflow: &workflowservice.UpdateWorkflowExecutionRequest{
								WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-update"},
								Request:           &updatepb.Request{},
							},
						},
					},
				},
			},
			expectedRoutingKey: namespace.RoutingKey{ID: "wf-update"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			capturedRoutingKey := namespace.RoutingKey{}
			handler := func(ctx context.Context, req any) (any, error) {
				capturedRoutingKey = GetRoutingKeyFromContext(ctx)
				return nil, nil
			}

			info := &grpc.UnaryServerInfo{
				FullMethod: api.WorkflowServicePrefix + tc.methodName,
			}

			_, err := interceptor.Intercept(context.Background(), tc.request, info, handler)
			require.NoError(t, err)
			require.Equal(t, tc.expectedRoutingKey, capturedRoutingKey)
		})
	}
}

func TestRoutingKeyContext(t *testing.T) {
	t.Run("RoundTrip", func(t *testing.T) {
		ctx := AddRoutingKeyToContext(context.Background(), namespace.RoutingKey{ID: "test-business-id"})
		require.Equal(t, namespace.RoutingKey{ID: "test-business-id"}, GetRoutingKeyFromContext(ctx))
	})

	t.Run("MissingReturnsRoutingKey{}", func(t *testing.T) {
		require.Equal(t, namespace.RoutingKey{}, GetRoutingKeyFromContext(context.Background()))
	})
}

func TestRoutingKeyInterceptor_MultipleExtractors(t *testing.T) {
	logger := log.NewTestLogger()

	// Create two custom extractors
	customExtractor1 := func(_ context.Context, req any, fullMethod string) namespace.RoutingKey {
		if fullMethod == "/custom.service/Method1" {
			return namespace.RoutingKey{ID: "extractor1-result"}
		}
		return namespace.RoutingKey{}
	}
	customExtractor2 := func(_ context.Context, req any, fullMethod string) namespace.RoutingKey {
		if fullMethod == "/custom.service/Method2" {
			return namespace.RoutingKey{ID: "extractor2-result"}
		}
		return namespace.RoutingKey{}
	}

	interceptor := NewRoutingKeyInterceptor(
		[]RoutingKeyExtractorFunc{customExtractor1, customExtractor2},
		logger,
	)

	testCases := []struct {
		name               string
		fullMethod         string
		expectedRoutingKey namespace.RoutingKey
	}{
		{
			name:               "FirstExtractorMatches",
			fullMethod:         "/custom.service/Method1",
			expectedRoutingKey: namespace.RoutingKey{ID: "extractor1-result"},
		},
		{
			name:               "SecondExtractorMatches",
			fullMethod:         "/custom.service/Method2",
			expectedRoutingKey: namespace.RoutingKey{ID: "extractor2-result"},
		},
		{
			name:               "NoExtractorMatches",
			fullMethod:         "/custom.service/Method3",
			expectedRoutingKey: namespace.RoutingKey{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			capturedRoutingKey := namespace.RoutingKey{}
			handler := func(ctx context.Context, req any) (any, error) {
				capturedRoutingKey = GetRoutingKeyFromContext(ctx)
				return nil, nil
			}

			info := &grpc.UnaryServerInfo{FullMethod: tc.fullMethod}
			_, err := interceptor.Intercept(context.Background(), nil, info, handler)
			require.NoError(t, err)
			require.Equal(t, tc.expectedRoutingKey, capturedRoutingKey)
		})
	}
}

func TestRoutingKeyInterceptor_WithExtractors(t *testing.T) {
	logger := log.NewTestLogger()

	// Original extractor
	originalExtractor := func(_ context.Context, req any, fullMethod string) namespace.RoutingKey {
		if fullMethod == "/original/Method" {
			return namespace.RoutingKey{ID: "original-result"}
		}
		return namespace.RoutingKey{}
	}

	// New extractor to prepend
	newExtractor := func(_ context.Context, req any, fullMethod string) namespace.RoutingKey {
		if fullMethod == "/new/Method" {
			return namespace.RoutingKey{ID: "new-result"}
		}
		return namespace.RoutingKey{}
	}

	// Create interceptor with original extractor
	originalInterceptor := NewRoutingKeyInterceptor(
		[]RoutingKeyExtractorFunc{originalExtractor},
		logger,
	)

	// Add new extractor using WithExtractors
	extendedInterceptor := originalInterceptor.WithExtractors(newExtractor)

	t.Run("NewExtractorMatchesFirst", func(t *testing.T) {
		capturedRoutingKey := namespace.RoutingKey{}
		handler := func(ctx context.Context, req any) (any, error) {
			capturedRoutingKey = GetRoutingKeyFromContext(ctx)
			return nil, nil
		}

		info := &grpc.UnaryServerInfo{FullMethod: "/new/Method"}
		_, err := extendedInterceptor.Intercept(context.Background(), nil, info, handler)
		require.NoError(t, err)
		require.Equal(t, namespace.RoutingKey{ID: "new-result"}, capturedRoutingKey)
	})

	t.Run("OriginalExtractorStillWorks", func(t *testing.T) {
		capturedRoutingKey := namespace.RoutingKey{}
		handler := func(ctx context.Context, req any) (any, error) {
			capturedRoutingKey = GetRoutingKeyFromContext(ctx)
			return nil, nil
		}

		info := &grpc.UnaryServerInfo{FullMethod: "/original/Method"}
		_, err := extendedInterceptor.Intercept(context.Background(), nil, info, handler)
		require.NoError(t, err)
		require.Equal(t, namespace.RoutingKey{ID: "original-result"}, capturedRoutingKey)
	})

	t.Run("OriginalInterceptorUnchanged", func(t *testing.T) {
		capturedRoutingKey := namespace.RoutingKey{}
		handler := func(ctx context.Context, req any) (any, error) {
			capturedRoutingKey = GetRoutingKeyFromContext(ctx)
			return nil, nil
		}

		// Original interceptor should not have the new extractor
		info := &grpc.UnaryServerInfo{FullMethod: "/new/Method"}
		_, err := originalInterceptor.Intercept(context.Background(), nil, info, handler)
		require.NoError(t, err)
		require.Equal(t, namespace.RoutingKey{}, capturedRoutingKey)
	})
}

func TestRoutingKeyInterceptor_FirstMatchingExtractorWins(t *testing.T) {
	logger := log.NewTestLogger()

	// Both extractors match the same method, but first should win
	extractor1 := func(_ context.Context, req any, fullMethod string) namespace.RoutingKey {
		if fullMethod == "/test/Method" {
			return namespace.RoutingKey{ID: "first-wins"}
		}
		return namespace.RoutingKey{}
	}
	extractor2 := func(_ context.Context, req any, fullMethod string) namespace.RoutingKey {
		if fullMethod == "/test/Method" {
			return namespace.RoutingKey{ID: "second-loses"}
		}
		return namespace.RoutingKey{}
	}

	interceptor := NewRoutingKeyInterceptor(
		[]RoutingKeyExtractorFunc{extractor1, extractor2},
		logger,
	)

	capturedRoutingKey := namespace.RoutingKey{}
	handler := func(ctx context.Context, req any) (any, error) {
		capturedRoutingKey = GetRoutingKeyFromContext(ctx)
		return nil, nil
	}

	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}
	_, err := interceptor.Intercept(context.Background(), nil, info, handler)
	require.NoError(t, err)
	require.Equal(t, namespace.RoutingKey{ID: "first-wins"}, capturedRoutingKey)
}

func TestMethodToPatternMapping(t *testing.T) {
	expectedMappings := map[string]RoutingKeyPattern{
		// PatternWorkflowID
		"StartWorkflowExecution":           PatternWorkflowID,
		"SignalWithStartWorkflowExecution": PatternWorkflowID,
		"PauseWorkflowExecution":           PatternWorkflowID,
		"UnpauseWorkflowExecution":         PatternWorkflowID,
		"RecordActivityTaskHeartbeatById":  PatternWorkflowID,
		"RespondActivityTaskCompletedById": PatternWorkflowID,
		"RespondActivityTaskCanceledById":  PatternWorkflowID,
		"RespondActivityTaskFailedById":    PatternWorkflowID,

		// PatternWorkflowExecution
		"DeleteWorkflowExecution":        PatternWorkflowExecution,
		"RequestCancelWorkflowExecution": PatternWorkflowExecution,
		"ResetWorkflowExecution":         PatternWorkflowExecution,
		"SignalWorkflowExecution":        PatternWorkflowExecution,
		"TerminateWorkflowExecution":     PatternWorkflowExecution,
		"UpdateWorkflowExecution":        PatternWorkflowExecution,
		"UpdateWorkflowExecutionOptions": PatternWorkflowExecution,

		// PatternExecution
		"DescribeWorkflowExecution":          PatternExecution,
		"GetWorkflowExecutionHistory":        PatternExecution,
		"GetWorkflowExecutionHistoryReverse": PatternExecution,
		"QueryWorkflow":                      PatternExecution,
		"ResetStickyTaskQueue":               PatternExecution,
		"ResetActivity":                      PatternExecution,
		"PauseActivity":                      PatternExecution,
		"UnpauseActivity":                    PatternExecution,
		"UpdateActivityOptions":              PatternExecution,
		"TriggerWorkflowRule":                PatternExecution,

		// PatternTaskToken
		"RecordActivityTaskHeartbeat":  PatternTaskToken,
		"RespondActivityTaskCompleted": PatternTaskToken,
		"RespondActivityTaskCanceled":  PatternTaskToken,
		"RespondActivityTaskFailed":    PatternTaskToken,
		"RespondWorkflowTaskCompleted": PatternTaskToken,
		"RespondWorkflowTaskFailed":    PatternTaskToken,

		// PatternMultiOperation
		"ExecuteMultiOperation": PatternMultiOperation,

		// PatternTaskQueueName
		"UpdateTaskQueueConfig": PatternTaskQueueName,

		// PatternTaskQueueNameFromMessage
		"ListTaskQueuePartitions": PatternTaskQueueNameFromMessage,

		// PatternDeploymentName
		"DescribeWorkerDeployment":          PatternDeploymentName,
		"DeleteWorkerDeployment":            PatternDeploymentName,
		"SetWorkerDeploymentCurrentVersion": PatternDeploymentName,
		"SetWorkerDeploymentManager":        PatternDeploymentName,
		"SetWorkerDeploymentRampingVersion": PatternDeploymentName,

		// PatternDeploymentVersion
		"DescribeWorkerDeploymentVersion":       PatternDeploymentVersion,
		"DeleteWorkerDeploymentVersion":         PatternDeploymentVersion,
		"UpdateWorkerDeploymentVersionMetadata": PatternDeploymentVersion,

		// PatternNamespace
		"FetchWorkerConfig":     PatternNamespace,
		"UpdateWorkerConfig":    PatternNamespace,
		"DescribeWorker":        PatternNamespace,
		"RecordWorkerHeartbeat": PatternNamespace,

		"PollWorkflowExecutionUpdate": PatternUpdateRef,

		// PatternPollerGroupID
		"PollWorkflowTaskQueue":     PatternPollerGroupID,
		"PollActivityTaskQueue":     PatternPollerGroupID,
		"PollNexusTaskQueue":        PatternPollerGroupID,
		"RespondQueryTaskCompleted": PatternPollerGroupID,
		"RespondNexusTaskCompleted": PatternPollerGroupID,
		"RespondNexusTaskFailed":    PatternPollerGroupID,
	}

	require.Equal(t, expectedMappings, methodToPattern)
}
