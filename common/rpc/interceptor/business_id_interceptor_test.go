package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasktoken"
	"google.golang.org/grpc"
)

func TestBusinessIDInterceptor_AllMethods(t *testing.T) {
	extractor := NewBusinessIDExtractor()
	logger := log.NewTestLogger()
	interceptor := NewBusinessIDInterceptor(
		[]BusinessIDExtractorFunc{WorkflowServiceExtractor(extractor)},
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
		expectedBusinessID string
	}{
		// PatternWorkflowID methods (direct WorkflowId field)
		{
			methodName:         "StartWorkflowExecution",
			request:            &workflowservice.StartWorkflowExecutionRequest{WorkflowId: "wf-id"},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "SignalWithStartWorkflowExecution",
			request:            &workflowservice.SignalWithStartWorkflowExecutionRequest{WorkflowId: "wf-id"},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "PauseWorkflowExecution",
			request:            &workflowservice.PauseWorkflowExecutionRequest{WorkflowId: "wf-id"},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "UnpauseWorkflowExecution",
			request:            &workflowservice.UnpauseWorkflowExecutionRequest{WorkflowId: "wf-id"},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "RecordActivityTaskHeartbeatById",
			request:            &workflowservice.RecordActivityTaskHeartbeatByIdRequest{WorkflowId: "wf-id"},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "RespondActivityTaskCompletedById",
			request:            &workflowservice.RespondActivityTaskCompletedByIdRequest{WorkflowId: "wf-id"},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "RespondActivityTaskCanceledById",
			request:            &workflowservice.RespondActivityTaskCanceledByIdRequest{WorkflowId: "wf-id"},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "RespondActivityTaskFailedById",
			request:            &workflowservice.RespondActivityTaskFailedByIdRequest{WorkflowId: "wf-id"},
			expectedBusinessID: "wf-id",
		},

		// PatternWorkflowExecution methods (GetWorkflowExecution().GetWorkflowId())
		{
			methodName:         "DeleteWorkflowExecution",
			request:            &workflowservice.DeleteWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "RequestCancelWorkflowExecution",
			request:            &workflowservice.RequestCancelWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "ResetWorkflowExecution",
			request:            &workflowservice.ResetWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "SignalWorkflowExecution",
			request:            &workflowservice.SignalWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "TerminateWorkflowExecution",
			request:            &workflowservice.TerminateWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "UpdateWorkflowExecution",
			request:            &workflowservice.UpdateWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "UpdateWorkflowExecutionOptions",
			request:            &workflowservice.UpdateWorkflowExecutionOptionsRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},

		// PatternExecution methods (GetExecution().GetWorkflowId())
		{
			methodName:         "DescribeWorkflowExecution",
			request:            &workflowservice.DescribeWorkflowExecutionRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "GetWorkflowExecutionHistory",
			request:            &workflowservice.GetWorkflowExecutionHistoryRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "GetWorkflowExecutionHistoryReverse",
			request:            &workflowservice.GetWorkflowExecutionHistoryReverseRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "QueryWorkflow",
			request:            &workflowservice.QueryWorkflowRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "ResetStickyTaskQueue",
			request:            &workflowservice.ResetStickyTaskQueueRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "ResetActivity",
			request:            &workflowservice.ResetActivityRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "PauseActivity",
			request:            &workflowservice.PauseActivityRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "UnpauseActivity",
			request:            &workflowservice.UnpauseActivityRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "UpdateActivityOptions",
			request:            &workflowservice.UpdateActivityOptionsRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "TriggerWorkflowRule",
			request:            &workflowservice.TriggerWorkflowRuleRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-id"}},
			expectedBusinessID: "wf-id",
		},

		// PatternTaskToken methods (TaskToken deserialization)
		{
			methodName:         "RecordActivityTaskHeartbeat",
			request:            &workflowservice.RecordActivityTaskHeartbeatRequest{TaskToken: createTaskToken("wf-id")},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "RespondActivityTaskCompleted",
			request:            &workflowservice.RespondActivityTaskCompletedRequest{TaskToken: createTaskToken("wf-id")},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "RespondActivityTaskCanceled",
			request:            &workflowservice.RespondActivityTaskCanceledRequest{TaskToken: createTaskToken("wf-id")},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "RespondActivityTaskFailed",
			request:            &workflowservice.RespondActivityTaskFailedRequest{TaskToken: createTaskToken("wf-id")},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "RespondWorkflowTaskCompleted",
			request:            &workflowservice.RespondWorkflowTaskCompletedRequest{TaskToken: createTaskToken("wf-id")},
			expectedBusinessID: "wf-id",
		},
		{
			methodName:         "RespondWorkflowTaskFailed",
			request:            &workflowservice.RespondWorkflowTaskFailedRequest{TaskToken: createTaskToken("wf-id")},
			expectedBusinessID: "wf-id",
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
			expectedBusinessID: "wf-id",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.methodName, func(t *testing.T) {
			var capturedBusinessID string
			handler := func(ctx context.Context, req any) (any, error) {
				capturedBusinessID = GetBusinessIDFromContext(ctx)
				return nil, nil
			}

			info := &grpc.UnaryServerInfo{
				FullMethod: api.WorkflowServicePrefix + tc.methodName,
			}

			_, err := interceptor.Intercept(context.Background(), tc.request, info, handler)
			require.NoError(t, err)
			require.Equal(t, tc.expectedBusinessID, capturedBusinessID)
		})
	}

	// Verify we tested all methods in methodToPattern
	require.Len(t, testCases, len(methodToPattern), "test cases should cover all methods in methodToPattern")
}

func TestBusinessIDInterceptor_SkipsNonWorkflowServiceAndUnmappedMethods(t *testing.T) {
	extractor := NewBusinessIDExtractor()
	logger := log.NewTestLogger()
	interceptor := NewBusinessIDInterceptor(
		[]BusinessIDExtractorFunc{WorkflowServiceExtractor(extractor)},
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
			var capturedBusinessID string
			handler := func(ctx context.Context, req any) (any, error) {
				capturedBusinessID = GetBusinessIDFromContext(ctx)
				return nil, nil
			}

			info := &grpc.UnaryServerInfo{FullMethod: tc.fullMethod}
			_, err := interceptor.Intercept(context.Background(), tc.request, info, handler)
			require.NoError(t, err)
			require.Equal(t, namespace.EmptyBusinessID, capturedBusinessID)
		})
	}
}

func TestBusinessIDInterceptor_EdgeCases(t *testing.T) {
	extractor := NewBusinessIDExtractor()
	logger := log.NewTestLogger()
	interceptor := NewBusinessIDInterceptor(
		[]BusinessIDExtractorFunc{WorkflowServiceExtractor(extractor)},
		logger,
	)

	testCases := []struct {
		name               string
		methodName         string
		request            any
		expectedBusinessID string
	}{
		{
			name:               "NilWorkflowExecution",
			methodName:         "TerminateWorkflowExecution",
			request:            &workflowservice.TerminateWorkflowExecutionRequest{WorkflowExecution: nil},
			expectedBusinessID: namespace.EmptyBusinessID,
		},
		{
			name:               "InvalidTaskToken",
			methodName:         "RespondActivityTaskCompleted",
			request:            &workflowservice.RespondActivityTaskCompletedRequest{TaskToken: []byte("invalid")},
			expectedBusinessID: namespace.EmptyBusinessID,
		},
		{
			name:               "EmptyMultiOperations",
			methodName:         "ExecuteMultiOperation",
			request:            &workflowservice.ExecuteMultiOperationRequest{Operations: nil},
			expectedBusinessID: namespace.EmptyBusinessID,
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
			expectedBusinessID: "wf-update",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var capturedBusinessID string
			handler := func(ctx context.Context, req any) (any, error) {
				capturedBusinessID = GetBusinessIDFromContext(ctx)
				return nil, nil
			}

			info := &grpc.UnaryServerInfo{
				FullMethod: api.WorkflowServicePrefix + tc.methodName,
			}

			_, err := interceptor.Intercept(context.Background(), tc.request, info, handler)
			require.NoError(t, err)
			require.Equal(t, tc.expectedBusinessID, capturedBusinessID)
		})
	}
}

func TestBusinessIDContext(t *testing.T) {
	t.Run("RoundTrip", func(t *testing.T) {
		ctx := AddBusinessIDToContext(context.Background(), "test-business-id")
		require.Equal(t, "test-business-id", GetBusinessIDFromContext(ctx))
	})

	t.Run("MissingReturnsEmptyBusinessID", func(t *testing.T) {
		require.Equal(t, namespace.EmptyBusinessID, GetBusinessIDFromContext(context.Background()))
	})
}

func TestBusinessIDInterceptor_MultipleExtractors(t *testing.T) {
	logger := log.NewTestLogger()

	// Create two custom extractors
	customExtractor1 := func(_ context.Context, req any, fullMethod string) string {
		if fullMethod == "/custom.service/Method1" {
			return "extractor1-result"
		}
		return ""
	}
	customExtractor2 := func(_ context.Context, req any, fullMethod string) string {
		if fullMethod == "/custom.service/Method2" {
			return "extractor2-result"
		}
		return ""
	}

	interceptor := NewBusinessIDInterceptor(
		[]BusinessIDExtractorFunc{customExtractor1, customExtractor2},
		logger,
	)

	testCases := []struct {
		name               string
		fullMethod         string
		expectedBusinessID string
	}{
		{
			name:               "FirstExtractorMatches",
			fullMethod:         "/custom.service/Method1",
			expectedBusinessID: "extractor1-result",
		},
		{
			name:               "SecondExtractorMatches",
			fullMethod:         "/custom.service/Method2",
			expectedBusinessID: "extractor2-result",
		},
		{
			name:               "NoExtractorMatches",
			fullMethod:         "/custom.service/Method3",
			expectedBusinessID: namespace.EmptyBusinessID,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var capturedBusinessID string
			handler := func(ctx context.Context, req any) (any, error) {
				capturedBusinessID = GetBusinessIDFromContext(ctx)
				return nil, nil
			}

			info := &grpc.UnaryServerInfo{FullMethod: tc.fullMethod}
			_, err := interceptor.Intercept(context.Background(), nil, info, handler)
			require.NoError(t, err)
			require.Equal(t, tc.expectedBusinessID, capturedBusinessID)
		})
	}
}

func TestBusinessIDInterceptor_WithExtractors(t *testing.T) {
	logger := log.NewTestLogger()

	// Original extractor
	originalExtractor := func(_ context.Context, req any, fullMethod string) string {
		if fullMethod == "/original/Method" {
			return "original-result"
		}
		return ""
	}

	// New extractor to prepend
	newExtractor := func(_ context.Context, req any, fullMethod string) string {
		if fullMethod == "/new/Method" {
			return "new-result"
		}
		return ""
	}

	// Create interceptor with original extractor
	originalInterceptor := NewBusinessIDInterceptor(
		[]BusinessIDExtractorFunc{originalExtractor},
		logger,
	)

	// Add new extractor using WithExtractors
	extendedInterceptor := originalInterceptor.WithExtractors(newExtractor)

	t.Run("NewExtractorMatchesFirst", func(t *testing.T) {
		var capturedBusinessID string
		handler := func(ctx context.Context, req any) (any, error) {
			capturedBusinessID = GetBusinessIDFromContext(ctx)
			return nil, nil
		}

		info := &grpc.UnaryServerInfo{FullMethod: "/new/Method"}
		_, err := extendedInterceptor.Intercept(context.Background(), nil, info, handler)
		require.NoError(t, err)
		require.Equal(t, "new-result", capturedBusinessID)
	})

	t.Run("OriginalExtractorStillWorks", func(t *testing.T) {
		var capturedBusinessID string
		handler := func(ctx context.Context, req any) (any, error) {
			capturedBusinessID = GetBusinessIDFromContext(ctx)
			return nil, nil
		}

		info := &grpc.UnaryServerInfo{FullMethod: "/original/Method"}
		_, err := extendedInterceptor.Intercept(context.Background(), nil, info, handler)
		require.NoError(t, err)
		require.Equal(t, "original-result", capturedBusinessID)
	})

	t.Run("OriginalInterceptorUnchanged", func(t *testing.T) {
		var capturedBusinessID string
		handler := func(ctx context.Context, req any) (any, error) {
			capturedBusinessID = GetBusinessIDFromContext(ctx)
			return nil, nil
		}

		// Original interceptor should not have the new extractor
		info := &grpc.UnaryServerInfo{FullMethod: "/new/Method"}
		_, err := originalInterceptor.Intercept(context.Background(), nil, info, handler)
		require.NoError(t, err)
		require.Equal(t, namespace.EmptyBusinessID, capturedBusinessID)
	})
}

func TestBusinessIDInterceptor_FirstMatchingExtractorWins(t *testing.T) {
	logger := log.NewTestLogger()

	// Both extractors match the same method, but first should win
	extractor1 := func(_ context.Context, req any, fullMethod string) string {
		if fullMethod == "/test/Method" {
			return "first-wins"
		}
		return ""
	}
	extractor2 := func(_ context.Context, req any, fullMethod string) string {
		if fullMethod == "/test/Method" {
			return "second-loses"
		}
		return ""
	}

	interceptor := NewBusinessIDInterceptor(
		[]BusinessIDExtractorFunc{extractor1, extractor2},
		logger,
	)

	var capturedBusinessID string
	handler := func(ctx context.Context, req any) (any, error) {
		capturedBusinessID = GetBusinessIDFromContext(ctx)
		return nil, nil
	}

	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}
	_, err := interceptor.Intercept(context.Background(), nil, info, handler)
	require.NoError(t, err)
	require.Equal(t, "first-wins", capturedBusinessID)
}

func TestMethodToPatternMapping(t *testing.T) {
	expectedMappings := map[string]BusinessIDPattern{
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
	}

	require.Equal(t, expectedMappings, methodToPattern)
}
