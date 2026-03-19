package tests

import (
	"cmp"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

var nexusStandaloneOpts = []testcore.TestOption{
	testcore.WithDedicatedCluster(),
	testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
	testcore.WithDynamicConfig(nexusoperation.Enabled, true),
}

func TestStandaloneNexusOperation(t *testing.T) {
	t.Parallel()

	t.Run("StartAndDescribe", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		testInput := payload.EncodeString("test-input")
		testHeader := map[string]string{"test-key": "test-value"}
		testUserMetadata := &sdkpb.UserMetadata{
			Summary: payload.EncodeString("test-summary"),
			Details: payload.EncodeString("test-details"),
		}
		testSearchAttributes := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": payload.EncodeString("test-value"),
			},
		}
		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:      "test-op",
			Endpoint:         endpointName,
			Input:            testInput,
			NexusHeader:      testHeader,
			UserMetadata:     testUserMetadata,
			SearchAttributes: testSearchAttributes,
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		// Describe without IncludeInput.
		descResp, err := s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		s.Equal(startResp.RunId, descResp.RunId)
		s.Nil(descResp.GetInput()) // not included by default

		info := descResp.GetInfo()
		protorequire.ProtoEqual(t, &nexuspb.NexusOperationExecutionInfo{
			OperationId:            "test-op",
			RunId:                  startResp.RunId,
			Endpoint:               endpointName,
			Service:                "test-service",
			Operation:              "test-operation",
			Status:                 enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
			State:                  enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED,
			ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
			NexusHeader:            testHeader,
			UserMetadata:           testUserMetadata,
			SearchAttributes:       testSearchAttributes,
			Attempt:                0,
			StateTransitionCount:   1,
			// Dynamic fields copied from actual response for comparison.
			RequestId:         info.GetRequestId(),
			ScheduleTime:      info.GetScheduleTime(),
			ExpirationTime:    info.GetExpirationTime(),
			ExecutionDuration: info.GetExecutionDuration(),
		}, info)
		s.NotEmpty(info.GetRequestId())
		s.NotNil(info.GetScheduleTime())
		s.NotNil(info.GetExpirationTime())
		s.NotNil(info.GetExecutionDuration())

		// Describe with IncludeInput.
		descResp, err = s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:    s.Namespace().String(),
			OperationId:  "test-op",
			RunId:        startResp.RunId,
			IncludeInput: true,
		})
		s.NoError(err)
		protorequire.ProtoEqual(t, testInput, descResp.GetInput())
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	t.Run("StartValidation", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "", // required field
		})
		s.Error(err)
		s.Contains(err.Error(), "operation_id is required")
	})

	t.Run("DescribeNotFound", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "does-not-exist",
		})
		var notFound *serviceerror.NotFound
		s.ErrorAs(err, &notFound)
	})

	t.Run("DescribeWrongRunId", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		_, err = s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       "00000000-0000-0000-0000-000000000000",
		})
		var notFound *serviceerror.NotFound
		s.ErrorAs(err, &notFound)
	})

	t.Run("IDConflictPolicy_Fail", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		resp1, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Second start with different request ID should fail.
		_, err = startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
			RequestId:   "different-request-id",
		})
		s.Error(err)

		// Second start with same request ID should return existing run.
		resp2, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)
		s.Equal(resp1.RunId, resp2.RunId)
		s.False(resp2.GetStarted())
	})

	t.Run("IDConflictPolicy_UseExisting", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		resp1, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		resp2, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:      "test-op",
			Endpoint:         endpointName,
			RequestId:        "different-request-id",
			IdConflictPolicy: enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
		})
		s.NoError(err)
		s.Equal(resp1.RunId, resp2.RunId)
		s.False(resp2.GetStarted())
	})
}

func createNexusEndpoint(s *testcore.TestEnv) string {
	name := testcore.RandomizedNexusEndpoint(s.T().Name())
	_, err := s.OperatorClient().CreateNexusEndpoint(s.Context(), &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: name,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: s.Namespace().String(),
						TaskQueue: "unused-for-test",
					},
				},
			},
		},
	})
	s.NoError(err)
	return name
}

func TestStandaloneNexusOperationList(t *testing.T) {
	t.Parallel()

	t.Run("ListAndVerifyFields", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "list-test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "OperationId = 'list-test-op'",
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		op := listResp.GetOperations()[0]
		protorequire.ProtoEqual(t, &nexuspb.NexusOperationExecutionListInfo{
			OperationId:          "list-test-op",
			RunId:                startResp.RunId,
			Endpoint:             endpointName,
			Service:              "test-service",
			Operation:            "test-operation",
			Status:               enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
			StateTransitionCount: op.GetStateTransitionCount(),
			StateSizeBytes:       op.GetStateSizeBytes(),
			SearchAttributes:     op.GetSearchAttributes(),
			// Dynamic fields copied from actual response for comparison.
			ScheduleTime: op.GetScheduleTime(),
		}, op)
		require.NotNil(t, op.GetScheduleTime())
	})

	t.Run("ListWithQueryFilter", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointA := createNexusEndpoint(s)
		endpointB := createNexusEndpoint(s)

		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "filter-op-1",
			Endpoint:    endpointA,
		})
		s.NoError(err)

		_, err = startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "filter-op-2",
			Endpoint:    endpointB,
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s'", endpointA),
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		require.Equal(t, "filter-op-1", listResp.GetOperations()[0].GetOperationId())
	})

	t.Run("ListWithCustomSearchAttributes", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		testSA := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": payload.EncodeString("list-sa-value"),
			},
		}
		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:      "sa-op",
			Endpoint:         endpointName,
			SearchAttributes: testSA,
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "CustomKeywordField = 'list-sa-value'",
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		require.Equal(t, "sa-op", listResp.GetOperations()[0].GetOperationId())
		returnedSA := listResp.GetOperations()[0].GetSearchAttributes().GetIndexedFields()["CustomKeywordField"]
		require.NotNil(t, returnedSA)
		var returnedValue string
		require.NoError(t, payload.Decode(returnedSA, &returnedValue))
		require.Equal(t, "list-sa-value", returnedValue)
	})

	t.Run("QueryByExecutionStatus", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "status-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "ExecutionStatus = 'Running' AND OperationId = 'status-op'",
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		require.Equal(t, "status-op", listResp.GetOperations()[0].GetOperationId())
	})

	t.Run("QueryByMultipleFields", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "multi-op",
			Endpoint:    endpointName,
			Service:     "multi-service",
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s' AND Service = 'multi-service'", endpointName),
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		require.Equal(t, "multi-op", listResp.GetOperations()[0].GetOperationId())
	})

	t.Run("PageSizeCapping", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		for i := range 2 {
			_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId: fmt.Sprintf("paged-op-%d", i),
				Endpoint:    endpointName,
			})
			s.NoError(err)
		}

		// Wait for both to be indexed.
		query := fmt.Sprintf("Endpoint = '%s'", endpointName)
		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     query,
			})
			require.NoError(t, err)
			require.Len(t, resp.GetOperations(), 2)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)

		// Override max page size to 1.
		cleanup := s.OverrideDynamicConfig(dynamicconfig.FrontendVisibilityMaxPageSize, 1)
		defer cleanup()

		// PageSize 0 should default to max (1), returning only 1 result.
		resp, err := s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  0,
			Query:     query,
		})
		require.NoError(t, err)
		require.Len(t, resp.GetOperations(), 1)

		// PageSize > max should also be capped. First page.
		resp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  2,
			Query:     query,
		})
		require.NoError(t, err)
		require.Len(t, resp.GetOperations(), 1)

		// Second page.
		resp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace:     s.Namespace().String(),
			PageSize:      2,
			Query:         query,
			NextPageToken: resp.GetNextPageToken(),
		})
		require.NoError(t, err)
		require.Len(t, resp.GetOperations(), 1)

		// No more results.
		resp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace:     s.Namespace().String(),
			PageSize:      2,
			Query:         query,
			NextPageToken: resp.GetNextPageToken(),
		})
		require.NoError(t, err)
		require.Empty(t, resp.GetOperations())
		require.Nil(t, resp.GetNextPageToken())
	})

	t.Run("InvalidQuery", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "invalid query syntax !!!",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "invalid query")
	})

	t.Run("InvalidSearchAttribute", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "NonExistentField = 'value'",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "NonExistentField")
	})

	t.Run("NamespaceNotFound", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: "non-existent-namespace",
		})
		s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))
		s.ErrorContains(err, "non-existent-namespace")
	})
}

func TestStandaloneNexusOperationCount(t *testing.T) {
	t.Parallel()

	t.Run("CountByOperationId", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "count-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := s.FrontendClient().CountNexusOperationExecutions(s.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "OperationId = 'count-op'",
			})
			require.NoError(t, err)
			require.Equal(t, int64(1), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	t.Run("CountByEndpoint", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		for i := range 3 {
			_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId: fmt.Sprintf("count-ep-op-%d", i),
				Endpoint:    endpointName,
			})
			s.NoError(err)
		}

		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := s.FrontendClient().CountNexusOperationExecutions(s.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s'", endpointName),
			})
			require.NoError(t, err)
			require.Equal(t, int64(3), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	t.Run("CountByExecutionStatus", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "count-status-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := s.FrontendClient().CountNexusOperationExecutions(s.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("ExecutionStatus = 'Running' AND Endpoint = '%s'", endpointName),
			})
			require.NoError(t, err)
			require.Equal(t, int64(1), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	t.Run("GroupByExecutionStatus", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		for i := range 3 {
			_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId: fmt.Sprintf("group-op-%d", i),
				Endpoint:    endpointName,
			})
			s.NoError(err)
		}

		var countResp *workflowservice.CountNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			countResp, err = s.FrontendClient().CountNexusOperationExecutions(s.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s' GROUP BY ExecutionStatus", endpointName),
			})
			require.NoError(t, err)
			require.Equal(t, int64(3), countResp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		require.Len(t, countResp.GetGroups(), 1)
		require.Equal(t, int64(3), countResp.GetGroups()[0].GetCount())
		var groupValue string
		require.NoError(t, payload.Decode(countResp.GetGroups()[0].GetGroupValues()[0], &groupValue))
		require.Equal(t, "Running", groupValue)
	})

	t.Run("GroupByUnsupportedField", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().CountNexusOperationExecutions(s.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "GROUP BY Endpoint",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "'GROUP BY' clause is only supported for ExecutionStatus")
	})
}

func startNexusOperation(
	s *testcore.TestEnv,
	req *workflowservice.StartNexusOperationExecutionRequest,
) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	req.Namespace = cmp.Or(req.Namespace, s.Namespace().String())
	req.Service = cmp.Or(req.Service, "test-service")
	req.Operation = cmp.Or(req.Operation, "test-operation")
	req.RequestId = cmp.Or(req.RequestId, s.Tv().RequestID())
	if req.ScheduleToCloseTimeout == nil {
		req.ScheduleToCloseTimeout = durationpb.New(10 * time.Minute)
	}
	return s.FrontendClient().StartNexusOperationExecution(s.Context(), req)
}
