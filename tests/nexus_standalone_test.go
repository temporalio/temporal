package tests

import (
	"cmp"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
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
	testcore.WithDynamicConfig(nexusoperation.ChasmNexusEnabled, true),
}

func TestStandaloneNexusOperation(t *testing.T) {
	t.Parallel()

	t.Run("StartAndDescribe", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

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
			Endpoint:               "test-endpoint",
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

		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
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

		resp1, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
		})
		s.NoError(err)

		// Second start with different request ID should fail.
		_, err = startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			RequestId:   "different-request-id",
		})
		s.Error(err)

		// Second start with same request ID should return existing run.
		resp2, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
		})
		s.NoError(err)
		s.Equal(resp1.RunId, resp2.RunId)
		s.False(resp2.GetStarted())
	})

	t.Run("IDConflictPolicy_UseExisting", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		resp1, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
		})
		s.NoError(err)

		resp2, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:      "test-op",
			RequestId:        "different-request-id",
			IdConflictPolicy: enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
		})
		s.NoError(err)
		s.Equal(resp1.RunId, resp2.RunId)
		s.False(resp2.GetStarted())
	})
}

func startNexusOperation(
	s *testcore.TestEnv,
	req *workflowservice.StartNexusOperationExecutionRequest,
) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	req.Namespace = cmp.Or(req.Namespace, s.Namespace().String())
	req.Endpoint = cmp.Or(req.Endpoint, "test-endpoint")
	req.Service = cmp.Or(req.Service, "test-service")
	req.Operation = cmp.Or(req.Operation, "test-operation")
	req.RequestId = cmp.Or(req.RequestId, s.Tv().RequestID())
	if req.ScheduleToCloseTimeout == nil {
		req.ScheduleToCloseTimeout = durationpb.New(10 * time.Minute)
	}
	return s.FrontendClient().StartNexusOperationExecution(s.Context(), req)
}
