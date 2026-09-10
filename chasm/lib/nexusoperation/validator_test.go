package nexusoperation

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/validation"
)

func newTestRegistry(config *Config) *validation.ValidatorRegistry {
	registry := validation.NewValidatorRegistry()
	_ = newDeleteNexusOperationExecutionValidator(config).RegisterValidator(registry)
	_ = newDescribeNexusOperationExecutionValidator(config).RegisterValidator(registry)
	_ = newPollNexusOperationExecutionValidator(config).RegisterValidator(registry)
	_ = newRequestCancelNexusOperationExecutionValidator(config).RegisterValidator(registry)
	_ = newTerminateNexusOperationExecutionValidator(config).RegisterValidator(registry)
	_ = newListNexusOperationExecutionsValidator().RegisterValidator(registry)
	_ = newCountNexusOperationExecutionsValidator().RegisterValidator(registry)
	_ = newStartNexusOperationExecutionValidator(config).RegisterValidator(registry)
	return registry
}

func testConfig() *Config {
	return &Config{
		MaxIDLengthLimit: func() int { return 50 },
	}
}

func TestValidateDeleteNexusOperationExecutionRequest(t *testing.T) {
	registry := newTestRegistry(testConfig())

	for _, tc := range []struct {
		name    string
		req     *workflowservice.DeleteNexusOperationExecutionRequest
		wantErr string
	}{
		{
			name: "valid with run_id",
			req: &workflowservice.DeleteNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
				RunId:       "a7d6f9c2-1234-5678-abcd-ef0123456789",
			},
		},
		{
			name: "valid without run_id",
			req: &workflowservice.DeleteNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
			},
		},
		{
			name:    "missing operation_id",
			req:     &workflowservice.DeleteNexusOperationExecutionRequest{Namespace: "ns"},
			wantErr: "operation_id is required",
		},
		{
			name: "operation_id too long",
			req: &workflowservice.DeleteNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // >50 chars
			},
			wantErr: "operation_id exceeds length limit",
		},
		{
			name: "invalid run_id",
			req: &workflowservice.DeleteNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
				RunId:       "not-a-uuid",
			},
			wantErr: "run_id is not a valid UUID",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validation.ValidateAndNormalize(registry, tc.req)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateDeleteNexusOperationExecutionResponse(t *testing.T) {
	registry := newTestRegistry(testConfig())
	resp := &workflowservice.DeleteNexusOperationExecutionResponse{}
	require.NoError(t, validation.ValidateAndNormalize(registry, resp))
}

func TestValidateDescribeNexusOperationExecutionRequest(t *testing.T) {
	registry := newTestRegistry(testConfig())

	for _, tc := range []struct {
		name    string
		req     *workflowservice.DescribeNexusOperationExecutionRequest
		wantErr string
	}{
		{
			name: "valid with run_id",
			req: &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
				RunId:       "a7d6f9c2-1234-5678-abcd-ef0123456789",
			},
		},
		{
			name: "valid without run_id",
			req: &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
			},
		},
		{
			name:    "missing operation_id",
			req:     &workflowservice.DescribeNexusOperationExecutionRequest{Namespace: "ns"},
			wantErr: "operation_id is required",
		},
		{
			name: "operation_id too long",
			req: &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
			wantErr: "operation_id exceeds length limit",
		},
		{
			name: "invalid run_id",
			req: &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
				RunId:       "not-a-uuid",
			},
			wantErr: "run_id is not a valid UUID",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validation.ValidateAndNormalize(registry, tc.req)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateDescribeNexusOperationExecutionResponse(t *testing.T) {
	registry := newTestRegistry(testConfig())
	resp := &workflowservice.DescribeNexusOperationExecutionResponse{}
	require.NoError(t, validation.ValidateAndNormalize(registry, resp))
}

func TestValidatePollNexusOperationExecutionRequest(t *testing.T) {
	registry := newTestRegistry(testConfig())

	for _, tc := range []struct {
		name    string
		req     *workflowservice.PollNexusOperationExecutionRequest
		wantErr string
	}{
		{
			name: "valid",
			req: &workflowservice.PollNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
				RunId:       "a7d6f9c2-1234-5678-abcd-ef0123456789",
				WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
			},
		},
		{
			name: "valid without run_id",
			req: &workflowservice.PollNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
				WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
			},
		},
		{
			name:    "missing operation_id",
			req:     &workflowservice.PollNexusOperationExecutionRequest{Namespace: "ns", WaitStage: enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED},
			wantErr: "operation_id is required",
		},
		{
			name:    "unspecified wait_stage",
			req:     &workflowservice.PollNexusOperationExecutionRequest{Namespace: "ns", OperationId: "op"},
			wantErr: "wait_stage must be specified",
		},
		{
			name: "invalid run_id",
			req: &workflowservice.PollNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
				RunId:       "not-a-uuid",
				WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
			},
			wantErr: "run_id is not a valid UUID",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validation.ValidateAndNormalize(registry, tc.req)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidatePollNexusOperationExecutionResponse(t *testing.T) {
	registry := newTestRegistry(testConfig())
	resp := &workflowservice.PollNexusOperationExecutionResponse{}
	require.NoError(t, validation.ValidateAndNormalize(registry, resp))
}

func TestValidateRequestCancelNexusOperationExecutionRequest(t *testing.T) {
	registry := newTestRegistry(testConfig())

	for _, tc := range []struct {
		name    string
		req     *workflowservice.RequestCancelNexusOperationExecutionRequest
		wantErr string
	}{
		{
			name: "valid",
			req: &workflowservice.RequestCancelNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
				RunId:       "a7d6f9c2-1234-5678-abcd-ef0123456789",
				RequestId:   "b8e7f0d1-abcd-ef01-2345-678901234567",
			},
		},
		{
			name: "valid without optional fields",
			req: &workflowservice.RequestCancelNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
			},
		},
		{
			name:    "missing operation_id",
			req:     &workflowservice.RequestCancelNexusOperationExecutionRequest{Namespace: "ns"},
			wantErr: "operation_id is required",
		},
		{
			name: "invalid request_id",
			req: &workflowservice.RequestCancelNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
				RequestId:   "not-a-uuid",
			},
			wantErr: "request_id is not a valid UUID",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validation.ValidateAndNormalize(registry, tc.req)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRequestCancelNexusOperationExecutionResponse(t *testing.T) {
	registry := newTestRegistry(testConfig())
	resp := &workflowservice.RequestCancelNexusOperationExecutionResponse{}
	require.NoError(t, validation.ValidateAndNormalize(registry, resp))
}

func TestValidateTerminateNexusOperationExecutionRequest(t *testing.T) {
	registry := newTestRegistry(testConfig())

	for _, tc := range []struct {
		name    string
		req     *workflowservice.TerminateNexusOperationExecutionRequest
		wantErr string
	}{
		{
			name: "valid",
			req: &workflowservice.TerminateNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
				RunId:       "a7d6f9c2-1234-5678-abcd-ef0123456789",
				RequestId:   "b8e7f0d1-abcd-ef01-2345-678901234567",
				Reason:      "terminated by admin",
			},
		},
		{
			name: "valid without optional fields",
			req: &workflowservice.TerminateNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
			},
		},
		{
			name:    "missing operation_id",
			req:     &workflowservice.TerminateNexusOperationExecutionRequest{Namespace: "ns"},
			wantErr: "operation_id is required",
		},
		{
			name: "invalid request_id",
			req: &workflowservice.TerminateNexusOperationExecutionRequest{
				Namespace:   "ns",
				OperationId: "op",
				RequestId:   "not-a-uuid",
			},
			wantErr: "request_id is not a valid UUID",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validation.ValidateAndNormalize(registry, tc.req)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateTerminateNexusOperationExecutionResponse(t *testing.T) {
	registry := newTestRegistry(testConfig())
	resp := &workflowservice.TerminateNexusOperationExecutionResponse{}
	require.NoError(t, validation.ValidateAndNormalize(registry, resp))
}

func TestValidateListNexusOperationExecutionsRequest(t *testing.T) {
	registry := newTestRegistry(testConfig())
	req := &workflowservice.ListNexusOperationExecutionsRequest{
		Namespace: "ns",
		PageSize:  10,
		Query:     "WorkflowType='foo'",
	}
	require.NoError(t, validation.ValidateAndNormalize(registry, req))
}

func TestValidateListNexusOperationExecutionsResponse(t *testing.T) {
	registry := newTestRegistry(testConfig())
	resp := &workflowservice.ListNexusOperationExecutionsResponse{}
	require.NoError(t, validation.ValidateAndNormalize(registry, resp))
}

func TestValidateCountNexusOperationExecutionsRequest(t *testing.T) {
	registry := newTestRegistry(testConfig())
	req := &workflowservice.CountNexusOperationExecutionsRequest{
		Namespace: "ns",
		Query:     "WorkflowType='foo'",
	}
	require.NoError(t, validation.ValidateAndNormalize(registry, req))
}

func TestValidateCountNexusOperationExecutionsResponse(t *testing.T) {
	registry := newTestRegistry(testConfig())
	resp := &workflowservice.CountNexusOperationExecutionsResponse{Count: 42}
	require.NoError(t, validation.ValidateAndNormalize(registry, resp))
}

func TestValidateStartNexusOperationExecutionRequest(t *testing.T) {
	registry := newTestRegistry(testConfig())

	for _, tc := range []struct {
		name    string
		req     *workflowservice.StartNexusOperationExecutionRequest
		wantErr string
	}{
		{
			name: "valid",
			req: &workflowservice.StartNexusOperationExecutionRequest{
				Namespace:   "ns",
				Endpoint:    "my-endpoint",
				Service:     "my-service",
				Operation:   "my-operation",
				OperationId: "my-op-id",
				RequestId:   "a7d6f9c2-1234-5678-abcd-ef0123456789",
			},
		},
		{
			name: "valid without optional fields",
			req: &workflowservice.StartNexusOperationExecutionRequest{
				Namespace: "ns",
				Endpoint:  "my-endpoint",
				Service:   "my-service",
				Operation: "my-operation",
			},
		},
		{
			name:    "missing endpoint",
			req:     &workflowservice.StartNexusOperationExecutionRequest{Namespace: "ns", Service: "svc", Operation: "op"},
			wantErr: "endpoint is required",
		},
		{
			name:    "missing service",
			req:     &workflowservice.StartNexusOperationExecutionRequest{Namespace: "ns", Endpoint: "ep", Operation: "op"},
			wantErr: "service is required",
		},
		{
			name:    "missing operation",
			req:     &workflowservice.StartNexusOperationExecutionRequest{Namespace: "ns", Endpoint: "ep", Service: "svc"},
			wantErr: "operation is required",
		},
		{
			name: "operation_id too long",
			req: &workflowservice.StartNexusOperationExecutionRequest{
				Namespace:   "ns",
				Endpoint:    "ep",
				Service:     "svc",
				Operation:   "op",
				OperationId: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
			wantErr: "operation_id exceeds length limit",
		},
		{
			name: "invalid request_id",
			req: &workflowservice.StartNexusOperationExecutionRequest{
				Namespace: "ns",
				Endpoint:  "ep",
				Service:   "svc",
				Operation: "op",
				RequestId: "not-a-uuid",
			},
			wantErr: "request_id is not a valid UUID",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validation.ValidateAndNormalize(registry, tc.req)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateStartNexusOperationExecutionResponse(t *testing.T) {
	registry := newTestRegistry(testConfig())
	resp := &workflowservice.StartNexusOperationExecutionResponse{RunId: "a7d6f9c2-1234-5678-abcd-ef0123456789", Started: true}
	require.NoError(t, validation.ValidateAndNormalize(registry, resp))
}
