package nexusoperation

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/validation"
)

func newTestRegistry(config *Config) *validation.ValidatorRegistry {
	v := newDeleteNexusOperationExecutionRequestValidator(config)
	registry := validation.NewValidatorRegistry()
	_ = v.RegisterValidator(registry)
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
