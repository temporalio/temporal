package enums

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestSetDefaultWorkflowIdPolicies(t *testing.T) {
	type policies struct {
		reuse    enumspb.WorkflowIdReusePolicy
		conflict enumspb.WorkflowIdConflictPolicy
	}

	for _, tc := range []struct {
		name                  string
		input                 policies
		defaultConflictPolicy enumspb.WorkflowIdConflictPolicy
		want                  policies
	}{
		{
			name: "unspecified policies get defaults",
			input: policies{
				reuse:    enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED,
				conflict: enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED,
			},
			defaultConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
			want: policies{
				reuse:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
				conflict: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
			},
		},
		{
			name: "explicitly set policies are not overridden",
			input: policies{
				reuse:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
				conflict: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			},
			defaultConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
			want: policies{
				reuse:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
				conflict: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			},
		},
		{
			name: "terminate-if-running reuse policy is migrated to terminate-existing conflict policy",
			input: policies{ //nolint:staticcheck // SA1019: intentional migration of deprecated policy
				reuse:    enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
				conflict: enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED,
			},
			defaultConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
			want: policies{
				reuse:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
				conflict: enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
			},
		},
		{
			name: "terminate-if-running reuse policy with an explicit conflict policy is not migrated",
			input: policies{ //nolint:staticcheck // SA1019: intentional migration of deprecated policy
				reuse:    enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
				conflict: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			},
			defaultConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
			want: policies{ //nolint:staticcheck // SA1019: intentional migration of deprecated policy
				reuse:    enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
				conflict: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, // same as before!
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			SetDefaultWorkflowIdPolicies(&tc.input.reuse, &tc.input.conflict, tc.defaultConflictPolicy)
			require.Equal(t, tc.want, tc.input)

			// check idempotency
			SetDefaultWorkflowIdPolicies(&tc.input.reuse, &tc.input.conflict, tc.defaultConflictPolicy)
			require.Equal(t, tc.want, tc.input)
		})
	}
}
