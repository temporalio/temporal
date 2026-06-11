package consts

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
)

func TestNewResourceExhaustedBusyWorkflowWithWorkflowID(t *testing.T) {
	err := NewResourceExhaustedBusyWorkflow("test-workflow-id")

	require.ErrorIs(t, err, ErrResourceExhaustedBusyWorkflow)
	require.Equal(t, "Workflow is busy. WorkflowId: test-workflow-id.", err.Error())

	st := serviceerror.ToStatus(err)
	require.Equal(t, codes.ResourceExhausted, st.Code())
	require.Equal(t, "Workflow is busy. WorkflowId: test-workflow-id.", st.Message())

	roundTrippedErr := serviceerror.FromStatus(st)
	var resourceExhaustedErr *serviceerror.ResourceExhausted
	require.ErrorAs(t, roundTrippedErr, &resourceExhaustedErr)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW, resourceExhaustedErr.Cause)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, resourceExhaustedErr.Scope)
}

func TestNewResourceExhaustedBusyWorkflowWithoutWorkflowID(t *testing.T) {
	err := NewResourceExhaustedBusyWorkflow("")

	require.Same(t, ErrResourceExhaustedBusyWorkflow, err)
	require.Equal(t, "Workflow is busy.", err.Error())
}
