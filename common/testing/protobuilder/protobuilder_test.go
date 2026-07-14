package protobuilder_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/protobuilder"
)

func defaults() *workflowservice.StartWorkflowExecutionRequest {
	return &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    "default-ns",
		Identity:     "default-identity",
		WorkflowType: &commonpb.WorkflowType{Name: "default-wf"},
	}
}

func TestWithDefaults_FillsUnsetFields(t *testing.T) {
	got := protobuilder.WithDefaults(&workflowservice.StartWorkflowExecutionRequest{}, defaults())
	require.Equal(t, "default-ns", got.Namespace)
	require.Equal(t, "default-identity", got.Identity)
	require.Equal(t, "default-wf", got.WorkflowType.GetName())
}

func TestWithDefaults_CallerWins(t *testing.T) {
	got := protobuilder.WithDefaults(&workflowservice.StartWorkflowExecutionRequest{
		Namespace:    "caller-ns",
		WorkflowType: &commonpb.WorkflowType{Name: "caller-wf"},
	}, defaults())
	require.Equal(t, "caller-ns", got.Namespace)              // caller-set scalar preserved
	require.Equal(t, "caller-wf", got.WorkflowType.GetName()) // message replaced, not merged
	require.Equal(t, "default-identity", got.Identity)        // gap filled
}

func TestWithDefaults_DoesNotMutateInput(t *testing.T) {
	in := &workflowservice.StartWorkflowExecutionRequest{}
	_ = protobuilder.WithDefaults(in, defaults())
	require.Empty(t, in.Namespace, "input literal must be left untouched so it can be reused")
}

func TestWithDefaults_CannotOverrideWithZeroValue(t *testing.T) {
	// A caller cannot force a scalar back to its zero value over a non-zero
	// default; proto3 has no presence for the empty string. This documents the
	// known limitation.
	got := protobuilder.WithDefaults(&workflowservice.StartWorkflowExecutionRequest{Namespace: ""}, defaults())
	require.Equal(t, "default-ns", got.Namespace)
}
