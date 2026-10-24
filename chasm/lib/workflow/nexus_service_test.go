package workflow

import (
	"context"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
)

func TestSignalWithStartWorkflowExecutionRejectsTimeSkippingConfigWhenFeatureDisabled(t *testing.T) {
	t.Parallel()
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespaceID(namespace.Name("test-namespace")).Return(namespace.ID("test-namespace-id"), nil)

	handler := &workflowServiceNexusHandler{
		config: Config{
			enableSignalWithStartFromWorkflow: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
			workflowTimeSkippingEnabled:       dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		},
		namespaceRegistry: registry,
	}
	_, err := handler.signalWithStartWorkflowExecution(
		context.Background(),
		&workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:          "test-namespace",
			TimeSkippingConfig: &commonpb.TimeSkippingConfig{Enabled: true},
		},
		nexus.StartOperationOptions{},
	)
	require.ErrorIs(t, err, ErrWorkflowTimeSkippingNotEnabled)
}
