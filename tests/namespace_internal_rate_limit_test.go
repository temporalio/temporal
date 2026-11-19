package tests

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (s *NamespaceInterceptorTestSuite) TestInternalPerNamespaceRateLimit() {
	t := s.T()
	env := testcore.NewEnv(t)
	rateLimitNamespace := "internal-per-ns-rps-" + uuid.NewString()
	// Use a second namespace so its limiters are created after these settings are installed.
	overrideForRateLimitNamespace := func(setting dynamicconfig.GenericSetting, value any) {
		env.GetTestCluster().OverrideDynamicConfig(t, setting, []dynamicconfig.ConstrainedValue{{
			Constraints: dynamicconfig.Constraints{Namespace: rateLimitNamespace},
			Value:       value,
		}})
	}
	overrideForRateLimitNamespace(dynamicconfig.FrontendMaxNamespaceRPSPerInstance, 1)
	overrideForRateLimitNamespace(dynamicconfig.FrontendMaxNamespaceBurstRatioPerInstance, float64(1))
	overrideForRateLimitNamespace(dynamicconfig.FrontendEnableInternalPerNSNamespaceRPS, true)
	// Leaves headroom for the namespace's own system worker polls, which share this budget.
	overrideForRateLimitNamespace(dynamicconfig.FrontendInternalPerNSMaxNamespaceRPSPerInstance, 20)

	ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
	defer cancel()
	_, err := env.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        rateLimitNamespace,
		Description:                      "namespace for internal per-namespace rate limit test",
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	require.NoError(t, err)

	// Reading the history of a missing workflow returns NotFound only after the request is admitted.
	readHistory := func(workflowID string) error {
		_, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: rateLimitNamespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		})
		return err
	}
	readUntilRateLimited := func(workflowID string) *serviceerror.ResourceExhausted {
		for range 200 {
			err := readHistory(workflowID)
			if resourceExhausted, ok := errors.AsType[*serviceerror.ResourceExhausted](err); ok {
				return resourceExhausted
			}
			_, notFound := errors.AsType[*serviceerror.NotFound](err)
			require.True(t, notFound, "expected missing execution before rate limit, got: %v", err)
		}
		require.FailNow(t, "rapid RPCs did not reach the rate limit")
		return nil
	}
	customerWorkflowID := uuid.NewString()
	systemWorkflowID := primitives.ScheduleWorkflowIDPrefix + uuid.NewString()

	customerRejected := readUntilRateLimited(customerWorkflowID)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, customerRejected.Cause)
	require.Equal(t, interceptor.ErrNamespaceRateLimitServerBusy.Message, customerRejected.Message)

	_, notFound := errors.AsType[*serviceerror.NotFound](readHistory(systemWorkflowID))
	require.True(t, notFound, "system workflow history read was throttled by the exhausted namespace budget")

	systemRejected := readUntilRateLimited(systemWorkflowID)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, systemRejected.Cause)
	require.Equal(t, interceptor.ErrInternalPerNSNamespaceRateLimitServerBusy.Message, systemRejected.Message)
}
