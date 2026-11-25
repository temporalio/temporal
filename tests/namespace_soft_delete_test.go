package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type namespaceSoftDeleteSuite struct {
	parallelsuite.Suite[*namespaceSoftDeleteSuite]
}

func TestNamespaceSoftDeleteSuite(t *testing.T) {
	parallelsuite.Run(t, &namespaceSoftDeleteSuite{})
}

func (s *namespaceSoftDeleteSuite) TestSoftDeleteNamespace() {
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.EnableNamespaceSoftDelete, true))

	_, err := env.OperatorClient().SoftDeleteNamespace(s.Context(), &operatorservice.SoftDeleteNamespaceRequest{
		Namespace: env.Namespace().String(),
	})
	s.NoError(err)

	descResp, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Namespace: env.Namespace().String(),
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_SOFT_DELETED, descResp.GetNamespaceInfo().GetState())

	// Rejected once the frontend's namespace registry has the new state.
	await.Require(s.Context(), s.T(), func(t *await.T) {
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    env.Namespace().String(),
			WorkflowId:   env.Tv().WorkflowID(),
			WorkflowType: env.Tv().WorkflowType(),
			TaskQueue:    env.Tv().TaskQueue(),
			RequestId:    env.Tv().RequestID(),
		})
		var invalidState *serviceerror.NamespaceInvalidState
		require.ErrorAs(t, err, &invalidState)
	}, 10*time.Second, 100*time.Millisecond)

	// A repeated call, here by ID, succeeds.
	_, err = env.OperatorClient().SoftDeleteNamespace(s.Context(), &operatorservice.SoftDeleteNamespaceRequest{
		NamespaceId: env.NamespaceID().String(),
	})
	s.NoError(err)
}

func (s *namespaceSoftDeleteSuite) TestSoftDeleteNamespace_SystemNamespace() {
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.EnableNamespaceSoftDelete, true))

	_, err := env.OperatorClient().SoftDeleteNamespace(s.Context(), &operatorservice.SoftDeleteNamespaceRequest{
		Namespace: primitives.SystemLocalNamespace,
	})
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)
}

func (s *namespaceSoftDeleteSuite) TestSoftDeleteNamespace_TimerDoesNotFire() {
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.EnableNamespaceSoftDelete, true))
	tv := env.Tv()
	runID := mustStartWorkflow(env, tv)
	_, err := env.TaskPoller().PollAndHandleWorkflowTask(tv,
		func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_START_TIMER,
					Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
						TimerId: "timer",
						// Long enough for history's namespace registry to see the soft delete first.
						StartToFireTimeout: durationpb.New(4 * time.Second),
					}},
				}},
			}, nil
		})
	s.NoError(err)

	_, err = env.OperatorClient().SoftDeleteNamespace(s.Context(), &operatorservice.SoftDeleteNamespaceRequest{
		Namespace: env.Namespace().String(),
	})
	s.NoError(err)

	// A fired timer is removed from mutable state.
	s.Never(func() bool {
		ms, err := env.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
			Archetype: chasm.WorkflowArchetype,
		})
		s.NoError(err)
		return len(ms.GetDatabaseMutableState().GetTimerInfos()) == 0
	}, 6*time.Second, 200*time.Millisecond)
}
