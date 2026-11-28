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
	"go.temporal.io/server/common/testing/testvars"
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

func (s *namespaceSoftDeleteSuite) TestRestoreNamespace_TimerFires() {
	env := testcore.NewEnv(s.T(), testcore.WithWorkerService("namespace restore job"), testcore.WithDynamicConfig(dynamicconfig.EnableNamespaceSoftDelete, true))
	tv := env.Tv()
	runID := mustStartWorkflow(env, tv)
	startTimer(s, env, tv, 2*time.Second)

	_, err := env.OperatorClient().SoftDeleteNamespace(s.Context(), &operatorservice.SoftDeleteNamespaceRequest{
		Namespace: env.Namespace().String(),
	})
	s.NoError(err)
	// The timer comes due while soft-deleted, and its task is discarded.
	requireTimerPendingFor(s, env, tv, runID, 4*time.Second)

	_, err = env.OperatorClient().RestoreNamespace(s.Context(), &operatorservice.RestoreNamespaceRequest{
		Namespace: env.Namespace().String(),
	})
	s.NoError(err)

	await.Require(s.Context(), s.T(), func(t *await.T) {
		resp, err := env.OperatorClient().DescribeNamespaceRestore(s.Context(), &operatorservice.DescribeNamespaceRestoreRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.NAMESPACE_RESTORE_STATE_COMPLETED, resp.GetState())
		require.NotEmpty(t, resp.GetRunId())
		require.NotNil(t, resp.GetCloseTime())
		require.GreaterOrEqual(t, resp.GetProcessedExecutionCount(), int64(1))
	}, 30*time.Second, 200*time.Millisecond)
	// The refresh regenerated the timer task, so the timer fires.
	await.Require(s.Context(), s.T(), func(t *await.T) {
		require.False(t, timerPending(s, env, tv, runID))
	}, 10*time.Second, 200*time.Millisecond)

	descResp, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Namespace: env.Namespace().String(),
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, descResp.GetNamespaceInfo().GetState())
}

func (s *namespaceSoftDeleteSuite) TestDescribeNamespaceRestore_NeverRestored() {
	env := testcore.NewEnv(s.T())

	_, err := env.OperatorClient().DescribeNamespaceRestore(s.Context(), &operatorservice.DescribeNamespaceRestoreRequest{
		Namespace: env.Namespace().String(),
	})
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)
}

func (s *namespaceSoftDeleteSuite) TestSoftDeleteNamespace_AbandonsRestore() {
	env := testcore.NewEnv(s.T(), testcore.WithWorkerService("namespace restore job"), testcore.WithDynamicConfig(dynamicconfig.EnableNamespaceSoftDelete, true))
	_, err := env.OperatorClient().SoftDeleteNamespace(s.Context(), &operatorservice.SoftDeleteNamespaceRequest{
		Namespace: env.Namespace().String(),
	})
	s.NoError(err)
	_, err = env.OperatorClient().RestoreNamespace(s.Context(), &operatorservice.RestoreNamespaceRequest{
		Namespace: env.Namespace().String(),
	})
	s.NoError(err)

	// Soft delete again before or after the job finishes: a running restore becomes FAILED, and a
	// completed one stays COMPLETED.
	_, err = env.OperatorClient().SoftDeleteNamespace(s.Context(), &operatorservice.SoftDeleteNamespaceRequest{
		Namespace: env.Namespace().String(),
	})
	s.NoError(err)
	await.Require(s.Context(), s.T(), func(t *await.T) {
		resp, err := env.OperatorClient().DescribeNamespaceRestore(s.Context(), &operatorservice.DescribeNamespaceRestoreRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(t, err)
		require.Contains(t, []enumspb.NamespaceRestoreState{enumspb.NAMESPACE_RESTORE_STATE_FAILED, enumspb.NAMESPACE_RESTORE_STATE_COMPLETED}, resp.GetState())
		require.NotNil(t, resp.GetCloseTime())
	}, 30*time.Second, 200*time.Millisecond)

	descResp, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Namespace: env.Namespace().String(),
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_SOFT_DELETED, descResp.GetNamespaceInfo().GetState())
}

func (s *namespaceSoftDeleteSuite) TestSoftDeleteNamespace_TimerDoesNotFire() {
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.EnableNamespaceSoftDelete, true))
	tv := env.Tv()
	runID := mustStartWorkflow(env, tv)
	// Long enough for history's namespace registry to see the soft delete first.
	startTimer(s, env, tv, 4*time.Second)

	_, err := env.OperatorClient().SoftDeleteNamespace(s.Context(), &operatorservice.SoftDeleteNamespaceRequest{
		Namespace: env.Namespace().String(),
	})
	s.NoError(err)

	requireTimerPendingFor(s, env, tv, runID, 6*time.Second)
}

// timerPending reports whether the workflow's timer is still in mutable state; a fired timer is
// removed from it.
func timerPending(s *namespaceSoftDeleteSuite, env *testcore.TestEnv, tv *testvars.TestVars, runID string) bool {
	ms, err := env.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	return len(ms.GetDatabaseMutableState().GetTimerInfos()) > 0
}

// requireTimerPendingFor checks that the timer doesn't fire for d. It polls on the test goroutine,
// since s.Never can still be running a check when the test ends.
func requireTimerPendingFor(s *namespaceSoftDeleteSuite, env *testcore.TestEnv, tv *testvars.TestVars, runID string, d time.Duration) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(d)
	for {
		s.Require().True(timerPending(s, env, tv, runID), "timer fired")
		select {
		case <-deadline:
			return
		case <-ticker.C:
		}
	}
}

func startTimer(s *namespaceSoftDeleteSuite, env *testcore.TestEnv, tv *testvars.TestVars, timeout time.Duration) {
	_, err := env.TaskPoller().PollAndHandleWorkflowTask(tv,
		func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_START_TIMER,
					Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
						TimerId:            "timer",
						StartToFireTimeout: durationpb.New(timeout),
					}},
				}},
			}, nil
		})
	s.NoError(err)
}
