package tests

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/testx/failpoint"
)

func TestFailpoint(t *testing.T) {
	t.Parallel()

	t.Run("gRPC failpoint fails a history call for the test namespace", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t)
		testcore.InjectFailpoint[error](env, failpoint.GRPCKey(historyservice.HistoryService_StartWorkflowExecution_FullMethodName)).
			Return(serviceerror.NewInvalidArgument("injected"), failpoint.Once)

		_, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowRequest(env, env.Tv()))
		require.ErrorContains(t, err, "injected")

		mustStartWorkflow(env, env.Tv())
	})

	t.Run("persistence failpoint fails a store method for the test namespace", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t)
		otherNamespace := namespace.Name(testcore.RandomizeStr("other-namespace"))
		_, err := env.RegisterNamespace(testcontext.For(t), otherNamespace, 1, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
		require.NoError(t, err)
		testcore.InjectFailpoint[error](env, failpoints.PersistenceKey(string(config.ExecutionStoreName), "CreateWorkflowExecution")).
			Return(errors.New("injected"), failpoint.Always)

		_, err = env.FrontendClient().StartWorkflowExecution(testcontext.For(t), startWorkflowRequest(env, env.Tv()))
		require.ErrorContains(t, err, "injected")

		otherRequest := startWorkflowRequest(env, env.Tv())
		otherRequest.Namespace = otherNamespace.String()
		_, err = env.FrontendClient().StartWorkflowExecution(testcontext.For(t), otherRequest)
		require.NoError(t, err)
	})

	t.Run("global persistence failpoint fails a store method", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
		fp := testcore.InjectGlobalFailpoint[error](env, failpoints.PersistenceKey(string(config.ExecutionStoreName), "CreateWorkflowExecution"))
		fp.Return(errors.New("injected"), failpoint.Always)

		_, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowRequest(env, env.Tv()))
		require.ErrorContains(t, err, "injected")

		fp.Disable()
		mustStartWorkflow(env, env.Tv())
	})
}
