package tests

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/testing/failpoint"
	"go.temporal.io/server/tests/testcore"
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

	t.Run("persistence failpoint fails a store method", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
		fp := testcore.InjectGlobalFailpoint[error](env, failpoint.PersistenceKey(string(config.ExecutionStoreName), "CreateWorkflowExecution"))
		fp.Return(errors.New("injected"), failpoint.Always)

		_, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowRequest(env, env.Tv()))
		require.ErrorContains(t, err, "injected")

		fp.Disable()
		mustStartWorkflow(env, env.Tv())
	})
}
