package tests

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/failpointpb"
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

	// The services of the test cluster share one failpoint registry, so the frontend applies each
	// request to it once, and ListFailpoints reports which services serve it. That shows the
	// frontend reached every service; that each registry is only changed once is covered by the
	// fan-out's unit tests.
	t.Run("frontend FailpointService toggles a failpoint for the test namespace", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t)
		testFailpointService(t, env, env.NamespaceID().String(), []string{"frontend", "history", "matching"})
	})

	t.Run("frontend FailpointService toggles a failpoint in the whole cluster", func(t *testing.T) {
		t.Parallel()
		// mTLS covers dialing the frontend and internode peers with their TLS configs.
		env := testcore.NewEnv(t,
			testcore.WithWorkerService("frontend fans failpoint requests out to the worker"),
			testcore.WithMTLS(),
		)
		testFailpointService(t, env, "", []string{"frontend", "history", "matching", "worker"})
	})
}

// testFailpointService fails StartWorkflowExecution in history with a failpoint configured through
// the frontend's FailpointService, then disables it.
func testFailpointService(t *testing.T, env *testcore.TestEnv, namespaceID string, wantServices []string) {
	t.Helper()
	client := testcore.FailpointClient(env)
	key := failpoint.GRPCKey(historyservice.HistoryService_StartWorkflowExecution_FullMethodName)

	_, err := client.InjectReturn(testcore.NewContext(), &failpointpb.InjectReturnRequest{
		Key:          key.Name(),
		NamespaceId:  namespaceID,
		ErrorName:    "InvalidArgument",
		ErrorMessage: "injected",
	})
	require.NoError(t, err)

	list, err := client.ListFailpoints(testcore.NewContext(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	var services []string
	for _, info := range list.Failpoints {
		if info.Key != key.Name() || info.NamespaceId != namespaceID {
			continue // A failpoint of another test on a shared cluster.
		}
		for _, host := range info.Hosts {
			service, _, _ := strings.Cut(host, "/")
			services = append(services, service)
		}
	}
	require.ElementsMatch(t, wantServices, services)

	_, err = env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowRequest(env, env.Tv()))
	var invalidArgument *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgument)
	require.ErrorContains(t, err, "injected")

	_, err = client.DisableFailpoint(testcore.NewContext(), &failpointpb.DisableFailpointRequest{Key: key.Name(), NamespaceId: namespaceID})
	require.NoError(t, err)
	mustStartWorkflow(env, env.Tv())
}
