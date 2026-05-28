package mixedbrain

import (
	"net"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/omes/devserver"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func registerDefaultNamespace(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()

	client := workflowservice.NewWorkflowServiceClient(conn)

	require.Eventually(t, func() bool {
		_, err := client.RegisterNamespace(t.Context(), &workflowservice.RegisterNamespaceRequest{
			Namespace:                        "default",
			WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
		})
		if err == nil {
			return true
		}
		st, ok := status.FromError(err)
		return ok && st.Code() == codes.AlreadyExists
	}, retryTimeout, time.Second, "failed to register default namespace")
}

func createNexusEndpoint(t *testing.T, conn *grpc.ClientConn, endpointName, namespace, taskQueue string) {
	t.Helper()

	client := operatorservice.NewOperatorServiceClient(conn)

	require.Eventually(t, func() bool {
		_, err := client.CreateNexusEndpoint(t.Context(), &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: endpointName,
				Target: &nexuspb.EndpointTarget{
					Variant: &nexuspb.EndpointTarget_Worker_{
						Worker: &nexuspb.EndpointTarget_Worker{
							Namespace: namespace,
							TaskQueue: taskQueue,
						},
					},
				},
			},
		})
		if err == nil {
			return true
		}
		st, ok := status.FromError(err)
		return ok && st.Code() == codes.AlreadyExists
	}, retryTimeout, time.Second, "failed to create nexus endpoint %s", endpointName)
}

// waitForClusterFormation waits until the server's reachable members include
// another devserver, confirming the servers discovered each other.
func waitForClusterFormation(t *testing.T, conn *grpc.ClientConn, timeout time.Duration, expected devserver.Ports) {
	t.Helper()

	client := adminservice.NewAdminServiceClient(conn)
	want := net.JoinHostPort(expected.Host, strconv.Itoa(expected.FrontendMembership))

	require.Eventually(t, func() bool {
		resp, err := client.DescribeCluster(t.Context(), &adminservice.DescribeClusterRequest{})
		if err != nil {
			return false
		}
		return slices.Contains(resp.GetMembershipInfo().GetReachableMembers(), want)
	}, timeout, time.Second, "cluster did not form within %v", timeout)
}
