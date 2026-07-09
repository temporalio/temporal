package mixedbrain

import (
	"context"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/omes/devserver"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func registerNamespace(t *testing.T, conn *grpc.ClientConn, namespace string) {
	t.Helper()

	client := workflowservice.NewWorkflowServiceClient(conn)

	require.Eventually(t, func() bool {
		_, err := client.RegisterNamespace(t.Context(), &workflowservice.RegisterNamespaceRequest{
			Namespace:                        namespace,
			WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
		})
		if err == nil {
			return true
		}
		st, ok := status.FromError(err)
		return ok && st.Code() == codes.AlreadyExists
	}, retryTimeout, time.Second, "failed to register namespace %s", namespace)
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

func serverLogger(t *testing.T, name, logPath string) (*zap.SugaredLogger, *os.File) {
	t.Helper()
	f, err := os.Create(logPath)
	require.NoError(t, err)
	return zap.NewNop().Sugar().With("server", name), f
}

func requireServerAlive(t *testing.T, name, address string) {
	t.Helper()

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err = adminservice.NewAdminServiceClient(conn).DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
	require.NoError(t, err, "%s server is not reachable", name)
}

func membershipPorts(p devserver.Ports) []int {
	return []int{
		p.FrontendMembership,
		p.HistoryMembership,
		p.MatchingMembership,
		p.WorkerMembership,
	}
}

// waitForClusterFormation waits until the server's reachable members include
// all membership ports from all provided servers, confirming the servers
// discovered each other. Reachable members use raw ringpop addresses (membership ports).
func waitForClusterFormation(t *testing.T, conn *grpc.ClientConn, timeout time.Duration, servers ...devserver.Ports) {
	t.Helper()

	client := adminservice.NewAdminServiceClient(conn)

	require.Eventually(t, func() bool {
		resp, err := client.DescribeCluster(t.Context(), &adminservice.DescribeClusterRequest{})
		if err != nil {
			return false
		}
		membership := resp.GetMembershipInfo()
		if membership == nil {
			return false
		}

		seen := map[int]bool{}
		for _, member := range membership.GetReachableMembers() {
			_, portStr, err := net.SplitHostPort(member)
			if err != nil {
				continue
			}
			port, err := strconv.Atoi(portStr)
			if err != nil {
				continue
			}
			seen[port] = true
		}

		for _, server := range servers {
			for _, port := range membershipPorts(server) {
				if !seen[port] {
					t.Logf("Waiting for cluster formation: port %d not yet visible", port)
					return false
				}
			}
		}
		return true
	}, timeout, time.Second, "cluster did not form within %v", timeout)
}
