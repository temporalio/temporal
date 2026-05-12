package mixedbrain

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

// waitForClusterFormation waits until the server's reachable members include
// expected membership ports, confirming the servers discovered each other.
// We can't enumerate ports per service (devserver chooses them dynamically),
// so we just wait until both servers see at least minMembers reachable members.
func waitForClusterFormation(t *testing.T, conn *grpc.ClientConn, timeout time.Duration, minMembers int) {
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

		// Count unique reachable members by host:port to avoid duplicate
		// service-level entries.
		seen := map[string]bool{}
		for _, member := range membership.GetReachableMembers() {
			host, portStr, err := net.SplitHostPort(member)
			if err != nil {
				continue
			}
			if _, err := strconv.Atoi(portStr); err != nil {
				continue
			}
			seen[host+":"+portStr] = true
		}
		if len(seen) < minMembers {
			t.Logf("Waiting for cluster formation: %d/%d members visible", len(seen), minMembers)
			return false
		}
		return true
	}, timeout, time.Second, "cluster did not form within %v", timeout)
}
