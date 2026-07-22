package mixedbrain

import (
	"context"
	"errors"
	"fmt"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

type serverInstance struct {
	name    string
	version string
	logPath string
	options devserver.Options

	server  *devserver.Server
	ports   devserver.Ports
	logFile *os.File
}

func (s *serverInstance) start(ctx context.Context, clusterEndpoint string) error {
	logFile, err := os.OpenFile(s.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open %s log: %w", s.name, err)
	}

	options := s.options
	options.Output = logFile
	options.ClusterEndpoint = devserver.ClusterEndpoint{RPCAddress: clusterEndpoint}
	if s.ports != (devserver.Ports{}) {
		options.Ports = &s.ports
	}
	server, err := devserver.Start(ctx, options)
	if err != nil {
		return errors.Join(fmt.Errorf("start %s server: %w", s.name, err), logFile.Close())
	}

	s.server = server
	s.ports = server.Ports()
	s.logFile = logFile
	return nil
}

func (s *serverInstance) stop() error {
	var errs []error
	if s.server != nil {
		if err := s.server.Stop(); err != nil {
			errs = append(errs, err)
		}
		s.server = nil
	}
	if s.logFile != nil {
		errs = append(errs, s.logFile.Close())
		s.logFile = nil
	}
	return errors.Join(errs...)
}

func (s *serverInstance) frontendAddress() string {
	if s.server == nil {
		return ""
	}
	return s.server.FrontendHostPort()
}

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
			for _, port := range []int{
				server.FrontendMembership,
				server.HistoryMembership,
				server.MatchingMembership,
				server.WorkerMembership,
			} {
				if !seen[port] {
					t.Logf("Waiting for cluster formation: port %d not yet visible", port)
					return false
				}
			}
		}
		return true
	}, timeout, time.Second, "cluster did not form within %v", timeout)
}

func waitForMembership(
	ctx context.Context,
	frontendAddress string,
	present []devserver.Ports,
	absent []devserver.Ports,
) error {
	conn, err := grpc.NewClient(frontendAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	client := adminservice.NewAdminServiceClient(conn)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastErr error
	for {
		requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := client.DescribeCluster(requestCtx, &adminservice.DescribeClusterRequest{})
		cancel()
		if err == nil {
			seen := membershipPorts(resp.GetMembershipInfo().GetReachableMembers())
			if membershipMatches(seen, present, absent) {
				return nil
			}
			lastErr = fmt.Errorf("membership has not converged")
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for membership: %w (last error: %v)", context.Cause(ctx), lastErr)
		case <-ticker.C:
		}
	}
}

func membershipPorts(members []string) map[int]struct{} {
	seen := make(map[int]struct{}, len(members))
	for _, member := range members {
		_, portString, err := net.SplitHostPort(member)
		if err != nil {
			continue
		}
		port, err := strconv.Atoi(portString)
		if err == nil {
			seen[port] = struct{}{}
		}
	}
	return seen
}

func membershipMatches(seen map[int]struct{}, present, absent []devserver.Ports) bool {
	for _, ports := range present {
		for _, port := range serverMembershipPorts(ports) {
			if _, ok := seen[port]; !ok {
				return false
			}
		}
	}
	for _, ports := range absent {
		for _, port := range serverMembershipPorts(ports) {
			if _, ok := seen[port]; ok {
				return false
			}
		}
	}
	return true
}

func serverMembershipPorts(ports devserver.Ports) []int {
	return []int{
		ports.FrontendMembership,
		ports.HistoryMembership,
		ports.MatchingMembership,
		ports.WorkerMembership,
	}
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
