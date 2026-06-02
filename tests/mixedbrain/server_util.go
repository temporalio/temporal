package mixedbrain

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strconv"
	"syscall"
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

type serverProcess struct {
	name    string
	cmd     *exec.Cmd
	cancel  context.CancelFunc
	logFile *os.File
	logPath string
	done    chan error
}

func startServerProcess(t *testing.T, name, binary, configDir, logPath string) *serverProcess {
	t.Helper()
	t.Logf("Starting %s: %s", name, binary)

	logFile, err := os.Create(logPath)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cmd := exec.CommandContext(ctx, binary,
		"--root", "/",
		"--config", configDir,
		"--allow-no-auth",
		"start",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Dir = sourceRoot()
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 15 * time.Second

	require.NoError(t, cmd.Start())

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	return &serverProcess{
		name:    name,
		cmd:     cmd,
		cancel:  cancel,
		logFile: logFile,
		logPath: logPath,
		done:    done,
	}
}

func (p *serverProcess) stop() {
	p.cancel()
	<-p.done
	_ = p.logFile.Close()
}

func (p *serverProcess) requireAlive(t *testing.T) {
	t.Helper()
	select {
	case err := <-p.done:
		p.done <- err // put back so stop() doesn't deadlock
		t.Fatalf("%s exited unexpectedly: %v (logs: %s)", p.name, err, p.logPath)
	default:
	}
}

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
// all membership ports from all provided port sets, confirming the servers
// discovered each other. Reachable members use raw ringpop addresses (membership ports).
func waitForClusterFormation(t *testing.T, conn *grpc.ClientConn, timeout time.Duration, portSets ...portSet) {
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

		for _, ps := range portSets {
			for _, port := range ps.membershipPorts() {
				if !seen[port] {
					t.Logf("Waiting for cluster formation: port %d not yet visible", port)
					return false
				}
			}
		}
		return true
	}, timeout, time.Second, "cluster did not form within %v", timeout)
}
