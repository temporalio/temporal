package mixedbrain

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
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
	mu      sync.Mutex
	name    string
	binary  string
	config  string
	cmd     *exec.Cmd
	cancel  context.CancelFunc
	logFile *os.File
	logPath string
	done    chan error
}

func startServerProcess(t *testing.T, name, binary, configDir, logPath string) *serverProcess {
	t.Helper()
	t.Logf("Starting %s: %s", name, binary)
	p := &serverProcess{
		name:    name,
		binary:  binary,
		config:  configDir,
		logPath: logPath,
	}
	require.NoError(t, p.start(t.Context(), false))
	return p
}

func (p *serverProcess) start(ctx context.Context, appendLog bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cmd != nil {
		return fmt.Errorf("%s is already running", p.name)
	}

	flag := os.O_CREATE | os.O_WRONLY
	if appendLog {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}
	logFile, err := os.OpenFile(p.logPath, flag, 0644)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(runCtx, p.binary,
		"--root", "/",
		"--config", p.config,
		"--allow-no-auth",
		"start",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Dir = sourceRoot()
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 15 * time.Second

	if err := cmd.Start(); err != nil {
		cancel()
		_ = logFile.Close()
		return err
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	p.cmd = cmd
	p.cancel = cancel
	p.logFile = logFile
	p.done = done
	return nil
}

// stop is idempotent: it may be called explicitly and again via t.Cleanup.
func (p *serverProcess) stop() {
	_ = p.stopErr()
}

func (p *serverProcess) stopErr() error {
	p.mu.Lock()
	if p.cmd == nil {
		p.mu.Unlock()
		return nil
	}
	cancel := p.cancel
	done := p.done
	logFile := p.logFile
	p.cmd = nil
	p.cancel = nil
	p.done = nil
	p.logFile = nil
	p.mu.Unlock()

	cancel()
	err := <-done
	_ = logFile.Close()
	return classifyProcessExit(err)
}

func (p *serverProcess) restart(ctx context.Context) error {
	if err := p.stopErr(); err != nil {
		return err
	}
	return p.start(ctx, true)
}

func (p *serverProcess) requireAlive(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	done := p.done
	p.mu.Unlock()
	if done == nil {
		t.Fatalf("%s is not running (logs: %s)", p.name, p.logPath)
	}
	select {
	case err := <-done:
		done <- err // put back so stop() doesn't deadlock
		t.Fatalf("%s exited unexpectedly: %v (logs: %s)", p.name, err, p.logPath)
	default:
	}
}

func classifyProcessExit(err error) error {
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return err
	}
	if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGTERM {
		return nil
	}
	return err
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
// all membership ports from all provided port sets, confirming the servers
// discovered each other. Reachable members use raw ringpop addresses (membership ports).
func waitForClusterFormation(t *testing.T, conn *grpc.ClientConn, timeout time.Duration, portSets ...portSet) {
	t.Helper()
	require.NoError(t, waitForClusterFormationErr(t.Context(), conn, timeout, portSets...))
}

func waitForClusterFormationErr(ctx context.Context, conn *grpc.ClientConn, timeout time.Duration, portSets ...portSet) error {
	client := adminservice.NewAdminServiceClient(conn)
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		ok, err := clusterFormed(ctx, client, portSets...)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("cluster did not form within %v", timeout)
		case <-ticker.C:
		}
	}
}

func clusterFormed(ctx context.Context, client adminservice.AdminServiceClient, portSets ...portSet) (bool, error) {
	resp, err := client.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
	if err != nil {
		return false, nil
	}
	membership := resp.GetMembershipInfo()
	if membership == nil {
		return false, nil
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
				return false, nil
			}
		}
	}
	return true, nil
}
