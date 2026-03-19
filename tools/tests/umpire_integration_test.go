package tests

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	otlptracegrpc "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
)

// TestWatchdogIntegration boots the umpire binary, sends a test OTEL span to it,
// and asserts that a softassert violation is observed in the logs.
//
// This test is SKIPPED unless UMPIRE_BIN is set to the path of the built binary.
// For backwards-compatibility it also respects UMPIRE_BIN if UMPIRE_BIN is unset.
// You can pass extra CLI args via UMPIRE_ARGS (or UMPIRE_ARGS).
func TestWatchdogIntegration(t *testing.T) {
	t.Parallel()

	bin := os.Getenv("UMPIRE_BIN")
	if bin == "" {
		// Back-compat with earlier env name used in docs.
		bin = os.Getenv("UMPIRE_BIN")
	}
	if bin == "" {
		t.Skip("set UMPIRE_BIN (or UMPIRE_BIN) to the path of the umpire binary to run this test")
	}

	// Reserve an ephemeral port for umpire to bind to.
	addr, release, err := reserveAddr()
	if err != nil {
		t.Fatalf("failed to reserve address: %v", err)
	}
	defer release()

	// Build arguments.
	args := []string{
		"--listen-addr", addr,
		"--retention", "1m",
		"--check-interval", "200ms",
		"--model", "ingresslatency", // placeholder model that emits softasserts
	}
	if extra := strings.TrimSpace(os.Getenv("UMPIRE_ARGS")); extra != "" {
		args = append(args, splitArgs(extra)...)
	} else if extra := strings.TrimSpace(os.Getenv("UMPIRE_ARGS")); extra != "" {
		args = append(args, splitArgs(extra)...)
	}

	// Start umpire with stdout/stderr capture.
	cmd := exec.Command(bin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("failed to capture stdout: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("failed to capture stderr: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start umpire: %v", err)
	}
	t.Logf("started umpire: %s %s", bin, strings.Join(args, " "))

	// Ensure process is terminated on test end.
	t.Cleanup(func() {
		// Try graceful stop first.
		_ = cmd.Process.Signal(syscall.SIGINT)
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = cmd.Process.Kill()
		}
	})

	// Prepare log scanners and violation detection.
	violationRe := regexp.MustCompile(`failed assertion:`) // prefix used by common/softassert
	var (
		logMu   sync.Mutex
		allLogs []string
	)
	addLog := func(prefix, line string) {
		logMu.Lock()
		allLogs = append(allLogs, fmt.Sprintf("%s%s", prefix, line))
		logMu.Unlock()
	}

	violationCh := make(chan string, 1)
	scan := func(prefix string, s *bufio.Scanner) {
		for s.Scan() {
			line := s.Text()
			addLog(prefix, line)
			if violationRe.MatchString(line) {
				select {
				case violationCh <- line:
				default:
				}
			}
		}
	}

	stdoutScanner := bufio.NewScanner(stdout)
	stderrScanner := bufio.NewScanner(stderr)
	go scan("[stdout] ", stdoutScanner)
	go scan("[stderr] ", stderrScanner)

	// Wait for umpire to bind and accept connections.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := waitForTCP(ctx, addr); err != nil {
		dumpLogs(t, &logMu, allLogs)
		t.Fatalf("umpire did not become ready at %s: %v", addr, err)
	}

	// Send a simple OTEL trace to umpire.
	sendCtx, sendCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer sendCancel()
	if err := sendSpan(sendCtx, addr); err != nil {
		dumpLogs(t, &logMu, allLogs)
		t.Fatalf("failed to send OTEL span: %v", err)
	}

	// Await a softassert violation line in logs.
	select {
	case line := <-violationCh:
		t.Logf("observed softassert violation: %s", line)
	case <-time.After(10 * time.Second):
		dumpLogs(t, &logMu, allLogs)
		t.Fatalf("did not observe a softassert violation in umpire logs within timeout")
	}
}

func reserveAddr() (string, func(), error) {
	// Bind to 127.0.0.1:0 to get a free port, then close it so the child process can bind.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", func() {}, err
	}
	addr := l.Addr().String()
	cleanup := func() { _ = l.Close() }
	// Close immediately to free the port and reduce, but not eliminate, the TOCTOU race.
	_ = l.Close()
	time.Sleep(50 * time.Millisecond)
	return addr, cleanup, nil
}

func waitForTCP(ctx context.Context, addr string) error {
	for {
		dialer := net.Dialer{Timeout: 200 * time.Millisecond}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for %s: %w", addr, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func sendSpan(ctx context.Context, endpoint string) error {
	exp, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithDialOption(grpc.WithBlock()),
	)
	if err != nil {
		return fmt.Errorf("create exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
	)
	defer func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tp.Shutdown(shCtx)
	}()

	otel.SetTracerProvider(tp)
	tr := tp.Tracer("umpire-integration-test")
	_, span := tr.Start(ctx, "test-span")
	span.End()

	// tiny delay to give exporter a chance to send before shutdown
	time.Sleep(50 * time.Millisecond)
	return nil
}

func splitArgs(s string) []string {
	parts := strings.Fields(s)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func dumpLogs(t *testing.T, mu *sync.Mutex, logs []string) {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	if len(logs) == 0 {
		t.Log("no umpire logs captured")
		return
	}
	t.Log("umpire logs:")
	for _, l := range logs {
		t.Log(l)
	}
}
