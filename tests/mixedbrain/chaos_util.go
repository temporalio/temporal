package mixedbrain

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

const (
	defaultProcessChaosInterval = time.Minute
	processChaosIntervalEnv     = "MIXED_BRAIN_PROCESS_CHAOS_INTERVAL"
)

type processChaosTarget struct {
	name string
	proc *serverProcess
}

func processChaosInterval(t *testing.T) time.Duration {
	t.Helper()

	raw := os.Getenv(processChaosIntervalEnv)
	if raw == "" {
		return defaultProcessChaosInterval
	}
	interval, err := time.ParseDuration(raw)
	require.NoError(t, err, "invalid %s", processChaosIntervalEnv)
	require.Greater(t, interval, time.Duration(0), "%s must be positive", processChaosIntervalEnv)
	return interval
}

func startProcessChaos(
	t *testing.T,
	conn *grpc.ClientConn,
	interval time.Duration,
	portSets []portSet,
	targets ...processChaosTarget,
) {
	t.Helper()
	require.NotEmpty(t, targets, "process chaos needs at least one target")

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				done <- nil
				return
			case <-ticker.C:
				target := targets[rand.Intn(len(targets))]
				if err := target.proc.restart(ctx); err != nil {
					done <- ignoreChaosCancel(ctx, fmt.Errorf("restart %s: %w", target.name, err))
					return
				}
				if err := waitForClusterFormationErr(ctx, conn, 90*time.Second, portSets...); err != nil {
					done <- ignoreChaosCancel(ctx, fmt.Errorf("wait after restarting %s: %w", target.name, err))
					return
				}
			}
		}
	}()
}

func ignoreChaosCancel(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	return err
}
