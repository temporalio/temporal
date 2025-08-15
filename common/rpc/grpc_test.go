package rpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/grpc/connectivity"
)

// fakeConn simulates the connState interface for testing by stepping through a predefined sequence of states transitions
type fakeConn struct {
	state connectivity.State
	ch    chan connectivity.State // drives state transitions
}

func (f *fakeConn) GetState() connectivity.State { return f.state }

func (f *fakeConn) WaitForStateChange(ctx context.Context, last connectivity.State) bool {
	state := <-f.ch

	if state != last {
		f.state = state
		return true
	}

	return f.WaitForStateChange(ctx, last)
}

// drive drives the sequence of state changes
func (f *fakeConn) drive(states []connectivity.State) {
	for _, s := range states {
		f.ch <- s
	}
}

type metricSnapshot struct {
	attempts       int
	success        int
	errors         int
	latencyRecords int
}

func snapshotMetrics(c *metricstest.Capture) metricSnapshot {
	m := c.Snapshot()
	sum := func(name string) int {
		rs := m[name]
		total := 0
		for _, r := range rs {
			if v, ok := r.Value.(int64); ok {
				total += int(v)
			}
		}
		return total
	}
	return metricSnapshot{
		attempts:       sum("dial_attempts"),
		success:        sum("dial_success"),
		errors:         sum("dial_error"),
		latencyRecords: len(m["dial_connect_latency"]),
	}
}

func TestWatchDialAttempts_Scenarios(t *testing.T) {
	type scenario struct {
		name     string
		states   []connectivity.State
		expected metricSnapshot
	}

	cases := []scenario{
		{
			name:     "success",
			states:   []connectivity.State{connectivity.Connecting, connectivity.Ready, connectivity.Shutdown},
			expected: metricSnapshot{attempts: 1, success: 1, errors: 0, latencyRecords: 1},
		},
		{
			name:     "failure",
			states:   []connectivity.State{connectivity.Connecting, connectivity.TransientFailure, connectivity.Shutdown},
			expected: metricSnapshot{attempts: 1, success: 0, errors: 1, latencyRecords: 1},
		},
		{
			name:     "retry then success",
			states:   []connectivity.State{connectivity.Connecting, connectivity.TransientFailure, connectivity.Connecting, connectivity.Ready, connectivity.Shutdown},
			expected: metricSnapshot{attempts: 2, success: 1, errors: 1, latencyRecords: 2},
		},
		{
			name:     "shutdown mid attempt counts as failure",
			states:   []connectivity.State{connectivity.Connecting, connectivity.Shutdown},
			expected: metricSnapshot{attempts: 1, success: 0, errors: 1, latencyRecords: 1},
		},
		{
			name:     "duplicate connecting does not double count",
			states:   []connectivity.State{connectivity.Connecting, connectivity.Connecting, connectivity.TransientFailure, connectivity.Shutdown},
			expected: metricSnapshot{attempts: 1, success: 0, errors: 1, latencyRecords: 1},
		},
		{
			name:     "ready without connecting",
			states:   []connectivity.State{connectivity.Ready, connectivity.Shutdown},
			expected: metricSnapshot{attempts: 0, success: 0, errors: 0, latencyRecords: 0},
		},
		{
			name:     "transient failure without connecting",
			states:   []connectivity.State{connectivity.TransientFailure, connectivity.Shutdown},
			expected: metricSnapshot{attempts: 0, success: 0, errors: 0, latencyRecords: 0},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := &fakeConn{connectivity.Idle, make(chan connectivity.State, 32)}
			captureHandler := metricstest.NewCaptureHandler()
			metricsCapture := captureHandler.StartCapture()
			defer captureHandler.StopCapture(metricsCapture)

			done := watchDialAttemptsImpl(fc, captureHandler)
			fc.drive(tc.states)
			<-done

			actual := snapshotMetrics(metricsCapture)
			require.Equal(t, tc.expected, actual)
		})
	}
}
