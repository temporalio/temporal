package rpc

import (
	"context"
	"net/http/httptrace"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

func TestDialTracer_DialSuccess(t *testing.T) {
	t.Parallel()

	addr := "127.0.0.1:1234"
	dt := newDialTracer(addr, metrics.NoopMetricsHandler, log.NewNoopLogger())

	ctx := context.Background()
	ctx, ndt := dt.beginNetworkDial(ctx)

	trace := httptrace.ContextClientTrace(ctx)
	require.NotNil(t, trace)

	trace.ConnectStart("tcp", addr)
	rewind(&ndt.connectStart)
	trace.ConnectDone("tcp", addr, nil)

	dt.endNetworkDial(ndt, nil)

	assert.Positive(t, ndt.connectDuration)
	assert.Equal(t, addr, ndt.connectAddr)
	assert.Nil(t, ndt.connectErr)
}

func TestDialTracer_DialError(t *testing.T) {
	t.Parallel()

	addr := "127.0.0.1:1234"
	dt := newDialTracer(addr, metrics.NoopMetricsHandler, log.NewNoopLogger())

	ctx := context.Background()
	ctx, ndt := dt.beginNetworkDial(ctx)

	trace := httptrace.ContextClientTrace(ctx)
	require.NotNil(t, trace)

	connectErr := assert.AnError
	trace.ConnectStart("tcp", addr)
	rewind(&ndt.connectStart)
	trace.ConnectDone("tcp", addr, connectErr)

	dt.endNetworkDial(ndt, nil)

	assert.Positive(t, ndt.connectDuration)
	assert.Equal(t, addr, ndt.connectAddr)
	assert.Equal(t, connectErr, ndt.connectErr)
}

func TestDialTracer_NoConnectStart(t *testing.T) {
	t.Parallel()

	addr := "127.0.0.1:1234"
	dt := newDialTracer(addr, metrics.NoopMetricsHandler, log.NewNoopLogger())

	ctx := context.Background()
	ctx, ndt := dt.beginNetworkDial(ctx)

	trace := httptrace.ContextClientTrace(ctx)
	require.NotNil(t, trace)

	trace.ConnectDone("tcp", addr, nil)

	dt.endNetworkDial(ndt, nil)

	assert.Zero(t, ndt.connectDuration)
	assert.Equal(t, addr, ndt.connectAddr)
	assert.Nil(t, ndt.connectErr)
}

// rewind moves a timestamp back by 1 millisecond to ensure positive durations in tests
func rewind(ts *time.Time) {
	*ts = ts.Add(-time.Millisecond)
}
