package rpc

import (
	"context"
	"net/http/httptrace"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type dialTracer struct {
	address        string
	metricsHandler metrics.Handler
	logger         log.Logger
}

// newDialTracer creates a dial tracer that logs errors during dial and produces metrics for different stages of the dial process (Connect)
func newDialTracer(
	address string,
	mh metrics.Handler,
	logger log.Logger,
) *dialTracer {
	l := log.With(
		logger,
		tag.NewStringTag("service", "client"),
		tag.NewStringTag("address", address),
	)

	return &dialTracer{
		address:        address,
		metricsHandler: mh,
		logger:         l,
	}
}

func (d *dialTracer) beginNetworkDial(ctx context.Context) (context.Context, *networkDialTrace) {
	ndt := &networkDialTrace{startedAt: time.Now()}

	// Build a ClientTrace capturing TCP connections.
	trace := &httptrace.ClientTrace{
		ConnectStart: func(_, _ string) {
			ndt.connectStart = time.Now()
		},
		ConnectDone: func(_ string, addr string, err error) {
			// ConnectStart may not have been called if the address is reused
			if !ndt.connectStart.IsZero() {
				ndt.connectDuration = time.Since(ndt.connectStart)
			}
			ndt.connectAddr = addr
			ndt.connectErr = err
		},
	}

	return httptrace.WithClientTrace(ctx, trace), ndt
}

func (d *dialTracer) endNetworkDial(ndt *networkDialTrace, dialErr error) {
	total := time.Since(ndt.startedAt)

	if dialErr != nil {
		fields := []tag.Tag{
			tag.NewDurationTag("totalDuration", total),
			tag.Error(dialErr),
			tag.ErrorType(dialErr),
			tag.NewDurationTag("connectDuration", ndt.connectDuration),
			tag.NewStringTag("connectAddr", ndt.connectAddr),
		}
		if ndt.connectErr != nil {
			fields = append(fields, tag.NewStringTag("connectErr", ndt.connectErr.Error()))
		}
		d.logger.Warn("network dial error", fields...)
		metrics.ServiceDialErrorCount.With(d.metricsHandler).Record(1)
	} else {
		metrics.ServiceDialSuccessCount.With(d.metricsHandler).Record(1)
	}

	if ndt.connectDuration > 0 {
		metrics.ServiceDialLatency.With(d.metricsHandler).Record(ndt.connectDuration)
	}
}

type networkDialTrace struct {
	startedAt time.Time

	connectStart    time.Time
	connectDuration time.Duration
	connectAddr     string
	connectErr      error
}
