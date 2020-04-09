package archiver

import (
	"time"

	"go.temporal.io/temporal/workflow"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
)

type (
	// Pump pumps archival requests into request channel
	Pump interface {
		Run() PumpResult
	}

	// PumpResult is the result of pumping requests into request channel
	PumpResult struct {
		PumpedHashes          []uint64
		UnhandledCarryover    []ArchiveRequest
		TimeoutWithoutSignals bool
	}

	pump struct {
		ctx           workflow.Context
		logger        log.Logger
		metricsClient metrics.Client
		carryover     []ArchiveRequest
		timeout       time.Duration
		requestLimit  int
		requestCh     workflow.Channel
		signalCh      workflow.Channel
	}
)

// NewPump returns a new Pump
func NewPump(
	ctx workflow.Context,
	logger log.Logger,
	metricsClient metrics.Client,
	carryover []ArchiveRequest,
	timeout time.Duration,
	requestLimit int,
	requestCh workflow.Channel,
	signalCh workflow.Channel,
) Pump {
	return &pump{
		ctx:           ctx,
		logger:        logger,
		metricsClient: metricsClient,
		carryover:     carryover,
		timeout:       timeout,
		requestLimit:  requestLimit,
		requestCh:     requestCh,
		signalCh:      signalCh,
	}
}

// Run pumps requests into request channel.
// Blocks until either timout occurs or request limit is satisfied.
// Returns a PumpResult which contains a summary of what was pumped.
// Upon returning request channel is closed.
func (p *pump) Run() PumpResult {
	sw := p.metricsClient.StartTimer(metrics.ArchiverPumpScope, metrics.ServiceLatency)

	carryoverBoundIndex := len(p.carryover)
	if carryoverBoundIndex > p.requestLimit {
		carryoverBoundIndex = p.requestLimit
	}
	var unhandledCarryover []ArchiveRequest
	for i := carryoverBoundIndex; i < len(p.carryover); i++ {
		unhandledCarryover = append(unhandledCarryover, p.carryover[i])
	}
	p.metricsClient.UpdateGauge(metrics.ArchiverPumpScope, metrics.ArchiverBacklogSizeGauge, float64(len(unhandledCarryover)))
	pumpResult := PumpResult{
		UnhandledCarryover: unhandledCarryover,
	}
	for i := 0; i < carryoverBoundIndex; i++ {
		request := p.carryover[i]
		p.requestCh.Send(p.ctx, request)
		pumpResult.PumpedHashes = append(pumpResult.PumpedHashes, hash(request))
	}
	if len(pumpResult.PumpedHashes) == p.requestLimit {
		sw.Stop()
		p.requestCh.Close()
		return pumpResult
	}
	selector := workflow.NewSelector(p.ctx)
	finished := false
	selector.AddFuture(workflow.NewTimer(p.ctx, p.timeout), func(_ workflow.Future) {
		p.metricsClient.IncCounter(metrics.ArchiverPumpScope, metrics.ArchiverPumpTimeoutCount)
		if len(p.carryover) == len(pumpResult.PumpedHashes) {
			p.metricsClient.IncCounter(metrics.ArchiverPumpScope, metrics.ArchiverPumpTimeoutWithoutSignalsCount)
			pumpResult.TimeoutWithoutSignals = true
		}
		finished = true
	})
	selector.AddReceive(p.signalCh, func(ch workflow.Channel, more bool) {
		if !more {
			p.logger.Error("signal channel channel closed unexpectedly")
			p.metricsClient.IncCounter(metrics.ArchiverPumpScope, metrics.ArchiverPumpSignalChannelClosedCount)
			finished = true
			return
		}
		var request ArchiveRequest
		ch.Receive(p.ctx, &request)
		p.requestCh.Send(p.ctx, request)
		pumpResult.PumpedHashes = append(pumpResult.PumpedHashes, hash(request))
		finished = len(pumpResult.PumpedHashes) == p.requestLimit
		if finished {
			p.metricsClient.IncCounter(metrics.ArchiverPumpScope, metrics.ArchiverPumpSignalThresholdCount)
		}
	})
	for !finished {
		selector.Select(p.ctx)
	}
	sw.Stop()
	p.requestCh.Close()
	return pumpResult
}
