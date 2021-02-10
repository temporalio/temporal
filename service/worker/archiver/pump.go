// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination pump_mock.go

package archiver

import (
	"time"

	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
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
		signalCh      workflow.ReceiveChannel
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
	signalCh workflow.ReceiveChannel,
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
	selector.AddReceive(p.signalCh, func(ch workflow.ReceiveChannel, more bool) {
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
