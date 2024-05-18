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

package deadlock

import (
	"context"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/fx"
	"google.golang.org/grpc/health"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/internal/goro"
)

type (
	params struct {
		fx.In

		Logger         log.SnTaggedLogger
		Collection     *dynamicconfig.Collection
		HealthServer   *health.Server
		MetricsHandler metrics.Handler

		Roots []common.Pingable `group:"deadlockDetectorRoots"`
	}

	config struct {
		DumpGoroutines    dynamicconfig.BoolPropertyFn
		FailHealthCheck   dynamicconfig.BoolPropertyFn
		AbortProcess      dynamicconfig.BoolPropertyFn
		Interval          dynamicconfig.DurationPropertyFn
		MaxWorkersPerRoot dynamicconfig.IntPropertyFn
	}

	deadlockDetector struct {
		logger         log.Logger
		healthServer   *health.Server
		metricsHandler metrics.Handler
		config         config
		roots          []common.Pingable
		loops          goro.Group
	}

	loopContext struct {
		dd      *deadlockDetector
		root    common.Pingable
		ch      chan common.PingCheck
		workers int32
	}
)

func NewDeadlockDetector(params params) *deadlockDetector {
	return &deadlockDetector{
		logger:         params.Logger,
		healthServer:   params.HealthServer,
		metricsHandler: params.MetricsHandler.WithTags(metrics.OperationTag(metrics.DeadlockDetectorScope)),
		config: config{
			DumpGoroutines:    dynamicconfig.DeadlockDumpGoroutines.Get(params.Collection),
			FailHealthCheck:   dynamicconfig.DeadlockFailHealthCheck.Get(params.Collection),
			AbortProcess:      dynamicconfig.DeadlockAbortProcess.Get(params.Collection),
			Interval:          dynamicconfig.DeadlockInterval.Get(params.Collection),
			MaxWorkersPerRoot: dynamicconfig.DeadlockMaxWorkersPerRoot.Get(params.Collection),
		},
		roots: params.Roots,
	}
}

func (dd *deadlockDetector) Start() error {
	for _, root := range dd.roots {
		loopCtx := &loopContext{
			dd:   dd,
			root: root,
			ch:   make(chan common.PingCheck),
		}
		dd.loops.Go(loopCtx.run)
	}
	return nil
}

func (dd *deadlockDetector) Stop() error {
	dd.loops.Cancel()
	// don't wait for workers to exit, they may be blocked
	return nil
}

func (dd *deadlockDetector) detected(name string) {
	dd.logger.Error("potential deadlock detected", tag.Name(name))

	if dd.config.DumpGoroutines() {
		dd.dumpGoroutines()
	}

	if dd.config.FailHealthCheck() {
		dd.logger.Info("marking grpc services unhealthy")
		dd.healthServer.Shutdown()
	}

	if dd.config.AbortProcess() {
		dd.logger.Fatal("deadlock detected", tag.Name(name))
	}
}

func (dd *deadlockDetector) dumpGoroutines() {
	profile := pprof.Lookup("goroutine")
	if profile == nil {
		dd.logger.Error("could not find goroutine profile")
		return
	}
	var b strings.Builder
	err := profile.WriteTo(&b, 1) // 1 is magic value that means "text format"
	if err != nil {
		dd.logger.Error("failed to get goroutine profile", tag.Error(err))
		return
	}
	// write it as a single log line with embedded newlines.
	// the value starts with "goroutine profile: total ...\n" so it should be clear
	dd.logger.Info("dumping goroutine profile for suspected deadlock")
	dd.logger.Info(b.String())
}

func (lc *loopContext) run(ctx context.Context) error {
	for {
		// ping blocks until it has passed all checks to a worker goroutine (using an
		// unbuffered channel).
		lc.ping(ctx, []common.Pingable{lc.root})

		timer := time.NewTimer(lc.dd.config.Interval())
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

func (lc *loopContext) ping(ctx context.Context, pingables []common.Pingable) {
	for _, pingable := range pingables {
		for _, check := range pingable.GetPingChecks() {
			select {
			case lc.ch <- check:
			case <-ctx.Done():
				return
			default:
				// maybe add another worker if blocked
				w := atomic.LoadInt32(&lc.workers)
				if w < int32(lc.dd.config.MaxWorkersPerRoot()) && atomic.CompareAndSwapInt32(&lc.workers, w, w+1) {
					lc.dd.loops.Go(lc.worker)
				}
				// blocking send
				select {
				case lc.ch <- check:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (lc *loopContext) worker(ctx context.Context) error {
	for {
		var check common.PingCheck
		select {
		case check = <-lc.ch:
		case <-ctx.Done():
			return nil
		}

		lc.dd.logger.Debug("starting ping check", tag.Name(check.Name))
		startTime := time.Now().UTC()

		// Using AfterFunc is cheaper than creating another goroutine to be the waiter, since
		// we expect to always cancel it. If the go runtime is so messed up that it can't
		// create a goroutine, that's a bigger problem than we can handle.
		t := time.AfterFunc(check.Timeout, func() {
			if ctx.Err() != nil {
				// deadlock detector was stopped
				return
			}
			lc.dd.detected(check.Name)
		})
		newPingables := check.Ping()
		t.Stop()
		if len(check.MetricsName) > 0 {
			lc.dd.metricsHandler.Timer(check.MetricsName).Record(time.Since(startTime))
		}

		lc.dd.logger.Debug("ping check succeeded", tag.Name(check.Name))

		lc.ping(ctx, newPingables)
	}
}
