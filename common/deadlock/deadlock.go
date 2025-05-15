package deadlock

import (
	"context"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/pingable"
	"go.uber.org/fx"
	"google.golang.org/grpc/health"
)

type (
	params struct {
		fx.In

		Logger         log.SnTaggedLogger
		Collection     *dynamicconfig.Collection
		HealthServer   *health.Server
		MetricsHandler metrics.Handler

		Roots []pingable.Pingable `group:"deadlockDetectorRoots"`
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
		roots          []pingable.Pingable
		pools          []*goro.AdaptivePool
		loops          goro.Group

		// number of suspected deadlocks that have not resolved yet
		current atomic.Int64
	}

	loopContext struct {
		dd   *deadlockDetector
		root pingable.Pingable
		p    *goro.AdaptivePool
	}
)

// CurrentSuspected returns the number of currently unresolved suspected deadlocks.
func (dd *deadlockDetector) CurrentSuspected() int64 {
	return dd.current.Load()
}

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
		pool := goro.NewAdaptivePool(
			clock.NewRealTimeSource(),
			0,
			dd.config.MaxWorkersPerRoot(),
			100*time.Millisecond,
			10,
		)
		dd.pools = append(dd.pools, pool)
		loopCtx := &loopContext{
			dd:   dd,
			root: root,
			p:    pool,
		}
		dd.loops.Go(loopCtx.run)
	}
	return nil
}

func (dd *deadlockDetector) Stop() error {
	for _, pool := range dd.pools {
		pool.Stop()
	}
	dd.loops.Cancel()
	// don't wait for workers to exit, they may be blocked
	return nil
}

func (dd *deadlockDetector) detected(name string) {
	dd.logger.Error("potential deadlock detected", tag.Name(name))

	metrics.DDSuspectedDeadlocks.With(dd.metricsHandler).Record(1)

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

func (dd *deadlockDetector) adjustCurrent(delta int64) {
	dd.current.Add(delta)
	metrics.DDCurrentSuspectedDeadlocks.With(dd.metricsHandler).Record(float64(dd.current.Load()))
}

func (lc *loopContext) run(ctx context.Context) error {
	for {
		// ping blocks until it has passed all checks to a worker goroutine (using an
		// unbuffered channel).
		lc.ping(ctx, []pingable.Pingable{lc.root})

		timer := time.NewTimer(lc.dd.config.Interval())
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

func (lc *loopContext) ping(ctx context.Context, pingables []pingable.Pingable) {
	for _, pingable := range pingables {
		for _, check := range pingable.GetPingChecks() {
			lc.p.Do(func() { lc.check(ctx, check) })
		}
	}
}

func (lc *loopContext) check(ctx context.Context, check pingable.Check) {
	lc.dd.logger.Debug("starting ping check", tag.Name(check.Name))
	startTime := time.Now().UTC()
	resolved := make(chan struct{})

	// Using AfterFunc is cheaper than creating another goroutine to be the waiter, since
	// we expect to always cancel it. If the go runtime is so messed up that it can't
	// create a goroutine, that's a bigger problem than we can handle.
	t := time.AfterFunc(check.Timeout, func() {
		if ctx.Err() != nil {
			// deadlock detector was stopped
			return
		}
		lc.dd.adjustCurrent(1)

		lc.dd.detected(check.Name)

		// Wait and see if Ping() returns past the timeout. If it's a true deadlock, it'll
		// block here forever.
		<-resolved
		lc.dd.adjustCurrent(-1)
	})
	newPingables := check.Ping()
	t.Stop()
	if len(check.MetricsName) > 0 {
		lc.dd.metricsHandler.Timer(check.MetricsName).Record(time.Since(startTime))
	}
	close(resolved)

	lc.dd.logger.Debug("ping check succeeded", tag.Name(check.Name))

	lc.ping(ctx, newPingables)
}
