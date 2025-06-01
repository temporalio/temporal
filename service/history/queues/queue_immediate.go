package queues

import (
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

var _ Queue = (*immediateQueue)(nil)

type (
	immediateQueue struct {
		*queueBase

		notifyCh chan struct{}
	}
)

func NewImmediateQueue(
	shard historyi.ShardContext,
	category tasks.Category,
	scheduler Scheduler,
	rescheduler Rescheduler,
	options *Options,
	hostRateLimiter quotas.RequestRateLimiter,
	grouper Grouper,
	logger log.Logger,
	metricsHandler metrics.Handler,
	factory ExecutableFactory,
) *immediateQueue {
	paginationFnProvider := func(r Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			ctx, cancel := newQueueIOContext()
			defer cancel()

			request := &persistence.GetHistoryTasksRequest{
				ShardID:             shard.GetShardID(),
				TaskCategory:        category,
				InclusiveMinTaskKey: r.InclusiveMin,
				ExclusiveMaxTaskKey: r.ExclusiveMax,
				BatchSize:           options.BatchSize(),
				NextPageToken:       paginationToken,
			}

			resp, err := shard.GetHistoryTasks(ctx, request)
			if err != nil {
				return nil, nil, err
			}

			return resp.Tasks, resp.NextPageToken, nil
		}
	}

	return &immediateQueue{
		queueBase: newQueueBase(
			shard,
			category,
			paginationFnProvider,
			scheduler,
			rescheduler,
			factory,
			options,
			hostRateLimiter,
			NoopReaderCompletionFn,
			grouper,
			logger,
			metricsHandler,
		),

		notifyCh: make(chan struct{}, 1),
	}
}

func (p *immediateQueue) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	p.logger.Info("", tag.LifeCycleStarting)
	defer p.logger.Info("", tag.LifeCycleStarted)

	p.queueBase.Start()

	p.shutdownWG.Add(1)
	go p.processEventLoop()

	p.notify()
}

func (p *immediateQueue) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	p.logger.Info("", tag.LifeCycleStopping)
	defer p.logger.Info("", tag.LifeCycleStopped)

	close(p.shutdownCh)

	if success := common.AwaitWaitGroup(&p.shutdownWG, time.Minute); !success {
		p.logger.Warn("", tag.LifeCycleStopTimedout)
	}

	p.queueBase.Stop()
}

func (p *immediateQueue) NotifyNewTasks(tasks []tasks.Task) {
	if len(tasks) == 0 {
		return
	}

	p.notify()
}

func (p *immediateQueue) processEventLoop() {
	defer p.shutdownWG.Done()

	pollTimer := time.NewTimer(backoff.Jitter(
		p.options.MaxPollInterval(),
		p.options.MaxPollIntervalJitterCoefficient(),
	))
	defer pollTimer.Stop()

	for {
		select {
		case <-p.shutdownCh:
			return
		default:
		}

		select {
		case <-p.shutdownCh:
			return
		case <-p.notifyCh:
			p.processNewRange()
		case <-pollTimer.C:
			p.processPollTimer(pollTimer)
		case <-p.checkpointTimer.C:
			p.checkpoint()
		case alert := <-p.alertCh:
			p.handleAlert(alert)
		}
	}
}

func (p *immediateQueue) processPollTimer(pollTimer *time.Timer) {
	p.processNewRange()
	pollTimer.Reset(backoff.Jitter(
		p.options.MaxPollInterval(),
		p.options.MaxPollIntervalJitterCoefficient(),
	))
}

func (p *immediateQueue) notify() {
	select {
	case p.notifyCh <- struct{}{}:
	default:
	}
}
