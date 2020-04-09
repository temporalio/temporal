package history

import (
	"time"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common/primitives"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/xdc"
)

const (
	historyRereplicationTimeout = 30 * time.Second
)

type (
	timerQueueStandbyProcessorImpl struct {
		shard                   ShardContext
		timerTaskFilter         taskFilter
		logger                  log.Logger
		metricsClient           metrics.Client
		timerGate               RemoteTimerGate
		timerQueueProcessorBase *timerQueueProcessorBase
		taskExecutor            queueTaskExecutor
	}
)

func newTimerQueueStandbyProcessor(
	shard ShardContext,
	historyService *historyEngineImpl,
	clusterName string,
	taskAllocator taskAllocator,
	historyRereplicator xdc.HistoryRereplicator,
	nDCHistoryResender xdc.NDCHistoryResender,
	logger log.Logger,
) *timerQueueStandbyProcessorImpl {

	timeNow := func() time.Time {
		return shard.GetCurrentTime(clusterName)
	}
	updateShardAckLevel := func(ackLevel timerKey) error {
		return shard.UpdateTimerClusterAckLevel(clusterName, ackLevel.VisibilityTimestamp)
	}
	logger = logger.WithTags(tag.ClusterName(clusterName))
	timerTaskFilter := func(taskInfo queueTaskInfo) (bool, error) {
		timer, ok := taskInfo.(*persistenceblobs.TimerTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.verifyStandbyTask(clusterName, primitives.UUIDString(timer.GetNamespaceId()), timer)
	}

	timerGate := NewRemoteTimerGate()
	timerGate.SetCurrentTime(shard.GetCurrentTime(clusterName))
	timerQueueAckMgr := newTimerQueueAckMgr(
		metrics.TimerStandbyQueueProcessorScope,
		shard,
		historyService.metricsClient,
		shard.GetTimerClusterAckLevel(clusterName),
		timeNow,
		updateShardAckLevel,
		logger,
		clusterName,
	)
	processor := &timerQueueStandbyProcessorImpl{
		shard:           shard,
		timerTaskFilter: timerTaskFilter,
		logger:          logger,
		metricsClient:   historyService.metricsClient,
		timerGate:       timerGate,
		timerQueueProcessorBase: newTimerQueueProcessorBase(
			metrics.TimerStandbyQueueProcessorScope,
			shard,
			historyService,
			timerQueueAckMgr,
			timerGate,
			shard.GetConfig().TimerProcessorMaxPollRPS,
			logger,
		),
		taskExecutor: newTimerQueueStandbyTaskExecutor(
			shard,
			historyService,
			historyRereplicator,
			nDCHistoryResender,
			logger,
			historyService.metricsClient,
			clusterName,
			shard.GetConfig(),
		),
	}
	processor.timerQueueProcessorBase.timerProcessor = processor
	return processor
}

func (t *timerQueueStandbyProcessorImpl) Start() {
	t.timerQueueProcessorBase.Start()
}

func (t *timerQueueStandbyProcessorImpl) Stop() {
	t.timerQueueProcessorBase.Stop()
}

//nolint:unused
func (t *timerQueueStandbyProcessorImpl) getTimerFiredCount() uint64 {
	return t.timerQueueProcessorBase.getTimerFiredCount()
}

func (t *timerQueueStandbyProcessorImpl) setCurrentTime(
	currentTime time.Time,
) {

	t.timerGate.SetCurrentTime(currentTime)
}

func (t *timerQueueStandbyProcessorImpl) retryTasks() {
	t.timerQueueProcessorBase.retryTasks()
}

func (t *timerQueueStandbyProcessorImpl) getTaskFilter() taskFilter {
	return t.timerTaskFilter
}

func (t *timerQueueStandbyProcessorImpl) getAckLevel() timerKey {
	return t.timerQueueProcessorBase.timerQueueAckMgr.getAckLevel()
}

//nolint:unused
func (t *timerQueueStandbyProcessorImpl) getReadLevel() timerKey {
	return t.timerQueueProcessorBase.timerQueueAckMgr.getReadLevel()
}

// NotifyNewTimers - Notify the processor about the new standby timer events arrival.
// This should be called each time new timer events arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueStandbyProcessorImpl) notifyNewTimers(
	timerTasks []persistence.Task,
) {

	t.timerQueueProcessorBase.notifyNewTimers(timerTasks)
}

func (t *timerQueueStandbyProcessorImpl) complete(
	taskInfo *taskInfo,
) {
	timerTask, ok := taskInfo.task.(*persistenceblobs.TimerTaskInfo)
	if !ok {
		return
	}
	t.timerQueueProcessorBase.complete(timerTask)
}

func (t *timerQueueStandbyProcessorImpl) process(
	taskInfo *taskInfo,
) (int, error) {
	// TODO: task metricScope should be determined when creating taskInfo
	metricScope := t.timerQueueProcessorBase.getTimerTaskMetricScope(int(taskInfo.task.GetTaskType()), false)
	return metricScope, t.taskExecutor.execute(taskInfo.task, taskInfo.shouldProcessTask)
}
