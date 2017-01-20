package history

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"

	m "code.uber.internal/devexp/minions/.gen/go/matching"
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/client/matching"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/persistence"
	"code.uber.internal/devexp/minions/common/util"
)

const (
	transferTaskBatchSize              = 10
	transferProcessorMinPollInterval   = 10 * time.Millisecond
	transferProcessorMaxPollInterval   = 10 * time.Second
	transferProcessorUpdateAckInterval = time.Second
	taskWorkerCount                    = 10
)

type (
	transferQueueProcessorImpl struct {
		ackMgr           *ackManager
		executionManager persistence.ExecutionManager
		matchingClient   matching.Client
		isStarted        int32
		isStopped        int32
		shutdownWG       sync.WaitGroup
		shutdownCh       chan struct{}
		logger           bark.Logger
	}

	// ackManager is created by transferQueueProcessor to keep track of the transfer queue ackLevel for the shard.
	// It keeps track of read level when dispatching transfer tasks to processor and maintains a map of outstanding tasks.
	// Outstanding tasks map uses the task id sequencer as the key, which is used by updateAckLevel to move the ack level
	// for the shard when all preceding tasks are acknowledged.
	ackManager struct {
		shard               ShardContext
		executionMgr        persistence.ExecutionManager
		logger              bark.Logger
		lk                  sync.RWMutex
		outstandingTasks    map[int64]bool
		readLevel           int64
		ackLevel            int64
		maxAllowedReadLevel int64
	}

	taskInfoWithLevel struct {
		readLevel int64
		taskInfo  *persistence.TaskInfo
	}
)

func newTransferQueueProcessor(shard ShardContext, executionManager persistence.ExecutionManager,
	matching matching.Client, logger bark.Logger) transferQueueProcessor {
	return &transferQueueProcessorImpl{
		ackMgr:           newAckManager(shard, executionManager, logger),
		executionManager: executionManager,
		matchingClient:   matching,
		shutdownCh:       make(chan struct{}),
		logger:           logger,
	}
}

func newAckManager(shard ShardContext, executionMgr persistence.ExecutionManager, logger bark.Logger) *ackManager {
	ackLevel := shard.GetTransferAckLevel()
	return &ackManager{
		shard:               shard,
		executionMgr:        executionMgr,
		outstandingTasks:    make(map[int64]bool),
		readLevel:           ackLevel,
		ackLevel:            ackLevel,
		maxAllowedReadLevel: shard.GetTransferSequenceNumber() - 1,
		logger:              logger,
	}
}

func (t *transferQueueProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.isStarted, 0, 1) {
		return
	}

	t.shutdownWG.Add(1)
	go t.processorPump()

	t.logger.Info("Transfer queue processor started.")
}

func (t *transferQueueProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&t.isStopped, 0, 1) {
		return
	}

	if atomic.LoadInt32(&t.isStarted) == 1 {
		close(t.shutdownCh)
	}

	if success := util.AwaitWaitGroup(&t.shutdownWG, time.Minute); !success {
		t.logger.Warn("Transfer queue processor timed out on shutdown.")
	}

	t.logger.Info("Transfer queue processor stopped.")
}

func (t *transferQueueProcessorImpl) UpdateMaxAllowedReadLevel(maxAllowedReadLevel int64) {
	t.ackMgr.updateMaxAllowedReadLevel(maxAllowedReadLevel)
}

func (t *transferQueueProcessorImpl) processorPump() {
	defer t.shutdownWG.Done()
	tasksCh := make(chan *persistence.TransferTaskInfo, transferTaskBatchSize)

	var workerWG sync.WaitGroup
	for i := 0; i < taskWorkerCount; i++ {
		workerWG.Add(1)
		go t.taskWorker(tasksCh, &workerWG)
	}

	pollInterval := transferProcessorMinPollInterval
	pollTimer := time.NewTimer(pollInterval)
	defer pollTimer.Stop()
	updateAckTimer := time.NewTimer(transferProcessorUpdateAckInterval)
	defer updateAckTimer.Stop()
	for {
		select {
		case <-t.shutdownCh:
			t.logger.Info("Transfer queue processor pump shutting down.")
			// This is the only pump which writes to tasksCh, so it is safe to close channel here
			close(tasksCh)
			if success := util.AwaitWaitGroup(&workerWG, 10*time.Second); !success {
				t.logger.Warn("Transfer queue processor timed out on worker shutdown.")
			}
			return
		case <-pollTimer.C:
			pollInterval = t.processTransferTasks(tasksCh, pollInterval)
			pollTimer = time.NewTimer(pollInterval)
		case <-updateAckTimer.C:
			t.ackMgr.updateAckLevel()
			updateAckTimer = time.NewTimer(transferProcessorUpdateAckInterval)
		}
	}
}

func (t *transferQueueProcessorImpl) processTransferTasks(tasksCh chan<- *persistence.TransferTaskInfo,
	prevPollInterval time.Duration) time.Duration {
	tasks, err := t.ackMgr.readTransferTasks()

	if err != nil {
		t.logger.Warnf("Processor unable to retrieve transfer tasks: %v", err)
		return minDuration(2*prevPollInterval, transferProcessorMaxPollInterval)
	}

	if len(tasks) == 0 {
		return minDuration(2*prevPollInterval, transferProcessorMaxPollInterval)
	}

	for _, tsk := range tasks {
		tasksCh <- tsk
	}

	return transferProcessorMinPollInterval
}

func (t *transferQueueProcessorImpl) taskWorker(tasksCh <-chan *persistence.TransferTaskInfo, workerWG *sync.WaitGroup) {
	defer workerWG.Done()
	for {
		select {
		case task, ok := <-tasksCh:
			if !ok {
				return
			}

			t.processTransferTask(task)
		}
	}
}

func (t *transferQueueProcessorImpl) processTransferTask(task *persistence.TransferTaskInfo) {
	t.logger.Debugf("Processing transfer task: %v", task.TaskID)
ProcessRetryLoop:
	for retryCount := 0; retryCount < 10; retryCount++ {
		select {
		case <-t.shutdownCh:
			return
		default:
			var err error
			execution := workflow.WorkflowExecution{WorkflowId: common.StringPtr(task.WorkflowID),
				RunId: common.StringPtr(task.RunID)}
			switch task.TaskType {
			case persistence.TaskTypeActivity:
				{
					taskList := &workflow.TaskList{
						Name: &task.TaskList,
					}
					err = t.matchingClient.AddActivityTask(&m.AddActivityTaskRequest{
						Execution:  &execution,
						TaskList:   taskList,
						ScheduleId: &task.ScheduleID,
					})
				}
			case persistence.TaskTypeDecision:
				{
					taskList := &workflow.TaskList{
						Name: &task.TaskList,
					}
					err = t.matchingClient.AddDecisionTask(&m.AddDecisionTaskRequest{
						Execution:  &execution,
						TaskList:   taskList,
						ScheduleId: &task.ScheduleID,
					})
				}
			}

			if err != nil {
				t.logger.WithField("error", err).Warn("Processor failed to create task")
				time.Sleep(100 * time.Millisecond)
				continue ProcessRetryLoop
			}

			t.ackMgr.completeTask(task.TaskID)
			return
		}
	}

	// All attempts to process transfer task failed.  We won't be able to move the ackLevel so panic
	t.logger.Fatalf("Retry count exceeded for transfer taskID: %v", task.TaskID)
}

func (a *ackManager) readTransferTasks() ([]*persistence.TransferTaskInfo, error) {
	response, err := a.executionMgr.GetTransferTasks(&persistence.GetTransferTasksRequest{
		ReadLevel:    atomic.LoadInt64(&a.readLevel),
		MaxReadLevel: atomic.LoadInt64(&a.maxAllowedReadLevel),
		BatchSize:    transferTaskBatchSize,
		RangeID:      a.shard.GetRangeID(),
	})

	if err != nil {
		return nil, err
	}

	tasks := response.Tasks
	if len(tasks) == 0 {
		return tasks, nil
	}

	a.lk.Lock()
	for _, task := range tasks {
		if a.readLevel >= task.TaskID {
			a.logger.Fatalf("Next task ID is less than current read level.  TaskID: %v, ReadLevel: %v", task.TaskID,
				a.readLevel)
		}
		a.logger.Debugf("Moving read level: %v", task.TaskID)
		a.readLevel = task.TaskID
		a.outstandingTasks[a.readLevel] = false
	}
	a.lk.Unlock()

	return tasks, nil
}

func (a *ackManager) completeTask(taskID int64) {
	a.lk.Lock()
	if _, ok := a.outstandingTasks[taskID]; ok {
		a.outstandingTasks[taskID] = true
	}
	a.lk.Unlock()
}

func (a *ackManager) updateAckLevel() {
	updatedAckLevel := int64(-1)
	a.lk.Lock()
MoveAckLevelLoop:
	for current := a.ackLevel + 1; current <= a.readLevel; current++ {
		if acked, ok := a.outstandingTasks[current]; ok {
			if acked {
				err := a.executionMgr.CompleteTransferTask(&persistence.CompleteTransferTaskRequest{
					Execution: workflow.WorkflowExecution{
						WorkflowId: common.StringPtr(uuid.New()),
						RunId:      common.StringPtr(uuid.New()),
					},
					TaskID: current,
				})

				if err != nil {
					a.logger.Warnf("Processor unable to complete transfer task '%v': %v", current, err)
					break MoveAckLevelLoop
				}
				a.logger.Debugf("Updating ack level: %v", current)
				a.ackLevel = current
				updatedAckLevel = current
				delete(a.outstandingTasks, current)
			} else {
				break MoveAckLevelLoop
			}
		}
	}
	a.lk.Unlock()

	if updatedAckLevel != -1 {
		a.shard.UpdateAckLevel(updatedAckLevel)
	}
}

func (a *ackManager) updateMaxAllowedReadLevel(maxAllowedReadLevel int64) {
	a.lk.Lock()
	a.logger.Debugf("Updating max allowed read level for transfer tasks: %v", maxAllowedReadLevel)
	if maxAllowedReadLevel > atomic.LoadInt64(&a.maxAllowedReadLevel) {
		atomic.StoreInt64(&a.maxAllowedReadLevel, maxAllowedReadLevel)
	}
	a.lk.Unlock()
}

func minDuration(x, y time.Duration) time.Duration {
	if x < y {
		return x
	}

	return y
}
