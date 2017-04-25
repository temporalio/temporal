package matching

import (
	"fmt"
	"sync/atomic"

	"github.com/uber-common/bark"
	s "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
)

const (
	outstandingTaskAppendsThreshold = 250
	maxTaskBatchSize                = 100
)

type (
	writeTaskResponse struct {
		err                 error
		persistenceResponse *persistence.CreateTasksResponse
	}

	writeTaskRequest struct {
		execution  *s.WorkflowExecution
		taskInfo   *persistence.TaskInfo
		rangeID    int64
		responseCh chan<- *writeTaskResponse
	}

	// taskWriter writes tasks sequentially to persistence
	taskWriter struct {
		tlMgr        *taskListManagerImpl
		taskListID   *taskListID
		taskManager  persistence.TaskManager
		appendCh     chan *writeTaskRequest
		maxReadLevel int64
		shutdownCh   chan struct{}
		logger       bark.Logger
	}
)

func newTaskWriter(tlMgr *taskListManagerImpl, shutdownCh chan struct{}) *taskWriter {
	return &taskWriter{
		tlMgr:       tlMgr,
		taskListID:  tlMgr.taskListID,
		taskManager: tlMgr.engine.taskManager,
		shutdownCh:  shutdownCh,
		appendCh:    make(chan *writeTaskRequest, outstandingTaskAppendsThreshold),
		logger:      tlMgr.logger,
	}
}

func (w *taskWriter) Start() {
	w.maxReadLevel = w.tlMgr.getTaskSequenceNumber() - 1
	go w.taskWriterLoop()
}

func (w *taskWriter) appendTask(execution *s.WorkflowExecution,
	taskInfo *persistence.TaskInfo, rangeID int64) (*persistence.CreateTasksResponse, error) {
	ch := make(chan *writeTaskResponse)
	req := &writeTaskRequest{
		execution:  execution,
		taskInfo:   taskInfo,
		rangeID:    rangeID,
		responseCh: ch,
	}

	select {
	case w.appendCh <- req:
		r := <-ch
		return r.persistenceResponse, r.err
	default: // channel is full, throttle
		return nil, createServiceBusyError()
	}
}

func (w *taskWriter) GetMaxReadLevel() int64 {
	return atomic.LoadInt64(&w.maxReadLevel)
}

func (w *taskWriter) taskWriterLoop() {
	defer close(w.appendCh)

writerLoop:
	for {
		select {
		case request := <-w.appendCh:
			{
				// read a batch of requests from the channel
				reqs := []*writeTaskRequest{request}
				reqs = w.getWriteBatch(reqs)
				batchSize := len(reqs)

				maxReadLevel := int64(0)

				taskIDs, err := w.tlMgr.newTaskIDs(batchSize)
				if err != nil {
					w.sendWriteResponse(reqs, err, nil)
					continue writerLoop
				}

				tasks := []*persistence.CreateTaskInfo{}
				rangeID := int64(0)
				for i, req := range reqs {
					tasks = append(tasks, &persistence.CreateTaskInfo{
						TaskID:    taskIDs[i],
						Execution: *req.execution,
						Data:      req.taskInfo,
					})
					if req.rangeID > rangeID {
						rangeID = req.rangeID // use the maximum rangeID provided for the write operation
					}
					maxReadLevel = taskIDs[i]
				}

				r, err := w.taskManager.CreateTasks(&persistence.CreateTasksRequest{
					DomainID:     w.taskListID.domainID,
					TaskList:     w.taskListID.taskListName,
					TaskListType: w.taskListID.taskType,
					Tasks:        tasks,
					// Note that newTaskID could increment range, so rangeID parameter
					// might be out of sync. This is OK as caller can just retry.
					RangeID: rangeID,
				})

				if err != nil {
					logPersistantStoreErrorEvent(w.logger, tagValueStoreOperationCreateTask, err,
						fmt.Sprintf("{taskID: [%v, %v], taskType: %v, taskList: %v}",
							taskIDs[0], taskIDs[batchSize-1], w.taskListID.taskType, w.taskListID.taskListName))
				}

				// Update the maxReadLevel after the writes are completed.
				if maxReadLevel > 0 {
					atomic.StoreInt64(&w.maxReadLevel, maxReadLevel)
				}

				w.sendWriteResponse(reqs, err, r)
			}
		case <-w.shutdownCh:
			break writerLoop
		}
	}
}

func (w *taskWriter) getWriteBatch(reqs []*writeTaskRequest) []*writeTaskRequest {
readLoop:
	for i := 0; i < maxTaskBatchSize; i++ {
		select {
		case req := <-w.appendCh:
			reqs = append(reqs, req)
		default: // channel is empty, don't block
			break readLoop
		}
	}
	return reqs
}

func (w *taskWriter) sendWriteResponse(reqs []*writeTaskRequest,
	err error, persistenceResponse *persistence.CreateTasksResponse) {
	for _, req := range reqs {
		resp := &writeTaskResponse{
			err:                 err,
			persistenceResponse: persistenceResponse,
		}

		req.responseCh <- resp
	}
}
