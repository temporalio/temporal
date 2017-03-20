package matching

import (
	"fmt"

	"github.com/uber-common/bark"
	s "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
)

const (
	outstandingTaskAppendsThreshold = 250
)

type (
	writeTaskResponse struct {
		err                 error
		persistenceResponse *persistence.CreateTaskResponse
	}

	writeTaskRequest struct {
		execution  *s.WorkflowExecution
		taskInfo   *persistence.TaskInfo
		rangeID    int64
		responseCh chan<- *writeTaskResponse
	}

	// taskWriter writes tasks sequentially to persistence
	taskWriter struct {
		tlMgr       *taskListManagerImpl
		taskListID  *taskListID
		taskManager persistence.TaskManager
		appendCh    chan *writeTaskRequest
		shutdownCh  chan struct{}
		logger      bark.Logger
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
	go w.taskWriterLoop()
}

func (w *taskWriter) appendTask(execution *s.WorkflowExecution,
	taskInfo *persistence.TaskInfo, rangeID int64) (*persistence.CreateTaskResponse, error) {
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

func (w *taskWriter) taskWriterLoop() {
	defer close(w.appendCh)

writerLoop:
	for {
		select {
		case req := <-w.appendCh:
			{
				// TODO: write a batch of tasks if more than one is available in the channel,
				// instead of one by one.
				taskID, err := w.tlMgr.newTaskID()
				if err != nil {
					w.sendWriteResponse(req, err, nil)
					continue writerLoop
				}

				r, err := w.taskManager.CreateTask(&persistence.CreateTaskRequest{
					Execution:    *req.execution,
					TaskList:     w.taskListID.taskListName,
					TaskListType: w.taskListID.taskType,
					Data:         req.taskInfo,
					TaskID:       taskID,
					// Note that newTaskID could increment range, so rangeID parameter
					// might be out of sync. This is OK as caller can just retry.
					RangeID: req.rangeID,
				})

				if err != nil {
					logPersistantStoreErrorEvent(w.logger, tagValueStoreOperationCreateTask, err,
						fmt.Sprintf("{taskID: %v, taskType: %v, taskList: %v}",
							taskID, w.taskListID.taskType, w.taskListID.taskListName))
				}

				w.sendWriteResponse(req, err, r)
			}
		case <-w.shutdownCh:
			break writerLoop
		}
	}
}

func (w *taskWriter) sendWriteResponse(req *writeTaskRequest,
	err error, persistenceResponse *persistence.CreateTaskResponse) {
	resp := &writeTaskResponse{
		err:                 err,
		persistenceResponse: persistenceResponse,
	}

	req.responseCh <- resp
}
