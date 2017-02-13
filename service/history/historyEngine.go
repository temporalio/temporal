package history

import (
	"errors"
	"fmt"
	"sync"

	h "code.uber.internal/devexp/minions/.gen/go/history"
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/client/matching"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/backoff"
	"code.uber.internal/devexp/minions/common/persistence"
	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
)

const (
	conditionalRetryCount = 5
)

type (
	historyEngineImpl struct {
		shard            ShardContext
		executionManager persistence.ExecutionManager
		txProcessor      transferQueueProcessor
		timerProcessor   timerQueueProcessor
		tokenSerializer  common.TaskTokenSerializer
		tracker          *pendingTaskTracker
		logger           bark.Logger
	}

	workflowExecutionContext struct {
		workflowExecution workflow.WorkflowExecution
		builder           *historyBuilder
		executionInfo     *persistence.WorkflowExecutionInfo
		historyService    *historyEngineImpl
		updateCondition   int64
		logger            bark.Logger
		tBuilder          *timerBuilder
		deleteTimerTask   persistence.Task
		msBuilder         *mutableStateBuilder
	}

	pendingTaskTracker struct {
		shard        ShardContext
		txProcessor  transferQueueProcessor
		logger       bark.Logger
		lk           sync.RWMutex
		pendingTasks map[int64]bool
		minID        int64
		maxID        int64
	}
)

var _ Engine = (*historyEngineImpl)(nil)

var (
	persistenceOperationRetryPolicy = common.CreatePersistanceRetryPolicy()

	// ErrDuplicate is exported temporarily for integration test
	ErrDuplicate = errors.New("Duplicate task, completing it")
	// ErrCreateEvent is exported temporarily for integration test
	ErrCreateEvent = errors.New("Can't create activity task started event")
	// ErrConflict is exported temporarily for integration test
	ErrConflict = errors.New("Conditional update failed")
	// ErrMaxAttemptsExceeded is exported temporarily for integration test
	ErrMaxAttemptsExceeded = errors.New("Maximum attempts exceeded to update history")
)

func newPendingTaskTracker(shard ShardContext, txProcessor transferQueueProcessor,
	logger bark.Logger) *pendingTaskTracker {
	return &pendingTaskTracker{
		shard:        shard,
		txProcessor:  txProcessor,
		pendingTasks: make(map[int64]bool),
		minID:        shard.GetTransferSequenceNumber(),
		maxID:        shard.GetTransferSequenceNumber(),
		logger:       logger,
	}
}

// NewEngineWithShardContext creates an instance of history engine
func NewEngineWithShardContext(shard ShardContext, matching matching.Client) Engine {
	logger := shard.GetLogger()
	executionManager := shard.GetExecutionManager()
	txProcessor := newTransferQueueProcessor(shard, matching)
	tracker := newPendingTaskTracker(shard, txProcessor, logger)
	historyEngImpl := &historyEngineImpl{
		shard:            shard,
		executionManager: executionManager,
		txProcessor:      txProcessor,
		tokenSerializer:  common.NewJSONTaskTokenSerializer(),
		tracker:          tracker,
		logger: logger.WithFields(bark.Fields{
			tagWorkflowComponent: tagValueHistoryEngineComponent,
		}),
	}
	historyEngImpl.timerProcessor = newTimerQueueProcessor(historyEngImpl, executionManager, logger)
	return historyEngImpl
}

// Start will spin up all the components needed to start serving this shard.
// Make sure all the components are loaded lazily so start can return immediately.  This is important because
// ShardController calls start sequentially for all the shards for a given host during startup.
func (e *historyEngineImpl) Start() {
	e.txProcessor.Start()
	e.timerProcessor.Start()
}

// Stop the service.
func (e *historyEngineImpl) Stop() {
	e.txProcessor.Stop()
	e.timerProcessor.Stop()
}

// StartWorkflowExecution starts a workflow execution
func (e *historyEngineImpl) StartWorkflowExecution(request *workflow.StartWorkflowExecutionRequest) (
	*workflow.StartWorkflowExecutionResponse, error) {
	executionID := request.GetWorkflowId()
	runID := uuid.New()
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(executionID),
		RunId:      common.StringPtr(runID),
	}

	// Generate first decision task event.
	taskList := request.GetTaskList().GetName()
	builder := newHistoryBuilder(e.logger)
	builder.AddWorkflowExecutionStartedEvent(request)
	dt := builder.AddDecisionTaskScheduledEvent(taskList, request.GetTaskStartToCloseTimeoutSeconds())

	// Serialize the history
	h, serializedError := builder.Serialize()
	if serializedError != nil {
		logHistorySerializationErrorEvent(e.logger, serializedError, fmt.Sprintf(
			"History serialization error on start workflow.  WorkflowID: %v, RunID: %v", executionID, runID))
		return nil, serializedError
	}

	id := e.tracker.getNextTaskID()
	defer e.tracker.completeTask(id)
	_, err := e.executionManager.CreateWorkflowExecution(&persistence.CreateWorkflowExecutionRequest{
		Execution:          workflowExecution,
		TaskList:           request.GetTaskList().GetName(),
		History:            h,
		ExecutionContext:   nil,
		NextEventID:        builder.nextEventID,
		LastProcessedEvent: 0,
		TransferTasks: []persistence.Task{&persistence.DecisionTask{
			TaskID:   id,
			TaskList: taskList, ScheduleID: dt.GetEventId(),
		}},
		RangeID: e.shard.GetRangeID(),
	})

	if err != nil {
		logPersistantStoreErrorEvent(e.logger, tagValueStoreOperationCreateWorkflowExecution, err,
			fmt.Sprintf("{WorkflowID: %v, RunID: %v}", executionID, runID))
		return nil, err
	}

	return &workflow.StartWorkflowExecutionResponse{
		RunId: workflowExecution.RunId,
	}, nil
}

// GetWorkflowExecutionHistory retrieves the history for given workflow execution
func (e *historyEngineImpl) GetWorkflowExecutionHistory(
	request *workflow.GetWorkflowExecutionHistoryRequest) (*workflow.GetWorkflowExecutionHistoryResponse, error) {
	r := &persistence.GetWorkflowExecutionRequest{
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(request.GetExecution().GetWorkflowId()),
			RunId:      common.StringPtr(request.GetExecution().GetRunId()),
		},
	}

	response, err := e.getWorkflowExecutionWithRetry(r)
	if err != nil {
		return nil, err
	}

	builder := newHistoryBuilder(e.logger)
	if err := builder.loadExecutionInfo(response.ExecutionInfo); err != nil {
		return nil, err
	}

	result := workflow.NewGetWorkflowExecutionHistoryResponse()
	result.History = builder.getHistory()

	return result, nil
}

func (e *historyEngineImpl) RecordDecisionTaskStarted(
	request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error) {
	context := newWorkflowExecutionContext(e, *request.WorkflowExecution)
	scheduleID := request.GetScheduleId()
	requestID := request.GetRequestId()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		builder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return nil, err1
		}

		// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
		// task is not outstanding than it is most probably a duplicate and complete the task.
		isRunning, startedEvent := builder.isDecisionTaskRunning(scheduleID)

		if !isRunning {
			// Looks like DecisionTask already completed as a result of another call.
			// It is OK to drop the task at this point.
			logDuplicateTaskEvent(context.logger, persistence.TaskListTypeDecision, request.GetTaskId(), requestID,
				scheduleID, emptyEventID, isRunning)

			return nil, &workflow.EntityNotExistsError{Message: "Decision task not found."}
		}

		if startedEvent != nil {
			// If decision is started as part of the current request scope then return a positive response
			if startedEvent.GetDecisionTaskStartedEventAttributes().GetRequestId() == requestID {
				return e.createRecordDecisionTaskStartedResponse(context, startedEvent), nil
			}

			// Looks like DecisionTask already started as a result of another call.
			// It is OK to drop the task at this point.
			logDuplicateTaskEvent(context.logger, persistence.TaskListTypeDecision, request.GetTaskId(), requestID,
				scheduleID, startedEvent.GetEventId(), isRunning)

			return nil, &workflow.EntityNotExistsError{Message: "Decision task already started."}
		}

		event := builder.AddDecisionTaskStartedEvent(scheduleID, requestID, request.PollRequest)
		if event == nil {
			// Let's retry and see if the decision still exist.
			continue Update_History_Loop
		}

		// Start a timer for the decision task.
		defer e.timerProcessor.NotifyNewTimer()
		timeOutTask := context.tBuilder.AddDecisionTimoutTask(scheduleID, builder)
		timerTasks := []persistence.Task{timeOutTask}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		if err2 := context.updateWorkflowExecution(nil, timerTasks); err2 != nil {
			if err2 == ErrConflict {
				continue Update_History_Loop
			}

			return nil, err2
		}

		return e.createRecordDecisionTaskStartedResponse(context, event), nil
	}

	return nil, ErrMaxAttemptsExceeded
}

func (e *historyEngineImpl) RecordActivityTaskStarted(
	request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error) {
	context := newWorkflowExecutionContext(e, *request.WorkflowExecution)
	scheduleID := request.GetScheduleId()
	requestID := request.GetRequestId()

	var builder *historyBuilder
Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err0 := context.loadWorkflowMutableState()
		if err0 != nil {
			return nil, err0
		}

		// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
		// task is not outstanding than it is most probably a duplicate and complete the task.
		isRunning, ai := msBuilder.isActivityRunning(scheduleID)
		if !isRunning {
			// Looks like ActivityTask already completed as a result of another call.
			// It is OK to drop the task at this point.
			logDuplicateTaskEvent(context.logger, persistence.TaskListTypeActivity, request.GetTaskId(), requestID,
				scheduleID, emptyEventID, isRunning)

			return nil, &workflow.EntityNotExistsError{Message: "Activity task not found."}
		}

		if ai.StartedID != emptyEventID {
			// If activity is started as part of the current request scope then return a positive response
			if builder != nil && ai.RequestID == requestID {
				response := h.NewRecordActivityTaskStartedResponse()
				response.StartedEvent = builder.GetEvent(ai.StartedID)
				response.ScheduledEvent = builder.GetEvent(scheduleID)
				return response, nil
			}

			// Looks like ActivityTask already started as a result of another call.
			// It is OK to drop the task at this point.
			logDuplicateTaskEvent(context.logger, persistence.TaskListTypeActivity, request.GetTaskId(), requestID,
				scheduleID, ai.StartedID, isRunning)

			return nil, &workflow.EntityNotExistsError{Message: "Activity task already started."}
		}

		var err1 error
		builder, err1 = context.loadWorkflowExecution()
		if err1 != nil {
			return nil, err1
		}

		event := builder.AddActivityTaskStartedEvent(scheduleID, requestID, request.PollRequest)
		if event == nil {
			// Let's retry and see if the activity still exist.
			continue Update_History_Loop
		}

		// Start a timer for the activity task.
		timerTasks := []persistence.Task{}
		defer e.timerProcessor.NotifyNewTimer()
		start2CloseTimeoutTask, err := context.tBuilder.AddStartToCloseActivityTimeout(scheduleID, msBuilder)
		if err != nil {
			return nil, err
		}
		timerTasks = append(timerTasks, start2CloseTimeoutTask)
		start2HeartBeatTimeoutTask, err := context.tBuilder.AddHeartBeatActivityTimeout(scheduleID, msBuilder)
		if err != nil {
			return nil, err
		}
		if start2HeartBeatTimeoutTask != nil {
			timerTasks = append(timerTasks, start2HeartBeatTimeoutTask)
		}

		ai.StartedID = event.GetEventId()
		ai.RequestID = requestID
		msBuilder.UpdatePendingActivity(scheduleID, ai)

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operationi again.
		if err2 := context.updateWorkflowExecution(nil, timerTasks); err2 != nil {
			if err2 == ErrConflict {
				continue Update_History_Loop
			}

			return nil, err2
		}

		response := h.NewRecordActivityTaskStartedResponse()
		response.StartedEvent = event
		response.ScheduledEvent = builder.GetEvent(scheduleID)
		return response, nil
	}

	return nil, ErrMaxAttemptsExceeded
}

// RespondDecisionTaskCompleted completes a decision task
func (e *historyEngineImpl) RespondDecisionTaskCompleted(request *workflow.RespondDecisionTaskCompletedRequest) error {
	token, err0 := e.tokenSerializer.Deserialize(request.GetTaskToken())
	if err0 != nil {
		return &workflow.BadRequestError{Message: "Error deserializing task token."}
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	context := newWorkflowExecutionContext(e, workflowExecution)

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		builder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}

		msBuilder, err1 := context.loadWorkflowMutableState()
		if err1 != nil {
			return err1
		}

		scheduleID := token.ScheduleID
		isRunning, startedEvent := builder.isDecisionTaskRunning(scheduleID)
		if !isRunning || startedEvent == nil {
			return &workflow.EntityNotExistsError{Message: "Decision task not found."}
		}

		startedID := startedEvent.GetEventId()
		completedEvent := builder.AddDecisionTaskCompletedEvent(scheduleID, startedID, request)
		completedID := completedEvent.GetEventId()
		isComplete := false
		transferTasks := []persistence.Task{}
		timerTasks := []persistence.Task{}

	Process_Decision_Loop:
		for _, d := range request.Decisions {
			switch d.GetDecisionType() {
			case workflow.DecisionType_ScheduleActivityTask:
				attributes := d.GetScheduleActivityTaskDecisionAttributes()
				scheduleEvent := builder.AddActivityTaskScheduledEvent(completedID, attributes)
				id := e.tracker.getNextTaskID()
				defer e.tracker.completeTask(id)
				transferTasks = append(transferTasks, &persistence.ActivityTask{
					TaskID:     id,
					TaskList:   attributes.GetTaskList().GetName(),
					ScheduleID: scheduleEvent.GetEventId(),
				})

				// Create activity timeouts.
				defer e.timerProcessor.NotifyNewTimer()
				Schedule2StartTimeoutTask := context.tBuilder.AddScheduleToStartActivityTimeout(
					scheduleEvent.GetEventId(), scheduleEvent, msBuilder)
				timerTasks = append(timerTasks, Schedule2StartTimeoutTask)

				Schedule2CloseTimeoutTask, err := context.tBuilder.AddScheduleToCloseActivityTimeout(
					scheduleEvent.GetEventId(), msBuilder)
				if err != nil {
					return err
				}
				timerTasks = append(timerTasks, Schedule2CloseTimeoutTask)

			case workflow.DecisionType_CompleteWorkflowExecution:
				if isComplete || builder.hasPendingTasks() {
					builder.AddCompleteWorkflowExecutionFailedEvent(completedID,
						workflow.WorkflowCompleteFailedCause_UNHANDLED_DECISION)
					continue Process_Decision_Loop
				}
				attributes := d.GetCompleteWorkflowExecutionDecisionAttributes()
				builder.AddCompletedWorkflowEvent(completedID, attributes)
				isComplete = true
			case workflow.DecisionType_FailWorkflowExecution:
				if isComplete || builder.hasPendingTasks() {
					builder.AddCompleteWorkflowExecutionFailedEvent(completedID,
						workflow.WorkflowCompleteFailedCause_UNHANDLED_DECISION)
					continue Process_Decision_Loop
				}
				attributes := d.GetFailWorkflowExecutionDecisionAttributes()
				builder.AddFailWorkflowEvent(completedID, attributes)
				isComplete = true
			case workflow.DecisionType_StartTimer:
				attributes := d.GetStartTimerDecisionAttributes()
				startTimerEvent := builder.AddTimerStartedEvent(completedID, attributes)
				nextTimerTask, err := context.tBuilder.AddUserTimer(attributes.GetTimerId(), attributes.GetStartToFireTimeoutSeconds(),
					startTimerEvent.GetEventId(), msBuilder)
				if err != nil {
					return err
				}
				if nextTimerTask != nil {
					timerTasks = append(timerTasks, nextTimerTask)
				}

			default:
				return &workflow.BadRequestError{Message: fmt.Sprintf("Unknown decision type: %v", d.GetDecisionType())}
			}
		}

		// Schedule another decision task if new events came in during this decision
		if (completedID - startedID) > 1 {
			newDecisionEvent := builder.ScheduleDecisionTask()
			id := e.tracker.getNextTaskID()
			defer e.tracker.completeTask(id)
			transferTasks = append(transferTasks, &persistence.DecisionTask{
				TaskID:     id,
				TaskList:   newDecisionEvent.GetDecisionTaskScheduledEventAttributes().GetTaskList().GetName(),
				ScheduleID: newDecisionEvent.GetEventId(),
			})
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
		// the history and try the operation again.
		if err := context.updateWorkflowExecutionWithContext(request.GetExecutionContext(), transferTasks, timerTasks); err != nil {
			if err == ErrConflict {
				continue Update_History_Loop
			}

			return err
		}

		if isComplete {
			// TODO: We need to keep completed executions for auditing purpose.  Need a design for keeping them around
			// for visibility purpose.
			context.deleteWorkflowExecution()
		}

		return nil
	}

	return ErrMaxAttemptsExceeded
}

// RespondActivityTaskCompleted completes an activity task.
func (e *historyEngineImpl) RespondActivityTaskCompleted(request *workflow.RespondActivityTaskCompletedRequest) error {
	token, err0 := e.tokenSerializer.Deserialize(request.GetTaskToken())
	if err0 != nil {
		return &workflow.BadRequestError{Message: "Error deserializing task token."}
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	context := newWorkflowExecutionContext(e, workflowExecution)

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		builder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}

		msBuilder, err1 := context.loadWorkflowMutableState()
		if err1 != nil {
			return err1
		}

		scheduleID := token.ScheduleID
		isRunning, startedEvent := builder.isActivityTaskRunning(scheduleID)
		if !isRunning || startedEvent == nil {
			return &workflow.EntityNotExistsError{Message: "Activity task not found."}
		}

		startedID := startedEvent.GetEventId()
		if builder.AddActivityTaskCompletedEvent(scheduleID, startedID, request) == nil {
			// Let's retry and see if the activity still exist.
			continue Update_History_Loop
		}

		msBuilder.DeletePendingActivity(scheduleID)

		var transferTasks []persistence.Task
		if !builder.hasPendingDecisionTask() {
			newDecisionEvent := builder.ScheduleDecisionTask()
			id := e.tracker.getNextTaskID()
			defer e.tracker.completeTask(id)
			transferTasks = []persistence.Task{&persistence.DecisionTask{
				TaskID:     id,
				TaskList:   newDecisionEvent.GetDecisionTaskScheduledEventAttributes().GetTaskList().GetName(),
				ScheduleID: newDecisionEvent.GetEventId(),
			}}
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		if err := context.updateWorkflowExecution(transferTasks, nil); err != nil {
			if err == ErrConflict {
				continue Update_History_Loop
			}

			return err
		}

		return nil
	}

	return ErrMaxAttemptsExceeded
}

// RespondActivityTaskFailed completes an activity task failure.
func (e *historyEngineImpl) RespondActivityTaskFailed(request *workflow.RespondActivityTaskFailedRequest) error {
	token, err0 := e.tokenSerializer.Deserialize(request.GetTaskToken())
	if err0 != nil {
		return &workflow.BadRequestError{Message: "Error deserializing task token."}
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	context := newWorkflowExecutionContext(e, workflowExecution)

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		builder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}

		msBuilder, err1 := context.loadWorkflowMutableState()
		if err1 != nil {
			return err1
		}

		scheduleID := token.ScheduleID
		isRunning, startedEvent := builder.isActivityTaskRunning(scheduleID)
		if !isRunning || startedEvent == nil {
			return &workflow.EntityNotExistsError{Message: "Activity task not found."}
		}

		startedID := startedEvent.GetEventId()
		if builder.AddActivityTaskFailedEvent(scheduleID, startedID, request) == nil {
			// Let's retry and see if the activity still exist.
			continue Update_History_Loop
		}

		msBuilder.DeletePendingActivity(scheduleID)

		var transferTasks []persistence.Task
		if !builder.hasPendingDecisionTask() {
			startWorkflowExecutionEvent := builder.GetEvent(firstEventID)
			startAttributes := startWorkflowExecutionEvent.GetWorkflowExecutionStartedEventAttributes()
			newDecisionEvent := builder.AddDecisionTaskScheduledEvent(startAttributes.GetTaskList().GetName(),
				startAttributes.GetTaskStartToCloseTimeoutSeconds())
			id := e.tracker.getNextTaskID()
			defer e.tracker.completeTask(id)
			transferTasks = []persistence.Task{&persistence.DecisionTask{
				TaskID:     id,
				TaskList:   startAttributes.GetTaskList().GetName(),
				ScheduleID: newDecisionEvent.GetEventId(),
			}}
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		if err := context.updateWorkflowExecution(transferTasks, nil); err != nil {
			if err == ErrConflict {
				continue Update_History_Loop
			}

			return err
		}

		return nil
	}

	return ErrMaxAttemptsExceeded
}

// RecordActivityTaskHeartbeat records an hearbeat for a task.
func (e *historyEngineImpl) RecordActivityTaskHeartbeat(
	request *workflow.RecordActivityTaskHeartbeatRequest) (*workflow.RecordActivityTaskHeartbeatResponse, error) {
	token, err0 := e.tokenSerializer.Deserialize(request.GetTaskToken())
	if err0 != nil {
		return nil, &workflow.BadRequestError{Message: "Error deserializing task token."}
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	context := newWorkflowExecutionContext(e, workflowExecution)

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err1 := context.loadWorkflowMutableState()
		if err1 != nil {
			return nil, err1
		}

		scheduleID := token.ScheduleID
		isRunning, ai := msBuilder.isActivityRunning(scheduleID)
		if !isRunning || ai.StartedID == emptyEventID {
			e.logger.Debugf("Activity HeartBeat: scheduleEventID: %v, ActivityInfo: %+v, Exist: %v",
				scheduleID, ai, isRunning)
			return nil, &workflow.EntityNotExistsError{Message: "Activity task not found."}
		}

		if ai.HeartbeatTimeout <= 0 {
			e.logger.Debugf("Activity HeartBeat: Schedule activity attributes: %+v", ai)
			return nil, &workflow.EntityNotExistsError{Message: "Activity task not configured to heartbeat."}
		}

		_, err1 = context.loadWorkflowExecution()
		if err1 != nil {
			return nil, err1
		}

		var timerTasks []persistence.Task
		var transferTasks []persistence.Task

		e.logger.Debugf("Activity HeartBeat: scheduleEventID: %v, ActivityInfo: %+v", scheduleID, ai)

		// Re-schedule next heartbeat.
		defer e.timerProcessor.NotifyNewTimer()
		start2HeartBeatTimeoutTask, _ := context.tBuilder.AddHeartBeatActivityTimeout(scheduleID, msBuilder)
		timerTasks = append(timerTasks, start2HeartBeatTimeoutTask)
		ai.Details = request.GetDetails()
		msBuilder.UpdatePendingActivity(scheduleID, ai)

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		if err := context.updateWorkflowExecution(transferTasks, timerTasks); err != nil {
			if err == ErrConflict {
				continue Update_History_Loop
			}

			return nil, err
		}

		return &workflow.RecordActivityTaskHeartbeatResponse{}, nil
	}

	return &workflow.RecordActivityTaskHeartbeatResponse{}, ErrMaxAttemptsExceeded
}

func (e *historyEngineImpl) getWorkflowExecutionWithRetry(
	request *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
	var response *persistence.GetWorkflowExecutionResponse
	op := func() error {
		var err error
		response, err = e.executionManager.GetWorkflowExecution(request)

		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (e *historyEngineImpl) deleteWorkflowExecutionWithRetry(
	request *persistence.DeleteWorkflowExecutionRequest) error {
	op := func() error {
		return e.executionManager.DeleteWorkflowExecution(request)
	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (e *historyEngineImpl) updateWorkflowExecutionWithRetry(
	request *persistence.UpdateWorkflowExecutionRequest) error {
	op := func() error {
		return e.executionManager.UpdateWorkflowExecution(request)

	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (e *historyEngineImpl) getWorkflowMutableStateWithRetry(
	request *persistence.GetWorkflowMutableStateRequest) (*persistence.GetWorkflowMutableStateResponse, error) {
	var response *persistence.GetWorkflowMutableStateResponse
	op := func() error {
		var err error
		response, err = e.executionManager.GetWorkflowMutableState(request)

		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (e *historyEngineImpl) createRecordDecisionTaskStartedResponse(context *workflowExecutionContext,
	startedEvent *workflow.HistoryEvent) *h.RecordDecisionTaskStartedResponse {
	builder := context.builder

	response := h.NewRecordDecisionTaskStartedResponse()
	response.WorkflowType = builder.getWorkflowType()
	if builder.previousDecisionStartedEvent() != emptyEventID {
		response.PreviousStartedEventId = common.Int64Ptr(builder.previousDecisionStartedEvent())
	}
	response.StartedEventId = common.Int64Ptr(startedEvent.GetEventId())
	response.History = builder.getHistory()

	return response
}

func newWorkflowExecutionContext(historyService *historyEngineImpl,
	execution workflow.WorkflowExecution) *workflowExecutionContext {
	logger := historyService.logger.WithFields(bark.Fields{
		tagWorkflowExecutionID: execution.GetWorkflowId(),
		tagWorkflowRunID:       execution.GetRunId(),
	})
	tBuilder := newTimerBuilder(&shardSeqNumGenerator{context: historyService.shard}, logger)

	return &workflowExecutionContext{
		workflowExecution: execution,
		historyService:    historyService,
		msBuilder:         &mutableStateBuilder{},
		tBuilder:          tBuilder,
		logger:            logger,
	}
}

// Used to either create or update the execution context for the task context.
// Update can happen when conditional write fails.
func (c *workflowExecutionContext) loadWorkflowExecution() (*historyBuilder, error) {
	response, err := c.historyService.getWorkflowExecutionWithRetry(&persistence.GetWorkflowExecutionRequest{
		Execution: c.workflowExecution})
	if err != nil {
		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationGetWorkflowExecution, err, "")
		return nil, err
	}

	c.executionInfo = response.ExecutionInfo
	c.updateCondition = response.ExecutionInfo.NextEventID
	builder := newHistoryBuilder(c.logger)
	if err := builder.loadExecutionInfo(response.ExecutionInfo); err != nil {
		return nil, err
	}
	c.builder = builder

	return builder, nil
}

func (c *workflowExecutionContext) loadWorkflowMutableState() (*mutableStateBuilder, error) {
	response, err := c.historyService.getWorkflowMutableStateWithRetry(&persistence.GetWorkflowMutableStateRequest{
		WorkflowID: c.workflowExecution.GetWorkflowId(),
		RunID:      c.workflowExecution.GetRunId()})

	if err != nil {
		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationGetWorkflowMutableState, err, "")
		return nil, err
	}

	msBuilder := newMutableStateBuilder(c.logger)
	if response != nil && response.State != nil {
		msBuilder.Load(response.State.ActivitInfos, response.State.TimerInfos)
	}

	c.msBuilder = msBuilder
	return msBuilder, nil
}

func (c *workflowExecutionContext) updateWorkflowExecutionWithContext(context []byte, transferTasks []persistence.Task,
	timerTasks []persistence.Task) error {
	c.executionInfo.ExecutionContext = context

	return c.updateWorkflowExecution(transferTasks, timerTasks)
}

func (c *workflowExecutionContext) updateWorkflowExecutionWithDeleteTask(transferTasks []persistence.Task,
	timerTasks []persistence.Task, deleteTimerTask persistence.Task) error {
	c.deleteTimerTask = deleteTimerTask

	return c.updateWorkflowExecution(transferTasks, timerTasks)
}

func (c *workflowExecutionContext) updateWorkflowExecution(transferTasks []persistence.Task, timerTasks []persistence.Task) error {
	updatedHistory, err := c.builder.Serialize()
	if err != nil {
		logHistorySerializationErrorEvent(c.logger, err, "Unable to serialize execution history for update.")
		return err
	}

	c.executionInfo.NextEventID = c.builder.nextEventID
	c.executionInfo.LastProcessedEvent = c.builder.previousDecisionStartedEvent()
	c.executionInfo.History = updatedHistory
	c.executionInfo.DecisionPending = c.builder.hasPendingDecisionTask()
	c.executionInfo.State = c.builder.getWorklowState()

	if err1 := c.historyService.updateWorkflowExecutionWithRetry(&persistence.UpdateWorkflowExecutionRequest{
		ExecutionInfo:       c.executionInfo,
		TransferTasks:       transferTasks,
		TimerTasks:          timerTasks,
		Condition:           c.updateCondition,
		DeleteTimerTask:     c.deleteTimerTask,
		RangeID:             c.historyService.shard.GetRangeID(),
		UpsertActivityInfos: c.msBuilder.updateActivityInfos,
		DeleteActivityInfo:  c.msBuilder.deleteActivityInfo,
		UpserTimerInfos:     c.msBuilder.updateTimerInfos,
		DeleteTimerInfos:    c.msBuilder.deleteTimerInfos,
	}); err1 != nil {
		switch err1.(type) {
		case *persistence.ConditionFailedError:
			return ErrConflict
		}

		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationUpdateWorkflowExecution, err,
			fmt.Sprintf("{updateCondition: %v}", c.updateCondition))
		return err1
	}

	// Update went through so update the condition for new updates
	c.updateCondition = c.builder.nextEventID
	return nil
}

func (c *workflowExecutionContext) deleteWorkflowExecution() error {
	err := c.historyService.deleteWorkflowExecutionWithRetry(&persistence.DeleteWorkflowExecutionRequest{
		ExecutionInfo: c.executionInfo,
	})
	if err != nil {
		// TODO: We will be needing a background job to delete all leaking workflow executions due to failed delete
		// We cannot return an error back to client at this stage.  For now just log and move on.
		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationDeleteWorkflowExecution, err,
			fmt.Sprintf("{updateCondition: %v}", c.updateCondition))
	}

	return err
}

func (t *pendingTaskTracker) getNextTaskID() int64 {
	t.lk.Lock()
	nextID := t.shard.GetTransferTaskID()
	if nextID != t.maxID+1 {
		t.logger.Fatalf("No holes allowed for nextID.  nextID: %v, MaxID: %v", nextID, t.maxID)
	}
	t.pendingTasks[nextID] = false
	t.maxID = nextID
	t.lk.Unlock()

	t.logger.Debugf("Generated new transfer task ID: %v", nextID)
	return nextID
}

func (t *pendingTaskTracker) completeTask(taskID int64) {
	t.lk.Lock()
	updatedMin := int64(-1)
	if _, ok := t.pendingTasks[taskID]; ok {
		t.logger.Debugf("Completing transfer task ID: %v", taskID)
		t.pendingTasks[taskID] = true

	UpdateMinLoop:
		for newMin := t.minID + 1; newMin <= t.maxID; newMin++ {
			if done, ok := t.pendingTasks[newMin]; ok && done {
				t.logger.Debugf("Updating minID for pending transfer tasks: %v", newMin)
				t.minID = newMin
				updatedMin = newMin
				delete(t.pendingTasks, newMin)
			} else {
				break UpdateMinLoop
			}
		}
	}

	t.lk.Unlock()

	if updatedMin != -1 {
		t.txProcessor.UpdateMaxAllowedReadLevel(updatedMin)
	}
}
