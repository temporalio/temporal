package persistence

import "github.com/temporalio/temporal/.gen/proto/persistenceblobs"

type (
	// statsComputer is to computing struct sizes after serialization
	statsComputer struct{}
)

func (sc *statsComputer) computeMutableStateStats(req *InternalGetWorkflowExecutionResponse) *MutableStateStats {
	executionInfoSize := computeExecutionInfoSize(req.State.ExecutionInfo)

	activityInfoCount := 0
	activityInfoSize := 0
	for _, ai := range req.State.ActivityInfos {
		activityInfoCount++
		activityInfoSize += computeActivityInfoSize(ai)
	}

	timerInfoCount := 0
	timerInfoSize := 0
	for _, ti := range req.State.TimerInfos {
		timerInfoCount++
		timerInfoSize += computeTimerInfoSize(ti)
	}

	childExecutionInfoCount := 0
	childExecutionInfoSize := 0
	for _, ci := range req.State.ChildExecutionInfos {
		childExecutionInfoCount++
		childExecutionInfoSize += computeChildInfoSize(ci)
	}

	signalInfoCount := 0
	signalInfoSize := 0
	for _, si := range req.State.SignalInfos {
		signalInfoCount++
		signalInfoSize += computeSignalInfoSize(si)
	}

	bufferedEventsCount := 0
	bufferedEventsSize := 0

	for _, be := range req.State.BufferedEvents {
		bufferedEventsCount++
		bufferedEventsSize += len(be.Data)
	}

	requestCancelInfoCount := len(req.State.RequestCancelInfos)

	totalSize := executionInfoSize
	totalSize += activityInfoSize
	totalSize += timerInfoSize
	totalSize += childExecutionInfoSize
	totalSize += signalInfoSize
	totalSize += bufferedEventsSize

	return &MutableStateStats{
		MutableStateSize:       totalSize,
		ExecutionInfoSize:      executionInfoSize,
		ActivityInfoSize:       activityInfoSize,
		TimerInfoSize:          timerInfoSize,
		ChildInfoSize:          childExecutionInfoSize,
		SignalInfoSize:         signalInfoSize,
		BufferedEventsSize:     bufferedEventsSize,
		ActivityInfoCount:      activityInfoCount,
		TimerInfoCount:         timerInfoCount,
		ChildInfoCount:         childExecutionInfoCount,
		SignalInfoCount:        signalInfoCount,
		BufferedEventsCount:    bufferedEventsCount,
		RequestCancelInfoCount: requestCancelInfoCount,
	}
}

func (sc *statsComputer) computeMutableStateUpdateStats(req *InternalUpdateWorkflowExecutionRequest) *MutableStateUpdateSessionStats {
	executionInfoSize := computeExecutionInfoSize(req.UpdateWorkflowMutation.ExecutionInfo)

	activityInfoCount := 0
	activityInfoSize := 0
	for _, ai := range req.UpdateWorkflowMutation.UpsertActivityInfos {
		activityInfoCount++
		activityInfoSize += computeActivityInfoSize(ai)
	}

	timerInfoCount := 0
	timerInfoSize := 0
	for _, ti := range req.UpdateWorkflowMutation.UpsertTimerInfos {
		timerInfoCount++
		timerInfoSize += computeTimerInfoSize(ti)
	}

	childExecutionInfoCount := 0
	childExecutionInfoSize := 0
	for _, ci := range req.UpdateWorkflowMutation.UpsertChildExecutionInfos {
		childExecutionInfoCount++
		childExecutionInfoSize += computeChildInfoSize(ci)
	}

	signalInfoCount := 0
	signalInfoSize := 0
	for _, si := range req.UpdateWorkflowMutation.UpsertSignalInfos {
		signalInfoCount++
		signalInfoSize += computeSignalInfoSize(si)
	}

	bufferedEventsSize := 0
	if req.UpdateWorkflowMutation.NewBufferedEvents != nil {
		bufferedEventsSize = len(req.UpdateWorkflowMutation.NewBufferedEvents.Data)
	}

	requestCancelInfoCount := len(req.UpdateWorkflowMutation.UpsertRequestCancelInfos)

	deleteActivityInfoCount := len(req.UpdateWorkflowMutation.DeleteActivityInfos)

	deleteTimerInfoCount := len(req.UpdateWorkflowMutation.DeleteTimerInfos)

	deleteChildInfoCount := 0
	if req.UpdateWorkflowMutation.DeleteChildExecutionInfo != nil {
		deleteChildInfoCount = 1
	}

	deleteSignalInfoCount := 0
	if req.UpdateWorkflowMutation.DeleteSignalInfo != nil {
		deleteSignalInfoCount = 1
	}

	deleteRequestCancelInfoCount := 0
	if req.UpdateWorkflowMutation.DeleteRequestCancelInfo != nil {
		deleteRequestCancelInfoCount = 1
	}

	totalSize := executionInfoSize
	totalSize += activityInfoSize
	totalSize += timerInfoSize
	totalSize += childExecutionInfoSize
	totalSize += signalInfoSize
	totalSize += bufferedEventsSize

	return &MutableStateUpdateSessionStats{
		MutableStateSize:             totalSize,
		ExecutionInfoSize:            executionInfoSize,
		ActivityInfoSize:             activityInfoSize,
		TimerInfoSize:                timerInfoSize,
		ChildInfoSize:                childExecutionInfoSize,
		SignalInfoSize:               signalInfoSize,
		BufferedEventsSize:           bufferedEventsSize,
		ActivityInfoCount:            activityInfoCount,
		TimerInfoCount:               timerInfoCount,
		ChildInfoCount:               childExecutionInfoCount,
		SignalInfoCount:              signalInfoCount,
		RequestCancelInfoCount:       requestCancelInfoCount,
		DeleteActivityInfoCount:      deleteActivityInfoCount,
		DeleteTimerInfoCount:         deleteTimerInfoCount,
		DeleteChildInfoCount:         deleteChildInfoCount,
		DeleteSignalInfoCount:        deleteSignalInfoCount,
		DeleteRequestCancelInfoCount: deleteRequestCancelInfoCount,
	}
}

func computeExecutionInfoSize(executionInfo *InternalWorkflowExecutionInfo) int {
	size := len(executionInfo.WorkflowID)
	size += len(executionInfo.TaskList)
	size += len(executionInfo.WorkflowTypeName)
	size += len(executionInfo.ParentWorkflowID)

	return size
}

func computeActivityInfoSize(ai *InternalActivityInfo) int {
	size := len(ai.ActivityID)
	if ai.ScheduledEvent != nil {
		size += len(ai.ScheduledEvent.Data)
	}
	if ai.StartedEvent != nil {
		size += len(ai.StartedEvent.Data)
	}
	size += len(ai.Details)

	return size
}

func computeTimerInfoSize(ti *persistenceblobs.TimerInfo) int {
	size := len(ti.GetTimerId())

	return size
}

func computeChildInfoSize(ci *InternalChildExecutionInfo) int {
	size := 0
	if ci.InitiatedEvent != nil {
		size += len(ci.InitiatedEvent.Data)
	}
	if ci.StartedEvent != nil {
		size += len(ci.StartedEvent.Data)
	}
	return size
}

func computeSignalInfoSize(si *persistenceblobs.SignalInfo) int {
	size := len(si.Name)
	size += len(si.Input)
	size += len(si.Control)

	return size
}
