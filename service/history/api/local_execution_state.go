package api

import (
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func ValidateLocalExecutionTask(mutableState historyi.MutableState) error {
	return ValidateLocalExecutionInfo(mutableState.GetExecutionInfo())
}

// PauseLocalExecutionForRemoteCommands keeps transfer tasks created by a local Workflow Task from
// being dispatched before the bridge has synchronized and released upstream ownership.
func PauseLocalExecutionForRemoteCommands(
	executionInfo *persistencespb.WorkflowExecutionInfo,
	commands []*commandpb.Command,
	localTaskQueue string,
	registeredActivityTypes []string,
) {
	if localTaskQueue == "" {
		return
	}
	registeredActivities := make(map[string]struct{}, len(registeredActivityTypes))
	for _, activityType := range registeredActivityTypes {
		registeredActivities[activityType] = struct{}{}
	}
	for _, command := range commands {
		if commandRequiresUpstream(command, localTaskQueue, registeredActivities) {
			localInfo := executionInfo.GetLocalExecutionInfo()
			if localInfo.GetBridgeState() == persistencespb.LocalExecutionInfo_BRIDGE_STATE_RUNNABLE {
				localInfo.BridgeState = persistencespb.LocalExecutionInfo_BRIDGE_STATE_PAUSED
			}
			return
		}
	}
}

// DisableLocalExecutionEagerActivities prevents an Activity that requires upstream execution from
// bypassing the transfer-task pause through eager dispatch in the Workflow Task response.
func DisableLocalExecutionEagerActivities(
	commands []*commandpb.Command,
	localTaskQueue string,
	registeredActivityTypes []string,
) {
	if localTaskQueue == "" {
		return
	}
	registeredActivities := make(map[string]struct{}, len(registeredActivityTypes))
	for _, activityType := range registeredActivityTypes {
		registeredActivities[activityType] = struct{}{}
	}
	for _, command := range commands {
		if command.GetCommandType() == enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK &&
			commandRequiresUpstream(command, localTaskQueue, registeredActivities) {
			if attributes := command.GetScheduleActivityTaskCommandAttributes(); attributes != nil {
				attributes.RequestEagerExecution = false
			}
		}
	}
}

func commandRequiresUpstream(
	command *commandpb.Command,
	localTaskQueue string,
	registeredActivities map[string]struct{},
) bool {
	switch command.GetCommandType() {
	case enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:
		attributes := command.GetScheduleActivityTaskCommandAttributes()
		if attributes == nil {
			return true
		}
		activityType := attributes.GetActivityType().GetName()
		_, registered := registeredActivities[activityType]
		return !registered || attributes.GetTaskQueue().GetName() != localTaskQueue
	case enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
		enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
		enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION:
		return true
	default:
		return false
	}
}

func ValidateLocalExecutionInfo(executionInfo *persistencespb.WorkflowExecutionInfo) error {
	switch executionInfo.GetLocalExecutionInfo().GetBridgeState() {
	case persistencespb.LocalExecutionInfo_BRIDGE_STATE_PAUSED:
		return serviceerror.NewUnavailable("local execution is paused for synchronization")
	case persistencespb.LocalExecutionInfo_BRIDGE_STATE_OWNERSHIP_LOST:
		return serviceerror.NewNotFound("local execution ownership was lost")
	default:
		return nil
	}
}
