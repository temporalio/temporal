package api

import (
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func ValidateLocalExecutionTask(mutableState historyi.MutableState) error {
	return ValidateLocalExecutionInfo(mutableState.GetExecutionInfo())
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
