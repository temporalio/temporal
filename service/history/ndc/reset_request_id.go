package ndc

import (
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// IsResetRequestIDInfo reports whether info is a ResetWorkflowExecution request marker.
func IsResetRequestIDInfo(info *persistencespb.RequestIDInfo) bool {
	return info != nil && info.GetEventType() == enumspb.EVENT_TYPE_UNSPECIFIED &&
		info.GetEventId() == common.EmptyEventID && info.GetAttachTime() == nil
}

func attachResetRequestID(mutableState historyi.MutableState, requestID string) {
	if requestID == "" {
		return
	}
	mutableState.AttachRequestID(requestID, enumspb.EVENT_TYPE_UNSPECIFIED, common.EmptyEventID)
}

func copyResetRequestIDs(
	rebuiltMutableState historyi.MutableState,
	currentExecutionState *persistencespb.WorkflowExecutionState,
) {
	for requestID, info := range currentExecutionState.GetRequestIds() {
		if IsResetRequestIDInfo(info) {
			attachResetRequestID(rebuiltMutableState, requestID)
		}
	}
}
