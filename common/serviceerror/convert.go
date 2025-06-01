package serviceerror

import (
	"go.temporal.io/api/serviceerror"
	errordetailsspb "go.temporal.io/server/api/errordetails/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FromStatus converts gRPC status to service error.
func FromStatus(st *status.Status) error {
	if st == nil || st.Code() == codes.OK {
		return nil
	}

	errDetails := extractErrorDetails(st)

	switch st.Code() {
	case codes.InvalidArgument:
		switch errDetails := errDetails.(type) {
		case *errordetailsspb.CurrentBranchChangedFailure:
			return newCurrentBranchChanged(st, errDetails)
		}
	case codes.AlreadyExists:
		switch errDetails.(type) {
		case *errordetailsspb.TaskAlreadyStartedFailure:
			return newTaskAlreadyStarted(st)
		}
	case codes.Aborted:
		switch errDetails := errDetails.(type) {
		case *errordetailsspb.ShardOwnershipLostFailure:
			return newShardOwnershipLost(st, errDetails)
		case *errordetailsspb.RetryReplicationFailure:
			return newRetryReplication(st, errDetails)
		case *errordetailsspb.SyncStateFailure:
			return newSyncState(st, errDetails)
		}
	case codes.Unavailable:
		switch errDetails.(type) {
		case *errordetailsspb.StickyWorkerUnavailableFailure:
			return newStickyWorkerUnavailable(st)
		}
	case codes.FailedPrecondition:
		switch errDetails.(type) {
		case *errordetailsspb.ObsoleteDispatchBuildIdFailure:
			return newObsoleteDispatchBuildId(st)
		case *errordetailsspb.ObsoleteMatchingTaskFailure:
			return newObsoleteMatchingTask(st)
		case *errordetailsspb.ActivityStartDuringTransitionFailure:
			return newActivityStartDuringTransition(st)
		}
	}

	return serviceerror.FromStatus(st)
}

func extractErrorDetails(st *status.Status) interface{} {
	details := st.Details()
	if len(details) > 0 {
		return details[0]
	}

	return nil
}
