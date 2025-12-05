package serviceerror

import (
	errordetailsspb "go.temporal.io/server/api/errordetails/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type (
	// SyncState represents sync state error.
	// nolint:staticcheck
	SyncState struct {
		Message             string
		NamespaceId         string
		WorkflowId          string
		RunId               string
		ArchetypeId         uint32
		VersionedTransition *persistencespb.VersionedTransition
		VersionHistories    *historyspb.VersionHistories
		st                  *status.Status
	}
)

// NewSyncState returns new SyncState error.
// nolint:staticcheck
func NewSyncState(
	message string,
	namespaceId string,
	workflowId string,
	runId string,
	archetypeId uint32,
	versionedTransition *persistencespb.VersionedTransition,
	versionHistories *historyspb.VersionHistories,
) error {
	return &SyncState{
		Message:             message,
		NamespaceId:         namespaceId,
		WorkflowId:          workflowId,
		RunId:               runId,
		ArchetypeId:         archetypeId,
		VersionedTransition: versionedTransition,
		VersionHistories:    versionHistories,
	}
}

// Error returns string message.
func (e *SyncState) Error() string {
	return e.Message
}

func (e *SyncState) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.Aborted, e.Message)
	st, _ = st.WithDetails(
		&errordetailsspb.SyncStateFailure{
			NamespaceId:         e.NamespaceId,
			WorkflowId:          e.WorkflowId,
			RunId:               e.RunId,
			ArchetypeId:         e.ArchetypeId,
			VersionedTransition: e.VersionedTransition,
			VersionHistories:    e.VersionHistories,
		},
	)
	return st
}

func (e *SyncState) Equal(err *SyncState) bool {
	return e.NamespaceId == err.NamespaceId &&
		e.WorkflowId == err.WorkflowId &&
		e.RunId == err.RunId &&
		e.ArchetypeId == err.ArchetypeId &&
		proto.Equal(e.VersionedTransition, err.VersionedTransition) &&
		proto.Equal(e.VersionHistories, err.VersionHistories)
}

func newSyncState(
	st *status.Status,
	errDetails *errordetailsspb.SyncStateFailure,
) error {
	return &SyncState{
		Message:             st.Message(),
		NamespaceId:         errDetails.GetNamespaceId(),
		WorkflowId:          errDetails.GetWorkflowId(),
		RunId:               errDetails.GetRunId(),
		ArchetypeId:         errDetails.GetArchetypeId(),
		VersionedTransition: errDetails.GetVersionedTransition(),
		VersionHistories:    errDetails.GetVersionHistories(),
		st:                  st,
	}
}
