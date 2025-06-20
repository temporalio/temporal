package serviceerror

import (
	errordetailsspb "go.temporal.io/server/api/errordetails/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// CurrentBranchChanged represents current branch changed error.
	CurrentBranchChanged struct {
		Message                    string
		CurrentBranchToken         []byte
		RequestBranchToken         []byte
		CurrentVersionedTransition *persistencespb.VersionedTransition
		RequestVersionedTransition *persistencespb.VersionedTransition
		st                         *status.Status
	}
)

// NewCurrentBranchChanged returns new CurrentBranchChanged error.
// TODO: Update CurrentBranchChanged with event id and event version. Do not use branch token bytes as branch identity.
func NewCurrentBranchChanged(currentBranchToken, requestBranchToken []byte,
	currentVersionedTransition, requestVersionedTransition *persistencespb.VersionedTransition) error {
	return &CurrentBranchChanged{
		Message:                    "Current and request branch tokens, or current and request versioned transitions, don't match.",
		CurrentBranchToken:         currentBranchToken,
		RequestBranchToken:         requestBranchToken,
		CurrentVersionedTransition: currentVersionedTransition,
		RequestVersionedTransition: requestVersionedTransition,
	}
}

// Error returns string message.
func (e *CurrentBranchChanged) Error() string {
	return e.Message
}

func (e *CurrentBranchChanged) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.InvalidArgument, e.Message)
	st, _ = st.WithDetails(
		&errordetailsspb.CurrentBranchChangedFailure{
			CurrentBranchToken:         e.CurrentBranchToken,
			RequestBranchToken:         e.RequestBranchToken,
			CurrentVersionedTransition: e.CurrentVersionedTransition,
			RequestVersionedTransition: e.RequestVersionedTransition,
		},
	)
	return st
}

func newCurrentBranchChanged(st *status.Status, errDetails *errordetailsspb.CurrentBranchChangedFailure) error {
	return &CurrentBranchChanged{
		Message:                    st.Message(),
		CurrentBranchToken:         errDetails.GetCurrentBranchToken(),
		RequestBranchToken:         errDetails.GetRequestBranchToken(),
		CurrentVersionedTransition: errDetails.GetCurrentVersionedTransition(),
		RequestVersionedTransition: errDetails.GetRequestVersionedTransition(),
		st:                         st,
	}
}
