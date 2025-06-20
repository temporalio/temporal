package serviceerror

import (
	errordetailsspb "go.temporal.io/server/api/errordetails/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// StickyWorkerUnavailable represents sticky worker unavailable error.
	StickyWorkerUnavailable struct {
		Message string
		st      *status.Status
	}
)

// NewStickyWorkerUnavailable returns new StickyWorkerUnavailable error.
func NewStickyWorkerUnavailable() error {
	return &StickyWorkerUnavailable{
		Message: "sticky worker unavailable, please use original task queue.",
	}
}

// Error returns string message.
func (e *StickyWorkerUnavailable) Error() string {
	return e.Message
}

func (e *StickyWorkerUnavailable) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.Unavailable, e.Message)
	st, _ = st.WithDetails(
		&errordetailsspb.StickyWorkerUnavailableFailure{},
	)
	return st
}

func newStickyWorkerUnavailable(st *status.Status) error {
	return &StickyWorkerUnavailable{
		Message: st.Message(),
		st:      st,
	}
}
