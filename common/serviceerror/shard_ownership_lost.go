package serviceerror

import (
	"fmt"

	errordetailsspb "go.temporal.io/server/api/errordetails/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// ShardOwnershipLost represents shard ownership lost error.
	ShardOwnershipLost struct {
		Message     string
		OwnerHost   string
		CurrentHost string
		st          *status.Status
	}
)

// NewShardOwnershipLost returns new ShardOwnershipLost error.
func NewShardOwnershipLost(ownerHost string, currentHost string) error {
	return &ShardOwnershipLost{
		Message:     fmt.Sprintf("Shard is owned by:%v but not by %v", ownerHost, currentHost),
		OwnerHost:   ownerHost,
		CurrentHost: currentHost,
	}
}

// Error returns string message.
func (e *ShardOwnershipLost) Error() string {
	return e.Message
}

func (e *ShardOwnershipLost) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.Aborted, e.Message)
	st, _ = st.WithDetails(
		&errordetailsspb.ShardOwnershipLostFailure{
			OwnerHost:   e.OwnerHost,
			CurrentHost: e.CurrentHost,
		},
	)
	return st
}

func newShardOwnershipLost(st *status.Status, errDetails *errordetailsspb.ShardOwnershipLostFailure) error {
	return &ShardOwnershipLost{
		Message:     st.Message(),
		OwnerHost:   errDetails.GetOwnerHost(),
		CurrentHost: errDetails.GetCurrentHost(),
		st:          st,
	}
}
