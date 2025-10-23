package callback

import (
	"time"

	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	Archetype chasm.Archetype = "Callback"
)

type Callback struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*callbackspb.CallbackState
}

func NewCallback(
	requestID string,
	registrationTime *timestamppb.Timestamp,
	state *callbackspb.CallbackState,
	cb *callbackspb.Callback,
) *Callback {
	return &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId:        requestID,
			RegistrationTime: registrationTime,
			Callback:         cb,
		},
	}
}

func (c *Callback) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	// TODO (seankane): implement lifecycle state
	return chasm.LifecycleStateRunning
}

func (c *Callback) State() callbackspb.CallbackStatus {
	return c.Status
}

func (c *Callback) SetState(status callbackspb.CallbackStatus) {
	c.Status = status
}

func (c *Callback) recordAttempt(ts time.Time) {
	c.Attempt++
	c.LastAttemptCompleteTime = timestamppb.New(ts)
}
