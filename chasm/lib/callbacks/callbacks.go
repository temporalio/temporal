package callbacks

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callbacks/gen/callbackspb/v1"
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
	requestId string,
	registrationTime *timestamppb.Timestamp,
	trigger *callbackspb.CallbackState_Trigger,
	cb *callbackspb.Callback,
) *Callback {
	return &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId:        requestId,
			RegistrationTime: registrationTime,
			Trigger:          trigger,
			Callback:         cb,
		},
	}
}

func (c *Callback) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (c *Callback) State() *callbackspb.CallbackState {
	return c.CallbackState
}

func (c *Callback) SetState(state *callbackspb.CallbackState) {
	c.CallbackState = state
}
