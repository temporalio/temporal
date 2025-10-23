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

// Callback represents a callback component in CHASM.
//
// This is the CHASM port of HSM's nexusInvocation struct from nexus_invocation.go:25-32.
type Callback struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*callbackspb.CallbackState

	// Interface to retrieve Nexus operation completion data
	CanGetNexusCompletion chasm.Field[CanGetNexusCompletion]
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
			Status:           callbackspb.CALLBACK_STATUS_STANDBY,
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

func (c Callback) recordAttempt(ts time.Time) {
	c.Attempt++
	c.LastAttemptCompleteTime = timestamppb.New(ts)
}
