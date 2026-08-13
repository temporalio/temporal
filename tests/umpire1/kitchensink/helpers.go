package kitchensink

import (
	"fmt"
	"math/rand"
	"time"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Using human-readable JSON encoding for payloads to aid with debugging.
var jsonPayloadConverter = converter.NewProtoJSONPayloadConverter()

// ActivityNameAndArgs maps an ExecuteActivityAction's activity variant to the
// registered activity name and its args. Shared by the workflow-scheduled path
// (worker launchActivity) and the standalone-activity client path so the two
// stay in sync. Unrecognized variants fall back to "noop".
func ActivityNameAndArgs(act *ExecuteActivityAction) (string, []any) {
	if delay := act.GetDelay(); delay != nil {
		return "delay", []any{delay.AsDuration()}
	} else if payload := act.GetPayload(); payload != nil {
		// Fill with pseudo-random, incompressible bytes so the payload occupies its full
		// configured size in history/persistence instead of compressing away. The rng is
		// seeded by size so its deterministic on replay
		inputData := make([]byte, payload.BytesToReceive)
		r := rand.New(rand.NewSource(int64(payload.BytesToReceive)))
		_, _ = r.Read(inputData)
		return "payload", []any{inputData, payload.BytesToReturn}
	} else if client := act.GetClient(); client != nil {
		return "client", []any{client}
	} else if retryable := act.GetRetryableError(); retryable != nil {
		return "retryable_error", []any{retryable}
	} else if timeout := act.GetTimeout(); timeout != nil {
		return "timeout", []any{timeout}
	} else if heartbeat := act.GetHeartbeat(); heartbeat != nil {
		return "heartbeat", []any{heartbeat}
	}
	return "noop", nil
}

// ConvertFromPBRetryPolicy converts a proto RetryPolicy into an SDK RetryPolicy.
func ConvertFromPBRetryPolicy(retryPolicy *common.RetryPolicy) *temporal.RetryPolicy {
	if retryPolicy == nil {
		return nil
	}
	p := temporal.RetryPolicy{
		BackoffCoefficient:     retryPolicy.BackoffCoefficient,
		MaximumAttempts:        retryPolicy.MaximumAttempts,
		NonRetryableErrorTypes: retryPolicy.NonRetryableErrorTypes,
	}
	if v := retryPolicy.MaximumInterval; v != nil {
		p.MaximumInterval = v.AsDuration()
	}
	if v := retryPolicy.InitialInterval; v != nil {
		p.InitialInterval = v.AsDuration()
	}
	return &p
}

type ActionFactory[T any] func(*T) *Action

func SingleActionSet(actions ...*Action) *ActionSet {
	return &ActionSet{
		Actions: actions,
	}
}

func ListActionSet(actions ...*Action) []*ActionSet {
	return []*ActionSet{
		{
			Actions: actions,
		},
	}
}

func ClientActions(clientActions ...*ClientAction) *ClientSequence {
	return &ClientSequence{
		ActionSets: []*ClientActionSet{
			{
				Actions: clientActions,
			},
		},
	}
}

func ClientActivity(clientSeq *ClientSequence, factory ActionFactory[ExecuteActivityAction]) *Action {
	activity := &ExecuteActivityAction{
		ActivityType: &ExecuteActivityAction_Client{
			Client: &ExecuteActivityAction_ClientActivity{
				ClientSequence: clientSeq,
			},
		},
	}
	return factory(activity)
}

func NoOpSingleActivityActionSet() *ActionSet {
	return &ActionSet{
		Actions: []*Action{
			{
				Variant: &Action_ExecActivity{
					ExecActivity: &ExecuteActivityAction{
						ActivityType:        &ExecuteActivityAction_Noop{},
						StartToCloseTimeout: &durationpb.Duration{Seconds: 5},
					},
				},
			},
			{
				Variant: &Action_ReturnResult{
					ReturnResult: &ReturnResultAction{
						ReturnThis: &common.Payload{},
					},
				},
			},
		},
	}
}

func ResourceConsumingActivity(bytesToAllocate uint64, cpuYieldEveryNIters uint32, cpuYieldForMs uint32, runForSeconds int64) *Action {
	return &Action{
		Variant: &Action_ExecActivity{
			ExecActivity: &ExecuteActivityAction{
				ActivityType: &ExecuteActivityAction_Resources{
					Resources: &ExecuteActivityAction_ResourcesActivity{
						BytesToAllocate:          bytesToAllocate,
						CpuYieldEveryNIterations: cpuYieldEveryNIters,
						CpuYieldForMs:            cpuYieldForMs,
						RunFor:                   &durationpb.Duration{Seconds: runForSeconds},
					},
				},
				StartToCloseTimeout: &durationpb.Duration{Seconds: runForSeconds * 2},
				RetryPolicy: &common.RetryPolicy{
					MaximumAttempts:    1,
					BackoffCoefficient: 1.0,
				},
			},
		},
	}
}

func NewEmptyReturnResultAction() *Action {
	return &Action{
		Variant: &Action_ReturnResult{
			ReturnResult: &ReturnResultAction{
				ReturnThis: &common.Payload{},
			},
		},
	}
}

func NewTimerAction(t time.Duration) *Action {
	return &Action{
		Variant: &Action_Timer{
			Timer: &TimerAction{
				Milliseconds: uint64(t.Milliseconds()),
			},
		},
	}
}

func DelayActivity(duration time.Duration, factory ActionFactory[ExecuteActivityAction]) *Action {
	activity := &ExecuteActivityAction{
		ActivityType: &ExecuteActivityAction_Delay{
			Delay: durationpb.New(duration),
		},
	}
	return factory(activity)
}

// DelayActivityWithCancellation creates a delay activity that will be cancelled after it starts
func DelayActivityWithCancellation(duration time.Duration, startToCloseTimeout time.Duration) *Action {
	return &Action{
		Variant: &Action_ExecActivity{
			ExecActivity: &ExecuteActivityAction{
				ActivityType: &ExecuteActivityAction_Delay{
					Delay: durationpb.New(duration),
				},
				StartToCloseTimeout: durationpb.New(startToCloseTimeout),
				AwaitableChoice: &AwaitableChoice{
					Condition: &AwaitableChoice_CancelAfterStarted{
						CancelAfterStarted: &emptypb.Empty{},
					},
				},
				Locality: &ExecuteActivityAction_Remote{
					Remote: &RemoteActivityOptions{},
				},
			},
		},
	}
}

func PayloadActivity(inSize, outSize int, factory ActionFactory[ExecuteActivityAction]) *Action {
	activity := &ExecuteActivityAction{
		ActivityType: &ExecuteActivityAction_Payload{
			Payload: &ExecuteActivityAction_PayloadActivity{
				BytesToReceive: int32(inSize),
				BytesToReturn:  int32(outSize),
			},
		},
	}
	return factory(activity)
}

func GenericActivity(activityType string, factory ActionFactory[ExecuteActivityAction]) *Action {
	activity := &ExecuteActivityAction{
		ActivityType: &ExecuteActivityAction_Generic{
			Generic: &ExecuteActivityAction_GenericActivity{
				Type: activityType,
			},
		},
	}
	return factory(activity)
}

func RetryableErrorActivity(failAttempts int32, factory ActionFactory[ExecuteActivityAction]) *Action {
	activity := &ExecuteActivityAction{
		ActivityType: &ExecuteActivityAction_RetryableError{
			RetryableError: &ExecuteActivityAction_RetryableErrorActivity{
				FailAttempts: failAttempts,
			},
		},
	}
	return factory(activity)
}

func TimeoutActivity(failAttempts int32, successDuration time.Duration, failureDuration time.Duration, startToCloseTimeout time.Duration, maxAttempts int32, initialInterval time.Duration, backoffCoefficient float64) *Action {
	if successDuration >= startToCloseTimeout {
		panic(fmt.Sprintf("successDuration (%v) must be < startToCloseTimeout (%v)", successDuration, startToCloseTimeout))
	}
	if failureDuration <= startToCloseTimeout {
		panic(fmt.Sprintf("failureDuration (%v) must be > startToCloseTimeout (%v)", failureDuration, startToCloseTimeout))
	}
	activity := &ExecuteActivityAction{
		ActivityType: &ExecuteActivityAction_Timeout{
			Timeout: &ExecuteActivityAction_TimeoutActivity{
				FailAttempts:    failAttempts,
				SuccessDuration: &durationpb.Duration{Seconds: int64(successDuration.Seconds())},
				FailureDuration: &durationpb.Duration{Seconds: int64(failureDuration.Seconds())},
			},
		},
	}
	factory := RemoteActivityWithRetry(startToCloseTimeout, maxAttempts, initialInterval, backoffCoefficient)
	return factory(activity)
}

func HeartbeatActivity(failAttempts int32, successDuration time.Duration, failureDuration time.Duration, startToCloseTimeout, heartbeatTimeout time.Duration, maxAttempts int32, initialInterval time.Duration, backoffCoefficient float64) *Action {
	if successDuration >= heartbeatTimeout {
		panic(fmt.Sprintf("successDuration (%v) must be < heartbeatTimeout (%v)", successDuration, heartbeatTimeout))
	}
	if failureDuration <= heartbeatTimeout {
		panic(fmt.Sprintf("failureDuration (%v) must be > heartbeatTimeout (%v)", failureDuration, heartbeatTimeout))
	}
	activity := &ExecuteActivityAction{
		ActivityType: &ExecuteActivityAction_Heartbeat{
			Heartbeat: &ExecuteActivityAction_HeartbeatTimeoutActivity{
				FailAttempts:    failAttempts,
				SuccessDuration: &durationpb.Duration{Seconds: int64(successDuration.Seconds())},
				FailureDuration: &durationpb.Duration{Seconds: int64(failureDuration.Seconds())},
			},
		},
	}
	factory := RemoteActivityWithHeartbeat(startToCloseTimeout, heartbeatTimeout, maxAttempts, initialInterval, backoffCoefficient)
	return factory(activity)
}

func DefaultRemoteActivity(activity *ExecuteActivityAction) *Action {
	activity.StartToCloseTimeout = &durationpb.Duration{Seconds: 60}
	activity.Locality = &ExecuteActivityAction_Remote{
		Remote: &RemoteActivityOptions{},
	}
	return &Action{
		Variant: &Action_ExecActivity{
			ExecActivity: activity,
		},
	}
}

func DefaultLocalActivity(activity *ExecuteActivityAction) *Action {
	activity.StartToCloseTimeout = &durationpb.Duration{Seconds: 60}
	activity.Locality = &ExecuteActivityAction_IsLocal{
		IsLocal: &emptypb.Empty{},
	}
	activity.RetryPolicy = &common.RetryPolicy{
		InitialInterval:    durationpb.New(10 * time.Millisecond),
		MaximumAttempts:    10,
		BackoffCoefficient: 2.0,
	}
	return &Action{
		Variant: &Action_ExecActivity{
			ExecActivity: activity,
		},
	}
}

// RemoteActivityWithRetry creates a remote activity with custom retry configuration
func RemoteActivityWithRetry(startToCloseTimeout time.Duration, maxAttempts int32, initialInterval time.Duration, backoffCoefficient float64) ActionFactory[ExecuteActivityAction] {
	return func(activity *ExecuteActivityAction) *Action {
		activity.StartToCloseTimeout = durationpb.New(startToCloseTimeout)
		activity.RetryPolicy = &common.RetryPolicy{
			MaximumAttempts:    maxAttempts,
			InitialInterval:    durationpb.New(initialInterval),
			BackoffCoefficient: backoffCoefficient,
		}
		activity.Locality = &ExecuteActivityAction_Remote{
			Remote: &RemoteActivityOptions{},
		}
		return &Action{
			Variant: &Action_ExecActivity{
				ExecActivity: activity,
			},
		}
	}
}

// RemoteActivityWithHeartbeat creates a remote activity with heartbeat timeout and retry configuration
func RemoteActivityWithHeartbeat(startToCloseTimeout, heartbeatTimeout time.Duration, maxAttempts int32, initialInterval time.Duration, backoffCoefficient float64) ActionFactory[ExecuteActivityAction] {
	return func(activity *ExecuteActivityAction) *Action {
		activity.StartToCloseTimeout = durationpb.New(startToCloseTimeout)
		activity.HeartbeatTimeout = durationpb.New(heartbeatTimeout)
		activity.RetryPolicy = &common.RetryPolicy{
			MaximumAttempts:    maxAttempts,
			InitialInterval:    durationpb.New(initialInterval),
			BackoffCoefficient: backoffCoefficient,
		}
		activity.Locality = &ExecuteActivityAction_Remote{
			Remote: &RemoteActivityOptions{},
		}
		return &Action{
			Variant: &Action_ExecActivity{
				ExecActivity: activity,
			},
		}
	}
}

func NewSetWorkflowStateAction(key, value string) *Action {
	return &Action{
		Variant: &Action_SetWorkflowState{
			SetWorkflowState: &WorkflowState{
				Kvs: map[string]string{key: value},
			},
		},
	}
}

func NewAwaitWorkflowStateAction(key, value string) *Action {
	return &Action{
		Variant: &Action_AwaitWorkflowState{
			AwaitWorkflowState: &AwaitWorkflowState{
				Key:   key,
				Value: value,
			},
		},
	}
}

func NewSignalActionsWithIDs(ids ...int32) []*ClientAction {
	actions := make([]*ClientAction, len(ids))
	for i, id := range ids {
		actions[i] = &ClientAction{
			Variant: &ClientAction_DoSignal{
				DoSignal: &DoSignal{
					Variant: &DoSignal_DoSignalActions_{
						DoSignalActions: &DoSignal_DoSignalActions{
							SignalId: id,
							Variant: &DoSignal_DoSignalActions_DoActions{
								DoActions: SingleActionSet(
									NewSetWorkflowStateAction(fmt.Sprintf("signal_%d", i), "received"),
								),
							},
						},
					},
				},
			},
		}
	}
	return actions
}

// NewSignalActionWithError creates a signal action that returns an error
func NewSignalActionWithError(signalID int32, errorMessage string) *ClientAction {
	return &ClientAction{
		Variant: &ClientAction_DoSignal{
			DoSignal: &DoSignal{
				Variant: &DoSignal_DoSignalActions_{
					DoSignalActions: &DoSignal_DoSignalActions{
						SignalId: signalID,
						Variant: &DoSignal_DoSignalActions_DoActions{
							DoActions: SingleActionSet(
								NewErrorAction(errorMessage),
							),
						},
					},
				},
			},
		},
	}
}

// NewErrorAction creates an action that returns an error
func NewErrorAction(errorMessage string) *Action {
	return &Action{
		Variant: &Action_ReturnError{
			ReturnError: &ReturnErrorAction{
				Failure: &failure.Failure{
					Message: errorMessage,
				},
			},
		},
	}
}

func ConvertToPayload(newInput any) *common.Payload {
	payload, err := jsonPayloadConverter.ToPayload(newInput)
	if err != nil {
		// this should never happen; but we don't want to swallow the error
		panic(fmt.Sprintf("failed to convert input %T to payload: %v", newInput, err))
	}
	return payload
}
