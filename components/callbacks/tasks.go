package callbacks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/hsm"
)

const (
	TaskTypeInvocation = "callbacks.Invocation"
	TaskTypeBackoff    = "callbacks.Backoff"
)

type InvocationTask struct {
	// The base URL for nexus callbacks.
	// Will have other meanings as more callback use cases are added.
	destination string
}

var _ hsm.Task = InvocationTask{}

func NewInvocationTask(destination string) InvocationTask {
	return InvocationTask{destination: destination}
}

func (InvocationTask) Type() string {
	return TaskTypeInvocation
}

func (t InvocationTask) Destination() string {
	return t.destination
}

func (t InvocationTask) Deadline() time.Time {
	return hsm.Immediate
}

func (InvocationTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	return hsm.ValidateState[enumsspb.CallbackState, Callback](node, enumsspb.CALLBACK_STATE_SCHEDULED)
}

type InvocationTaskSerializer struct{}

func (InvocationTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return InvocationTask{destination: attrs.Destination}, nil
}

func (InvocationTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type BackoffTask struct {
	deadline time.Time
}

var _ hsm.Task = BackoffTask{}

func (BackoffTask) Type() string {
	return TaskTypeBackoff
}

func (t BackoffTask) Deadline() time.Time {
	return t.deadline
}

func (BackoffTask) Destination() string {
	return ""
}

func (BackoffTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	return hsm.ValidateState[enumsspb.CallbackState, Callback](node, enumsspb.CALLBACK_STATE_BACKING_OFF)
}

type BackoffTaskSerializer struct{}

func (BackoffTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return BackoffTask{deadline: attrs.Deadline}, nil
}

func (BackoffTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

func RegisterTaskSerializers(reg *hsm.Registry) error {
	if err := reg.RegisterTaskSerializer(TaskTypeInvocation, InvocationTaskSerializer{}); err != nil {
		return err
	}
	if err := reg.RegisterTaskSerializer(TaskTypeBackoff, BackoffTaskSerializer{}); err != nil { // nolint:revive
		return err
	}
	return nil
}
