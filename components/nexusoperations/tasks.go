package nexusoperations

import (
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
)

const (
	TaskTypeTimeout            = "nexusoperations.Timeout"
	TaskTypeInvocation         = "nexusoperations.Invocation"
	TaskTypeBackoff            = "nexusoperations.Backoff"
	TaskTypeCancelation        = "nexusoperations.Cancelation"
	TaskTypeCancelationBackoff = "nexusoperations.CancelationBackoff"
)

var errSerializationCast = errors.New("cannot serialize HSM task. unable to cast to expected type")

type TimeoutTask struct {
	deadline time.Time
}

var _ hsm.Task = TimeoutTask{}

func (TimeoutTask) Type() string {
	return TaskTypeTimeout
}

func (t TimeoutTask) Deadline() time.Time {
	return t.deadline
}

func (TimeoutTask) Destination() string {
	return ""
}

// Validate checks if the timeout task is still valid to execute for the given node state.
func (t TimeoutTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
	op, err := hsm.MachineData[Operation](node)
	if err != nil {
		return err
	}
	if !TransitionTimedOut.Possible(op) {
		return fmt.Errorf(
			"%w: %w: cannot timeout machine in state %v",
			consts.ErrStaleReference,
			hsm.ErrInvalidTransition,
			op.State(),
		)
	}
	return nil
}

type TimeoutTaskSerializer struct{}

func (TimeoutTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return TimeoutTask{deadline: attrs.Deadline}, nil
}

func (TimeoutTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type InvocationTask struct {
	EndpointName string
	Attempt      int32
}

var _ hsm.Task = InvocationTask{}

func (InvocationTask) Type() string {
	return TaskTypeInvocation
}

func (InvocationTask) Deadline() time.Time {
	return hsm.Immediate
}

func (t InvocationTask) Destination() string {
	return t.EndpointName
}

func (InvocationTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
	return hsm.ValidateState[enumsspb.NexusOperationState, Operation](node, enumsspb.NEXUS_OPERATION_STATE_SCHEDULED)
}

type InvocationTaskSerializer struct{}

func (InvocationTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	var info persistencespb.NexusInvocationTaskInfo
	err := proto.Unmarshal(data, &info)
	if err != nil {
		return nil, serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return InvocationTask{EndpointName: attrs.Destination, Attempt: info.Attempt}, nil
}

func (InvocationTaskSerializer) Serialize(task hsm.Task) ([]byte, error) {
	switch task := task.(type) {
	case InvocationTask:
		return proto.Marshal(&persistencespb.NexusInvocationTaskInfo{Attempt: task.Attempt})
	default:
		return nil, serviceerror.NewInternalf("unknown HSM task type while serializing: %v", task)
	}
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

func (t BackoffTask) Destination() string {
	return ""
}

func (t BackoffTask) Validate(_ *persistencespb.StateMachineRef, node *hsm.Node) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
	return hsm.ValidateState[enumsspb.NexusOperationState, Operation](node, enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF)
}

type BackoffTaskSerializer struct{}

func (BackoffTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return BackoffTask{deadline: attrs.Deadline}, nil
}

func (BackoffTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type CancelationTask struct {
	EndpointName string
	Attempt      int32
}

var _ hsm.Task = CancelationTask{}

func (CancelationTask) Type() string {
	return TaskTypeCancelation
}

func (CancelationTask) Deadline() time.Time {
	return hsm.Immediate
}

func (t CancelationTask) Destination() string {
	return t.EndpointName
}

func (CancelationTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
	return hsm.ValidateState[enumspb.NexusOperationCancellationState, Cancelation](node, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED)
}

type CancelationTaskSerializer struct{}

func (CancelationTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	var info persistencespb.NexusCancelationTaskInfo
	err := proto.Unmarshal(data, &info)
	if err != nil {
		return nil, serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return CancelationTask{EndpointName: attrs.Destination, Attempt: info.Attempt}, nil
}

func (CancelationTaskSerializer) Serialize(task hsm.Task) ([]byte, error) {
	switch task := task.(type) {
	case CancelationTask:
		return proto.Marshal(&persistencespb.NexusCancelationTaskInfo{Attempt: task.Attempt})
	default:
		return nil, serviceerror.NewInternalf("unknown HSM task type while serializing: %v", task)
	}
}

type CancelationBackoffTask struct {
	deadline time.Time
}

var _ hsm.Task = CancelationBackoffTask{}

func (CancelationBackoffTask) Type() string {
	return TaskTypeCancelationBackoff
}

func (t CancelationBackoffTask) Deadline() time.Time {
	return t.deadline
}

func (CancelationBackoffTask) Destination() string {
	return ""
}

func (CancelationBackoffTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
	return hsm.ValidateState[enumspb.NexusOperationCancellationState, Cancelation](node, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF)
}

type CancelationBackoffTaskSerializer struct{}

func (CancelationBackoffTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return CancelationBackoffTask{deadline: attrs.Deadline}, nil
}

func (CancelationBackoffTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

func RegisterTaskSerializers(reg *hsm.Registry) error {
	if err := reg.RegisterTaskSerializer(TaskTypeTimeout, TimeoutTaskSerializer{}); err != nil {
		return err
	}
	if err := reg.RegisterTaskSerializer(TaskTypeInvocation, InvocationTaskSerializer{}); err != nil {
		return err
	}
	if err := reg.RegisterTaskSerializer(TaskTypeBackoff, BackoffTaskSerializer{}); err != nil {
		return err
	}
	if err := reg.RegisterTaskSerializer(TaskTypeCancelation, CancelationTaskSerializer{}); err != nil {
		return err
	}
	if err := reg.RegisterTaskSerializer(TaskTypeCancelationBackoff, CancelationBackoffTaskSerializer{}); err != nil { // nolint:revive
		return err
	}
	return nil
}
