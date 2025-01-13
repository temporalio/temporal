package activity

import (
	"time"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	Activity struct {
		// ALL FIELDS MUST BE EXPORTED for reflection to work

		// In V1, we will only support only one non-chasm.XXX field in the struct.
		// and that field must be a proto.Message.
		// TODO: define a serializer/deserializer interface?
		//
		// Framework will try to recognize the type and do serialization/deserialization
		// proto.Message is recommended so the component get compatibility if state definition changes
		Data persistencepb.ActivityInfo // proto.Message

		// At the end of a transition, framework will use reflection to understant the component
		// tree structure.

		// TODO: also support field name tag, so the fields can be renamed
		Input  *chasm.Field[*common.Payload] `chasm:"lazy"`
		Output *chasm.Field[*common.Payload] `chasm:"lazy"`

		EventNotifier *chasm.Field[EventNotifier]

		// forward compatibility in case new method got added to the chasm.Component interface
		chasm.UnimplementedComponent
	}
)

func NewScheduledActivity(
	chasmContext chasm.MutableContext,
	params *NewActivityRequest,
) (*Activity, *NewActivityResponse, error) {
	// after return framework will use reflection to analyze
	// and understand the structure of the component tree
	activity := &Activity{
		// State: persistencepb.ActivityInfo{},
	}
	if params.notifier != nil {
		// we need to give some guidance here, likely the implementation of the
		// notifier will just be the parent component itself (say Workflow),
		// as the handling logic will need to change the state of the parent component
		activity.EventNotifier = chasm.NewComponentPointerField(chasmContext, params.notifier)
	}

	_, err := activity.Schedule(chasmContext, &ScheduleRequest{
		Input: params.Input,
	})
	if err != nil {
		return nil, &NewActivityResponse{}, err
	}

	return activity, &NewActivityResponse{}, nil
}

func (i *Activity) Schedule(
	chasmContext chasm.MutableContext,
	req *ScheduleRequest,
) (*ScheduleResponse, error) {
	// also validate current state etc.

	i.Data.ScheduledTime = timestamppb.New(chasmContext.Now(i))
	i.Input = chasm.NewDataField(chasmContext, &common.Payload{
		Data: req.Input,
	})

	if err := chasmContext.AddTask(
		i,
		chasm.TaskAttributes{}, // immediate task
		DispatchTask{},
	); err != nil {
		return nil, err
	}
	if err := chasmContext.AddTask(
		i,
		chasm.TaskAttributes{
			ScheduledTime: chasmContext.Now(i).Add(10 * time.Second),
		},
		TimeoutTask{
			TimeoutType: TimeoutTypeScheduleToStart,
		},
	); err != nil {
		return nil, nil
	}

	return &ScheduleResponse{}, nil
}

func (i *Activity) GetDispatchInfo(
	chasmContext chasm.Context,
	t *DispatchTask,
) (*matchingservice.AddActivityTaskRequest, error) {
	panic("not implemented")
}

func (i *Activity) RecordStarted(
	chasmContext chasm.MutableContext,
	req *RecordStartedRequest,
) (*RecordStartedResponse, error) {

	// only this field will be updated
	i.Data.StartedTime = timestamppb.New(chasmContext.Now(i))
	// update other states

	payload, err := i.Input.Get(chasmContext)
	if err != nil {
		return nil, err
	}

	if err := chasmContext.AddTask(
		i,
		chasm.TaskAttributes{
			ScheduledTime: chasmContext.Now(i).Add(10 * time.Second),
		},
		TimeoutTask{
			TimeoutType: TimeoutTypeStartToClose,
		},
	); err != nil {
		return nil, nil
	}

	return &RecordStartedResponse{
		Input: payload.Data,
	}, nil
}

func (i *Activity) RecordCompleted(
	chasmContext chasm.MutableContext,
	req *RecordCompletedRequest,
) (*RecordCompletedResponse, error) {
	// say we have a completedTime field in ActivityInfo
	// i.State.CompletedTime = timestamppb.New(chasmContext.Now())
	output := &common.Payload{
		Data: req.Output,
	}
	i.Output = chasm.NewDataField(chasmContext, output)

	completedEvent := ActivityCompletedEvent{
		Output: output,
	}
	if notifier, err := i.EventNotifier.Get(chasmContext); err != nil && notifier != nil {
		if err := notifier.OnCompletion(completedEvent); err != nil {
			return nil, err
		}
	}

	return &RecordCompletedResponse{}, nil
}

func (i *Activity) Describe(
	_ chasm.Context,
	_ *DescribeActivityRequest,
) (*DescribeActivityResponse, error) {
	panic("not implemented")
}

func (i *Activity) LifecycleState() chasm.LifecycleState {
	panic("not implemented")
}
