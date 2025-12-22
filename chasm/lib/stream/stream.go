package stream

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

type Stream struct {
	chasm.UnimplementedComponent
	*streampb.StreamState
	Messages chasm.Map[int64, *commonpb.Payload]
}

func newStream(_ *workflowservice.AddToStreamRequest) *Stream {
	return &Stream{
		StreamState: &streampb.StreamState{},
		Messages:    make(chasm.Map[int64, *commonpb.Payload]),
	}
}

func (s *Stream) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (s *Stream) AddMessages(ctx chasm.MutableContext, messages []*commonpb.Payload) (int64, error) {
	var messageID int64
	for _, message := range messages {
		messageID = s.Tail
		s.Tail++
		s.Messages[messageID] = chasm.NewDataField(ctx, message)
	}
	return messageID, nil
}
