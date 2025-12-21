package stream

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

type handler struct {
	streampb.UnimplementedStreamServiceServer
}

func newHandler() *handler {
	return &handler{}
}

// AddToStream appends messages to the stream.
func (h *handler) AddToStream(
	ctx context.Context,
	req *streampb.AddToStreamRequest,
) (*streampb.AddToStreamResponse, error) {
	request := req.GetFrontendRequest()
	key := chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  request.GetStreamId(),
		RunID:       request.GetRunId(),
	}
	}, nil
}

// PollStream long-polls for new messages on the stream.
func (h *handler) PollStream(
	ctx context.Context,
	req *streampb.PollStreamRequest,
) (*streampb.PollStreamResponse, error) {
	return &streampb.PollStreamResponse{
		FrontendResponse: &workflowservice.PollStreamResponse{},
	}, nil
}
