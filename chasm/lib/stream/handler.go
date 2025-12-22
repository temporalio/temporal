package stream

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
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

	// This is an upsert: we have not added a separate CreateStream API. However, CHASM lacks an
	// upsert API, so we are doing two transactions currently.
	// TODO: do this in one transaction

	_, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Stream](key),
		(*Stream).AddMessages,
		request.GetMessages(),
	)
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		// Stream doesn't exist, create it with the messages
		_, _, _, err = chasm.NewExecution(
			ctx,
			key,
			func(ctx chasm.MutableContext, req *workflowservice.AddToStreamRequest) (*Stream, int64, error) {
				s := newStream(req)
				_, err := s.AddMessages(ctx, req.GetMessages())
				return s, 0, err
			},
			request,
		)
	}
	if err != nil {
		return nil, err
	}
	return &streampb.AddToStreamResponse{
		FrontendResponse: &workflowservice.AddToStreamResponse{},
	}, nil
}

// PollStream long-polls for new messages on the stream.
func (h *handler) PollStream(
	ctx context.Context,
	req *streampb.PollStreamRequest,
) (*streampb.PollStreamResponse, error) {
	request := req.GetFrontendRequest()
	key := chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  request.GetStreamId(),
		RunID:       request.GetRunId(),
	}

	response, _, err := chasm.PollComponent(
		ctx,
		chasm.NewComponentRef[*Stream](key),
		func(
			s *Stream,
			ctx chasm.Context,
			req *workflowservice.PollStreamRequest,
		) (*workflowservice.PollStreamResponse, bool, error) {
			// TODO: this is fake implementation. Need to design correct wait condition predicate,
			// obtain correct set of new messages, pass cursor token back to caller, etc.
			messages := make([]*commonpb.Payload, 0, len(s.Messages))
			for _, field := range s.Messages {
				messages = append(messages, field.Get(ctx))
			}
			return &workflowservice.PollStreamResponse{
				Messages: messages,
			}, true, nil
		},
		request,
	)
	if err != nil {
		return nil, err
	}
	return &streampb.PollStreamResponse{
		FrontendResponse: response,
	}, nil
}
