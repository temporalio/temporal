package stream

import (
	"context"
	"sort"

	commonpb "go.temporal.io/api/common/v1"
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

// CreateStream creates a new stream with optional initial messages.
func (h *handler) CreateStream(
	ctx context.Context,
	req *streampb.CreateStreamRequest,
) (*streampb.CreateStreamResponse, error) {
	request := req.GetFrontendRequest()

	// Use the provided RunId (or empty string if not provided)
	// CHASM identifies streams by (NamespaceID, StreamID, RunID)
	// An empty RunId is valid and commonly used
	runID := request.GetRunId()

	key := chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  request.GetStreamId(),
		RunID:       runID,
	}

	resp, executionKey, _, err := chasm.NewExecution(
		ctx,
		key,
		CreateStream,
		req,
	)

	if err != nil {
		return nil, err
	}

	// Use the generated RunID from CHASM
	resp.FrontendResponse.RunId = executionKey.RunID

	return resp, nil
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

	// Simple update - single transaction
	_, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Stream](key),
		(*Stream).AddMessages,
		request.GetMessages(),
	)
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

			// Collect message IDs and sort them to ensure deterministic ordering
			messageIDs := make([]int64, 0, len(s.Messages))
			for id := range s.Messages {
				messageIDs = append(messageIDs, id)
			}
			// Sort by message ID (which corresponds to insertion order)
			sort.Slice(messageIDs, func(i, j int) bool {
				return messageIDs[i] < messageIDs[j]
			})

			// Build messages array in sorted order
			messages := make([]*commonpb.Payload, 0, len(messageIDs))
			for _, id := range messageIDs {
				messages = append(messages, s.Messages[id].Get(ctx))
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
