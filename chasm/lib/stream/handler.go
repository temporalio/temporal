package stream

import (
	"context"
	"encoding/binary"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

const serverMaxMessages = 1000

type handler struct {
	streampb.UnimplementedStreamServiceServer
}

func newHandler() *handler {
	return &handler{}
}

// encodeCursor converts a message ID to an opaque bytes token.
// The cursor is an int64 encoded as 8 bytes using BigEndian byte order.
func encodeCursor(messageID int64) []byte {
	if messageID < 0 {
		messageID = 0
	}
	cursor := make([]byte, 8)
	binary.BigEndian.PutUint64(cursor, uint64(messageID))
	return cursor
}

// decodeCursor extracts a message ID from an opaque bytes token.
// Returns the message ID and an error if the cursor is invalid.
// An empty cursor is valid and represents starting from the beginning (message ID 0).
func decodeCursor(cursor []byte) (int64, error) {
	if len(cursor) == 0 {
		// Empty cursor means start from beginning
		return 0, nil
	}

	if len(cursor) != 8 {
		return 0, serviceerror.NewInvalidArgument(
			fmt.Sprintf("invalid cursor: expected 8 bytes, got %d", len(cursor)),
		)
	}

	messageID := int64(binary.BigEndian.Uint64(cursor))
	if messageID < 0 {
		return 0, serviceerror.NewInvalidArgument(
			fmt.Sprintf("invalid cursor: negative message ID %d", messageID),
		)
	}

	return messageID, nil
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

	// Decode cursor to get starting message ID
	startMessageID, err := decodeCursor(request.GetCursor())
	if err != nil {
		return nil, err
	}

	// Default to 100 if not specified, cap at serverMaxMessages
	maxMessages := request.GetMaxMessages()
	if maxMessages <= 0 {
		maxMessages = 100
	}
	maxMessages = min(maxMessages, serverMaxMessages)

	response, _, err := chasm.PollComponent(
		ctx,
		chasm.NewComponentRef[*Stream](key),
		func(
			s *Stream,
			ctx chasm.Context,
			req *workflowservice.PollStreamRequest,
		) (*workflowservice.PollStreamResponse, bool, error) {
			if s.Tail <= startMessageID {
				// No new messages yet, keep polling
				return nil, false, nil
			}

			// Calculate end position: either maxMessages ahead or tail, whichever is smaller
			endMessageID := min(startMessageID+int64(maxMessages), s.Tail)

			// Build messages array
			expectedCount := endMessageID - startMessageID
			messages := make([]*commonpb.Payload, 0, expectedCount)
			for id := startMessageID; id < endMessageID; id++ {
				msg, ok := s.Messages[id]
				if !ok {
					break
				}
				messages = append(messages, msg.Get(ctx))
			}

			lastMessageID := startMessageID + int64(len(messages)) - 1
			if len(messages) == 0 {
				lastMessageID = startMessageID
			}

			// Calculate next cursor: points to AFTER the last returned message
			nextCursor := s.Tail
			if len(messages) > 0 {
				nextCursor = lastMessageID + 1
			}

			return &workflowservice.PollStreamResponse{
				Messages:   messages,
				NextCursor: encodeCursor(nextCursor),
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
