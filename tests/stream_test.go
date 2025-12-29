package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type streamSuite struct {
	testcore.FunctionalTestBase
	tv *testvars.TestVars
}

func TestStreamSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(streamSuite))
}

func (s *streamSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuite()
	s.OverrideDynamicConfig(
		dynamicconfig.EnableChasm,
		true,
	)
	s.OverrideDynamicConfig(
		activity.Enabled,
		true,
	)
}

func (s *streamSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.tv = testvars.New(s.T())
}

func (s *streamSuite) TestHappyPath() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Create the stream with initial messages
	_, err := s.FrontendClient().CreateStream(ctx, &workflowservice.CreateStreamRequest{
		Namespace: s.Namespace().String(),
		StreamId:  "my-stream-id",
		Messages: []*commonpb.Payload{
			{Data: []byte("hello")},
			{Data: []byte("world")},
		},
	})
	s.NoError(err)

	// First poll: read from beginning (no cursor)
	resp1, err := s.FrontendClient().PollStream(ctx, &workflowservice.PollStreamRequest{
		Namespace:   s.Namespace().String(),
		StreamId:    "my-stream-id",
		MaxMessages: 100, // Request up to 100 messages
	})
	s.NoError(err)
	s.Equal(2, len(resp1.Messages))
	s.Equal([]byte("hello"), resp1.Messages[0].Data)
	s.Equal([]byte("world"), resp1.Messages[1].Data)
	s.NotNil(resp1.NextCursor, "NextCursor should be set")

	// Add more messages
	_, err = s.FrontendClient().AddToStream(ctx, &workflowservice.AddToStreamRequest{
		Namespace: s.Namespace().String(),
		StreamId:  "my-stream-id",
		Messages: []*commonpb.Payload{
			{Data: []byte("goodbye")},
			{Data: []byte("friend")},
		},
	})
	s.NoError(err)

	// Second poll: read ONLY new messages using cursor
	resp2, err := s.FrontendClient().PollStream(ctx, &workflowservice.PollStreamRequest{
		Namespace:   s.Namespace().String(),
		StreamId:    "my-stream-id",
		Cursor:      resp1.NextCursor, // Resume from where we left off
		MaxMessages: 100,
	})
	s.NoError(err)
	s.Equal(2, len(resp2.Messages))
	s.Equal([]byte("goodbye"), resp2.Messages[0].Data)
	s.Equal([]byte("friend"), resp2.Messages[1].Data)
	s.NotNil(resp2.NextCursor, "NextCursor should be set")

	// Fourth poll: read from beginning again (no cursor)
	// Should get ALL messages
	respAll, err := s.FrontendClient().PollStream(ctx, &workflowservice.PollStreamRequest{
		Namespace:   s.Namespace().String(),
		StreamId:    "my-stream-id",
		MaxMessages: 100,
	})
	s.NoError(err)
	s.Equal(4, len(respAll.Messages), "Should get all messages when no cursor")
	s.Equal([]byte("hello"), respAll.Messages[0].Data)
	s.Equal([]byte("world"), respAll.Messages[1].Data)
	s.Equal([]byte("goodbye"), respAll.Messages[2].Data)
	s.Equal([]byte("friend"), respAll.Messages[3].Data)
}

// TestCreateEmptyThenAdd tests creating an empty stream and then adding messages to it in a separate call.
// This is important for the streaming API to support creating streams without initial messages.
func (s *streamSuite) TestCreateEmptyThenAdd() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Step 1: Create an empty stream (no initial messages)
	createResp, err := s.FrontendClient().CreateStream(ctx, &workflowservice.CreateStreamRequest{
		Namespace: s.Namespace().String(),
		StreamId:  "empty-stream-id",
		// No Messages provided - create empty stream
	})
	s.NoError(err, "CreateStream should succeed")
	s.NotEmpty(createResp.GetRunId(), "RunId should be returned")

	// Step 2: Add messages to the stream in a separate API call
	_, err = s.FrontendClient().AddToStream(ctx, &workflowservice.AddToStreamRequest{
		Namespace: s.Namespace().String(),
		StreamId:  "empty-stream-id",
		RunId:     createResp.GetRunId(),
		Messages: []*commonpb.Payload{
			{
				Data: []byte("first"),
			},
			{
				Data: []byte("message"),
			},
		},
	})
	s.NoError(err, "AddToStream should succeed on empty stream")

	// Step 3: Verify the messages were added by polling the stream
	pollResp, err := s.FrontendClient().PollStream(ctx, &workflowservice.PollStreamRequest{
		Namespace: s.Namespace().String(),
		StreamId:  "empty-stream-id",
		RunId:     createResp.GetRunId(),
	})
	s.NoError(err, "PollStream should succeed")

	messages := pollResp.GetMessages()
	s.Equal(2, len(messages), "Should have 2 messages")
	s.Equal([]byte("first"), messages[0].GetData())
	s.Equal([]byte("message"), messages[1].GetData())
}

func (s *streamSuite) TestAddToNonExistentStreamFails() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Try to add to a stream that doesn't exist - should fail
	_, err := s.FrontendClient().AddToStream(ctx, &workflowservice.AddToStreamRequest{
		Namespace: s.Namespace().String(),
		StreamId:  "nonexistent-stream",
		Messages: []*commonpb.Payload{
			{
				Data: []byte("fail"),
			},
		},
	})
	s.Error(err)
	s.Contains(err.Error(), "not found")
}

// TestCursorWithMaxMessages tests that the MaxMessages limit works correctly
// with cursor-based pagination, allowing clients to fetch messages in batches.
func (s *streamSuite) TestCursorWithMaxMessages() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Create a stream with 10 messages
	initialMessages := make([]*commonpb.Payload, 10)
	for i := 0; i < 10; i++ {
		initialMessages[i] = &commonpb.Payload{
			Data: []byte(fmt.Sprintf("message-%d", i)),
		}
	}

	_, err := s.FrontendClient().CreateStream(ctx, &workflowservice.CreateStreamRequest{
		Namespace: s.Namespace().String(),
		StreamId:  "batch-stream",
		Messages:  initialMessages,
	})
	s.NoError(err)

	// Poll in batches of 3 messages each
	// Expected: 10 messages total, batch size 3 => 4 polls (3+3+3+1)
	var allMessages []*commonpb.Payload
	var cursor []byte
	batchSize := int32(3)

	// Poll multiple times to fetch all messages in batches
	for i := 0; i < 5; i++ { // Max 5 iterations to avoid infinite loop
		resp, err := s.FrontendClient().PollStream(ctx, &workflowservice.PollStreamRequest{
			Namespace:   s.Namespace().String(),
			StreamId:    "batch-stream",
			Cursor:      cursor,
			MaxMessages: batchSize,
		})
		s.NoError(err)

		messages := resp.GetMessages()

		// Verify we don't get more than MaxMessages in a single response
		if len(messages) > 0 {
			s.LessOrEqual(len(messages), int(batchSize), "Should not exceed MaxMessages limit")
			allMessages = append(allMessages, messages...)
		}

		cursor = resp.GetNextCursor()
		s.NotNil(cursor, "NextCursor should always be set")

		// Stop when we've received all expected messages
		if len(allMessages) >= 10 {
			break
		}
	}

	// Verify we got all 10 messages in the correct order
	s.Equal(10, len(allMessages), "Should have retrieved all 10 messages")
	for i := 0; i < 10; i++ {
		expected := fmt.Sprintf("message-%d", i)
		s.Equal([]byte(expected), allMessages[i].Data, "Messages should be in correct order")
	}
}

// TestInvalidCursor tests error handling for malformed cursors.
func (s *streamSuite) TestInvalidCursor() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Create a stream with some messages
	_, err := s.FrontendClient().CreateStream(ctx, &workflowservice.CreateStreamRequest{
		Namespace: s.Namespace().String(),
		StreamId:  "cursor-test-stream",
		Messages: []*commonpb.Payload{
			{Data: []byte("msg1")},
			{Data: []byte("msg2")},
		},
	})
	s.NoError(err)

	// Test 1: Empty cursor should succeed (reads from beginning)
	resp, err := s.FrontendClient().PollStream(ctx, &workflowservice.PollStreamRequest{
		Namespace:   s.Namespace().String(),
		StreamId:    "cursor-test-stream",
		Cursor:      []byte{}, // Empty cursor
		MaxMessages: 100,
	})
	s.NoError(err, "Empty cursor should be valid")
	s.Equal(2, len(resp.Messages), "Empty cursor should read from beginning")

	// Test 2: Invalid cursor length (not 8 bytes) should fail
	_, err = s.FrontendClient().PollStream(ctx, &workflowservice.PollStreamRequest{
		Namespace:   s.Namespace().String(),
		StreamId:    "cursor-test-stream",
		Cursor:      []byte{1, 2, 3}, // Wrong length: 3 bytes instead of 8
		MaxMessages: 100,
	})
	s.Error(err, "Invalid cursor length should return error")
	s.Contains(err.Error(), "invalid cursor", "Error should mention invalid cursor")

	// Test 3: Another invalid cursor length (too long)
	_, err = s.FrontendClient().PollStream(ctx, &workflowservice.PollStreamRequest{
		Namespace:   s.Namespace().String(),
		StreamId:    "cursor-test-stream",
		Cursor:      []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, // 10 bytes instead of 8
		MaxMessages: 100,
	})
	s.Error(err, "Invalid cursor length should return error")
	s.Contains(err.Error(), "invalid cursor", "Error should mention invalid cursor")
}
