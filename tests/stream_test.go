package tests

import (
	"context"
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

	// Create the stream first
	_, err := s.FrontendClient().CreateStream(ctx, &workflowservice.CreateStreamRequest{
		Namespace: s.Namespace().String(),
		StreamId:  "my-stream-id",
		InitialMessages: []*commonpb.Payload{
			{
				Data: []byte("hello"),
			},
			{
				Data: []byte("world"),
			},
		},
	})
	s.NoError(err)

	resp, err := s.FrontendClient().PollStream(ctx, &workflowservice.PollStreamRequest{
		Namespace: s.Namespace().String(),
		StreamId:  "my-stream-id",
	})
	s.NoError(err)
	messages := resp.GetMessages()
	s.Equal(2, len(messages))
	s.Equal([]byte("hello"), messages[0].GetData())
	s.Equal([]byte("world"), messages[1].GetData())

	_, err = s.FrontendClient().AddToStream(ctx, &workflowservice.AddToStreamRequest{
		Namespace: s.Namespace().String(),
		StreamId:  "my-stream-id",
		Messages: []*commonpb.Payload{
			{
				Data: []byte("hello"),
			},
			{
				Data: []byte("again"),
			},
		},
	})
	s.NoError(err)

	resp, err = s.FrontendClient().PollStream(ctx, &workflowservice.PollStreamRequest{
		Namespace: s.Namespace().String(),
		StreamId:  "my-stream-id",
	})
	s.NoError(err)
	messages = resp.GetMessages()
	s.Equal(4, len(messages))
	s.Equal([]byte("hello"), messages[0].GetData())
	s.Equal([]byte("world"), messages[1].GetData())
	s.Equal([]byte("hello"), messages[2].GetData())
	s.Equal([]byte("again"), messages[3].GetData())
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
		// No InitialMessages provided - create empty stream
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
