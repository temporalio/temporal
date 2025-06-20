package protocol_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/server/common/protocol"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestNilSafety(t *testing.T) {
	t.Parallel()

	t.Run("nil message", func(t *testing.T) {
		pt, mt := protocol.IdentifyOrUnknown(nil)
		require.Equal(t, protocol.TypeUnknown, pt)
		require.Equal(t, protocol.MessageTypeUnknown, mt)
	})

	t.Run("nil message body", func(t *testing.T) {
		pt, mt := protocol.IdentifyOrUnknown(&protocolpb.Message{})
		require.Equal(t, protocol.TypeUnknown, pt)
		require.Equal(t, protocol.MessageTypeUnknown, mt)
	})
}

func TestWithValidMessage(t *testing.T) {
	t.Parallel()

	var empty emptypb.Empty
	var body anypb.Any
	require.NoError(t, body.MarshalFrom(&empty))

	msg := protocolpb.Message{Body: &body}

	pt, mt := protocol.IdentifyOrUnknown(&msg)

	require.Equal(t, "google.protobuf", pt.String())
	require.Equal(t, "google.protobuf.Empty", mt.String())
}

func TestWithInvalidBody(t *testing.T) {
	t.Parallel()

	var empty emptypb.Empty
	var body anypb.Any
	require.NoError(t, body.MarshalFrom(&empty))

	msg := protocolpb.Message{Body: &body}
	msg.Body.TypeUrl = "this isn't valid"

	_, _, err := protocol.Identify(&msg)
	require.Error(t, err)
}
