package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

func TestIsEmptyPayload(t *testing.T) {
	require.True(t, IsEmptyPayload(nil))
	require.True(t, IsEmptyPayload(&commonpb.Payload{}))
	require.False(t, IsEmptyPayload(&commonpb.Payload{Data: []byte("x")}))
	require.False(t, IsEmptyPayload(&commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/null")}}))
}
