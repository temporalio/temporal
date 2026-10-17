package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
)

func TestLongPollBuffer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		timeout  time.Duration
		expected time.Duration
	}{
		{
			name:     "more than one buffer left",
			timeout:  time.Minute,
			expected: common.DefaultLongPollBuffer,
		},
		{
			name:     "no more than one buffer left",
			timeout:  900 * time.Millisecond,
			expected: 0,
		},
		{
			name:     "deadline already passed",
			timeout:  -time.Second,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), tt.timeout)
			defer cancel()
			require.Equal(t, tt.expected, longPollBuffer(ctx))
		})
	}

	t.Run("no deadline", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, common.DefaultLongPollBuffer, longPollBuffer(t.Context()))
	})
}
