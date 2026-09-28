package github

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunShortSHA(t *testing.T) {
	tests := []struct {
		name     string
		sha      string
		expected string
	}{
		{
			name:     "normal SHA",
			sha:      "abc1234567890defghijk",
			expected: "abc1234",
		},
		{
			name:     "short SHA",
			sha:      "abc123",
			expected: "abc123",
		},
		{
			name:     "exactly 7 chars",
			sha:      "abc1234",
			expected: "abc1234",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := Run{HeadSHA: tt.sha}
			require.Equal(t, tt.expected, run.ShortSHA())
		})
	}
}
