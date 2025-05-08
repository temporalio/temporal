package matcher

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/serviceerror"
)

// TestNewMatcherError ensures that NewMatcherError formats its output correctly
// and returns an object of type *Error.
func TestNewMatcherError(t *testing.T) {
	tests := []struct {
		name        string
		format      string
		args        []any
		expectedMsg string
	}{
		{
			name:        "simple message",
			format:      "a simple message",
			args:        nil,
			expectedMsg: "a simple message",
		},
		{
			name:        "formatted message",
			format:      "value = %d, str = %s",
			args:        []any{42, "hello"},
			expectedMsg: "value = 42, str = hello",
		},
		{
			name:        "no format specifiers",
			format:      "no format specifiers",
			args:        nil,
			expectedMsg: "no format specifiers",
		},
		{
			name:        "multiple format specifiers",
			format:      "%s: %s -> %d",
			args:        []any{"error", "some detail", 123},
			expectedMsg: "error: some detail -> 123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var matchErr *serviceerror.InvalidArgument
			err := NewMatcherError(tt.format, tt.args...)

			assert.Error(t, err)
			assert.ErrorAs(t, err, &matchErr)
			assert.Equal(t, tt.expectedMsg, matchErr.Error())
		})
	}
}
