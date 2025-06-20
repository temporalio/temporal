package sqlquery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExtractStringValue(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    any
		expectError bool
	}{
		{
			name:        "empty string",
			input:       "",
			expected:    "",
			expectError: true,
		},
		{
			name:        "empty quoted string",
			input:       "",
			expected:    "",
			expectError: true,
		},
		{
			name:     "quoted string",
			input:    "'hello world'",
			expected: "hello world",
		},
		{
			name:     "quoted number string",
			input:    "'123'",
			expected: "123",
		},
		{
			name:     "quoted string with spaces",
			input:    "'  spaced  '",
			expected: "  spaced  ",
		},
		{
			name:     "empty quoted string",
			input:    "''",
			expected: "",
		},
		{
			name:        "not quoted string",
			input:       "not_quoted",
			expectError: true,
		},
		{
			name:        "quoted only at start",
			input:       "'missing_end",
			expectError: true,
		},
		{
			name:        "quoted only at end",
			input:       "missing_start'",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := ExtractStringValue(tt.input)
			if tt.expectError {
				assert.Error(t, err)
				assert.Empty(t, val)
				assert.Contains(t, err.Error(), "not a string value")
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, val)

			}
		})
	}
}

func TestConvertSqlValue(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    any
		expectError bool
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "quoted string",
			input:    "'hello world'",
			expected: "hello world",
		},
		{
			name:     "quoted number string",
			input:    "'123'",
			expected: "123",
		},
		{
			name:     "quoted string with spaces",
			input:    "'  spaced  '",
			expected: "  spaced  ",
		},
		{
			name:     "int value",
			input:    "12345",
			expected: int64(12345),
		},
		{
			name:     "negative int value",
			input:    "-42",
			expected: int64(-42),
		},
		{
			name:     "float value",
			input:    "3.14159",
			expected: 3.14159,
		},
		{
			name:        "unquoted string",
			input:       "abc",
			expectError: true,
		},
		{
			name:        "mixed number string",
			input:       "123abc",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := ParseValue(tt.input)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, val)
			}
		})
	}
}

func TestConvertToTime(t *testing.T) {
	t.Run("acceptance", func(t *testing.T) {
		// RFC3339 format requires a "T" separator and a timezone, e.g. "2025-01-11T13:14:15Z".
		input := "'2023-10-26T14:30:00Z'"
		expectedTime, _ := time.Parse(DefaultDateTimeFormat, "2023-10-26T14:30:00Z")
		actualTime, err := ConvertToTime(input)
		assert.NoError(t, err)
		assert.Equal(t, expectedTime, actualTime)
	})

	t.Run("NoQuotes", func(t *testing.T) {
		input := "2025-01-11T13:14:15Z"
		actualTime, err := ConvertToTime(input)
		assert.Error(t, err)
		assert.Zero(t, actualTime)
	})

	t.Run("invalid time format", func(t *testing.T) {
		input := "'2025-13-40 14:02:04'"
		actualTime, err := ConvertToTime(input)
		assert.Error(t, err)
		assert.Zero(t, actualTime)
	})

	t.Run("random string", func(t *testing.T) {
		input := "'abs'"
		actualTime, err := ConvertToTime(input)
		assert.Error(t, err)
		assert.Zero(t, actualTime)
	})
}
