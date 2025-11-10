package query

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseExecutionDurationStr(t *testing.T) {
	s := assert.New(t)

	testCases := []struct {
		input         string
		expectedValue time.Duration
		expectedErr   error
	}{
		{
			input:         "123",
			expectedValue: time.Duration(123),
		},
		{
			input:         "123s",
			expectedValue: 123 * time.Second,
		},
		{
			input:         "123m",
			expectedValue: 123 * time.Minute,
		},
		{
			input:         "123h",
			expectedValue: 123 * time.Hour,
		},
		{
			input:         "123d",
			expectedValue: 123 * 24 * time.Hour,
		},
		{
			input:         "01:02:03",
			expectedValue: 1*time.Hour + 2*time.Minute + 3*time.Second,
		},
		{
			input:       "01:60:03",
			expectedErr: errors.New("invalid duration"),
		},
		{
			input:       "123q",
			expectedErr: errors.New("invalid duration"),
		},
	}

	for _, tc := range testCases {
		got, err := ParseExecutionDurationStr(tc.input)
		if tc.expectedErr == nil {
			s.Equal(tc.expectedValue, got)
			s.NoError(err)
		} else {
			s.Error(err)
			s.Contains(err.Error(), tc.expectedErr.Error())
		}
	}
}

func TestParseRelativeOrAbsoluteTime(t *testing.T) {
	now := time.Now().UTC()

	t.Run("relative durations", func(t *testing.T) {
		testCases := []struct {
			name            string
			input           string
			expectedBefore  time.Time
			expectedAfter   time.Time
			allowedVariance time.Duration
		}{
			{
				name:            "5 minutes",
				input:           "5m",
				expectedBefore:  now.Add(5*time.Minute - time.Second),
				expectedAfter:   now.Add(5*time.Minute + time.Second),
				allowedVariance: 2 * time.Second,
			},
			{
				name:            "1 hour",
				input:           "1h",
				expectedBefore:  now.Add(1*time.Hour - time.Second),
				expectedAfter:   now.Add(1*time.Hour + time.Second),
				allowedVariance: 2 * time.Second,
			},
			{
				name:            "30 seconds",
				input:           "30s",
				expectedBefore:  now.Add(30*time.Second - time.Second),
				expectedAfter:   now.Add(30*time.Second + time.Second),
				allowedVariance: 2 * time.Second,
			},
			{
				name:            "2 days",
				input:           "2d",
				expectedBefore:  now.Add(48*time.Hour - time.Second),
				expectedAfter:   now.Add(48*time.Hour + time.Second),
				allowedVariance: 2 * time.Second,
			},
			{
				name:            "1 hour 30 minutes",
				input:           "1h30m",
				expectedBefore:  now.Add(90*time.Minute - time.Second),
				expectedAfter:   now.Add(90*time.Minute + time.Second),
				allowedVariance: 2 * time.Second,
			},
			{
				name:            "500 milliseconds",
				input:           "500ms",
				expectedBefore:  now.Add(500*time.Millisecond - time.Second),
				expectedAfter:   now.Add(500*time.Millisecond + time.Second),
				allowedVariance: 2 * time.Second,
			},
			{
				name:            "negative 5 minutes",
				input:           "-5m",
				expectedBefore:  now.Add(-5*time.Minute - time.Second),
				expectedAfter:   now.Add(-5*time.Minute + time.Second),
				allowedVariance: 2 * time.Second,
			},
			{
				name:            "negative 1 hour",
				input:           "-1h",
				expectedBefore:  now.Add(-1*time.Hour - time.Second),
				expectedAfter:   now.Add(-1*time.Hour + time.Second),
				allowedVariance: 2 * time.Second,
			},
			{
				name:            "negative 2 days",
				input:           "-2d",
				expectedBefore:  now.Add(-48*time.Hour - time.Second),
				expectedAfter:   now.Add(-48*time.Hour + time.Second),
				allowedVariance: 2 * time.Second,
			},
			{
				name:            "negative 1 hour 30 minutes",
				input:           "-1h30m",
				expectedBefore:  now.Add(-90*time.Minute - time.Second),
				expectedAfter:   now.Add(-90*time.Minute + time.Second),
				allowedVariance: 2 * time.Second,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := ParseRelativeOrAbsoluteTime(tc.input)
				require.NoError(t, err)

				// Check that the result is within acceptable range
				// (accounting for test execution time)
				assert.True(t, result.After(tc.expectedBefore) || result.Equal(tc.expectedBefore),
					"result %v should be after or equal to %v", result, tc.expectedBefore)
				assert.True(t, result.Before(tc.expectedAfter) || result.Equal(tc.expectedAfter),
					"result %v should be before or equal to %v", result, tc.expectedAfter)
			})
		}
	})

	t.Run("absolute timestamps", func(t *testing.T) {
		testCases := []struct {
			name     string
			input    string
			expected time.Time
		}{
			{
				name:     "RFC3339Nano with timezone",
				input:    "2025-10-16T15:04:05.999999999Z",
				expected: time.Date(2025, 10, 16, 15, 4, 5, 999999999, time.UTC),
			},
			{
				name:     "RFC3339Nano with offset",
				input:    "2025-10-16T15:04:05.123456789-08:00",
				expected: time.Date(2025, 10, 16, 23, 4, 5, 123456789, time.UTC),
			},
			{
				name:     "RFC3339 without nanoseconds",
				input:    "2025-10-16T15:04:05Z",
				expected: time.Date(2025, 10, 16, 15, 4, 5, 0, time.UTC),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := ParseRelativeOrAbsoluteTime(tc.input)
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			})
		}
	})

	t.Run("invalid inputs", func(t *testing.T) {
		testCases := []struct {
			name  string
			input string
		}{
			{
				name:  "invalid duration unit",
				input: "5q",
			},
			{
				name:  "invalid timestamp format",
				input: "2025-13-40",
			},
			{
				name:  "random string",
				input: "invalid",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := ParseRelativeOrAbsoluteTime(tc.input)
				assert.Error(t, err)
			})
		}
	})
}
