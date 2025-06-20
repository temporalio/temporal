package query

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
