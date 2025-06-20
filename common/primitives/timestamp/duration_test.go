package timestamp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestValidateAndCapProtoDuration(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                  string
		timerDuration         *durationpb.Duration
		expectedErr           error
		expectedTimerDuration *durationpb.Duration
	}{
		{
			name:                  "nil timer duration",
			timerDuration:         nil,
			expectedErr:           nil,
			expectedTimerDuration: nil,
		},
		{
			name:                  "negative timer duration",
			timerDuration:         durationpb.New(-time.Minute),
			expectedErr:           errNegativeDuration,
			expectedTimerDuration: nil,
		},
		{
			name:                  "zero timer duration",
			timerDuration:         durationpb.New(0),
			expectedErr:           nil,
			expectedTimerDuration: durationpb.New(0),
		},
		{
			name:                  "valid timer duration",
			timerDuration:         durationpb.New(time.Hour),
			expectedErr:           nil,
			expectedTimerDuration: durationpb.New(time.Hour),
		},
		{
			name:                  "mismatched signs",
			timerDuration:         &durationpb.Duration{Seconds: 360, Nanos: -100},
			expectedErr:           errMismatchedSigns,
			expectedTimerDuration: nil,
		},
		{
			name:                  "large duration",
			timerDuration:         durationpb.New(280 * 365 * 24 * time.Hour),
			expectedErr:           nil,
			expectedTimerDuration: durationpb.New(maxAllowedDuration),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			actualErr := ValidateAndCapProtoDuration(tc.timerDuration)

			assert.Equal(t, tc.expectedErr, actualErr)
			if tc.expectedErr == nil {
				assert.Equal(t, tc.expectedTimerDuration, tc.timerDuration)
			}
		})
	}
}
