package activity

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

var defaultRequestID = uuid.MustParse("123e4567-e89b-12d3-a456-426614174000").String()

func TestValidateAndUpdateRequestID(t *testing.T) {
	tests := []struct {
		name            string
		existingID      string
		incomingID      string
		isErr           bool
		expectedIDAfter string
	}{
		{
			name:            "sets when existing empty",
			existingID:      "",
			incomingID:      defaultRequestID,
			isErr:           false,
			expectedIDAfter: defaultRequestID,
		},
		{
			name:            "no op when same id",
			existingID:      defaultRequestID,
			incomingID:      defaultRequestID,
			isErr:           false,
			expectedIDAfter: defaultRequestID,
		},
		{
			name:            "error when different id",
			existingID:      defaultRequestID,
			incomingID:      uuid.New().String(),
			isErr:           true,
			expectedIDAfter: defaultRequestID,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := &Activity{}
			attempt := &activitypb.ActivityAttemptState{RequestId: tc.existingID}

			err := a.validateAndUpdateRequestID(attempt, tc.incomingID)

			if tc.isErr {
				require.Error(t, err)
				var alreadyStarted *serviceerrors.TaskAlreadyStarted
				require.ErrorAs(t, err, &alreadyStarted, "expected TaskAlreadyStarted error, got %T: %v", err, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.expectedIDAfter, attempt.GetRequestId())
		})
	}
}
