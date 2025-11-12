package activity

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var defaultRequestID = uuid.MustParse("123e4567-e89b-12d3-a456-426614174000").String()

var testNow = time.Date(2022, 1, 2, 3, 4, 5, 0, time.UTC)

func newTestActivity(ctx *chasm.MockMutableContext, attempt *activitypb.ActivityAttemptState) *Activity {
	ctx.HandleNow = func(chasm.Component) time.Time { return testNow }
	return &Activity{
		ActivityState: &activitypb.ActivityState{
			ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
			ScheduleToStartTimeout: durationpb.New(2 * time.Minute),
			StartToCloseTimeout:    durationpb.New(3 * time.Minute),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		Attempt:     chasm.NewDataField(ctx, attempt),
		Outcome:     chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{}),
	}
}

func TestRecordActivityTaskStarted_RequestIDScenarios(t *testing.T) {
	testCases := []struct {
		name              string
		attempt           *activitypb.ActivityAttemptState
		params            RecordActivityTaskStartedParams
		expectedError     error
		responseExpected  bool
		expectedRequestID string
		expectedWorker    string
		checkStartedTime  bool
		useOriginalStart  bool // if true, expect started time to remain original; else expect it to be testNow
	}{
		{
			name: "fresh start sets request id and fields",
			attempt: &activitypb.ActivityAttemptState{
				Count: 1,
			},
			params: RecordActivityTaskStartedParams{
				RequestID:      defaultRequestID,
				WorkerIdentity: "worker-A",
			},
			responseExpected:  true,
			expectedRequestID: defaultRequestID,
			expectedWorker:    "worker-A",
			checkStartedTime:  true,
			useOriginalStart:  false,
		},
		{
			name: "idempotent retry with same request id is no-op",
			attempt: &activitypb.ActivityAttemptState{
				Count:              1,
				RequestId:          defaultRequestID,
				LastStartedTime:    timestamppb.New(testNow.Add(-time.Minute)),
				LastWorkerIdentity: "worker-A",
			},
			params: RecordActivityTaskStartedParams{
				RequestID:      defaultRequestID, // same as existing
				WorkerIdentity: "worker-B",       // should be ignored
			},
			responseExpected:  true,
			expectedRequestID: defaultRequestID,
			expectedWorker:    "worker-A",
			checkStartedTime:  true,
			useOriginalStart:  true,
		},
		{
			name: "different request id returns TaskAlreadyStarted error",
			attempt: &activitypb.ActivityAttemptState{
				Count:     1,
				RequestId: defaultRequestID,
			},
			params: RecordActivityTaskStartedParams{
				RequestID:      uuid.New().String(),
				WorkerIdentity: "worker-C",
			},
			expectedError:     serviceerrors.NewTaskAlreadyStarted("Activity"),
			responseExpected:  false,
			expectedRequestID: defaultRequestID,
			checkStartedTime:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}

			attempt := tc.attempt
			a := newTestActivity(ctx, attempt)

			resp, err := a.RecordActivityTaskStarted(ctx, tc.params)

			if tc.expectedError != nil {
				require.Error(t, err)
				require.Equal(t, tc.expectedError, err)
				require.Nil(t, resp)
			} else {
				require.NoError(t, err)
				if tc.responseExpected {
					require.NotNil(t, resp)
					require.Equal(t, attempt.GetCount(), resp.GetAttempt())
					if attempt.GetLastStartedTime() != nil {
						require.NotNil(t, resp.GetStartedTime())
						require.Equal(t, attempt.GetLastStartedTime().AsTime(), resp.GetStartedTime().AsTime())
					}

				}
			}

			require.Equal(t, tc.expectedRequestID, attempt.GetRequestId())
			if tc.expectedWorker != "" {
				require.Equal(t, tc.expectedWorker, attempt.GetLastWorkerIdentity())
			}

			if tc.checkStartedTime {
				if tc.useOriginalStart {
					require.Equal(t, tc.attempt.GetLastStartedTime().AsTime(), attempt.GetLastStartedTime().AsTime())
				} else {
					require.Equal(t, timestamppb.New(testNow).AsTime(), attempt.GetLastStartedTime().AsTime())
				}
			}
		})
	}
}
