package nexusoperation

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/protoutils"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestIsWaitStageReached(t *testing.T) {
	t.Parallel()

	ctx := &chasm.MockContext{}
	allStatuses := protoutils.EnumValues[nexusoperationpb.OperationStatus]()

	tests := []struct {
		name       string
		waitStage  enumspb.NexusOperationWaitStage
		reached    []nexusoperationpb.OperationStatus
		notReached []nexusoperationpb.OperationStatus
	}{
		{
			name:       "Unspecified",
			waitStage:  enumspb.NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED,
			notReached: allStatuses,
		},
		{
			name:      "Started",
			waitStage: enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
			reached: []nexusoperationpb.OperationStatus{
				nexusoperationpb.OPERATION_STATUS_STARTED,
				nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
				nexusoperationpb.OPERATION_STATUS_FAILED,
				nexusoperationpb.OPERATION_STATUS_CANCELED,
				nexusoperationpb.OPERATION_STATUS_TERMINATED,
				nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			},
			notReached: []nexusoperationpb.OperationStatus{
				nexusoperationpb.OPERATION_STATUS_UNSPECIFIED,
				nexusoperationpb.OPERATION_STATUS_SCHEDULED,
				nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			},
		},
		{
			name:      "Closed",
			waitStage: enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
			reached: []nexusoperationpb.OperationStatus{
				nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
				nexusoperationpb.OPERATION_STATUS_FAILED,
				nexusoperationpb.OPERATION_STATUS_CANCELED,
				nexusoperationpb.OPERATION_STATUS_TERMINATED,
				nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			},
			notReached: []nexusoperationpb.OperationStatus{
				nexusoperationpb.OPERATION_STATUS_UNSPECIFIED,
				nexusoperationpb.OPERATION_STATUS_SCHEDULED,
				nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
				nexusoperationpb.OPERATION_STATUS_STARTED,
			},
		},
	}

	coveredWaitStages := []enumspb.NexusOperationWaitStage{}
	for _, tt := range tests {
		coveredWaitStages = append(coveredWaitStages, tt.waitStage)
		t.Run(tt.name, func(t *testing.T) {
			op := newTestOperation()

			coveredStatuses := append(slices.Clone(tt.reached), tt.notReached...)
			require.ElementsMatch(t, allStatuses, coveredStatuses)

			for _, status := range tt.reached {
				op.Status = status
				require.Truef(t, op.isWaitStageReached(ctx, tt.waitStage), "expected %s to match %s", status, tt.waitStage)
			}

			for _, status := range tt.notReached {
				op.Status = status
				require.Falsef(t, op.isWaitStageReached(ctx, tt.waitStage), "expected %s not to match %s", status, tt.waitStage)
			}
		})
	}

	allWaitStages := protoutils.EnumValues[enumspb.NexusOperationWaitStage]()
	require.ElementsMatch(t, allWaitStages, coveredWaitStages)
}

func newScheduledTestOperation(t *testing.T, ctx *chasm.MockMutableContext) *Operation {
	t.Helper()
	op := newTestOperation()
	require.NoError(t, TransitionScheduled.Apply(op, ctx, EventScheduled{}))
	return op
}

func TestHandleNexusCompletion(t *testing.T) {
	newStartedOp := func(t *testing.T, ctx *chasm.MockMutableContext) *Operation {
		t.Helper()
		op := newScheduledTestOperation(t, ctx)
		require.NoError(t, TransitionStarted.Apply(op, ctx, EventStarted{OperationToken: "tok"}))
		return op
	}
	newCtx := func() *chasm.MockMutableContext {
		return &chasm.MockMutableContext{
			MockContext: chasm.MockContext{
				HandleNow: func(chasm.Component) time.Time { return defaultTime },
			},
		}
	}

	t.Run("Success", func(t *testing.T) {
		t.Run("AfterStarted", func(t *testing.T) {
			ctx := newCtx()
			op := newStartedOp(t, ctx)
			err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: op.GetRequestId(),
				Outcome: &persistencespb.ChasmNexusCompletion_Success{
					Success: mustToPayload(t, "result"),
				},
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, op.GetStatus())
		})

		t.Run("CompletionBeforeStart", func(t *testing.T) {
			ctx := newCtx()
			op := newScheduledTestOperation(t, ctx)
			startTime := defaultTime.Add(-time.Second)
			err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				StartTime:      timestamppb.New(startTime),
				RequestId:      op.GetRequestId(),
				OperationToken: "tok",
				Outcome: &persistencespb.ChasmNexusCompletion_Success{
					Success: mustToPayload(t, "result"),
				},
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, op.GetStatus())
			require.Equal(t, "tok", op.GetOperationToken())
			require.Equal(t, startTime, op.GetStartedTime().AsTime())
		})

		t.Run("CompletionBeforeStartWithoutStartTime", func(t *testing.T) {
			ctx := newCtx()
			op := newScheduledTestOperation(t, ctx)
			err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId:      op.GetRequestId(),
				OperationToken: "tok",
				Outcome: &persistencespb.ChasmNexusCompletion_Success{
					Success: mustToPayload(t, "result"),
				},
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, op.GetStatus())
			require.Equal(t, "tok", op.GetOperationToken())
			require.Equal(t, defaultTime, op.GetStartedTime().AsTime())
		})
	})

	t.Run("Failure", func(t *testing.T) {
		t.Run("AfterStarted", func(t *testing.T) {
			ctx := newCtx()
			op := newStartedOp(t, ctx)
			err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: op.GetRequestId(),
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{
					Failure: &failurepb.Failure{Message: "oops"},
				},
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, op.GetStatus())
		})

		t.Run("CompletionBeforeStart", func(t *testing.T) {
			ctx := newCtx()
			op := newScheduledTestOperation(t, ctx)
			startTime := defaultTime.Add(-time.Second)
			err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				StartTime:      timestamppb.New(startTime),
				RequestId:      op.GetRequestId(),
				OperationToken: "tok",
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{
					Failure: &failurepb.Failure{Message: "oops"},
				},
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, op.GetStatus())
			require.Equal(t, "tok", op.GetOperationToken())
			require.Equal(t, startTime, op.GetStartedTime().AsTime())
		})
	})

	t.Run("Canceled", func(t *testing.T) {
		t.Run("AfterStarted", func(t *testing.T) {
			ctx := newCtx()
			op := newStartedOp(t, ctx)
			err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: op.GetRequestId(),
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{
					Failure: &failurepb.Failure{
						Message: "canceled",
						FailureInfo: &failurepb.Failure_CanceledFailureInfo{
							CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
						},
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_CANCELED, op.GetStatus())
		})

		t.Run("CompletionBeforeStart", func(t *testing.T) {
			ctx := newCtx()
			op := newScheduledTestOperation(t, ctx)
			startTime := defaultTime.Add(-time.Second)
			err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				StartTime:      timestamppb.New(startTime),
				RequestId:      op.GetRequestId(),
				OperationToken: "tok",
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{
					Failure: &failurepb.Failure{
						Message: "canceled",
						FailureInfo: &failurepb.Failure_CanceledFailureInfo{
							CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
						},
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_CANCELED, op.GetStatus())
			require.Equal(t, "tok", op.GetOperationToken())
			require.Equal(t, startTime, op.GetStartedTime().AsTime())
		})
	})
}

func TestDescribeOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		status          nexusoperationpb.OperationStatus
		outcome         *nexusoperationpb.OperationOutcome // nil means no outcome set
		lastAttemptFail *failurepb.Failure
		expectedResult  *commonpb.Payload
		expectedFailure *failurepb.Failure
	}{
		{
			name:   "Successful",
			status: nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
			outcome: &nexusoperationpb.OperationOutcome{
				Variant: &nexusoperationpb.OperationOutcome_Successful_{
					Successful: &nexusoperationpb.OperationOutcome_Successful{Result: payload.EncodeString("result")},
				},
			},
			expectedResult: payload.EncodeString("result"),
		},
		{
			name:   "Failed",
			status: nexusoperationpb.OPERATION_STATUS_FAILED,
			outcome: &nexusoperationpb.OperationOutcome{
				Variant: &nexusoperationpb.OperationOutcome_Failed_{
					Failed: &nexusoperationpb.OperationOutcome_Failed{
						Failure: &failurepb.Failure{Message: "outcome failure"},
					},
				},
			},
			expectedFailure: &failurepb.Failure{Message: "outcome failure"},
		},
		{
			name:            "NoOutcome_FallsBackToLastAttemptFailure",
			status:          nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			lastAttemptFail: &failurepb.Failure{Message: "last attempt failure"},
			expectedFailure: &failurepb.Failure{Message: "last attempt failure"},
		},
		{
			name:   "Outcome_PreferredOverLastAttemptFailure",
			status: nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			outcome: &nexusoperationpb.OperationOutcome{
				Variant: &nexusoperationpb.OperationOutcome_Failed_{
					Failed: &nexusoperationpb.OperationOutcome_Failed{
						Failure: &failurepb.Failure{Message: "operation timed out"},
					},
				},
			},
			lastAttemptFail: &failurepb.Failure{Message: "last attempt failure"},
			expectedFailure: &failurepb.Failure{Message: "operation timed out"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			op := NewOperation(&nexusoperationpb.OperationState{
				Status:             tc.status,
				LastAttemptFailure: tc.lastAttemptFail,
			})
			if tc.outcome != nil {
				op.Outcome = chasm.NewDataField(ctx, tc.outcome)
			}

			result, failure := op.outcome(ctx)
			protorequire.ProtoEqual(t, tc.expectedResult, result)
			protorequire.ProtoEqual(t, tc.expectedFailure, failure)
		})
	}
}
