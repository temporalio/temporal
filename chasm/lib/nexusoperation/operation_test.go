package nexusoperation

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/testing/protoutils"
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
