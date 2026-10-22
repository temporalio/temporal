package describeworkflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common"
)

func TestAddCallbackAttachmentEventReferences(t *testing.T) {
	t.Parallel()

	newWorkflow := func(
		t *testing.T,
		references map[string]*chasmworkflowpb.CallbackAttachmentEventReference,
	) (*chasmworkflow.Workflow, *chasm.MockMutableContext) {
		t.Helper()
		ctx := &chasm.MockMutableContext{}
		wf := &chasmworkflow.Workflow{
			Updates: chasm.Map[string, *chasmworkflow.WorkflowUpdate]{
				"update-id": chasm.NewComponentField(ctx, &chasmworkflow.WorkflowUpdate{
					UpdateState: &chasmworkflowpb.UpdateState{
						UpdateId:                          "update-id",
						CallbackAttachmentEventReferences: references,
					},
				}),
			},
		}
		return wf, ctx
	}

	t.Run("AddsCHASMOnlyReferences", func(t *testing.T) {
		wf, ctx := newWorkflow(t, map[string]*chasmworkflowpb.CallbackAttachmentEventReference{
			"durable-request-id": {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
				EventId:   42,
			},
			"buffered-request-id": {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				EventId:   common.BufferedEventID,
			},
		})
		requestIDInfos := make(map[string]*workflowpb.RequestIdInfo)

		addCallbackAttachmentEventReferences(requestIDInfos, wf, ctx)

		require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED, requestIDInfos["durable-request-id"].GetEventType())
		require.Equal(t, int64(42), requestIDInfos["durable-request-id"].GetEventId())
		require.False(t, requestIDInfos["durable-request-id"].GetBuffered())
		require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, requestIDInfos["buffered-request-id"].GetEventType())
		require.Zero(t, requestIDInfos["buffered-request-id"].GetEventId())
		require.True(t, requestIDInfos["buffered-request-id"].GetBuffered())
	})

	t.Run("PreservesLegacyReference", func(t *testing.T) {
		wf, ctx := newWorkflow(t, map[string]*chasmworkflowpb.CallbackAttachmentEventReference{
			"request-id": {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
				EventId:   42,
			},
		})
		legacyInfo := &workflowpb.RequestIdInfo{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			EventId:   11,
		}
		requestIDInfos := map[string]*workflowpb.RequestIdInfo{"request-id": legacyInfo}

		addCallbackAttachmentEventReferences(requestIDInfos, wf, ctx)

		require.Same(t, legacyInfo, requestIDInfos["request-id"])
	})
}
