package migration

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/chasm"
)

func TestExecutionInfo_Marshal(t *testing.T) {
	t.Parallel()

	// OSS v1.29 uses *commonpb.WorkflowExecution,
	// so validate the encoded data can still be decoded to the old definition.
	// i.e. we can downgrade to OSS v1.29.
	executionInfo := &ExecutionInfo{
		executionInfoNewJSON: executionInfoNewJSON{
			BusinessID:  "business-id-1",
			RunID:       "run-id-1",
			ArchetypeID: chasm.UnspecifiedArchetypeID,
		},
	}

	encoded, err := json.Marshal(executionInfo)
	require.NoError(t, err)

	var workflowExecution *commonpb.WorkflowExecution
	err = json.Unmarshal(encoded, &workflowExecution)
	require.NoError(t, err)
	require.Equal(t, executionInfo.BusinessID, workflowExecution.WorkflowId)
	require.Equal(t, executionInfo.RunID, workflowExecution.RunId)

	// ExecutionInfo is not directly used as activity input/output,
	// instead, it's used as a field in another struct.
	// Test that case and do encoding/decoding with an actual SDK data coverter.
	listResponse := listWorkflowsResponse{
		Executions: []*ExecutionInfo{
			executionInfo,
			{
				executionInfoNewJSON: executionInfoNewJSON{
					BusinessID:  "business-id-2",
					RunID:       "run-id-2",
					ArchetypeID: chasm.UnspecifiedArchetypeID,
				},
			},
		},
		NextPageToken: []byte("next-page-token"),
	}
	dataConverter := converter.GetDefaultDataConverter()
	payload, err := dataConverter.ToPayload(listResponse)
	require.NoError(t, err)

	listResponseLegacy := struct {
		Executions    []commonpb.WorkflowExecution
		NextPageToken []byte
	}{}
	err = dataConverter.FromPayload(payload, &listResponseLegacy)
	require.NoError(t, err)
	require.Equal(t, listResponse.NextPageToken, listResponseLegacy.NextPageToken)
	for idx := 0; idx != len(listResponse.Executions); idx++ {
		require.Equal(t, listResponse.Executions[idx].BusinessID, listResponseLegacy.Executions[idx].WorkflowId)
		require.Equal(t, listResponse.Executions[idx].RunID, listResponseLegacy.Executions[idx].RunId)
	}
}

func TestExecutionInfo_Unmarshal(t *testing.T) {
	t.Parallel()

	// OSS v1.29 uses *commonpb.WorkflowExecution,
	// so validate the encoded data can be decoded to the new definition.
	// i.e. we can upgrade from OSS v1.29.
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "business-id-1",
		RunId:      "run-id-1",
	}

	encoded, err := json.Marshal(workflowExecution)
	require.NoError(t, err)

	var executionInfo *ExecutionInfo
	err = json.Unmarshal(encoded, &executionInfo)
	require.NoError(t, err)
	require.Equal(t, workflowExecution.WorkflowId, executionInfo.BusinessID)
	require.Equal(t, workflowExecution.RunId, executionInfo.RunID)

	// ExecutionInfo is not directly used as activity input/output,
	// instead, it's used as a field in another struct.
	// Test that case and do encoding/decoding with an actual SDK data coverter.
	listResponseLegacy := struct {
		Executions    []*commonpb.WorkflowExecution
		NextPageToken []byte
	}{
		Executions: []*commonpb.WorkflowExecution{
			workflowExecution,
			{
				WorkflowId: "business-id-2",
				RunId:      "run-id-2",
			},
		},
		NextPageToken: []byte("next-page-token"),
	}

	dataConverter := converter.GetDefaultDataConverter()
	payload, err := dataConverter.ToPayload(listResponseLegacy)
	require.NoError(t, err)

	var listResponse listWorkflowsResponse
	err = dataConverter.FromPayload(payload, &listResponse)
	require.NoError(t, err)
	require.Equal(t, listResponseLegacy.NextPageToken, listResponse.NextPageToken)
	for idx := 0; idx != len(listResponse.Executions); idx++ {
		require.Equal(t, listResponseLegacy.Executions[idx].WorkflowId, listResponse.Executions[idx].BusinessID)
		require.Equal(t, listResponseLegacy.Executions[idx].RunId, listResponse.Executions[idx].RunID)
	}
}
