package migration

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
)

func TestExecutionInfo_Marshal_OSS(t *testing.T) {
	t.Parallel()

	// This test validates the marshaling logic is compatible with the
	// unmarshaling logic in OSS v1.30, i.e. we can downgrade to OSS v1.30.
	//
	// Since the unmarshaling logic is the same as the one in OSS v1.30.
	// Just do a basic marshal/unmarshal test here.
	executionInfo := &ExecutionInfo{
		BusinessID:  "business-id-1",
		RunID:       "run-id-1",
		ArchetypeID: chasm.UnspecifiedArchetypeID,
	}

	encoded, err := json.Marshal(executionInfo)
	require.NoError(t, err)

	var executionInfoUnmarshaled *ExecutionInfo
	err = json.Unmarshal(encoded, &executionInfoUnmarshaled)
	require.NoError(t, err)
	require.Equal(t, executionInfo.BusinessID, executionInfoUnmarshaled.BusinessID)
	require.Equal(t, executionInfo.RunID, executionInfoUnmarshaled.RunID)

	// ExecutionInfo is not directly used as activity input/output,
	// instead, it's used as a field in another struct.
	// Test that case and do encoding/decoding with an actual SDK data coverter.
	listResponse := listWorkflowsResponse{
		Executions: []*ExecutionInfo{
			executionInfo,
			{
				BusinessID:  "business-id-2",
				RunID:       "run-id-2",
				ArchetypeID: chasm.UnspecifiedArchetypeID,
			},
		},
		NextPageToken: []byte("next-page-token"),
	}
	dataConverter := converter.GetDefaultDataConverter()
	payload, err := dataConverter.ToPayload(listResponse)
	require.NoError(t, err)

	listResponseUnmarshaled := struct {
		Executions    []*ExecutionInfo
		NextPageToken []byte
	}{}
	err = dataConverter.FromPayload(payload, &listResponseUnmarshaled)
	require.NoError(t, err)
	require.Equal(t, listResponse.NextPageToken, listResponseUnmarshaled.NextPageToken)
	for idx := 0; idx != len(listResponse.Executions); idx++ {
		require.Equal(t, listResponse.Executions[idx].BusinessID, listResponseUnmarshaled.Executions[idx].BusinessID)
		require.Equal(t, listResponse.Executions[idx].RunID, listResponseUnmarshaled.Executions[idx].RunID)
	}
}

func TestExecutionInfo_Marshal_Cloud(t *testing.T) {
	t.Parallel()

	// This test validates the marshaling logic is compatible with the
	// unmarshaling logic in cloud/v1.30.0-149, i.e. we can downgrade to cloud/v1.30.0-149.

	executionInfo := &ExecutionInfo{
		BusinessID:  "business-id-1",
		RunID:       "run-id-1",
		ArchetypeID: chasm.WorkflowArchetypeID,
	}

	encoded, err := json.Marshal(executionInfo)
	require.NoError(t, err)

	// cloud/v1.30.0-149 uses *replicationspb.MigrationExecutionInfo for unmarshaling.
	var migrationExecutionInfo *replicationspb.MigrationExecutionInfo
	err = json.Unmarshal(encoded, &migrationExecutionInfo)
	require.NoError(t, err)
	require.Equal(t, executionInfo.BusinessID, migrationExecutionInfo.BusinessId)
	require.Equal(t, executionInfo.RunID, migrationExecutionInfo.RunId)

	// ExecutionInfo is not directly used as activity input/output,
	// instead, it's used as a field in another struct.
	// Test that case and do encoding/decoding with an actual SDK data coverter.
	listResponse := &listWorkflowsResponse{
		Executions: []*ExecutionInfo{
			executionInfo,
			{
				BusinessID:  "business-id-2",
				RunID:       "run-id-2",
				ArchetypeID: chasm.WorkflowArchetypeID,
			},
		},
		NextPageToken: []byte("next-page-token"),
	}
	dataConverter := converter.GetDefaultDataConverter()
	payload, err := dataConverter.ToPayload(listResponse)
	require.NoError(t, err)

	listResponseLegacy := struct {
		Executions    []*replicationspb.MigrationExecutionInfo
		NextPageToken []byte
	}{}
	err = dataConverter.FromPayload(payload, &listResponseLegacy)
	require.NoError(t, err)
	require.Equal(t, listResponse.NextPageToken, listResponseLegacy.NextPageToken)
	for idx := 0; idx != len(listResponse.Executions); idx++ {
		require.Equal(t, listResponse.Executions[idx].BusinessID, listResponseLegacy.Executions[idx].BusinessId)
		require.Equal(t, listResponse.Executions[idx].RunID, listResponseLegacy.Executions[idx].RunId)
	}
}

func TestExecutionInfo_Unmarshal_OSS(t *testing.T) {
	t.Parallel()

	// This test validates the unmarshaling logic is compatible with the
	// marshaling logic in OSS v1.30, i.e. we can upgrade from OSS v1.30.

	// OSS v1.30 uses executionInfoLegacyJSON for marshaling.
	executionInfoLegacy := &executionInfoLegacyJSON{
		WorkflowID: "business-id-1",
		RunID:      "run-id-1",
	}

	encoded, err := json.Marshal(executionInfoLegacy)
	require.NoError(t, err)

	var executionInfo *ExecutionInfo
	err = json.Unmarshal(encoded, &executionInfo)
	require.NoError(t, err)
	require.Equal(t, executionInfoLegacy.WorkflowID, executionInfo.BusinessID)
	require.Equal(t, executionInfoLegacy.RunID, executionInfo.RunID)

	// ExecutionInfo is not directly used as activity input/output,
	// instead, it's used as a field in another struct.
	// Test that case and do encoding/decoding with an actual SDK data coverter.
	listResponseLegacy := struct {
		Executions    []*executionInfoLegacyJSON
		NextPageToken []byte
	}{
		Executions: []*executionInfoLegacyJSON{
			executionInfoLegacy,
			{
				WorkflowID: "business-id-2",
				RunID:      "run-id-2",
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
		require.Equal(t, listResponseLegacy.Executions[idx].WorkflowID, listResponse.Executions[idx].BusinessID)
		require.Equal(t, listResponseLegacy.Executions[idx].RunID, listResponse.Executions[idx].RunID)
	}
}

func TestExecutionInfo_Unmarshal_Cloud(t *testing.T) {
	t.Parallel()

	// This test validates the unmarshaling logic is compatible with the
	// marshaling logic in cloud/v1.30.0-149, i.e. we can upgrade from cloud/v1.30.0-149.

	// cloud/v1.30.0-149 uses *replicationspb.MigrationExecutionInfo for marshaling.
	migrationExecutionInfo := &replicationspb.MigrationExecutionInfo{
		BusinessId:  "business-id-1",
		RunId:       "run-id-1",
		ArchetypeId: chasm.WorkflowArchetypeID,
	}
	encoded, err := json.Marshal(migrationExecutionInfo)
	require.NoError(t, err)

	var executionInfo *ExecutionInfo
	err = json.Unmarshal(encoded, &executionInfo)
	require.NoError(t, err)
	require.Equal(t, migrationExecutionInfo.BusinessId, executionInfo.BusinessID)
	require.Equal(t, migrationExecutionInfo.RunId, executionInfo.RunID)

	// ExecutionInfo is not directly used as activity input/output,
	// instead, it's used as a field in another struct.
	// Test that case and do encoding/decoding with an actual SDK data coverter.
	listResponseLegacy := struct {
		Executions    []*replicationspb.MigrationExecutionInfo
		NextPageToken []byte
	}{
		Executions: []*replicationspb.MigrationExecutionInfo{
			migrationExecutionInfo,
			{
				BusinessId:  "business-id-2",
				RunId:       "run-id-2",
				ArchetypeId: chasm.WorkflowArchetypeID,
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
		require.Equal(t, listResponseLegacy.Executions[idx].BusinessId, listResponse.Executions[idx].BusinessID)
		require.Equal(t, listResponseLegacy.Executions[idx].RunId, listResponse.Executions[idx].RunID)
	}
}
