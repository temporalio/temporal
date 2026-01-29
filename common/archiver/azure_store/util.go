package azure_store

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/searchattribute"
)

func constructHistoryFilenamePrefix(namespaceID, workflowID, runID string) string {
	return fmt.Sprintf("%s_%s_%s", namespaceID, workflowID, runID)
}

func constructHistoryFilenameMultipart(namespaceID, workflowID, runID string, closeFailoverVersion int64, part int) string {
	return fmt.Sprintf("%s_%d_%d.history", constructHistoryFilenamePrefix(namespaceID, workflowID, runID), closeFailoverVersion, part)
}

func extractCloseFailoverVersion(filename string) (int64, int, error) {
	// filename format: namespaceID_workflowID_runID_closeFailoverVersion_part.history
	basename := strings.TrimSuffix(filename, ".history")
	parts := strings.Split(basename, "_")
	if len(parts) < 5 {
		return 0, 0, fmt.Errorf("invalid filename")
	}
	version, err := strconv.ParseInt(parts[len(parts)-2], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	part, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return 0, 0, err
	}
	return version, part, nil
}

func encode(record *archiverspb.VisibilityRecord) ([]byte, error) {
	encoder := codec.NewJSONPBEncoder()
	return encoder.Encode(record)
}

func decodeVisibilityRecord(data []byte) (*archiverspb.VisibilityRecord, error) {
	record := &archiverspb.VisibilityRecord{}
	encoder := codec.NewJSONPBEncoder()
	err := encoder.Decode(data, record)
	return record, err
}

func serializeToken(token interface{}) ([]byte, error) {
	if token == nil {
		return nil, nil
	}
	return json.Marshal(token)
}

func deserializeGetHistoryToken(data []byte) (*getHistoryToken, error) {
	var token getHistoryToken
	err := json.Unmarshal(data, &token)
	return &token, err
}

func deserializeQueryVisibilityToken(data []byte) (*queryVisibilityToken, error) {
	var token queryVisibilityToken
	err := json.Unmarshal(data, &token)
	return &token, err
}

func convertToExecutionInfo(record *archiverspb.VisibilityRecord, saTypeMap searchattribute.NameTypeMap) (*workflowpb.WorkflowExecutionInfo, error) {
	searchAttributes, err := searchattribute.Parse(record.SearchAttributes, &saTypeMap)
	if err != nil {
		return nil, err
	}

	return &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: record.GetWorkflowId(),
			RunId:      record.GetRunId(),
		},
		Type: &commonpb.WorkflowType{
			Name: record.WorkflowTypeName,
		},
		StartTime:         record.StartTime,
		ExecutionTime:     record.ExecutionTime,
		CloseTime:         record.CloseTime,
		ExecutionDuration: record.ExecutionDuration,
		Status:            record.Status,
		HistoryLength:     record.HistoryLength,
		Memo:              record.Memo,
		SearchAttributes:  searchAttributes,
	}, nil
}

func constructVisibilityFilename(namespaceID, workflowTypeName, workflowID, runID, indexKey string, timestamp time.Time) string {
	return fmt.Sprintf("%s_%s_%d_%s_%s_%s.visibility", namespaceID, indexKey, timestamp.UnixNano(), workflowTypeName, workflowID, runID)
}

func constructVisibilityFilenamePrefix(namespaceID, indexKey string) string {
	return fmt.Sprintf("%s_%s", namespaceID, indexKey)
}
