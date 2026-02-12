package azure_store

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dgryski/go-farm"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver/azure_store/connector"
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

func constructVisibilityFilename(namespace, workflowTypeName, workflowID, runID, tag string, t time.Time) string {
	prefix := constructVisibilityFilenamePrefix(namespace, tag)
	return fmt.Sprintf("%s_%s_%s_%s_%s.visibility", prefix, t.Format(time.RFC3339), hash(workflowTypeName), hash(workflowID), hash(runID))
}

func constructVisibilityFilenamePrefix(namespaceID, tag string) string {
	return fmt.Sprintf("%s/%s", namespaceID, tag)
}

func constructTimeBasedSearchKey(namespaceID, tag string, t time.Time, precision string) string {
	var timeFormat = ""
	switch precision {
	case PrecisionSecond:
		timeFormat = ":05"
		fallthrough
	case PrecisionMinute:
		timeFormat = ":04" + timeFormat
		fallthrough
	case PrecisionHour:
		timeFormat = "15" + timeFormat
		fallthrough
	case PrecisionDay:
		timeFormat = "2006-01-02T" + timeFormat
	}

	return fmt.Sprintf("%s_%s", constructVisibilityFilenamePrefix(namespaceID, tag), t.Format(timeFormat))
}

func hash(s string) (result string) {
	if s != "" {
		return fmt.Sprintf("%v", farm.Fingerprint64([]byte(s)))
	}
	return
}

func newRunIDPrecondition(runID string) connector.Precondition {
	return func(subject interface{}) bool {
		if runID == "" {
			return true
		}

		fileName, ok := subject.(string)
		if !ok {
			return false
		}

		if strings.Contains(fileName, runID) {
			fileNameParts := strings.SplitN(fileName, "_", 5)
			if len(fileNameParts) != 5 {
				return true
			}
			return strings.Contains(fileName, fileNameParts[4])
		}

		return false
	}
}

func newWorkflowIDPrecondition(workflowID string) connector.Precondition {
	return func(subject interface{}) bool {
		if workflowID == "" {
			return true
		}

		fileName, ok := subject.(string)
		if !ok {
			return false
		}

		if strings.Contains(fileName, workflowID) {
			fileNameParts := strings.SplitN(fileName, "_", 5)
			if len(fileNameParts) != 5 {
				return true
			}
			return strings.Contains(fileName, fileNameParts[3])
		}

		return false
	}
}

func newWorkflowTypeNamePrecondition(workflowTypeName string) connector.Precondition {
	return func(subject interface{}) bool {
		if workflowTypeName == "" {
			return true
		}

		fileName, ok := subject.(string)
		if !ok {
			return false
		}

		if strings.Contains(fileName, workflowTypeName) {
			fileNameParts := strings.SplitN(fileName, "_", 5)
			if len(fileNameParts) != 5 {
				return true
			}
			return strings.Contains(fileName, fileNameParts[2])
		}

		return false
	}
}

func isRetryableError(err error) bool {
	return err == errRetryable
}
