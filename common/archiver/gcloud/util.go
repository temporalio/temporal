// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package gcloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dgryski/go-farm"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/gcloud/connector"
)

func encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func decodeHistoryBatches(data []byte) ([]*shared.History, error) {
	historyBatches := []*shared.History{}
	err := json.Unmarshal(data, &historyBatches)
	if err != nil {
		return nil, err
	}
	return historyBatches, nil
}

func constructHistoryFilename(domainID, workflowID, runID string, version int64) string {
	combinedHash := constructHistoryFilenamePrefix(domainID, workflowID, runID)
	return fmt.Sprintf("%s_%v.history", combinedHash, version)
}

func constructHistoryFilenameMultipart(domainID, workflowID, runID string, version int64, partNumber int) string {
	combinedHash := constructHistoryFilenamePrefix(domainID, workflowID, runID)
	return fmt.Sprintf("%s_%v_%v.history", combinedHash, version, partNumber)
}

func constructHistoryFilenamePrefix(domainID, workflowID, runID string) string {
	return strings.Join([]string{hash(domainID), hash(workflowID), hash(runID)}, "")
}

func constructVisibilityFilenamePrefix(domainID, tag string) string {
	return fmt.Sprintf("%s/%s", domainID, tag)
}

func constructTimeBasedSearchKey(domainID, tag string, timestamp int64, precision string) string {
	t := time.Unix(0, timestamp).In(time.UTC)
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

	return fmt.Sprintf("%s_%s", constructVisibilityFilenamePrefix(domainID, tag), t.Format(timeFormat))
}

func hash(s string) (result string) {
	if s != "" {
		return fmt.Sprintf("%v", farm.Fingerprint64([]byte(s)))
	}
	return
}

func contextExpired(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func deserializeGetHistoryToken(bytes []byte) (*getHistoryToken, error) {
	token := &getHistoryToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func extractCloseFailoverVersion(filename string) (int64, int, error) {
	filenameParts := strings.FieldsFunc(filename, func(r rune) bool {
		return r == '_' || r == '.'
	})
	if len(filenameParts) != 4 {
		return -1, 0, errors.New("unknown filename structure")
	}

	failoverVersion, err := strconv.ParseInt(filenameParts[1], 10, 64)
	if err != nil {
		return -1, 0, err
	}

	highestPart, err := strconv.Atoi(filenameParts[2])
	return failoverVersion, highestPart, err
}

func serializeToken(token interface{}) ([]byte, error) {
	if token == nil {
		return nil, nil
	}
	return json.Marshal(token)
}

func decodeVisibilityRecord(data []byte) (*visibilityRecord, error) {
	record := &visibilityRecord{}
	err := json.Unmarshal(data, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func constructVisibilityFilename(domain, workflowTypeName, workflowID, runID, tag string, timestamp int64) string {
	t := time.Unix(0, timestamp).In(time.UTC)
	prefix := constructVisibilityFilenamePrefix(domain, tag)
	return fmt.Sprintf("%s_%s_%s_%s_%s.visibility", prefix, t.Format(time.RFC3339), hash(workflowTypeName), hash(workflowID), hash(runID))
}

func deserializeQueryVisibilityToken(bytes []byte) (*queryVisibilityToken, error) {
	token := &queryVisibilityToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func convertToExecutionInfo(record *visibilityRecord) *shared.WorkflowExecutionInfo {
	return &shared.WorkflowExecutionInfo{
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(record.WorkflowID),
			RunId:      common.StringPtr(record.RunID),
		},
		Type: &shared.WorkflowType{
			Name: common.StringPtr(record.WorkflowTypeName),
		},
		StartTime:     common.Int64Ptr(record.StartTimestamp),
		ExecutionTime: common.Int64Ptr(record.ExecutionTimestamp),
		CloseTime:     common.Int64Ptr(record.CloseTimestamp),
		CloseStatus:   record.CloseStatus.Ptr(),
		HistoryLength: common.Int64Ptr(record.HistoryLength),
		Memo:          record.Memo,
		SearchAttributes: &shared.SearchAttributes{
			IndexedFields: archiver.ConvertSearchAttrToBytes(record.SearchAttributes),
		},
	}
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
			fileNameParts := strings.Split(fileName, "_")
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
			fileNameParts := strings.Split(fileName, "_")
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
			fileNameParts := strings.Split(fileName, "_")
			if len(fileNameParts) != 5 {
				return true
			}
			return strings.Contains(fileName, fileNameParts[2])
		}

		return false
	}
}

func isRetryableError(err error) (retryable bool) {
	switch err.Error() {
	case connector.ErrBucketNotFound.Error(),
		archiver.ErrURISchemeMismatch.Error(),
		archiver.ErrInvalidURI.Error():
		retryable = false
	default:
		retryable = true
	}

	return
}
