// Copyright (c) 2017 Uber Technologies, Inc.
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

package archiver

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dgryski/go-farm"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/blobstore/blob"
	"github.com/uber/cadence/common/logging"
)

type (
	// HistoryBlobHeader is the header attached to all history blobs
	HistoryBlobHeader struct {
		DomainName           *string `json:"domain_name,omitempty"`
		DomainID             *string `json:"domain_id,omitempty"`
		WorkflowID           *string `json:"workflow_id,omitempty"`
		RunID                *string `json:"run_id,omitempty"`
		CurrentPageToken     *int    `json:"current_page_token,omitempty"`
		NextPageToken        *int    `json:"next_page_token,omitempty"`
		FirstFailoverVersion *int64  `json:"first_failover_version,omitempty"`
		LastFailoverVersion  *int64  `json:"last_failover_version,omitempty"`
		FirstEventID         *int64  `json:"first_event_id,omitempty"`
		LastEventID          *int64  `json:"last_event_id,omitempty"`
		UploadDateTime       *string `json:"upload_date_time,omitempty"`
		UploadCluster        *string `json:"upload_cluster,omitempty"`
		EventCount           *int64  `json:"event_count,omitempty"`
		CloseFailoverVersion *int64  `json:"close_failover_version,omitempty"`
	}

	// HistoryBlob is the serializable data that forms the body of a blob
	HistoryBlob struct {
		Header *HistoryBlobHeader `json:"header"`
		Body   *shared.History    `json:"body"`
	}
)

var (
	errInvalidKeyInput = errors.New("invalid input to construct history blob key")
)

// NewHistoryBlobKey returns a key for history blob
func NewHistoryBlobKey(domainID, workflowID, runID string, pageToken int) (blob.Key, error) {
	if len(domainID) == 0 || len(workflowID) == 0 || len(runID) == 0 {
		return nil, errInvalidKeyInput
	}
	if pageToken < common.FirstBlobPageToken {
		return nil, errInvalidKeyInput
	}
	domainIDHash := fmt.Sprintf("%v", farm.Fingerprint64([]byte(domainID)))
	workflowIDHash := fmt.Sprintf("%v", farm.Fingerprint64([]byte(workflowID)))
	runIDHash := fmt.Sprintf("%v", farm.Fingerprint64([]byte(runID)))
	combinedHash := strings.Join([]string{domainIDHash, workflowIDHash, runIDHash}, "")
	return blob.NewKey("history", combinedHash, StringPageToken(pageToken))
}

// StringPageToken converts input blob page token to string form
func StringPageToken(pageToken int) string {
	return strconv.Itoa(pageToken)
}

// ConvertHeaderToTags converts header into metadata tags for blob
func ConvertHeaderToTags(header *HistoryBlobHeader) (map[string]string, error) {
	var tempMap map[string]interface{}
	bytes, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(bytes, &tempMap); err != nil {
		return nil, err
	}
	result := make(map[string]string, len(tempMap))
	for k, v := range tempMap {
		result[k] = fmt.Sprintf("%v", v)
	}
	return result, nil
}

func hashArchiveRequest(archiveRequest ArchiveRequest) uint64 {
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(archiveRequest)
	return farm.Fingerprint64(b.Bytes())
}

func hashesEqual(a []uint64, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	aMap := make(map[uint64]int)
	for _, elem := range a {
		aMap[elem] = aMap[elem] + 1
	}
	for _, elem := range b {
		count := aMap[elem]
		if count == 0 {
			return false
		}
		aMap[elem] = aMap[elem] - 1
	}
	return true
}

func tagLoggerWithRequest(logger bark.Logger, request ArchiveRequest) bark.Logger {
	return logger.WithFields(bark.Fields{
		logging.TagArchiveRequestDomainID:             request.DomainID,
		logging.TagArchiveRequestWorkflowID:           request.WorkflowID,
		logging.TagArchiveRequestRunID:                request.RunID,
		logging.TagArchiveRequestEventStoreVersion:    request.EventStoreVersion,
		logging.TagArchiveRequestBranchToken:          string(request.BranchToken),
		logging.TagArchiveRequestNextEventID:          request.NextEventID,
		logging.TagArchiveRequestCloseFailoverVersion: request.CloseFailoverVersion,
	})
}
