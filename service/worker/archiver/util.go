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
	"time"

	"github.com/dgryski/go-farm"
	"go.uber.org/cadence/activity"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
)

// MaxArchivalIterationTimeout returns the max allowed timeout for a single iteration of archival workflow
func MaxArchivalIterationTimeout() time.Duration {
	return workflowStartToCloseTimeout / 2
}

func hash(i interface{}) uint64 {
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(i) //nolint:errcheck
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

func tagLoggerWithHistoryRequest(logger log.Logger, request *ArchiveRequest) log.Logger {
	return logger.WithTags(
		tag.ShardID(request.ShardID),
		tag.ArchivalRequestDomainID(request.DomainID),
		tag.ArchivalRequestDomainName(request.DomainName),
		tag.ArchivalRequestWorkflowID(request.WorkflowID),
		tag.ArchivalRequestRunID(request.RunID),
		tag.ArchivalRequestBranchToken(request.BranchToken),
		tag.ArchivalRequestNextEventID(request.NextEventID),
		tag.ArchivalRequestCloseFailoverVersion(request.CloseFailoverVersion),
		tag.ArchivalURI(request.URI),
	)
}

func tagLoggerWithVisibilityRequest(logger log.Logger, request *ArchiveRequest) log.Logger {
	return logger.WithTags(
		tag.ArchivalRequestDomainID(request.DomainID),
		tag.ArchivalRequestDomainName(request.DomainName),
		tag.ArchivalRequestWorkflowID(request.WorkflowID),
		tag.ArchivalRequestRunID(request.RunID),
		tag.ArchivalURI(request.URI),
	)
}

func tagLoggerWithActivityInfo(logger log.Logger, activityInfo activity.Info) log.Logger {
	return logger.WithTags(
		tag.WorkflowID(activityInfo.WorkflowExecution.ID),
		tag.WorkflowRunID(activityInfo.WorkflowExecution.RunID),
		tag.Attempt(activityInfo.Attempt))
}

func convertSearchAttributesToString(searchAttr map[string][]byte) map[string]string {
	searchAttrStr := make(map[string]string)
	for k, v := range searchAttr {
		searchAttrStr[k] = string(v)
	}
	return searchAttrStr
}
