// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
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

package archiver

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/dgryski/go-farm"
	"go.temporal.io/sdk/activity"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// MaxArchivalIterationTimeout returns the max allowed timeout for a single iteration of archival workflow
func MaxArchivalIterationTimeout() time.Duration {
	return workflowRunTimeout / 2
}

func hash(i interface{}) uint64 {
	var b bytes.Buffer
	// please make sure encoder is deterministic (especially when encoding map objects)
	// use json not gob here as json will sort map keys, while gob is non-deterministic
	_ = json.NewEncoder(&b).Encode(i)
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
	return log.With(
		logger,
		tag.ShardID(request.ShardID),
		tag.ArchivalRequestNamespaceID(request.NamespaceID),
		tag.ArchivalRequestNamespace(request.Namespace),
		tag.ArchivalRequestWorkflowID(request.WorkflowID),
		tag.ArchivalRequestRunID(request.RunID),
		tag.ArchivalRequestBranchToken(request.BranchToken),
		tag.ArchivalRequestNextEventID(request.NextEventID),
		tag.ArchivalRequestCloseFailoverVersion(request.CloseFailoverVersion),
		tag.ArchivalURI(request.HistoryURI),
	)
}

func tagLoggerWithVisibilityRequest(logger log.Logger, request *ArchiveRequest) log.Logger {
	return log.With(
		logger,
		tag.ArchivalRequestNamespaceID(request.NamespaceID),
		tag.ArchivalRequestNamespace(request.Namespace),
		tag.ArchivalRequestWorkflowID(request.WorkflowID),
		tag.ArchivalRequestRunID(request.RunID),
		tag.ArchivalURI(request.HistoryURI),
	)
}

func tagLoggerWithActivityInfo(logger log.Logger, activityInfo activity.Info) log.Logger {
	return log.With(
		logger,
		tag.WorkflowID(activityInfo.WorkflowExecution.ID),
		tag.WorkflowRunID(activityInfo.WorkflowExecution.RunID),
		tag.Attempt(activityInfo.Attempt))
}
