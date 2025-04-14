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

// Generates all generated files in this package:
//go:generate go run ../../../../cmd/tools/genrpcserverinterceptors -copyright_file ../../../../LICENSE

package logtags

import (
	"strings"

	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/tasktoken"
)

type (
	WorkflowTags struct {
		serializer *tasktoken.Serializer
		logger     log.Logger
	}
)

func NewWorkflowTags(
	serializer *tasktoken.Serializer,
	logger log.Logger,
) *WorkflowTags {
	return &WorkflowTags{
		serializer: serializer,
		logger:     logger,
	}
}

func (wt *WorkflowTags) Extract(req any, fullMethod string) []tag.Tag {
	if req == nil {
		return nil
	}
	switch {
	case strings.HasPrefix(fullMethod, api.WorkflowServicePrefix):
		return wt.extractFromWorkflowServiceServerMessage(req)
	case strings.HasPrefix(fullMethod, api.OperatorServicePrefix):
		// OperatorService doesn't have a single API with workflow tags.
		return nil
	case strings.HasPrefix(fullMethod, api.AdminServicePrefix):
		return wt.extractFromAdminServiceServerMessage(req)
	case strings.HasPrefix(fullMethod, api.HistoryServicePrefix):
		return wt.extractFromHistoryServiceServerMessage(req)
	case strings.HasPrefix(fullMethod, api.MatchingServicePrefix):
		return wt.extractFromMatchingServiceServerMessage(req)
	default:
		return nil
	}
}

func (wt *WorkflowTags) fromTaskToken(taskTokenBytes []byte) []tag.Tag {
	if len(taskTokenBytes) == 0 {
		return nil
	}
	taskToken, err := wt.serializer.Deserialize(taskTokenBytes)
	if err != nil {
		wt.logger.Warn("unable to deserialize task token while getting workflow tags", tag.Error(err))
		return nil
	}
	return []tag.Tag{tag.WorkflowID(taskToken.WorkflowId), tag.WorkflowRunID(taskToken.RunId)}
}
