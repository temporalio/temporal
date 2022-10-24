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

package api

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
)

func TestValidateStartWorkflowExecutionRequest(t *testing.T) {
	controller := gomock.NewController(t)
	mockShard := shard.NewTestContext(
		controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 1,
				RangeId: 1,
			}},
		tests.NewDynamicConfig(),
	)
	defer controller.Finish()

	testCases := []struct {
		setupRequestFn func(*historyservice.StartWorkflowExecutionRequest)
		validationFn   func(error)
	}{
		{
			setupRequestFn: func(request *historyservice.StartWorkflowExecutionRequest) {},
			validationFn: func(validationErr error) {
				assert.Error(t, validationErr, "startRequest doesn't have request id and runID, it should error out")
			},
		},
		{
			setupRequestFn: func(request *historyservice.StartWorkflowExecutionRequest) {
				request.StartRequest.RequestId = "request-id"
			},
			validationFn: func(validationErr error) {
				assert.NoError(t, validationErr)
			},
		},
		{
			setupRequestFn: func(request *historyservice.StartWorkflowExecutionRequest) {
				request.RunId = uuid.New()
			},
			validationFn: func(validationErr error) {
				assert.NoError(t, validationErr)
			},
		},
		{
			setupRequestFn: func(request *historyservice.StartWorkflowExecutionRequest) {
				request.StartRequest.RequestId = "request-id"
				request.StartRequest.Memo = &commonpb.Memo{Fields: map[string]*commonpb.Payload{
					"data": payload.EncodeBytes(make([]byte, 4*1024*1024)),
				}}
			},
			validationFn: func(validationErr error) {
				assert.Error(t, validationErr, "memo should be too big")
			},
		},
	}

	for _, tc := range testCases {
		request := &historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId:               "ID",
				WorkflowType:             &commonpb.WorkflowType{Name: "testType"},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: "taskptr"},
				Input:                    payloads.EncodeString("input"),
				WorkflowExecutionTimeout: timestamp.DurationPtr(20 * time.Second),
				WorkflowRunTimeout:       timestamp.DurationPtr(10 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(10 * time.Second),
				Identity:                 "identity",
			},
		}
		tc.setupRequestFn(request)

		tc.validationFn(
			ValidateStartWorkflowExecutionRequest(
				context.Background(),
				request,
				mockShard,
				tests.LocalNamespaceEntry,
				"StartWorkflowExecution",
			),
		)
	}
}
