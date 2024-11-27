// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package tests

import (
	"context"
	"errors"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/tests/testcore"
)

type NexusTestBaseSuite struct {
	testcore.ClientFunctionalSuite
}

func (s *NexusTestBaseSuite) mustToPayload(v any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(v)
	s.NoError(err)
	return payload
}

func (s *NexusTestBaseSuite) nexusTaskPoller(ctx context.Context, taskQueue string, handler func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)) {
	s.versionedNexusTaskPoller(ctx, taskQueue, "", handler)
}

func (s *NexusTestBaseSuite) versionedNexusTaskPoller(ctx context.Context, taskQueue, buildID string, handler func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)) {
	var vc *commonpb.WorkerVersionCapabilities

	if buildID != "" {
		vc = &commonpb.WorkerVersionCapabilities{
			BuildId:       buildID,
			UseVersioning: true,
		}
	}
	res, err := s.GetTestCluster().FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: s.Namespace(),
		Identity:  uuid.NewString(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		WorkerVersionCapabilities: vc,
	})
	// The test is written in a way that it doesn't expect the poll to be unblocked and it may cancel this context when it completes.
	if ctx.Err() != nil {
		return
	}
	// There's no clean way to propagate this error back to the test that's worthwhile. Panic is good enough.
	if err != nil {
		panic(err)
	}
	if res.Request.GetStartOperation().GetService() != "test-service" && res.Request.GetCancelOperation().GetService() != "test-service" {
		panic("expected service to be test-service")
	}
	response, handlerError := handler(res)
	if handlerError != nil {
		_, err = s.GetTestCluster().FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
			Namespace: s.Namespace(),
			Identity:  uuid.NewString(),
			TaskToken: res.TaskToken,
			Error:     handlerError,
		})
		// Ignore if context is already cancelled or if the task is not found.
		if err != nil && ctx.Err() == nil && !errors.As(err, new(*serviceerror.NotFound)) {
			// There's no clean way to propagate this error back to the test that's worthwhile. Panic is good enough.
			panic(err)
		}
	} else if response != nil {
		_, err = s.GetTestCluster().FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: s.Namespace(),
			Identity:  uuid.NewString(),
			TaskToken: res.TaskToken,
			Response:  response,
		})
		// Ignore if context is already cancelled or if the task is not found.
		if err != nil && ctx.Err() == nil && !errors.As(err, new(*serviceerror.NotFound)) {
			// There's no clean way to propagate this error back to the test that's worthwhile. Panic is good enough.
			panic(err)
		}
	}
}
