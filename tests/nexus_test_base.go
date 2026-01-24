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
	testcore.FunctionalTestBase
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
		vc = commonpb.WorkerVersionCapabilities_builder{
			BuildId:       buildID,
			UseVersioning: true,
		}.Build()
	}
	res, err := s.GetTestCluster().FrontendClient().PollNexusTaskQueue(ctx, workflowservice.PollNexusTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		Identity:  uuid.NewString(),
		TaskQueue: taskqueuepb.TaskQueue_builder{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}.Build(),
		WorkerVersionCapabilities: vc,
	}.Build())
	// The test is written in a way that it doesn't expect the poll to be unblocked and it may cancel this context when it completes.
	if ctx.Err() != nil {
		return
	}
	// There's no clean way to propagate this error back to the test that's worthwhile. Panic is good enough.
	if err != nil {
		panic(err)
	}
	if res.GetRequest().GetStartOperation().GetService() != "test-service" && res.GetRequest().GetCancelOperation().GetService() != "test-service" {
		panic("expected service to be test-service")
	}
	response, handlerError := handler(res)
	if handlerError != nil {
		_, err = s.GetTestCluster().FrontendClient().RespondNexusTaskFailed(ctx, workflowservice.RespondNexusTaskFailedRequest_builder{
			Namespace: s.Namespace().String(),
			Identity:  uuid.NewString(),
			TaskToken: res.GetTaskToken(),
			Error:     handlerError,
		}.Build())
		// Ignore if context is already cancelled or if the task is not found.
		if err != nil && ctx.Err() == nil && !errors.As(err, new(*serviceerror.NotFound)) {
			// There's no clean way to propagate this error back to the test that's worthwhile. Panic is good enough.
			panic(err)
		}
	} else if response != nil {
		_, err = s.GetTestCluster().FrontendClient().RespondNexusTaskCompleted(ctx, workflowservice.RespondNexusTaskCompletedRequest_builder{
			Namespace: s.Namespace().String(),
			Identity:  uuid.NewString(),
			TaskToken: res.GetTaskToken(),
			Response:  response,
		}.Build())
		// Ignore if context is already cancelled or if the task is not found.
		if err != nil && ctx.Err() == nil && !errors.As(err, new(*serviceerror.NotFound)) {
			// There's no clean way to propagate this error back to the test that's worthwhile. Panic is good enough.
			panic(err)
		}
	}
}
