package main

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/log"
	workerscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
)

type actionExecution struct {
	WorkflowID string
	RunID      string
}

type actionStartCapture struct {
	interceptor.WorkerInterceptorBase

	nextActivityID int
	starts         map[string]*schedulespb.StartWorkflowRequest
}

func (c *actionStartCapture) InterceptWorkflow(
	_ workflow.Context,
	next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	return &actionStartCaptureInbound{
		WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next},
		capture:                        c,
	}
}

type actionStartCaptureInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	capture *actionStartCapture
}

func (i *actionStartCaptureInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&actionStartCaptureOutbound{
		WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound},
		capture:                         i.capture,
	})
}

type actionStartCaptureOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	capture *actionStartCapture
}

func (o *actionStartCaptureOutbound) ExecuteLocalActivity(
	ctx workflow.Context,
	activityType string,
	args ...interface{},
) workflow.Future {
	o.capture.nextActivityID++
	if activityType == "StartWorkflow" && len(args) == 1 {
		if request, ok := args[0].(*schedulespb.StartWorkflowRequest); ok {
			o.capture.starts[fmt.Sprint(o.capture.nextActivityID)] = proto.CloneOf(request)
		}
	}
	return o.Next.ExecuteLocalActivity(ctx, activityType, args...)
}

func extractActionExecutions(history *historypb.History) ([]actionExecution, error) {
	localStarts, err := captureLocalActionStarts(history)
	if err != nil {
		return nil, err
	}

	startsByScheduledEvent := make(map[int64]*schedulespb.StartWorkflowRequest)
	seen := make(map[actionExecution]struct{})
	var executions []actionExecution
	appendExecution := func(workflowID, runID string) {
		if workflowID == "" || runID == "" {
			return
		}
		execution := actionExecution{WorkflowID: workflowID, RunID: runID}
		if _, ok := seen[execution]; ok {
			return
		}
		seen[execution] = struct{}{}
		executions = append(executions, execution)
	}

	for _, event := range history.GetEvents() {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			attributes := event.GetActivityTaskScheduledEventAttributes()
			if attributes.GetActivityType().GetName() != "StartWorkflow" {
				continue
			}
			var request schedulespb.StartWorkflowRequest
			if err := converter.GetDefaultDataConverter().FromPayloads(attributes.GetInput(), &request); err != nil {
				return nil, fmt.Errorf("decode StartWorkflow activity request: %w", err)
			}
			startsByScheduledEvent[event.GetEventId()] = &request
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			attributes := event.GetActivityTaskCompletedEventAttributes()
			request, ok := startsByScheduledEvent[attributes.GetScheduledEventId()]
			if !ok {
				continue
			}
			var response schedulespb.StartWorkflowResponse
			if err := converter.GetDefaultDataConverter().FromPayloads(attributes.GetResult(), &response); err != nil {
				return nil, fmt.Errorf("decode StartWorkflow activity response: %w", err)
			}
			appendExecution(request.GetRequest().GetWorkflowId(), response.GetRunId())
		case enumspb.EVENT_TYPE_MARKER_RECORDED:
			attributes := event.GetMarkerRecordedEventAttributes()
			if attributes.GetMarkerName() != "LocalActivity" || attributes.GetFailure() != nil {
				continue
			}
			var metadata struct {
				ActivityID   string
				ActivityType string
			}
			if err := converter.GetDefaultDataConverter().FromPayloads(attributes.GetDetails()["data"], &metadata); err != nil {
				return nil, fmt.Errorf("decode local activity metadata: %w", err)
			}
			if metadata.ActivityType != "StartWorkflow" {
				continue
			}
			request, ok := localStarts[metadata.ActivityID]
			if !ok {
				return nil, fmt.Errorf("local StartWorkflow activity %q was not captured during replay", metadata.ActivityID)
			}
			var response schedulespb.StartWorkflowResponse
			if err := converter.GetDefaultDataConverter().FromPayloads(attributes.GetDetails()["result"], &response); err != nil {
				return nil, fmt.Errorf("decode local StartWorkflow response: %w", err)
			}
			appendExecution(request.GetRequest().GetWorkflowId(), response.GetRunId())
		}
	}
	return executions, nil
}

func captureLocalActionStarts(history *historypb.History) (map[string]*schedulespb.StartWorkflowRequest, error) {
	if !historyHasLocalActionStart(history) {
		return nil, nil
	}
	capture := &actionStartCapture{starts: make(map[string]*schedulespb.StartWorkflowRequest)}
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		DataConverter: converter.GetDefaultDataConverter(),
		Interceptors:  []interceptor.WorkerInterceptor{capture},
	})
	if err != nil {
		return nil, fmt.Errorf("create action extraction replayer: %w", err)
	}
	replayer.RegisterWorkflowWithOptions(
		workerscheduler.SchedulerWorkflow,
		workflow.RegisterOptions{Name: workerscheduler.WorkflowType},
	)
	if err := replayer.ReplayWorkflowHistory(log.NewSdkLogger(log.NewNoopLogger()), proto.CloneOf(history)); err != nil {
		return nil, fmt.Errorf("capture local StartWorkflow activities: %w", err)
	}
	return capture.starts, nil
}

func historyHasLocalActionStart(history *historypb.History) bool {
	for _, event := range history.GetEvents() {
		attributes := event.GetMarkerRecordedEventAttributes()
		if attributes.GetMarkerName() != "LocalActivity" {
			continue
		}
		var metadata struct {
			ActivityType string
		}
		if converter.GetDefaultDataConverter().FromPayloads(attributes.GetDetails()["data"], &metadata) == nil &&
			metadata.ActivityType == "StartWorkflow" {
			return true
		}
	}
	return false
}
