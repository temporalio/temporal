package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/resource"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SchedulerIdleTaskExecutorOptions struct {
	fx.In

	Config *Config
}

type SchedulerIdleTaskExecutor struct {
	config *Config
}

func NewSchedulerIdleTaskExecutor(opts SchedulerIdleTaskExecutorOptions) *SchedulerIdleTaskExecutor {
	return &SchedulerIdleTaskExecutor{
		config: opts.Config,
	}
}

func (r *SchedulerIdleTaskExecutor) Execute(
	ctx chasm.MutableContext,
	scheduler *Scheduler,
	_ chasm.TaskAttributes,
	_ *schedulerpb.SchedulerIdleTask,
) error {
	scheduler.Closed = true
	return nil
}

func (r *SchedulerIdleTaskExecutor) Validate(
	ctx chasm.Context,
	scheduler *Scheduler,
	taskAttrs chasm.TaskAttributes,
	task *schedulerpb.SchedulerIdleTask,
) (bool, error) {
	idleTimeTotal := task.IdleTimeTotal.AsDuration()
	idleExpiration, isIdle := scheduler.getIdleExpiration(ctx, idleTimeTotal, time.Time{})

	// If the scheduler has since woken up, or its idle expiration time changed, this
	// task must be obsolete.
	if !isIdle || idleExpiration.Compare(taskAttrs.ScheduledTime) != 0 {
		return false, nil
	}

	return !scheduler.Closed, nil
}

type SchedulerCallbacksTaskExecutorOptions struct {
	fx.In

	Config         *Config
	HistoryClient  resource.HistoryClient
	FrontendClient workflowservice.WorkflowServiceClient
}

type SchedulerCallbacksTaskExecutor struct {
	config         *Config
	historyClient  resource.HistoryClient
	frontendClient workflowservice.WorkflowServiceClient
}

func NewSchedulerCallbacksTaskExecutor(opts SchedulerCallbacksTaskExecutorOptions) *SchedulerCallbacksTaskExecutor {
	return &SchedulerCallbacksTaskExecutor{
		config:         opts.Config,
		historyClient:  opts.HistoryClient,
		frontendClient: opts.FrontendClient,
	}
}

// watchResult holds the outcome of watchRunningStart for a single BufferedStart.
// A nil completed field means the callback was successfully attached and the
// workflow is still running.
type watchResult struct {
	completed *schedulespb.CompletedResult
}

func (r *SchedulerCallbacksTaskExecutor) Execute(
	ctx context.Context,
	schedulerRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *schedulerpb.SchedulerCallbacksTask,
) error {
	var scheduler *Scheduler
	var starts []*schedulespb.BufferedStart
	var callback *commonpb.Callback

	// Read scheduler state and generate the Nexus callback token.
	_, err := chasm.ReadComponent(
		ctx,
		schedulerRef,
		func(s *Scheduler, ctx chasm.Context, _ any) (struct{}, error) {
			scheduler = &Scheduler{
				SchedulerState: common.CloneProto(s.SchedulerState),
			}

			invoker := s.Invoker.Get(ctx)
			for _, start := range invoker.BufferedStarts {
				if needsCallback(start) {
					starts = append(starts, common.CloneProto(start))
				}
			}

			cb, err := chasm.GenerateNexusCallback(ctx, s)
			if err != nil {
				return struct{}{}, err
			}
			callback = common.CloneProto(cb)

			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to read component: %w", err)
	}

	// Attach callbacks and check workflow status.
	results := make(map[string]*watchResult, len(starts))
	for _, start := range starts {
		result, err := r.watchRunningStart(ctx, scheduler, start, callback)
		if err != nil {
			return err
		}
		results[start.RequestId] = result
	}

	// Apply results to the invoker's BufferedStarts.
	_, _, err = chasm.UpdateComponent(
		ctx,
		schedulerRef,
		func(s *Scheduler, ctx chasm.MutableContext, _ any) (chasm.NoValue, error) {
			invoker := s.Invoker.Get(ctx)
			for _, start := range invoker.BufferedStarts {
				if result, ok := results[start.RequestId]; ok {
					start.HasCallback = true
					if result.completed != nil {
						start.Completed = result.completed
					}
				}
			}
			return nil, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to update component state: %w", err)
	}

	return nil
}

// watchRunningStart will attach a Nexus completion callback to a running
// BufferedStart. If the start's workflow has already closed, the start is updated
// to indicate it has completed. Intended for migration/anti-entropy cases.
func (r *SchedulerCallbacksTaskExecutor) watchRunningStart(
	ctx context.Context,
	scheduler *Scheduler,
	start *schedulespb.BufferedStart,
	callback *commonpb.Callback,
) (*watchResult, error) {
	// Describe the workflow to ensure it exists and is still running.
	descResp, err := r.historyClient.DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
		NamespaceId: scheduler.NamespaceId,
		Request: &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: scheduler.Namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: start.WorkflowId,
				RunId:      start.RunId,
			},
		},
	})
	if err != nil {
		var notFoundErr *serviceerror.NotFound
		if errors.As(err, &notFoundErr) {
			return &watchResult{
				completed: &schedulespb.CompletedResult{
					Status:    enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
					CloseTime: timestamppb.Now(),
				},
			}, nil
		}
		return nil, err
	}

	wfInfo := descResp.GetWorkflowExecutionInfo()
	wfProgressing := wfInfo.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING ||
		wfInfo.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED

	if !wfProgressing {
		return &watchResult{
			completed: &schedulespb.CompletedResult{
				Status:    wfInfo.GetStatus(),
				CloseTime: wfInfo.GetCloseTime(),
			},
		}, nil
	}

	// Workflow is still running. Attach a Nexus completion callback by issuing
	// a StartWorkflowExecution with USE_EXISTING conflict policy. REJECT_DUPLICATE
	// reuse policy prevents accidentally starting a new workflow if the original
	// completes between the describe and this call.
	requestSpec := scheduler.GetSchedule().GetAction().GetStartWorkflow()

	_, err = r.frontendClient.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                scheduler.Namespace,
		WorkflowId:               start.WorkflowId,
		RequestId:                start.RequestId,
		Identity:                 scheduler.identity(),
		WorkflowType:             requestSpec.GetWorkflowType(),
		TaskQueue:                requestSpec.GetTaskQueue(),
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		CompletionCallbacks:      []*commonpb.Callback{callback},
		OnConflictOptions: &workflowpb.OnConflictOptions{
			AttachRequestId:           true,
			AttachCompletionCallbacks: true,
		},
	})
	if err != nil {
		// WorkflowExecutionAlreadyStarted: workflow completed between describe
		// and this attach call (REJECT_DUPLICATE rejects completed workflows).
		if isAlreadyStartedError(err) {
			return &watchResult{
				completed: &schedulespb.CompletedResult{
					Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					CloseTime: timestamppb.Now(),
				},
			}, nil
		}
		return nil, err
	}

	// Callback attached successfully.
	return &watchResult{}, nil
}

func (r *SchedulerCallbacksTaskExecutor) Validate(
	ctx chasm.Context,
	scheduler *Scheduler,
	taskAttrs chasm.TaskAttributes,
	task *schedulerpb.SchedulerCallbacksTask,
) (bool, error) {
	invoker := scheduler.Invoker.Get(ctx)
	for _, start := range invoker.BufferedStarts {
		if needsCallback(start) {
			return true, nil
		}
	}
	return false, nil
}

func needsCallback(start *schedulespb.BufferedStart) bool {
	return !start.HasCallback && start.GetRunId() != "" && start.GetCompleted() != nil
}
