package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/fx"
)

type (
	SchedulerMigrateToWorkflowTaskExecutorOptions struct {
		fx.In

		Config         *Config
		MetricsHandler metrics.Handler
		BaseLogger     log.Logger
		HistoryClient  resource.HistoryClient
	}

	SchedulerMigrateToWorkflowTaskExecutor struct {
		config         *Config
		metricsHandler metrics.Handler
		baseLogger     log.Logger
		historyClient  resource.HistoryClient
	}
)

func NewSchedulerMigrateToWorkflowTaskExecutor(
	opts SchedulerMigrateToWorkflowTaskExecutorOptions,
) *SchedulerMigrateToWorkflowTaskExecutor {
	return &SchedulerMigrateToWorkflowTaskExecutor{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		baseLogger:     opts.BaseLogger,
		historyClient:  opts.HistoryClient,
	}
}

func (e *SchedulerMigrateToWorkflowTaskExecutor) Validate(
	_ chasm.Context,
	scheduler *Scheduler,
	_ chasm.TaskAttributes,
	_ *schedulerpb.SchedulerMigrateToWorkflowTask,
) (bool, error) {
	if scheduler.Closed {
		return false, nil
	}
	return scheduler.MigrationToWorkflowPending, nil
}

func (e *SchedulerMigrateToWorkflowTaskExecutor) Execute(
	ctx context.Context,
	schedulerRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *schedulerpb.SchedulerMigrateToWorkflowTask,
) error {
	// Read and deep copy the scheduler state outside the MS lock.
	var schedulerState *schedulerpb.SchedulerState
	var generatorState *schedulerpb.GeneratorState
	var invokerState *schedulerpb.InvokerState
	var backfillerStates map[string]*schedulerpb.BackfillerState
	var lastCompletionResult *schedulerpb.LastCompletionResult
	var searchAttributes map[string]*commonpb.Payload
	var memo map[string]*commonpb.Payload

	_, err := chasm.ReadComponent(
		ctx,
		schedulerRef,
		func(s *Scheduler, ctx chasm.Context, _ any) (struct{}, error) {
			schedulerState = common.CloneProto(s.SchedulerState)

			generator := s.Generator.Get(ctx)
			generatorState = common.CloneProto(generator.GeneratorState)

			invoker := s.Invoker.Get(ctx)
			invokerState = common.CloneProto(invoker.InvokerState)

			bStates := make(map[string]*schedulerpb.BackfillerState, len(s.Backfillers))
			for id, field := range s.Backfillers {
				bStates[id] = common.CloneProto(field.Get(ctx).BackfillerState)
			}
			backfillerStates = bStates

			lastCompletionResult = common.CloneProto(s.LastCompletionResult.Get(ctx))

			visibility := s.Visibility.Get(ctx)
			searchAttributes = visibility.CustomSearchAttributes(ctx)
			memo = visibility.CustomMemo(ctx)

			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to read scheduler state: %w", err)
	}

	// Restore the pre-migration paused state so the V1 workflow receives
	// the correct schedule state (not the migration-imposed pause).
	if schedulerState.GetSchedule().GetState() != nil {
		schedulerState.Schedule.State.Paused = schedulerState.PreMigrationPaused
		schedulerState.Schedule.State.Notes = schedulerState.PreMigrationNotes
	}

	// Convert CHASM state to V1 StartScheduleArgs.
	args := migration.CHASMToLegacyStartScheduleArgs(
		schedulerState,
		generatorState,
		invokerState,
		backfillerStates,
		lastCompletionResult,
		searchAttributes,
		memo,
		time.Now().UTC(),
	)

	// Serialize the V1 workflow input.
	inputPayloads, err := sdk.PreferProtoDataConverter.ToPayloads(args)
	if err != nil {
		return fmt.Errorf("failed to serialize schedule args: %w", err)
	}

	// Build and send StartWorkflowExecution request.
	workflowID := legacyscheduler.WorkflowIDPrefix + schedulerState.GetScheduleId()
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                schedulerState.GetNamespace(),
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: legacyscheduler.WorkflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                    inputPayloads,
		Identity:                 fmt.Sprintf("temporal-scheduler-migration-%s-%s", schedulerState.GetNamespace(), schedulerState.GetScheduleId()),
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		Memo:                     &commonpb.Memo{Fields: memo},
		SearchAttributes:         &commonpb.SearchAttributes{IndexedFields: searchAttributes},
		Priority:                 &commonpb.Priority{},
	}

	_, err = e.historyClient.StartWorkflowExecution(
		ctx,
		common.CreateHistoryStartWorkflowRequest(schedulerState.GetNamespaceId(), startReq, nil, nil, time.Now().UTC()),
	)
	if err != nil {
		// Treat already-started as success for idempotency.
		var alreadyStartedErr *serviceerror.WorkflowExecutionAlreadyStarted
		if !errors.As(err, &alreadyStartedErr) {
			return fmt.Errorf("failed to start V1 scheduler workflow: %w", err)
		}
	}

	return nil
}
