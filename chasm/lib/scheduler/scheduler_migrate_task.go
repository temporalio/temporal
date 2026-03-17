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
	schedulespb "go.temporal.io/server/api/schedule/v1"
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
	// Read state and convert to V1 args inside the ReadComponent callback,
	// where we have access to the CHASM context for consistent time.
	type readResult struct {
		args             *schedulespb.StartScheduleArgs
		namespace        string
		namespaceID      string
		scheduleID       string
		searchAttributes map[string]*commonpb.Payload
		memo             map[string]*commonpb.Payload
		now              time.Time
	}
	var result readResult

	_, err := chasm.ReadComponent(
		ctx,
		schedulerRef,
		func(s *Scheduler, ctx chasm.Context, _ any) (struct{}, error) {
			now := ctx.Now(s)
			schedulerState := common.CloneProto(s.SchedulerState)
			generatorState := common.CloneProto(s.Generator.Get(ctx).GeneratorState)
			invokerState := common.CloneProto(s.Invoker.Get(ctx).InvokerState)

			bStates := make(map[string]*schedulerpb.BackfillerState, len(s.Backfillers))
			for id, field := range s.Backfillers {
				bStates[id] = common.CloneProto(field.Get(ctx).BackfillerState)
			}

			lastCompletionResult := common.CloneProto(s.LastCompletionResult.Get(ctx))

			visibility := s.Visibility.Get(ctx)
			searchAttributes := visibility.CustomSearchAttributes(ctx)
			memo := visibility.CustomMemo(ctx)

			// Restore the pre-migration paused state so the V1 workflow receives
			// the correct schedule state (not the migration-imposed pause).
			if schedulerState.GetSchedule().GetState() != nil {
				schedulerState.Schedule.State.Paused = schedulerState.PreMigrationPaused
				schedulerState.Schedule.State.Notes = schedulerState.PreMigrationNotes
			}

			result = readResult{
				args: migration.CHASMToLegacyStartScheduleArgs(
					schedulerState,
					generatorState,
					invokerState,
					bStates,
					lastCompletionResult,
					searchAttributes,
					memo,
					now,
				),
				namespace:        schedulerState.GetNamespace(),
				namespaceID:      schedulerState.GetNamespaceId(),
				scheduleID:       schedulerState.GetScheduleId(),
				searchAttributes: searchAttributes,
				memo:             memo,
				now:              now,
			}
			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to read scheduler state: %w", err)
	}

	// Serialize the V1 workflow input.
	inputPayloads, err := sdk.PreferProtoDataConverter.ToPayloads(result.args)
	if err != nil {
		return fmt.Errorf("failed to serialize schedule args: %w", err)
	}

	// Build and send StartWorkflowExecution request.
	workflowID := legacyscheduler.WorkflowIDPrefix + result.scheduleID
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                result.namespace,
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: legacyscheduler.WorkflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                    inputPayloads,
		Identity:                 fmt.Sprintf("temporal-scheduler-migration-%s-%s", result.namespace, result.scheduleID),
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		Memo:                     &commonpb.Memo{Fields: result.memo},
		SearchAttributes:         &commonpb.SearchAttributes{IndexedFields: result.searchAttributes},
	}

	_, err = e.historyClient.StartWorkflowExecution(
		ctx,
		common.CreateHistoryStartWorkflowRequest(result.namespaceID, startReq, nil, nil, result.now),
	)
	if err != nil {
		// Treat already-started as success for idempotency.
		var alreadyStartedErr *serviceerror.WorkflowExecutionAlreadyStarted
		if !errors.As(err, &alreadyStartedErr) {
			return fmt.Errorf("failed to start V1 scheduler workflow: %w", err)
		}
	}

	// Mark the CHASM scheduler as closed now that the V1 workflow is running.
	_, _, err = chasm.UpdateComponent(
		ctx,
		schedulerRef,
		func(s *Scheduler, ctx chasm.MutableContext, _ any) (chasm.NoValue, error) {
			s.Closed = true
			s.MigrationToWorkflowPending = false
			return nil, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to close CHASM scheduler after migration: %w", err)
	}

	return nil
}
