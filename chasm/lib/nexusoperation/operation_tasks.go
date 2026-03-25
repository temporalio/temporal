package nexusoperation

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/fx"
)

// OperationTaskHandlerOptions is the fx parameter object for common options supplied to all operation task handlers.
type OperationTaskHandlerOptions struct {
	fx.In

	Config *Config

	MetricsHandler metrics.Handler
	Logger         log.Logger
}

type OperationInvocationTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*nexusoperationpb.InvocationTask]
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationInvocationTaskHandler(opts OperationTaskHandlerOptions) *OperationInvocationTaskHandler {
	return &OperationInvocationTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *OperationInvocationTaskHandler) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationTask,
) (bool, error) {
	return false, serviceerror.NewUnimplemented("unimplemented")
}

func (h *OperationInvocationTaskHandler) Execute(
	ctx context.Context,
	opRef chasm.ComponentRef,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationTask,
) error {
	return serviceerror.NewUnimplemented("unimplemented")
}

type OperationBackoffTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationBackoffTaskHandler(opts OperationTaskHandlerOptions) *OperationBackoffTaskHandler {
	return &OperationBackoffTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *OperationBackoffTaskHandler) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationBackoffTask,
) (bool, error) {
	return op.Status == nexusoperationpb.OPERATION_STATUS_BACKING_OFF && op.GetAttempt() == task.GetAttempt(), nil
}

func (h *OperationBackoffTaskHandler) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationBackoffTask,
) error {
	return transitionRescheduled.Apply(op, ctx, EventRescheduled{})
}

type OperationScheduleToStartTimeoutTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationScheduleToStartTimeoutTaskHandler(opts OperationTaskHandlerOptions) *OperationScheduleToStartTimeoutTaskHandler {
	return &OperationScheduleToStartTimeoutTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *OperationScheduleToStartTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToStartTimeoutTask,
) (bool, error) {
	// Only timeout if we haven't started yet.
	switch op.Status {
	case nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF:
		return true, nil
	default:
		return false, nil
	}
}

func (h *OperationScheduleToStartTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToStartTimeoutTask,
) error {
	return op.OnTimedOut(ctx, op, &failurepb.Failure{
		Message: "operation timed out",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			},
		},
	})
}

type OperationStartToCloseTimeoutTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationStartToCloseTimeoutTaskHandler(opts OperationTaskHandlerOptions) *OperationStartToCloseTimeoutTaskHandler {
	return &OperationStartToCloseTimeoutTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *OperationStartToCloseTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.StartToCloseTimeoutTask,
) (bool, error) {
	return op.Status == nexusoperationpb.OPERATION_STATUS_STARTED, nil
}

func (h *OperationStartToCloseTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.StartToCloseTimeoutTask,
) error {
	return op.OnTimedOut(ctx, op, &failurepb.Failure{
		Message: "operation timed out",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			},
		},
	})
}

type OperationScheduleToCloseTimeoutTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationScheduleToCloseTimeoutTaskHandler(opts OperationTaskHandlerOptions) *OperationScheduleToCloseTimeoutTaskHandler {
	return &OperationScheduleToCloseTimeoutTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *OperationScheduleToCloseTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToCloseTimeoutTask,
) (bool, error) {
	return TransitionTimedOut.Possible(op), nil
}

func (h *OperationScheduleToCloseTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToCloseTimeoutTask,
) error {
	return op.OnTimedOut(ctx, op, &failurepb.Failure{
		Message: "operation timed out",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			},
		},
	})
}
