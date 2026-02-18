package nexusoperation

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/fx"
)

// OperationTaskExecutorOptions is the fx parameter object for common options supplied to all operation task executors.
type OperationTaskExecutorOptions struct {
	fx.In

	Config *Config

	MetricsHandler metrics.Handler
	Logger         log.Logger
}

type OperationInvocationTaskExecutor struct {
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationInvocationTaskExecutor(opts OperationTaskExecutorOptions) *OperationInvocationTaskExecutor {
	return &OperationInvocationTaskExecutor{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (e *OperationInvocationTaskExecutor) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationTask,
) (bool, error) {
	return false, serviceerror.NewUnimplemented("unimplemented")
}

func (e *OperationInvocationTaskExecutor) Execute(
	ctx context.Context,
	opRef chasm.ComponentRef,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationTask,
) error {
	return serviceerror.NewUnimplemented("unimplemented")
}

type OperationBackoffTaskExecutor struct {
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationBackoffTaskExecutor(opts OperationTaskExecutorOptions) *OperationBackoffTaskExecutor {
	return &OperationBackoffTaskExecutor{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (e *OperationBackoffTaskExecutor) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationBackoffTask,
) (bool, error) {
	return false, serviceerror.NewUnimplemented("unimplemented")
}

func (e *OperationBackoffTaskExecutor) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationBackoffTask,
) error {
	return serviceerror.NewUnimplemented("unimplemented")
}

type OperationScheduleToStartTimeoutTaskExecutor struct {
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationScheduleToStartTimeoutTaskExecutor(opts OperationTaskExecutorOptions) *OperationScheduleToStartTimeoutTaskExecutor {
	return &OperationScheduleToStartTimeoutTaskExecutor{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (e *OperationScheduleToStartTimeoutTaskExecutor) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToStartTimeoutTask,
) (bool, error) {
	return false, serviceerror.NewUnimplemented("unimplemented")
}

func (e *OperationScheduleToStartTimeoutTaskExecutor) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToStartTimeoutTask,
) error {
	return serviceerror.NewUnimplemented("unimplemented")
}

type OperationStartToCloseTimeoutTaskExecutor struct {
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationStartToCloseTimeoutTaskExecutor(opts OperationTaskExecutorOptions) *OperationStartToCloseTimeoutTaskExecutor {
	return &OperationStartToCloseTimeoutTaskExecutor{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (e *OperationStartToCloseTimeoutTaskExecutor) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.StartToCloseTimeoutTask,
) (bool, error) {
	return false, serviceerror.NewUnimplemented("unimplemented")
}

func (e *OperationStartToCloseTimeoutTaskExecutor) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.StartToCloseTimeoutTask,
) error {
	return serviceerror.NewUnimplemented("unimplemented")
}

type OperationScheduleToCloseTimeoutTaskExecutor struct {
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationScheduleToCloseTimeoutTaskExecutor(opts OperationTaskExecutorOptions) *OperationScheduleToCloseTimeoutTaskExecutor {
	return &OperationScheduleToCloseTimeoutTaskExecutor{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (e *OperationScheduleToCloseTimeoutTaskExecutor) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToCloseTimeoutTask,
) (bool, error) {
	return false, serviceerror.NewUnimplemented("unimplemented")
}

func (e *OperationScheduleToCloseTimeoutTaskExecutor) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToCloseTimeoutTask,
) error {
	return serviceerror.NewUnimplemented("unimplemented")
}
