package nexusoperation

import (
	"context"

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
	panic("implement me")
}

func (e *OperationInvocationTaskExecutor) Execute(
	ctx context.Context,
	opRef chasm.ComponentRef,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationTask,
) error {
	panic("implement me")
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
	panic("implement me")
}

func (e *OperationBackoffTaskExecutor) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationBackoffTask,
) error {
	panic("implement me")
}

type OperationTimeoutTaskExecutor struct {
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationTimeoutTaskExecutor(opts OperationTaskExecutorOptions) *OperationTimeoutTaskExecutor {
	return &OperationTimeoutTaskExecutor{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (e *OperationTimeoutTaskExecutor) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationTimeoutTask,
) (bool, error) {
	panic("implement me")
}

func (e *OperationTimeoutTaskExecutor) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationTimeoutTask,
) error {
	panic("implement me")
}
