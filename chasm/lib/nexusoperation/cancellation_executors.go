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

type CancellationTaskExecutorOptions struct {
	fx.In

	Config *Config

	MetricsHandler metrics.Handler
	Logger         log.Logger
}

type CancellationTaskExecutor struct {
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewCancellationTaskExecutor(opts CancellationTaskExecutorOptions) *CancellationTaskExecutor {
	return &CancellationTaskExecutor{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (e *CancellationTaskExecutor) Validate(
	ctx chasm.Context,
	cancellation *Cancellation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.CancellationTask,
) (bool, error) {
	return false, serviceerror.NewUnimplemented("unimplemented")
}

func (e *CancellationTaskExecutor) Execute(
	ctx context.Context,
	cancelRef chasm.ComponentRef,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.CancellationTask,
) error {
	return serviceerror.NewUnimplemented("unimplemented")
}

type CancellationBackoffTaskExecutor struct {
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewCancellationBackoffTaskExecutor(opts CancellationTaskExecutorOptions) *CancellationBackoffTaskExecutor {
	return &CancellationBackoffTaskExecutor{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (e *CancellationBackoffTaskExecutor) Validate(
	ctx chasm.Context,
	cancellation *Cancellation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.CancellationBackoffTask,
) (bool, error) {
	return false, serviceerror.NewUnimplemented("unimplemented")
}

func (e *CancellationBackoffTaskExecutor) Execute(
	ctx chasm.MutableContext,
	cancellation *Cancellation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.CancellationBackoffTask,
) error {
	return serviceerror.NewUnimplemented("unimplemented")
}
