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

type cancellationTaskHandlerOptions struct {
	fx.In

	Config *Config

	MetricsHandler metrics.Handler
	Logger         log.Logger
}

type cancellationTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*nexusoperationpb.CancellationTask]
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func newCancellationTaskHandler(opts cancellationTaskHandlerOptions) *cancellationTaskHandler {
	return &cancellationTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *cancellationTaskHandler) Validate(
	ctx chasm.Context,
	cancellation *Cancellation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.CancellationTask,
) (bool, error) {
	return false, serviceerror.NewUnimplemented("unimplemented")
}

func (h *cancellationTaskHandler) Execute(
	ctx context.Context,
	cancelRef chasm.ComponentRef,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.CancellationTask,
) error {
	return serviceerror.NewUnimplemented("unimplemented")
}

type cancellationBackoffTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func newCancellationBackoffTaskHandler(opts cancellationTaskHandlerOptions) *cancellationBackoffTaskHandler {
	return &cancellationBackoffTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *cancellationBackoffTaskHandler) Validate(
	ctx chasm.Context,
	cancellation *Cancellation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.CancellationBackoffTask,
) (bool, error) {
	return false, serviceerror.NewUnimplemented("unimplemented")
}

func (h *cancellationBackoffTaskHandler) Execute(
	ctx chasm.MutableContext,
	cancellation *Cancellation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.CancellationBackoffTask,
) error {
	return serviceerror.NewUnimplemented("unimplemented")
}
