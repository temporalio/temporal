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

type CancellationTaskHandlerOptions struct {
	fx.In

	Config *Config

	MetricsHandler metrics.Handler
	Logger         log.Logger
}

type CancellationTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*nexusoperationpb.CancellationTask]
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewCancellationTaskHandler(opts CancellationTaskHandlerOptions) *CancellationTaskHandler {
	return &CancellationTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *CancellationTaskHandler) Validate(
	ctx chasm.Context,
	cancellation *Cancellation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.CancellationTask,
) (bool, error) {
	return false, serviceerror.NewUnimplemented("unimplemented")
}

func (h *CancellationTaskHandler) Execute(
	ctx context.Context,
	cancelRef chasm.ComponentRef,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.CancellationTask,
) error {
	return serviceerror.NewUnimplemented("unimplemented")
}

type CancellationBackoffTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewCancellationBackoffTaskHandler(opts CancellationTaskHandlerOptions) *CancellationBackoffTaskHandler {
	return &CancellationBackoffTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *CancellationBackoffTaskHandler) Validate(
	ctx chasm.Context,
	cancellation *Cancellation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.CancellationBackoffTask,
) (bool, error) {
	return false, serviceerror.NewUnimplemented("unimplemented")
}

func (h *CancellationBackoffTaskHandler) Execute(
	ctx chasm.MutableContext,
	cancellation *Cancellation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.CancellationBackoffTask,
) error {
	return serviceerror.NewUnimplemented("unimplemented")
}
