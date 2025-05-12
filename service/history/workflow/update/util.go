package update

import (
	"fmt"

	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/telemetry"
	"google.golang.org/protobuf/proto"
)

type (
	instrumentation struct {
		log     log.Logger
		metrics metrics.Handler
		tracer  trace.Tracer
	}
)

var (
	noopInstrumentation = instrumentation{
		log:     log.NewNoopLogger(),
		metrics: metrics.NoopMetricsHandler,
		tracer:  telemetry.NoopTracer,
	}
)

func invalidArgf(tmpl string, args ...any) error {
	return serviceerror.NewInvalidArgument(fmt.Sprintf(tmpl, args...))
}

func internalErrorf(tmpl string, args ...any) error {
	return serviceerror.NewInternal(fmt.Sprintf(tmpl, args...))
}

func (i *instrumentation) countRequestMsg() {
	i.oneOf(metrics.MessageTypeRequestWorkflowExecutionUpdateCounter.Name())
}

func (i *instrumentation) countAcceptanceMsg() {
	i.oneOf(metrics.MessageTypeAcceptWorkflowExecutionUpdateCounter.Name())
}

func (i *instrumentation) countRejectionMsg() {
	i.oneOf(metrics.MessageTypeRejectWorkflowExecutionUpdateCounter.Name())
}

func (i *instrumentation) countResponseMsg() {
	i.oneOf(metrics.MessageTypeRespondWorkflowExecutionUpdateCounter.Name())
}

func (i *instrumentation) countRateLimited() {
	i.oneOf(metrics.WorkflowExecutionUpdateRequestRateLimited.Name())
}

func (i *instrumentation) countRegistrySizeLimited(updateCount, registrySize, payloadSize int) {
	i.oneOf(metrics.WorkflowExecutionUpdateRegistrySizeLimited.Name())
	// TODO: remove log once limit is enforced everywhere
	i.log.Warn("update registry size limit reached",
		tag.NewInt("registry-size", registrySize),
		tag.NewInt("payload-size", payloadSize),
		tag.NewInt("update-count", updateCount))
}

func (i *instrumentation) countTooMany() {
	i.oneOf(metrics.WorkflowExecutionUpdateTooMany.Name())
}

func (i *instrumentation) countAborted() {
	i.oneOf(metrics.WorkflowExecutionUpdateAborted.Name())
}

func (i *instrumentation) countContinueAsNewSuggestions() {
	i.oneOf(metrics.WorkflowExecutionUpdateContinueAsNewSuggestions.Name())
}

func (i *instrumentation) countSent() {
	i.oneOf(metrics.WorkflowExecutionUpdateSentToWorker.Name())
}

func (i *instrumentation) countSentAgain() {
	i.oneOf(metrics.WorkflowExecutionUpdateSentToWorkerAgain.Name())
}

func (i *instrumentation) invalidStateTransition(updateID string, msg proto.Message, state state) {
	i.oneOf(metrics.InvalidStateTransitionWorkflowExecutionUpdateCounter.Name())
	i.log.Error("invalid state transition attempted",
		tag.NewStringTag("update-id", updateID),
		tag.NewStringTag("message", fmt.Sprintf("%T", msg)),
		tag.NewStringerTag("state", state))
}

func (i *instrumentation) updateRegistrySize(size int) {
	i.metrics.Histogram(metrics.WorkflowExecutionUpdateRegistrySize.Name(), metrics.Bytes).Record(int64(size))
}

func (i *instrumentation) oneOf(counterName string) {
	i.metrics.Counter(counterName).Record(1)
}

func (i *instrumentation) stateChange(updateID string, from, to state) {
	i.log.Debug("update state change",
		tag.NewStringTag("update-id", updateID),
		tag.NewStringerTag("from-state", from),
		tag.NewStringerTag("to-state", to),
	)
}
