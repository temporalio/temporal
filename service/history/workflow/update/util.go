package update

import (
	"fmt"

	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/telemetry"
	"google.golang.org/protobuf/proto"
)

type (
	instrumentation struct {
		log       log.Logger
		metrics   metrics.Handler
		tracer    trace.Tracer
		namespace string
	}
)

var (
	noopInstrumentation = instrumentation{
		log:     log.NewNoopLogger(),
		metrics: metrics.NoopMetricsHandler,
		tracer:  telemetry.NoopTracer,
	}
)

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
	i.log.Warn("update registry size limit reached",
		tag.Int("registry-size", registrySize),
		tag.Int("payload-size", payloadSize),
		tag.Int("update-count", updateCount),
		tag.String("namespace", i.namespace),
	)
}

func (i *instrumentation) countTooMany() {
	i.oneOf(metrics.WorkflowExecutionUpdateTooMany.Name())
}

func (i *instrumentation) countAborted(updateID string, reason AbortReason) {
	i.metrics.Counter(metrics.WorkflowExecutionUpdateAborted.Name()).
		Record(1, metrics.ReasonTag(metrics.ReasonString(reason.String())))
	i.log.Debug("update aborted",
		tag.String("reason", reason.String()),
		tag.String("update-id", updateID),
	)
}

func (i *instrumentation) countSent() {
	i.oneOf(metrics.WorkflowExecutionUpdateSentToWorker.Name())
}

func (i *instrumentation) countSentAgain() {
	i.oneOf(metrics.WorkflowExecutionUpdateSentToWorkerAgain.Name())
}

func (i *instrumentation) invalidStateTransition(updateID string, msg proto.Message, state state) {
	softassert.Fail(
		i.log,
		"invalid state transition attempted",
		tag.ComponentWorkflowUpdate,
		tag.String("update-id", updateID),
		tag.String("message", fmt.Sprintf("%T", msg)),
		tag.Stringer("state", state),
		tag.String("namespace", i.namespace),
	)
}

func (i *instrumentation) updateRegistrySize(size int) {
	i.metrics.Histogram(metrics.WorkflowExecutionUpdateRegistrySize.Name(), metrics.Bytes).Record(int64(size))
}

func (i *instrumentation) oneOf(counterName string) {
	i.metrics.Counter(counterName).Record(1)
}

func (i *instrumentation) stateChange(updateID string, from, to state) {
	i.log.Debug(
		"update state change",
		tag.ComponentWorkflowUpdate,
		tag.String("update-id", updateID),
		tag.Stringer("from-state", from),
		tag.Stringer("to-state", to),
	)
}
