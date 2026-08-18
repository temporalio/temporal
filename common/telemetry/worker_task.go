package telemetry

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// WorkerTaskSpanAttributes describes a worker task handled by an RPC span.
type WorkerTaskSpanAttributes struct {
	Type        string
	ID          string
	NamespaceID string
	WorkflowID  string
	RunID       string
	ActivityID  string
	TaskQueue   string
}

// AnnotateWorkerTaskSpan adds worker task attributes to span.
func AnnotateWorkerTaskSpan(span trace.Span, attrs WorkerTaskSpanAttributes) {
	// Non-recording spans discard attributes, so avoid constructing them.
	if !span.IsRecording() {
		return
	}
	kvs := make([]attribute.KeyValue, 0, 7)
	if attrs.Type != "" {
		kvs = append(kvs, attribute.String(WorkerTaskTypeKey, attrs.Type))
	}
	if attrs.ID != "" {
		kvs = append(kvs, attribute.String(WorkerTaskIDKey, attrs.ID))
	}
	if attrs.NamespaceID != "" {
		kvs = append(kvs, attribute.String(WorkerTaskNamespaceIDKey, attrs.NamespaceID))
	}
	if attrs.WorkflowID != "" {
		kvs = append(kvs, attribute.String(WorkerTaskWorkflowIDKey, attrs.WorkflowID))
	}
	if attrs.RunID != "" {
		kvs = append(kvs, attribute.String(WorkerTaskRunIDKey, attrs.RunID))
	}
	if attrs.ActivityID != "" {
		kvs = append(kvs, attribute.String(WorkerTaskActivityIDKey, attrs.ActivityID))
	}
	if attrs.TaskQueue != "" {
		kvs = append(kvs, attribute.String(WorkerTaskTaskQueueKey, attrs.TaskQueue))
	}
	span.SetAttributes(kvs...)
}
