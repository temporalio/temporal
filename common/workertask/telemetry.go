package workertask

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	typeKey        = "worker_task.type"
	idKey          = "worker_task.id"
	namespaceIDKey = "worker_task.namespace_id"
	workflowIDKey  = "worker_task.workflow_id"
	runIDKey       = "worker_task.run_id"
	activityIDKey  = "worker_task.activity_id"
	taskQueueKey   = "worker_task.task_queue"

	TypeWorkflow = "workflow"
	TypeActivity = "activity"
	TypeNexus    = "nexus"
)

// SpanAttributes describes a worker task handled by an RPC span.
type SpanAttributes struct {
	Type        string
	ID          string
	NamespaceID string
	WorkflowID  string
	RunID       string
	ActivityID  string
	TaskQueue   string
}

// AnnotateSpan adds worker task attributes to span.
func AnnotateSpan(span trace.Span, attrs SpanAttributes) {
	// Non-recording spans discard attributes, so avoid constructing them.
	if !span.IsRecording() {
		return
	}
	kvs := make([]attribute.KeyValue, 0, 7)
	if attrs.Type != "" {
		kvs = append(kvs, attribute.String(typeKey, attrs.Type))
	}
	if attrs.ID != "" {
		kvs = append(kvs, attribute.String(idKey, attrs.ID))
	}
	if attrs.NamespaceID != "" {
		kvs = append(kvs, attribute.String(namespaceIDKey, attrs.NamespaceID))
	}
	if attrs.WorkflowID != "" {
		kvs = append(kvs, attribute.String(workflowIDKey, attrs.WorkflowID))
	}
	if attrs.RunID != "" {
		kvs = append(kvs, attribute.String(runIDKey, attrs.RunID))
	}
	if attrs.ActivityID != "" {
		kvs = append(kvs, attribute.String(activityIDKey, attrs.ActivityID))
	}
	if attrs.TaskQueue != "" {
		kvs = append(kvs, attribute.String(taskQueueKey, attrs.TaskQueue))
	}
	span.SetAttributes(kvs...)
}
