package fact

// This file preserves the disabled Workflow Update support for future work.
// All archived source is commented out intentionally.

// tests/umpirev1/fact/update_span.go
// package fact
//
// import (
// 	"go.opentelemetry.io/otel/attribute"
// 	"go.temporal.io/server/common/telemetry"
// 	"go.temporal.io/server/common/testing/umpire"
// )
//
// // importUpdateSpanEvent extracts the update ID, workflow ID, and the routed
// // entity identity (WorkflowUpdate keyed under its parent Workflow) shared by
// // all update-lifecycle span facts. Returns an empty updateID when the event
// // carries no update ID, in which case the fact should be discarded.
// func importUpdateSpanEvent(attrs attribute.Set) (updateID, workflowID string, path *umpire.EntityPath) {
// 	if v, ok := attrs.Value(telemetry.AttrUpdateID); ok {
// 		updateID = v.AsString()
// 	}
// 	if v, ok := attrs.Value(telemetry.AttrWorkflowID); ok {
// 		workflowID = v.AsString()
// 	}
// 	var namespaceID string
// 	if v, ok := attrs.Value(telemetry.AttrNamespaceID); ok {
// 		namespaceID = v.AsString()
// 	}
// 	if updateID == "" {
// 		return "", "", nil
// 	}
// 	uid := umpire.NewEntityID(WorkflowUpdateType, updateID)
// 	var parents []umpire.EntityID
// 	if workflowID != "" {
// 		parents = append(parents, umpire.NewEntityID(WorkflowType, workflowID))
// 	}
// 	return updateID, workflowID, nsPath(namespaceID, uid, parents...)
// }

// tests/umpirev1/fact/update_span_test.go
// package fact
//
// import (
// 	"testing"
//
// 	"github.com/stretchr/testify/require"
// 	"go.opentelemetry.io/otel/attribute"
// 	"go.temporal.io/server/common/telemetry"
// )
//
// func updateEventAttrs(extra ...attribute.KeyValue) attribute.Set {
// 	base := []attribute.KeyValue{
// 		telemetry.AttrUpdateID.String("upd1"),
// 		telemetry.AttrWorkflowID.String("wf1"),
// 		telemetry.AttrRunID.String("run1"),
// 	}
// 	return attribute.NewSet(append(base, extra...)...)
// }
//
// func TestUpdateSpanFacts_ImportSpanEvent(t *testing.T) {
// 	attrs := updateEventAttrs()
//
// 	admitted := &WorkflowUpdateAdmitted{}
// 	require.True(t, admitted.ImportSpanEvent(attrs))
// 	require.Equal(t, "upd1", admitted.UpdateID)
// 	require.Equal(t, "wf1", admitted.WorkflowID)
// 	require.Equal(t, telemetry.EventWorkflowUpdateAdmitted, admitted.Name())
//
// 	// EntityPath routes the update under its parent workflow.
// 	ident := admitted.TargetEntity()
// 	require.NotNil(t, ident)
// 	require.Equal(t, WorkflowUpdateType, ident.EntityID.Type)
// 	require.Equal(t, "upd1", ident.EntityID.ID)
// 	parent := ident.Parent()
// 	require.NotNil(t, parent)
// 	require.Equal(t, WorkflowType, parent.EntityID.Type)
// 	require.Equal(t, "wf1", parent.EntityID.ID)
//
// 	accepted := &WorkflowUpdateAccepted{}
// 	require.True(t, accepted.ImportSpanEvent(attrs))
// 	require.Equal(t, "upd1", accepted.UpdateID)
//
// 	rejected := &WorkflowUpdateRejected{}
// 	require.True(t, rejected.ImportSpanEvent(attrs))
// 	require.Equal(t, "upd1", rejected.UpdateID)
// }
//
// func TestWorkflowUpdateCompleted_Outcome(t *testing.T) {
// 	success := &WorkflowUpdateCompleted{}
// 	require.True(t, success.ImportSpanEvent(updateEventAttrs(
// 		telemetry.AttrUpdateOutcome.String(telemetry.UpdateOutcomeSuccess))))
// 	require.True(t, success.IsSuccess())
//
// 	failure := &WorkflowUpdateCompleted{}
// 	require.True(t, failure.ImportSpanEvent(updateEventAttrs(
// 		telemetry.AttrUpdateOutcome.String(telemetry.UpdateOutcomeFailure))))
// 	require.False(t, failure.IsSuccess())
// }
//
// func TestUpdateSpanFacts_DiscardedWithoutUpdateID(t *testing.T) {
// 	attrs := attribute.NewSet(telemetry.AttrWorkflowID.String("wf1"))
// 	f := &WorkflowUpdateAccepted{}
// 	require.False(t, f.ImportSpanEvent(attrs))
// 	require.Nil(t, f.TargetEntity())
// }

// tests/umpirev1/fact/workflow_update_aborted.go
// package fact
//
// import (
// 	"go.opentelemetry.io/otel/attribute"
// 	"go.temporal.io/server/common/telemetry"
// 	"go.temporal.io/server/common/testing/umpire"
// )
//
// // WorkflowUpdateAborted represents a workflow update being aborted by the
// // history service (e.g., workflow closed, registry cleared).
// type WorkflowUpdateAborted struct {
// 	UpdateID    string
// 	WorkflowID  string
// 	NamespaceID string
// 	AbortReason string
// 	EntityPath  *umpire.EntityPath
// }
//
// func (e *WorkflowUpdateAborted) Name() string {
// 	return telemetry.EventWorkflowUpdateAborted
// }
//
// func (e *WorkflowUpdateAborted) TargetEntity() *umpire.EntityPath {
// 	return e.EntityPath
// }
//
// func (e *WorkflowUpdateAborted) ImportSpanEvent(attrs attribute.Set) bool {
// 	if v, ok := attrs.Value(telemetry.AttrUpdateID); ok {
// 		e.UpdateID = v.AsString()
// 	}
// 	if v, ok := attrs.Value(telemetry.AttrWorkflowID); ok {
// 		e.WorkflowID = v.AsString()
// 	}
// 	if v, ok := attrs.Value(telemetry.AttrAbortReason); ok {
// 		e.AbortReason = v.AsString()
// 	}
// 	if v, ok := attrs.Value(telemetry.AttrNamespaceID); ok {
// 		e.NamespaceID = v.AsString()
// 	}
// 	if e.UpdateID == "" {
// 		return false
// 	}
// 	updateID := umpire.NewEntityID(WorkflowUpdateType, e.UpdateID)
// 	var parents []umpire.EntityID
// 	if e.WorkflowID != "" {
// 		parents = append(parents, umpire.NewEntityID(WorkflowType, e.WorkflowID))
// 	}
// 	e.EntityPath = nsPath(e.NamespaceID, updateID, parents...)
// 	return true
// }

// tests/umpirev1/fact/workflow_update_accepted.go
// package fact
//
// import (
// 	"go.opentelemetry.io/otel/attribute"
// 	"go.temporal.io/server/common/telemetry"
// 	"go.temporal.io/server/common/testing/umpire"
// )
//
// // WorkflowUpdateAccepted represents a workflow update being accepted by a worker.
// type WorkflowUpdateAccepted struct {
// 	UpdateID   string
// 	WorkflowID string
// 	EntityPath *umpire.EntityPath
// }
//
// func (e *WorkflowUpdateAccepted) Name() string {
// 	return telemetry.EventWorkflowUpdateAccepted
// }
//
// func (e *WorkflowUpdateAccepted) TargetEntity() *umpire.EntityPath {
// 	return e.EntityPath
// }
//
// func (e *WorkflowUpdateAccepted) ImportSpanEvent(attrs attribute.Set) bool {
// 	e.UpdateID, e.WorkflowID, e.EntityPath = importUpdateSpanEvent(attrs)
// 	return e.UpdateID != ""
// }

// tests/umpirev1/fact/workflow_update_admitted.go
// package fact
//
// import (
// 	"go.opentelemetry.io/otel/attribute"
// 	"go.temporal.io/server/common/telemetry"
// 	"go.temporal.io/server/common/testing/umpire"
// )
//
// // WorkflowUpdateAdmitted represents a workflow update being admitted to the
// // history update registry.
// type WorkflowUpdateAdmitted struct {
// 	UpdateID   string
// 	WorkflowID string
// 	EntityPath *umpire.EntityPath
// }
//
// func (e *WorkflowUpdateAdmitted) Name() string {
// 	return telemetry.EventWorkflowUpdateAdmitted
// }
//
// func (e *WorkflowUpdateAdmitted) TargetEntity() *umpire.EntityPath {
// 	return e.EntityPath
// }
//
// func (e *WorkflowUpdateAdmitted) ImportSpanEvent(attrs attribute.Set) bool {
// 	e.UpdateID, e.WorkflowID, e.EntityPath = importUpdateSpanEvent(attrs)
// 	return e.UpdateID != ""
// }

// tests/umpirev1/fact/workflow_update_completed.go
// package fact
//
// import (
// 	"go.opentelemetry.io/otel/attribute"
// 	"go.temporal.io/server/common/telemetry"
// 	"go.temporal.io/server/common/testing/umpire"
// )
//
// // WorkflowUpdateCompleted represents a workflow update being completed.
// type WorkflowUpdateCompleted struct {
// 	UpdateID   string
// 	WorkflowID string
// 	Success    bool
// 	EntityPath *umpire.EntityPath
// }
//
// func (e *WorkflowUpdateCompleted) Name() string {
// 	return telemetry.EventWorkflowUpdateCompleted
// }
//
// func (e *WorkflowUpdateCompleted) TargetEntity() *umpire.EntityPath {
// 	return e.EntityPath
// }
//
// func (e *WorkflowUpdateCompleted) IsSuccess() bool {
// 	return e.Success
// }
//
// func (e *WorkflowUpdateCompleted) ImportSpanEvent(attrs attribute.Set) bool {
// 	e.UpdateID, e.WorkflowID, e.EntityPath = importUpdateSpanEvent(attrs)
// 	if v, ok := attrs.Value(telemetry.AttrUpdateOutcome); ok {
// 		e.Success = v.AsString() == telemetry.UpdateOutcomeSuccess
// 	}
// 	return e.UpdateID != ""
// }

// tests/umpirev1/fact/workflow_update_rejected.go
// package fact
//
// import (
// 	"go.opentelemetry.io/otel/attribute"
// 	"go.temporal.io/server/common/telemetry"
// 	"go.temporal.io/server/common/testing/umpire"
// )
//
// // WorkflowUpdateRejected represents a workflow update being rejected by a worker.
// type WorkflowUpdateRejected struct {
// 	UpdateID   string
// 	WorkflowID string
// 	EntityPath *umpire.EntityPath
// }
//
// func (e *WorkflowUpdateRejected) Name() string {
// 	return telemetry.EventWorkflowUpdateRejected
// }
//
// func (e *WorkflowUpdateRejected) TargetEntity() *umpire.EntityPath {
// 	return e.EntityPath
// }
//
// func (e *WorkflowUpdateRejected) ImportSpanEvent(attrs attribute.Set) bool {
// 	e.UpdateID, e.WorkflowID, e.EntityPath = importUpdateSpanEvent(attrs)
// 	return e.UpdateID != ""
// }

// tests/umpirev1/fact/workflow_update_requested.go
// package fact
//
// import (
// 	historyservice "go.temporal.io/server/api/historyservice/v1"
// 	"go.temporal.io/server/common/testing/umpire"
// )
//
// // WorkflowUpdateRequested represents a workflow update request.
// type WorkflowUpdateRequested struct {
// 	Request    *historyservice.UpdateWorkflowExecutionRequest
// 	EntityPath *umpire.EntityPath
// }
//
// func (e *WorkflowUpdateRequested) Name() string {
// 	return "WorkflowUpdateRequested"
// }
//
// func (e *WorkflowUpdateRequested) TargetEntity() *umpire.EntityPath {
// 	return e.EntityPath
// }
//
// func (e *WorkflowUpdateRequested) ImportRequest(request any) bool {
// 	// Only the internal historyservice request carries the namespace ID (a UUID)
// 	// that roots the entity consistently with the update-lifecycle span facts.
// 	// The frontend workflowservice request carries only the namespace name, so
// 	// observing it would split the update into a second, differently-rooted entity.
// 	req, ok := request.(*historyservice.UpdateWorkflowExecutionRequest)
// 	if !ok || req.GetRequest().GetRequest().GetMeta().GetUpdateId() == "" {
// 		return false
// 	}
// 	e.Request = req
// 	updateID := umpire.NewEntityID(WorkflowUpdateType, e.UpdateID())
// 	var parents []umpire.EntityID
// 	if wfID := e.WorkflowID(); wfID != "" {
// 		parents = append(parents, umpire.NewEntityID(WorkflowType, wfID))
// 	}
// 	e.EntityPath = nsPath(e.Request.GetNamespaceId(), updateID, parents...)
// 	return true
// }
//
// func (e *WorkflowUpdateRequested) UpdateID() string {
// 	return e.Request.GetRequest().GetRequest().GetMeta().GetUpdateId()
// }
//
// func (e *WorkflowUpdateRequested) WorkflowID() string {
// 	return e.Request.GetRequest().GetWorkflowExecution().GetWorkflowId()
// }
//
// func (e *WorkflowUpdateRequested) HandlerName() string {
// 	return e.Request.GetRequest().GetRequest().GetInput().GetName()
// }
//
// tests/umpirev1/fact/constants.go
// WorkflowUpdateType umpire.EntityType = "WorkflowUpdate"
