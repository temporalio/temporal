package model

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	ws "go.temporal.io/api/workflowservice/v1"
	s "go.temporal.io/server/common/testing/stamp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Router struct {
	s.Router
}

// ====
// Internal routes

func (r *Router) OnClusterStarted(
	req IncomingAction[ClusterStarted],
) {
	_ = getCluster(r, req)
}

func (r *Router) OnNamespaceCreated(
	req IncomingAction[NamespaceCreated],
) {
	c := getCluster(r, req)
	_ = s.Consume[*Namespace](r, c, s.ID(req.Request.Name), req)
}

func (r *Router) OnNewTaskQueue(
	req IncomingAction[NewTaskQueue],
) {
	c := getCluster(r, req)
	ns := s.Consume[*Namespace](r, c, req.Request.NamespaceName, req)
	_ = s.Consume[*TaskQueue](r, ns, req.Request.TaskQueueName, req)
}

func (r *Router) OnNewWorkflowClient(
	req IncomingAction[NewWorkflowClient],
) {
	c := getCluster(r, req)
	ns := s.Consume[*Namespace](r, c, req.Request.TaskQueue.GetNamespace().GetID(), req)
	tq := s.Consume[*TaskQueue](r, ns, req.Request.TaskQueue.GetID(), req)
	_ = s.Consume[*WorkflowClient](r, tq, req.Request.ClientName, req)
}

func (r *Router) OnNewWorkflowWorker(
	req IncomingAction[NewWorkflowWorker],
) {
	c := getCluster(r, req)
	ns := s.Consume[*Namespace](r, c, req.Request.TaskQueue.GetNamespace().GetID(), req)
	tq := s.Consume[*TaskQueue](r, ns, req.Request.TaskQueue.GetID(), req)
	_ = s.Consume[*WorkflowWorker](r, tq, req.Request.WorkerName, req)
}

// ====
// Routes for go.temporal.io/api/workflowservice/v1

func (r *Router) OnStartWorkflowExecution(
	req IncomingAction[*ws.StartWorkflowExecutionRequest],
) func(OutgoingAction[*ws.StartWorkflowExecutionResponse]) {
	_ = routeToWorkflowExecution(r, &commonpb.WorkflowExecution{WorkflowId: req.Request.WorkflowId}, req)
	return nil
}

func (r *Router) OnTerminateWorkflowExecution(
	req IncomingAction[*ws.TerminateWorkflowExecutionRequest],
) func(OutgoingAction[*ws.TerminateWorkflowExecutionResponse]) {
	_ = routeToWorkflowExecution(r, req.Request.WorkflowExecution, req)
	return nil
}

func (r *Router) OnPollWorkflowTaskQueue(
	req IncomingAction[*ws.PollWorkflowTaskQueueRequest],
) func(OutgoingAction[*ws.PollWorkflowTaskQueueResponse]) {
	return func(out OutgoingAction[*ws.PollWorkflowTaskQueueResponse]) {
		if out.ResponseErr != nil {
			return
		}
		if out.Response.TaskToken == nil {
			// TODO: mark action as "failed"?
			// empty poll result
			return
		}
		// TODO: use out.Response.WorkflowExecution instead
		wfe := routeToWorkflowExecution(r, &commonpb.WorkflowExecution{WorkflowId: out.Response.WorkflowExecution.WorkflowId}, req)
		_ = s.Consume[*WorkflowTask](r, wfe, s.NewAliasID("latest"), req)
		for _, msg := range out.Response.Messages {
			_ = s.Consume[*WorkflowUpdate](r, wfe, s.ID(msg.ProtocolInstanceId), req)
		}
	}
}

func (r *Router) OnRespondWorkflowTaskCompleted(
	req IncomingAction[*ws.RespondWorkflowTaskCompletedRequest],
) func(OutgoingAction[*ws.RespondWorkflowTaskCompletedResponse]) {
	tt := mustDeserializeTaskToken(req.Request.TaskToken)
	wfe := routeToWorkflowExecution(r, &commonpb.WorkflowExecution{WorkflowId: tt.WorkflowId, RunId: tt.WorkflowId}, req)
	_ = s.Consume[*WorkflowTask](r, wfe, s.NewAliasID("latest"), req)
	for _, msg := range req.Request.Messages {
		_ = s.Consume[*WorkflowUpdate](r, wfe, s.ID(msg.ProtocolInstanceId), req)
	}
	return nil
}

func (r *Router) OnGetWorkflowExecutionHistory(
	req IncomingAction[*ws.GetWorkflowExecutionHistoryRequest],
) func(OutgoingAction[*ws.GetWorkflowExecutionHistoryResponse]) {
	_ = routeToWorkflowExecution(r, req.Request.Execution, req)
	return nil
}

func (r *Router) OnUpdateWorkflowExecution(
	req IncomingAction[*ws.UpdateWorkflowExecutionRequest],
) func(OutgoingAction[*ws.UpdateWorkflowExecutionResponse]) {
	wfe := routeToWorkflowExecution(r, req.Request.WorkflowExecution, req)
	// TODO: handle unspecified update id
	_ = s.Consume[*WorkflowUpdate](r, wfe, s.ID(req.Request.Request.Meta.UpdateId), req)
	return nil
}

func (r *Router) OnPollWorkflowExecutionUpdate(
	req IncomingAction[*ws.PollWorkflowExecutionUpdateRequest],
) func(OutgoingAction[*ws.PollWorkflowExecutionUpdateResponse]) {
	wfe := routeToWorkflowExecution(r, req.Request.UpdateRef.WorkflowExecution, req)
	// TODO: handle unspecified update id
	_ = s.Consume[*WorkflowUpdate](r, wfe, s.ID(req.Request.UpdateRef.UpdateId), req)
	return nil
}

// ====
// Helpers

func routeToWorkflowExecution[T proto.Message](
	r *Router,
	target *commonpb.WorkflowExecution,
	req IncomingAction[T],
) s.Model[*WorkflowExecution] {
	ns := getNamespace(r, req)
	wf := s.Consume[*Workflow](r, ns, s.ID(target.WorkflowId), req)
	// TODO: use tt.RunId - but need to migrate from alias first
	wfe := s.Consume[*WorkflowExecution](r, wf, s.NewAliasID("latest"), req)
	_ = s.Consume[*WorkflowExecutionHistory](r, wfe, s.NewAliasID("latest"), req)
	return wfe
}

func getCluster[T any](r *Router, req IncomingAction[T]) s.Model[*Cluster] {
	return s.Consume[*Cluster](r, s.RootMdl, s.ID(req.Cluster), req)
}

func getNamespace[T proto.Message](
	r *Router,
	req IncomingAction[T],
) s.Model[*Namespace] {
	c := getCluster(r, req)
	if id := s.ID(findProtoValueByNameType[string](req.Request, "namespace", protoreflect.StringKind)); id != "" {
		return s.Consume[*Namespace](r, c, id, req)
	}
	panic(fmt.Sprintf("failed to find namespace name in %T", req.Request))
}

func getTaskQueue[T proto.Message](
	r *Router,
	ns s.Model[*Namespace],
	req IncomingAction[T],
) s.Model[*TaskQueue] {
	if id := s.ID(findProtoValueByFullName[string](req.Request, "temporal.api.taskqueue.v1.TaskQueue.name")); id != "" {
		return s.Consume[*TaskQueue](r, ns, id, req)
	}
	panic(fmt.Sprintf("failed to find task queue name in %T", req.Request))
}
