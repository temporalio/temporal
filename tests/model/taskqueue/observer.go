package taskqueue

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore/umpire"
	"google.golang.org/grpc"
)

// methodsTouching are the gRPC methods this observer ensures-creates a task
// queue from. Any successful call to one of them brings the queue into
// World.TaskQueues if it isn't already there.
var methodsTouching = []string{
	workflowservice.WorkflowService_PollNexusTaskQueue_FullMethodName,
	workflowservice.WorkflowService_PollWorkflowTaskQueue_FullMethodName,
	workflowservice.WorkflowService_PollActivityTaskQueue_FullMethodName,
	workflowservice.WorkflowService_DescribeTaskQueue_FullMethodName,
	workflowservice.WorkflowService_StartWorkflowExecution_FullMethodName,
}

// observer ensures task-queue entities exist in the World whenever an RPC
// carrying a task queue is observed.
type observer struct {
	store *umpire.EntityStore[*Entity]
	u     *umpire.Umpire
}

func newObserver(deps Deps) *observer {
	return &observer{store: deps.Store, u: deps.Umpire}
}

func (o *observer) ObservedMethods() []string {
	out := make([]string, len(methodsTouching))
	copy(out, methodsTouching)
	return out
}

func (o *observer) Strategy() umpire.Strategy {
	matchSet := make(map[string]struct{}, len(methodsTouching))
	for _, m := range methodsTouching {
		matchSet[m] = struct{}{}
	}
	return umpire.Strategy{
		Name: "task-queue-observer",
		Server: func(
			ctx context.Context,
			req any,
			info *grpc.UnaryServerInfo,
			handler grpc.UnaryHandler,
		) (any, error) {
			resp, err := handler(ctx, req)
			if err != nil {
				return resp, err
			}
			if _, ok := matchSet[info.FullMethod]; !ok {
				return resp, err
			}
			o.ensureFromRequest(req)
			return resp, err
		},
	}
}

func (o *observer) ensureFromRequest(req any) {
	ns, name, kind, typ, ok := taskQueueFromRequest(req)
	if !ok {
		return
	}
	key := ns + "/" + name
	if _, exists := o.store.Get(key); exists {
		return
	}
	o.store.Add(&Entity{
		Name:      name,
		Namespace: ns,
		Kind:      kind,
		Type:      typ,
		Status:    StatusRegistered,
	})
	o.u.Record(&umpire.EntityCreated{
		Type:     "task-queue",
		EntityID: key,
		Parents:  map[string]string{"namespace": ns},
	})
}

// taskQueueFromRequest extracts (namespace, name, kind, type) from any RPC
// whose request carries a task queue.
func taskQueueFromRequest(req any) (ns string, name string, kind enumspb.TaskQueueKind, typ enumspb.TaskQueueType, ok bool) {
	switch r := req.(type) {
	case *workflowservice.PollNexusTaskQueueRequest:
		return r.GetNamespace(), r.GetTaskQueue().GetName(), r.GetTaskQueue().GetKind(), enumspb.TASK_QUEUE_TYPE_NEXUS, true
	case *workflowservice.PollWorkflowTaskQueueRequest:
		return r.GetNamespace(), r.GetTaskQueue().GetName(), r.GetTaskQueue().GetKind(), enumspb.TASK_QUEUE_TYPE_WORKFLOW, true
	case *workflowservice.PollActivityTaskQueueRequest:
		return r.GetNamespace(), r.GetTaskQueue().GetName(), r.GetTaskQueue().GetKind(), enumspb.TASK_QUEUE_TYPE_ACTIVITY, true
	case *workflowservice.DescribeTaskQueueRequest:
		return r.GetNamespace(), r.GetTaskQueue().GetName(), r.GetTaskQueue().GetKind(), r.GetTaskQueueType(), true
	case *workflowservice.StartWorkflowExecutionRequest:
		return r.GetNamespace(), r.GetTaskQueue().GetName(), r.GetTaskQueue().GetKind(), enumspb.TASK_QUEUE_TYPE_WORKFLOW, true
	}
	return "", "", 0, 0, false
}
