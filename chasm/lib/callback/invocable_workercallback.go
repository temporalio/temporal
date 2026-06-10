package callback

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

// PROTOTYPE
//
// invokeWorkerCallback simply spawns a standalone activity. The SAA will then ensure durable execution of
// a callback (registered as an Activity) within a worker.
type invokeWorkerCallback struct {
	callback   *callbackspb.Callback_Nexus
	completion nexusrpc.CompleteOperationOptions
	requestID  string
}

func (c invokeWorkerCallback) WrapError(_ invocationResult, err error) error {
	return err
}

func (c invokeWorkerCallback) Invoke(
	ctx context.Context,
	// IMPORTANT: The namespace of the callback's execution. Which we expect to ALSO match the
	// namespace the SAA will be called from. Otherwise, worker callbacks could dispatch activities
	// into _other_ namespaces, and that would be a big security problem.
	ns *namespace.Namespace,
	h *invocationTaskHandler,
	task *callbackspb.InvocationTask,
	taskAttr chasm.TaskAttributes,
) invocationResult {

	// TODO: Somehow redupe/idempotency/activity invocation ID reuse/conflict policies need to be accounted for.

	// Gather routing information from the callback's headers.
	header := c.callback.GetHeader()
	targetNamespaceName := header[commonnexus.WorkerCallbackTargetNamespaceHeader]
	targetActivityType := header[commonnexus.WorkerCallbackTargetActivityHeader]
	targetTaskQueue := header[commonnexus.WorkerCallbackTargetTaskQueueHeader]
	if targetNamespaceName == "" || targetActivityType == "" || targetTaskQueue == "" {
		// Misconfigured callback: nothing a retry can fix. Fail permanently.
		err := logInternalError(h.logger, "worker callback missing required header", nil)
		return invocationResultFail{err}
	}

	// Confirm the SAA to run the worker callback is targeting the same namespace of the callback
	// component being invoked.
	targetNamespace, err := h.namespaceRegistry.GetNamespace(namespace.Name(targetNamespaceName))
	if err != nil {
		// Transient registry errors are retryable; an unknown namespace will keep failing but is
		// cheap to retry and surfaces clearly in logs.
		return invocationResultRetry{err: fmt.Errorf("resolve target namespace %q: %w", targetNamespaceName, err)}
	}
	if ns.ID() != targetNamespace.ID() {
		err := fmt.Errorf("target namespace (%s) does not match invocation namespace (%s)", ns.ID().String(), targetNamespace.ID().String())
		err2 := logInternalError(h.logger, "namespace mismatch", err)
		return invocationResultFail{err2}
	}

	// Build the activity's input.
	// TODO: Expand this to include more information, such as the contextual data about where the args came from.
	if c.completion.Error != nil {
		err := logInternalError(h.logger, "NYI: Failures not supported yet", err)
		return invocationResultFail{err}
	}
	var input *commonpb.Payloads
	if c.completion.Result != nil {
		p, ok := c.completion.Result.(*commonpb.Payload)
		if !ok {
			err := logInternalError(h.logger, fmt.Sprintf("expected *commonpb.Payload result, got %T", c.completion.Result), nil)
			return invocationResultFail{err}
		}
		input = &commonpb.Payloads{Payloads: []*commonpb.Payload{p}}
	}

	// Idempotency. Side-effect tasks retry, so the activity ID and request ID must be
	// deterministic across attempts; derive them from the callback's stable request ID so a
	// re-delivered task dedupes onto the same execution instead of spawning a duplicate.
	idempotencyKey := fmt.Sprintf("worker-callback-%s", c.requestID)

	// SECURITY: We are bypassing all sorts of validations for SAA found in activity/frontend.go.
	startReq := &workflowservice.StartActivityExecutionRequest{
		Namespace:    targetNamespaceName,
		ActivityId:   idempotencyKey,
		RequestId:    idempotencyKey,
		ActivityType: &commonpb.ActivityType{Name: targetActivityType},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: targetTaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Input:                  input,
		ScheduleToCloseTimeout: durationpb.New(h.config.RequestTimeout(targetNamespaceName, taskAttr.Destination)),
	}

	// Dispatch the activity.
	//
	// IMPORTANT: we cannot call chasm.StartExecution[*activity.Activity] / activity.NewStandaloneActivity
	// directly here — the activity library already imports this callback package, so importing it
	// back would create a cycle. Instead, dispatch through the activity ActivityService (hosted in
	// History), the same way invocableInternal RPCs to History for completion delivery. This
	// requires an activitypb.ActivityServiceClient to be injected into invocationTaskHandler (see
	// fx.go / invocable_internal.go's historyClient for the pattern).
	//
	// TODO(chrsmith): wire activitypb.ActivityServiceClient into the handler and replace this block
	// with:
	//   _, err := h.activityClient.StartActivityExecution(ctx, &activitypb.StartActivityExecutionRequest{
	//       NamespaceId:     targetNamespace.ID().String(),
	//       FrontendRequest: startReq,
	//   })
	//   ... map ExecutionAlreadyStarted -> invocationResultOK, transient errors -> invocationResultRetry.
	_ = targetNamespace
	_ = startReq
	h.logger.Warn("worker callback dispatch not yet wired to ActivityService; request prepared but not sent")

	return invocationResultOK{}
}
