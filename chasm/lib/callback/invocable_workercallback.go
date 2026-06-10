package callback

import (
	"context"
	"errors"
	"fmt"
	"log"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
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
	log.Printf("invokeWorkerCallback::Invoke 🤞")
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

	// PROTOTYPE: reuse the callback request timeout as the activity's execution timeout. These are
	// distinct concepts (callback delivery vs. activity run time); a dedicated/ configurable value
	// would be better.
	activityTimeout := h.config.RequestTimeout(targetNamespaceName, taskAttr.Destination)

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
		ScheduleToCloseTimeout: durationpb.New(activityTimeout),
		// StartToCloseTimeout MUST be non-zero. TransitionStarted schedules the start-to-close
		// timeout at (startedTime + StartToCloseTimeout); a zero value fires it immediately, which
		// times out and reschedules the attempt the instant it starts — discarding the worker's
		// successful completion (its token's stamp is now stale) and looping until ScheduleToClose
		// closes the activity. Normally activity/frontend.go would default this; we bypass it.
		StartToCloseTimeout: durationpb.New(activityTimeout),
		// Bound retries explicitly. An empty RetryPolicy means MaximumAttempts == 0 (unlimited), so
		// a permanently-failing callback activity would retry until ScheduleToClose. One attempt is
		// the right default for a delivery callback; raise this if callbacks should be retried.
		RetryPolicy: &commonpb.RetryPolicy{MaximumAttempts: 1},
		// These must be set: the activity handler rejects the UNSPECIFIED (zero) values. Combined
		// with the deterministic RequestId above, retried side-effect tasks dedupe onto the same
		// execution rather than spawning duplicates.
		IdReusePolicy:    enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE,
		IdConflictPolicy: enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL,
	}

	// Dispatch the activity.
	//
	// We cannot call chasm.StartExecution[*activity.Activity] / activity.NewStandaloneActivity
	// directly here — the activity library already imports this callback package, so importing it
	// back would create a cycle. We also can't use the in-process CHASM engine: side-effect tasks
	// run on the shard owning the *callback's* execution, while the target SAA lives on a shard
	// keyed by the (target namespace, activity ID) pair, which may be owned by another host.
	//
	// Instead we go through activitypb.ActivityServiceClient — the shard-routing ("layered") client
	// for the standalone-activity service. It redirects the StartActivityExecution RPC to whichever
	// history host owns the target shard, mirroring how invocableInternal RPCs to History for
	// completion delivery. The client is a generated package (no import cycle) and is injected via
	// fx; see invocationTaskHandlerOptions.ActivityClient.
	if h.activityClient == nil {
		// Should not happen in the history service, where the client is provided. If it does, retry
		// rather than fail permanently — a misconfiguration is operator-fixable.
		return invocationResultRetry{err: logInternalError(h.logger,
			"worker callback dispatch requires an ActivityServiceClient, but none is configured", nil)}
	}

	log.Printf("Calling StartActivityExecution...")
	_, err = h.activityClient.StartActivityExecution(ctx, &activitypb.StartActivityExecutionRequest{
		NamespaceId:     targetNamespace.ID().String(),
		FrontendRequest: startReq,
	})
	if err != nil {
		// A retried side-effect task re-issues the same RequestId, so the engine normally dedupes
		// silently. But if a prior attempt already created the execution and the dedup surfaces as
		// AlreadyStarted, treat the callback as delivered.
		if _, ok := errors.AsType[*serviceerror.ActivityExecutionAlreadyStarted](err); ok {
			log.Printf("Got serviceerror.ActivityExecutionAlreadyStarted error")
			return invocationResultOK{}
		}
		if isRetryableRPCResponse(err) {
			log.Printf("Got retryable RPC response: %v\n", err)
			return invocationResultRetry{err: err}
		}
		log.Printf("Got failure result: %v\n", err)
		return invocationResultFail{err}
	}

	log.Printf("The SANO was created successfully. Godspeed.")
	return invocationResultOK{}
}
