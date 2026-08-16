package action

import (
	"context"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/workflowservice/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/umpire"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ---- Realizers ----

// rpcStartStandalone realizes StartNexusOperationExecution: creates a standalone operation
// (its own execution) and binds it — unspecified→scheduled.
type rpcStartStandalone struct{}

func (rpcStartStandalone) Install(umpire.RealizeContext, umpire.Action) error { return nil }

func (rpcStartStandalone) Fire(ctx context.Context, rc umpire.RealizeContext, a umpire.Action) error {
	c := rc.(*Ctx)
	opID := fmt.Sprintf("umpire-action-op-%d", c.Iter)
	resp, err := c.Env.FrontendClient().StartNexusOperationExecution(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		Namespace:              c.Env.Namespace().String(),
		OperationId:            opID,
		Endpoint:               c.Endpoint,
		Service:                "service",
		Operation:              "operation",
		RequestId:              opID,
		ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
	})
	if err != nil {
		return err
	}
	c.RunID = resp.GetRunId()
	bindFresh(rc, a, opID) // op identity == its execution id (== WorkflowID in telemetry)
	return nil
}

// handlerBlock holds the start attempt so the operation stays scheduled (no effect of its
// own; a companion action, e.g. terminate, acts while it is held). Reactive.
type handlerBlock struct{}

func (handlerBlock) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	rc.(*Ctx).Handler.setBlock()
	return nil
}
func (handlerBlock) Fire(context.Context, umpire.RealizeContext, umpire.Action) error { return nil }

// handlerRetryable fails the start attempt retryably, sending the operation into backoff —
// scheduled→backing_off. Reactive.
type handlerRetryable struct{}

func (handlerRetryable) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	rc.(*Ctx).Handler.setStart(nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "umpire action: injected retryable failure"))
	return nil
}
func (handlerRetryable) Fire(context.Context, umpire.RealizeContext, umpire.Action) error { return nil }

// handlerSyncOk returns a synchronous success from the handler — scheduled→succeeded. Reactive.
type handlerSyncOk struct{}

func (handlerSyncOk) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	rc.(*Ctx).Handler.setStart(&nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil)
	return nil
}
func (handlerSyncOk) Fire(context.Context, umpire.RealizeContext, umpire.Action) error { return nil }

// handlerOpFailed fails the operation from the handler — scheduled→failed. Reactive.
type handlerOpFailed struct{}

func (handlerOpFailed) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	rc.(*Ctx).Handler.setStart(nil, nexus.NewOperationFailedErrorf("umpire action: injected operation failure"))
	return nil
}
func (handlerOpFailed) Fire(context.Context, umpire.RealizeContext, umpire.Action) error { return nil }

// handlerOpCanceled reports the operation canceled from the handler — scheduled→canceled.
// Reactive.
type handlerOpCanceled struct{}

func (handlerOpCanceled) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	rc.(*Ctx).Handler.setStart(nil, nexus.NewOperationCanceledErrorf("umpire action: injected cancellation"))
	return nil
}
func (handlerOpCanceled) Fire(context.Context, umpire.RealizeContext, umpire.Action) error {
	return nil
}

// timerForceTimeout installs the NexusOperationForceTimeout hook so the operation times out
// from `from` (scheduled or backing_off) deterministically, no real timer wait. Reactive.
type timerForceTimeout struct{ from string }

func (t timerForceTimeout) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	rc.(*Ctx).Env.InjectHook(testhooks.NewHook(testhooks.NexusOperationForceTimeout, t.from))
	return nil
}
func (timerForceTimeout) Fire(context.Context, umpire.RealizeContext, umpire.Action) error {
	return nil
}

// rpcTerminate realizes TerminateNexusOperationExecution on the bound standalone operation.
type rpcTerminate struct{}

func (rpcTerminate) Install(umpire.RealizeContext, umpire.Action) error { return nil }
func (rpcTerminate) Fire(ctx context.Context, rc umpire.RealizeContext, _ umpire.Action) error {
	c := rc.(*Ctx)
	opID, _ := rc.Binding("op")
	_, err := c.Env.FrontendClient().TerminateNexusOperationExecution(ctx, &workflowservice.TerminateNexusOperationExecutionRequest{
		Namespace:   c.Env.Namespace().String(),
		OperationId: opID,
		RunId:       c.RunID,
		Reason:      "umpire action: reach terminated",
	})
	return err
}

// handlerAsync realizes handler:AsyncAck: installs the mock handler to acknowledge the start
// asynchronously — scheduled→started. Reactive: the work is in Install.
type handlerAsync struct{}

func (handlerAsync) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	rc.(*Ctx).Handler.setStart(&nexus.HandlerStartOperationResultAsync{OperationToken: "umpire-action-token"}, nil)
	return nil
}

func (handlerAsync) Fire(context.Context, umpire.RealizeContext, umpire.Action) error { return nil }

// completion realizes callback:Complete(...): delivers an async completion to the captured
// callback — started→{succeeded,failed,canceled} (opErr nil = success).
type completion struct{ opErr *nexus.OperationError }

func (completion) Install(umpire.RealizeContext, umpire.Action) error { return nil }

func (co completion) Fire(ctx context.Context, rc umpire.RealizeContext, _ umpire.Action) error {
	c := rc.(*Ctx)
	var cb callback
	select {
	case cb = <-c.Handler.captured:
	case <-ctx.Done():
		return ctx.Err()
	}
	client := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{Serializer: commonnexus.PayloadSerializer})
	opts := nexusrpc.CompleteOperationOptions{Header: nexus.Header{commonnexus.CallbackTokenHeader: cb.token}}
	if co.opErr != nil {
		opts.Error = co.opErr
	} else {
		opts.Result = payload.EncodeString("umpire-action-result")
	}
	return client.CompleteOperation(ctx, cb.url, opts)
}
