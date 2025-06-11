package callbacks

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CanGetHSMCompletionCallbackArg interface {
	GetHSMCompletionCallbackArg(ctx context.Context) (*persistencespb.HSMCompletionCallbackArg, error)
}

type hsmInvocation struct {
	hsm         *persistencespb.Callback_HSM
	callbackArg *persistencespb.HSMCompletionCallbackArg
}

func isRetryableRpcResponse(err error) bool {
	var st *status.Status
	stGetter, ok := err.(interface{ Status() *status.Status })
	if ok {
		st = stGetter.Status()
	} else {
		st, ok = status.FromError(err)
		if !ok {
			// Not a gRPC induced error
			return false
		}
	}
	// nolint:exhaustive
	switch st.Code() {
	case codes.Canceled,
		codes.Unknown,
		codes.Unavailable,
		codes.DeadlineExceeded,
		codes.ResourceExhausted,
		codes.Aborted,
		codes.Internal:
		return true
	default:
		return false
	}
}

func (s hsmInvocation) WrapError(invocationResult, error) error {
	// No short-circuit
	return nil
}

func (s hsmInvocation) Invoke(ctx context.Context, ns *namespace.Namespace, e taskExecutor, task InvocationTask) invocationResult {
	// TODO(Tianyu): Will this ever be too big for an RPC call?
	callbackArgSerialized, err := s.callbackArg.Marshal()
	if err != nil {
		return invocationResultFail{fmt.Errorf("failed to serialize completion event: %w", err)}
	}

	request := historyservice.InvokeStateMachineMethodRequest{
		NamespaceId: s.hsm.NamespaceId,
		WorkflowId:  s.hsm.WorkflowId,
		RunId:       s.hsm.RunId,
		Ref:         s.hsm.Ref,
		MethodName:  s.hsm.Method,
		Input:       callbackArgSerialized,
	}

	startTime := time.Now()
	_, err = e.HistoryClient.InvokeStateMachineMethod(ctx, &request)

	// Log down metrics about the call
	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(task.Destination())
	statusCodeTag := metrics.OutcomeTag(fmt.Sprintf("status:%d", status.Code(err)))

	e.MetricsHandler.Counter(RequestCounter.Name()).Record(1, namespaceTag, destTag, statusCodeTag)
	e.MetricsHandler.Timer(RequestLatencyHistogram.Name()).Record(time.Since(startTime), namespaceTag, destTag, statusCodeTag)

	if err != nil {
		e.Logger.Error("Callback request failed", tag.Error(err))
		if isRetryableRpcResponse(err) {
			return invocationResultRetry{err}
		}
		return invocationResultFail{err}
	}
	return invocationResultOK{}
}
