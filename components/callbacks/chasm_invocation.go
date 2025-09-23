package callbacks

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/queues"
)

type chasmInvocation struct {
	nexus      *persistencespb.Callback_Nexus
	attempt    int32
	completion nexus.OperationCompletion
}

var ErrUnimplementedHandler = serviceerror.NewUnimplemented("component does not implement NexusCompletionHandler")

func (c chasmInvocation) WrapError(result invocationResult, err error) error {
	if failure, ok := result.(invocationResultFail); ok {
		return queues.NewUnprocessableTaskError(failure.err.Error())
	}

	if retry, ok := result.(invocationResultRetry); ok {
		return queues.NewDestinationDownError(retry.err.Error(), err)
	}

	return err
}

func (c chasmInvocation) Invoke(ctx context.Context, ns *namespace.Namespace, e taskExecutor, task InvocationTask) invocationResult {
	// Get back the base64-encoded ComponentRef from the header.
	encodedRef, ok := c.nexus.GetHeader()[chasm.NexusComponentRefHeader]
	if !ok {
		return invocationResultFail{errors.New("callback missing CHASM header")}
	}

	decodedRef, err := base64.RawURLEncoding.DecodeString(encodedRef)
	if err != nil {
		return invocationResultFail{fmt.Errorf("failed to decode CHASM ComponentRef: %v", err)}
	}

	ref, err := chasm.DeserializeComponentRef(decodedRef)
	if err != nil {
		return invocationResultFail{fmt.Errorf("failed to deserialize CHASM ComponentRef: %v", err)}
	}

	// Attempt to access the component and call its invocation method. We execute
	// this similarly as we would a pure task (holding an exclusive lock), as the
	// assumption is that the accessed component will be recording (or generating a
	// task) based on this result.
	_, err = e.ChasmEngine.UpdateComponent(ctx, ref, func(ctx chasm.MutableContext, component chasm.Component) error {
		handler, ok := component.(chasm.NexusCompletionHandler)
		if !ok {
			return ErrUnimplementedHandler
		}

		return handler.HandleNexusCompletion(ctx, c.completion)
	})
	if errors.Is(err, ErrUnimplementedHandler) {
		return invocationResultFail{err}
	} else if err != nil {
		return invocationResultRetry{err}
	}

	return invocationResultOK{}
}
